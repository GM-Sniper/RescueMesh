//! Task 5: our own heartbeats (not gRPC's health-check service or HTTP/2 keepalive).
//!
//! One background worker per peer. Every heartbeat_interval it sends
//! `Heartbeat { from_node, seq }` in its own small task with the rpc_deadline,
//! so heartbeats go out on time even when replies are slow, and a dead peer
//! never delays heartbeats to a healthy one. A reply from the right node with
//! the same seq tells the failure detector we heard from it. Silence alone is
//! what makes the detector suspect a peer.

use std::sync::Arc;
use std::time::Duration;

use common::NodeId;
use common::pb::HeartbeatRequest;
use tokio::time::{MissedTickBehavior, interval, timeout};

use crate::failure_detector::PeerEvent;
use crate::state::NodeState;

/// How often the ticker re-checks the detector. A change is noticed at most this
/// late, which is small next to suspect_after (1.5 s).
const TICK: Duration = Duration::from_millis(100);

/// Starts the ticker: every TICK, ask the detector who changed and log it.
/// Paused while muted; on unmute it starts a fresh detector (like a reboot).
pub fn spawn_ticker(state: Arc<NodeState>) {
    tokio::spawn(async move {
        let mut ticker = interval(TICK);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut was_muted = false;
        loop {
            ticker.tick().await;
            if state.is_muted() {
                was_muted = true; // a muted node hears nothing: its view means nothing
                continue;
            }
            if was_muted {
                was_muted = false;
                state.reset_detector();
                tracing::info!("unmuted: starting a fresh failure detector");
                continue;
            }
            for event in state.check_peers() {
                match event {
                    PeerEvent::PeerDown(peer) => {
                        tracing::warn!(peer, "peer suspected down");
                        if let Some(log) = &state.fault_log {
                            log.peer_down(peer);
                        }
                    }
                    PeerEvent::PeerUp(peer) => {
                        tracing::info!(peer, "peer alive again");
                        if let Some(log) = &state.fault_log {
                            log.peer_up(peer);
                        }
                    }
                }
            }
        }
    });
}

/// Starts one heartbeat worker per peer. They run until the node stops.
pub fn spawn_senders(state: Arc<NodeState>) {
    for peer in state.peers.ids() {
        tokio::spawn(send_loop(state.clone(), peer));
    }
}

async fn send_loop(state: Arc<NodeState>, peer: NodeId) {
    let timing = state.config.timing;
    let mut ticker = interval(timing.heartbeat_interval());
    // If the runtime was busy and ticks were missed, carry on from now instead of bursting.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut seq: u64 = 0;
    loop {
        ticker.tick().await;
        // None while muted: a silent node sends nothing.
        let Some(mut client) = state.peer_for_sending(peer) else {
            continue;
        };
        seq += 1;
        let state = state.clone();
        let request = HeartbeatRequest {
            from_node: state.id,
            seq,
        };
        // Own task: the ticker never waits for a reply. At most
        // rpc_deadline / heartbeat_interval calls per peer are pending at once.
        tokio::spawn(async move {
            match timeout(timing.rpc_deadline(), client.heartbeat(request)).await {
                Ok(Ok(reply)) => {
                    let reply = reply.into_inner();
                    if reply.node_id == peer && reply.seq == seq {
                        state.heard_from(peer);
                    } else {
                        tracing::warn!(
                            peer,
                            answered_by = reply.node_id,
                            "heartbeat answered by the wrong node; check the config addresses"
                        );
                    }
                }
                Ok(Err(status)) => {
                    tracing::debug!(peer, seq, code = ?status.code(), "heartbeat failed")
                }
                Err(_) => tracing::debug!(peer, seq, "heartbeat timed out"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common::config::ClusterConfig;
    use common::pb::FaultCommand;
    use common::pb::node_client::NodeClient;
    use common::pb::node_server::NodeServer;

    use super::*;
    use crate::failure_detector::PeerEvent;
    use crate::peers::Peers;
    use crate::service::NodeService;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// Nodes 1 and 2 on free ports, with short timings so tests finish quickly.
    fn two_node_config() -> ClusterConfig {
        ClusterConfig::parse(&format!(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:{}\"\n\
             [timing]\nheartbeat_interval_ms = 50\nsuspect_after_ms = 150\n\
             rpc_deadline_ms = 100\nconnect_timeout_ms = 100\n",
            free_port(),
            free_port()
        ))
        .unwrap()
    }

    fn state_for(id: NodeId, cfg: &ClusterConfig) -> Arc<NodeState> {
        let peers = Peers::new(cfg, id).unwrap();
        Arc::new(NodeState::new(id, cfg.clone(), peers))
    }

    #[tokio::test]
    async fn live_peer_stays_alive() {
        let cfg = two_node_config();
        // A real node 2, answering heartbeats.
        let addr = cfg.node(2).unwrap().addr.parse().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(NodeServer::new(NodeService::new(state_for(2, &cfg))))
                .serve(addr),
        );

        let node1 = state_for(1, &cfg);
        spawn_senders(node1.clone());
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(node1.check_peers().is_empty(), "node 2 must not be suspected");
        }
        assert_eq!(node1.alive_peers(), vec![2]);
    }

    #[tokio::test]
    async fn missing_peer_goes_down() {
        let cfg = two_node_config(); // nothing listens on node 2's port
        let node1 = state_for(1, &cfg);
        spawn_senders(node1.clone());
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(node1.check_peers(), vec![PeerEvent::PeerDown(2)]);
        assert!(node1.alive_peers().is_empty());
    }

    // ---- 5d: the ticker, on the fake clock (default timing: suspect after 1.5 s).

    fn three_node_state() -> Arc<NodeState> {
        let cfg = ClusterConfig::parse(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:50052\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:50053\"\n",
        )
        .unwrap();
        state_for(1, &cfg)
    }

    #[tokio::test(start_paused = true)]
    async fn ticker_detects_silent_peers() {
        let s = three_node_state();
        spawn_ticker(s.clone());
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(s.alive_peers(), vec![2, 3]);
        tokio::time::sleep(Duration::from_millis(600)).await; // 1.6 s of silence
        assert!(s.alive_peers().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn unmute_starts_a_fresh_detector() {
        let s = three_node_state();
        spawn_ticker(s.clone());
        s.mute_for(Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(5300)).await; // 0.3 s after the mute
        // Without the reset, 5 s of self-caused silence would mark both peers down.
        assert_eq!(s.alive_peers(), vec![2, 3]);
    }

    // ---- 5f: the plan's done-check with 3 real nodes, real network, real clock.

    /// Starts one full node like main does: server, heartbeat senders, ticker.
    fn start_node(id: NodeId, cfg: &ClusterConfig) -> Arc<NodeState> {
        let state = state_for(id, cfg);
        let addr = cfg.node(id).unwrap().addr.parse().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(NodeServer::new(NodeService::new(state.clone())))
                .serve(addr),
        );
        spawn_senders(state.clone());
        spawn_ticker(state.clone());
        state
    }

    /// Polls every 10 ms until `cond` holds; fails the test after `max`.
    async fn wait_until(what: &str, max: Duration, cond: impl Fn() -> bool) -> Duration {
        let start = std::time::Instant::now();
        while !cond() {
            assert!(start.elapsed() < max, "timed out waiting for: {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        start.elapsed()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn muting_node_2_is_detected_by_the_others_and_it_comes_back() {
        let cfg = ClusterConfig::parse(&format!(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:{}\"\n\
             [timing]\nheartbeat_interval_ms = 50\nsuspect_after_ms = 150\n\
             rpc_deadline_ms = 100\nconnect_timeout_ms = 100\n",
            free_port(),
            free_port(),
            free_port()
        ))
        .unwrap();
        let (n1, n2, n3) = (start_node(1, &cfg), start_node(2, &cfg), start_node(3, &cfg));
        let limit = Duration::from_secs(2);

        // 1. Everyone sees everyone.
        wait_until("all alive", limit, || {
            n1.alive_peers() == [2, 3] && n3.alive_peers() == [1, 2]
        })
        .await;

        // 2. Mute node 2 for 800 ms through a real InjectFault call.
        let mut client = NodeClient::connect(format!("http://{}", cfg.node(2).unwrap().addr))
            .await
            .unwrap();
        let ack = client
            .inject_fault(FaultCommand {
                fault_id: "test".into(),
                target_node: 2,
                duration_ms: 800,
                issuer_node: 0,
                term: 0,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);

        // 3. Nodes 1 and 3 drop node 2 on their own, within suspect + tick + margin.
        let detected = wait_until("node 2 suspected", limit, || {
            n1.alive_peers() == [3] && n3.alive_peers() == [1]
        })
        .await;
        assert!(
            detected < Duration::from_millis(450),
            "detection took {detected:?}; expected about 150 ms + 100 ms tick"
        );

        // 4. After the mute, node 2 is back for both, and node 2 itself sees both
        //    (fresh detector on unmute, then real heartbeats).
        wait_until("node 2 alive again", limit, || {
            n1.alive_peers() == [2, 3] && n3.alive_peers() == [1, 2] && n2.alive_peers() == [1, 3]
        })
        .await;
    }
}
