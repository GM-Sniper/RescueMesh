//! Task 5: our own heartbeats (not gRPC's health-check service or HTTP/2 keepalive).
//!
//! One background worker per peer. Every heartbeat_interval it sends
//! `Heartbeat { from_node, seq }` in its own small task with the rpc_deadline,
//! so heartbeats go out on time even when replies are slow, and a dead peer
//! never delays heartbeats to a healthy one. A reply from the right node with
//! the same seq tells the failure detector we heard from it. Silence alone is
//! what makes the detector suspect a peer.

use std::sync::Arc;

use common::NodeId;
use common::pb::HeartbeatRequest;
use tokio::time::{MissedTickBehavior, interval, timeout};

use crate::state::NodeState;

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
}
