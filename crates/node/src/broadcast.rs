//! Task 6: broadcast(). Sends the same call to every peer at the same time,
//! waits at most `deadline`, and returns what each peer answered.
//!
//! - Generic over the call: the caller passes "given a client, make this RPC",
//!   so the same helper serves Step B's Bid, Replicate and FaultCommand.
//! - Every configured peer gets the call, not only the ones the detector thinks
//!   are alive: the calls run in parallel, so a dead peer costs no extra time,
//!   and a peer that just came back is not left out. The caller decides what a
//!   missing answer means.
//! - Deadlines are ours (`tokio::time::timeout`), not tonic's.
//! - While this node is muted nothing is sent (`peer_for_sending`).

use std::future::Future;
use std::time::Duration;

use common::NodeId;
use common::pb::node_client::NodeClient;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tonic::transport::Channel;
use tonic::{Response, Status};

use crate::state::NodeState;

/// How one peer's part of a broadcast ended.
#[derive(Debug)]
pub enum PeerResult<T> {
    /// The peer answered in time.
    Reply(T),
    /// The peer answered with an error, or could not be reached.
    Failed(Status),
    /// No answer before the deadline (what a muted or hung peer looks like).
    TimedOut,
    /// This node is muted, so nothing was sent.
    NotSent,
}

/// Calls `call` on every peer at once and returns one result per peer, sorted by ID.
/// Takes at most about `deadline`. If the caller stops waiting (drops this future),
/// all calls still pending are cancelled.
pub async fn broadcast<T, F, Fut>(
    state: &NodeState,
    deadline: Duration,
    call: F,
) -> Vec<(NodeId, PeerResult<T>)>
where
    F: Fn(NodeClient<Channel>) -> Fut,
    Fut: Future<Output = Result<Response<T>, Status>> + Send + 'static,
    T: Send + 'static,
{
    let mut results = Vec::new();
    let mut tasks = JoinSet::new();
    for peer in state.peers.ids() {
        let Some(client) = state.peer_for_sending(peer) else {
            results.push((peer, PeerResult::NotSent));
            continue;
        };
        let reply = call(client);
        tasks.spawn(async move {
            let result = match timeout(deadline, reply).await {
                Ok(Ok(response)) => PeerResult::Reply(response.into_inner()),
                Ok(Err(status)) => PeerResult::Failed(status),
                Err(_) => PeerResult::TimedOut,
            };
            (peer, result)
        });
    }
    // Collected as they finish, not in start order.
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((peer, result)) => results.push((peer, result)),
            // A bug (panic) in one call must not lose the other peers' results.
            Err(e) => tracing::error!(error = %e, "broadcast call panicked"),
        }
    }
    results.sort_by_key(|(peer, _)| *peer);
    results
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use common::config::ClusterConfig;
    use common::pb::HeartbeatRequest;
    use common::pb::node_server::NodeServer;

    use super::*;
    use crate::peers::Peers;
    use crate::service::NodeService;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn three_node_config() -> ClusterConfig {
        ClusterConfig::parse(&format!(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:{}\"\n\
             [timing]\nconnect_timeout_ms = 200\n",
            free_port(),
            free_port(),
            free_port()
        ))
        .unwrap()
    }

    fn state_for(id: NodeId, cfg: &ClusterConfig) -> Arc<NodeState> {
        let peers = Peers::new(cfg, id).unwrap();
        Arc::new(NodeState::new(id, cfg.clone(), peers))
    }

    /// A real server for node `id` (answers heartbeats), in the background.
    async fn serve(id: NodeId, cfg: &ClusterConfig) -> Arc<NodeState> {
        let state = state_for(id, cfg);
        let addr = cfg.node(id).unwrap().addr.parse().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(NodeServer::new(NodeService::new(state.clone())))
                .serve(addr),
        );
        tokio::time::sleep(Duration::from_millis(50)).await; // let it start listening
        state
    }

    /// The sample call for these tests: a heartbeat from node 1.
    fn heartbeat(
        mut client: NodeClient<Channel>,
    ) -> impl Future<Output = Result<Response<common::pb::HeartbeatReply>, Status>> {
        async move {
            client
                .heartbeat(HeartbeatRequest { from_node: 1, seq: 1 })
                .await
        }
    }

    const DEADLINE: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn all_peers_answer() {
        let cfg = three_node_config();
        serve(2, &cfg).await;
        serve(3, &cfg).await;
        let node1 = state_for(1, &cfg);

        let results = broadcast(&node1, DEADLINE, heartbeat).await;
        assert_eq!(results.len(), 2);
        for (peer, result) in results {
            match result {
                PeerResult::Reply(r) => assert_eq!(r.node_id, peer),
                other => panic!("peer {peer}: expected a reply, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn missing_peer_fails_fast() {
        let cfg = three_node_config();
        serve(2, &cfg).await; // node 3 is not running
        let node1 = state_for(1, &cfg);

        let start = Instant::now();
        let results = broadcast(&node1, DEADLINE, heartbeat).await;
        assert!(matches!(results[0], (2, PeerResult::Reply(_))));
        assert!(matches!(results[1], (3, PeerResult::Failed(_))), "{:?}", results[1]);
        assert!(start.elapsed() < DEADLINE, "took {:?}", start.elapsed());
    }

    #[tokio::test]
    async fn muted_node_sends_nothing() {
        let cfg = three_node_config();
        let node1 = state_for(1, &cfg);
        node1.mute_for(Duration::from_secs(5));

        let start = Instant::now();
        let results = broadcast(&node1, DEADLINE, heartbeat).await;
        assert!(matches!(results[..], [(2, PeerResult::NotSent), (3, PeerResult::NotSent)]));
        assert!(start.elapsed() < Duration::from_millis(50));
    }
}
