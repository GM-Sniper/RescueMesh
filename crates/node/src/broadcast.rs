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
//! - Retry policy: at most one retry, only after a fast `Unavailable` (could not
//!   reach the peer, e.g. a connection broken by a restart), and only with the time
//!   left before the deadline. Timeouts and other errors are final. Retrying is safe
//!   because every state-changing message carries a request_id / fault_id, so a
//!   receiver ignores a duplicate.

use std::future::Future;
use std::time::Duration;

use common::NodeId;
use common::pb::node_client::NodeClient;
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tonic::transport::Channel;
use tonic::{Code, Response, Status};

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
/// Takes at most about `deadline`, retries included. If the caller stops waiting
/// (drops this future), all calls still pending are cancelled.
///
/// `call` must be `Clone + Send + 'static` so each peer's task can make the call
/// again for its one retry: write it with `move` and clone the request inside.
pub async fn broadcast<T, F, Fut>(
    state: &NodeState,
    deadline: Duration,
    call: F,
) -> Vec<(NodeId, PeerResult<T>)>
where
    F: Fn(NodeClient<Channel>) -> Fut + Clone + Send + 'static,
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
        let call = call.clone();
        tasks.spawn(async move { (peer, call_with_retry(peer, client, deadline, call).await) });
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

/// One peer's part: the call, plus at most one retry if it failed fast with
/// `Unavailable` and time is left. The retry only gets the remaining time.
async fn call_with_retry<T, F, Fut>(
    peer: NodeId,
    client: NodeClient<Channel>,
    deadline: Duration,
    call: F,
) -> PeerResult<T>
where
    F: Fn(NodeClient<Channel>) -> Fut,
    Fut: Future<Output = Result<Response<T>, Status>>,
{
    let start = Instant::now();
    match timeout(deadline, call(client.clone())).await {
        Ok(Err(status)) if status.code() == Code::Unavailable => {
            let left = deadline.saturating_sub(start.elapsed());
            if left.is_zero() {
                return PeerResult::Failed(status);
            }
            tracing::debug!(peer, error = %status.message(), "broadcast: peer unavailable, retrying once");
            to_peer_result(timeout(left, call(client)).await)
        }
        // A timeout (deadline used up) or any other answer is final.
        first => to_peer_result(first),
    }
}

fn to_peer_result<T, E>(outcome: Result<Result<Response<T>, Status>, E>) -> PeerResult<T> {
    match outcome {
        Ok(Ok(response)) => PeerResult::Reply(response.into_inner()),
        Ok(Err(status)) => PeerResult::Failed(status),
        Err(_) => PeerResult::TimedOut,
    }
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

    // ---- 6b: deadlines. A muted peer keeps its connection open and never answers.

    const SHORT: Duration = Duration::from_millis(300);

    #[tokio::test]
    async fn silent_peer_times_out_without_holding_up_the_other() {
        let cfg = three_node_config();
        serve(2, &cfg).await;
        let node3 = serve(3, &cfg).await;
        node3.mute_for(Duration::from_secs(5));
        let node1 = state_for(1, &cfg);

        let start = Instant::now();
        let results = broadcast(&node1, SHORT, heartbeat).await;
        let took = start.elapsed();
        assert!(matches!(results[0], (2, PeerResult::Reply(_))), "{:?}", results[0]);
        assert!(matches!(results[1], (3, PeerResult::TimedOut)), "{:?}", results[1]);
        // It really waited for node 3, but only about one deadline.
        assert!(took >= SHORT && took < SHORT + Duration::from_millis(150), "took {took:?}");
    }

    #[tokio::test]
    async fn calls_run_in_parallel() {
        let cfg = three_node_config();
        serve(2, &cfg).await.mute_for(Duration::from_secs(5));
        serve(3, &cfg).await.mute_for(Duration::from_secs(5));
        let node1 = state_for(1, &cfg);

        let start = Instant::now();
        let results = broadcast(&node1, SHORT, heartbeat).await;
        let took = start.elapsed();
        assert!(matches!(results[..], [(2, PeerResult::TimedOut), (3, PeerResult::TimedOut)]));
        // One after another would take 2 x 300 ms; in parallel it is about 300 ms.
        assert!(took < Duration::from_millis(450), "took {took:?}: calls did not run in parallel");
    }

    // ---- 6c: retry policy, with fake calls (no network) that count their attempts.
    // One peer only (node 2), so the attempt count is that peer's.

    use std::sync::atomic::{AtomicUsize, Ordering};

    fn two_node_state() -> Arc<NodeState> {
        let cfg = ClusterConfig::parse(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:50052\"\n",
        )
        .unwrap();
        state_for(1, &cfg)
    }

    /// A fake call: attempt n (0, 1, ...) waits `delay(n)`, then returns `outcome(n)`.
    fn fake(
        attempts: Arc<AtomicUsize>,
        delay: fn(usize) -> Duration,
        outcome: fn(usize) -> Result<u32, Status>,
    ) -> impl Fn(NodeClient<Channel>) -> std::pin::Pin<
        Box<dyn Future<Output = Result<Response<u32>, Status>> + Send>,
    > + Clone
    + Send
    + 'static {
        move |_client| {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                tokio::time::sleep(delay(n)).await;
                outcome(n).map(Response::new)
            })
        }
    }

    fn no_delay(_: usize) -> Duration {
        Duration::ZERO
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_once_is_retried() {
        let n = Arc::new(AtomicUsize::new(0));
        let call = fake(n.clone(), no_delay, |a| {
            if a == 0 { Err(Status::unavailable("blip")) } else { Ok(7) }
        });
        let results = broadcast(&two_node_state(), SHORT, call).await;
        assert!(matches!(results[..], [(2, PeerResult::Reply(7))]), "{results:?}");
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn at_most_one_retry() {
        let n = Arc::new(AtomicUsize::new(0));
        let call = fake(n.clone(), no_delay, |_| Err(Status::unavailable("down")));
        let results = broadcast(&two_node_state(), SHORT, call).await;
        assert!(matches!(results[..], [(2, PeerResult::Failed(_))]));
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn other_errors_are_not_retried() {
        let n = Arc::new(AtomicUsize::new(0));
        let call = fake(n.clone(), no_delay, |_| Err(Status::invalid_argument("no")));
        let results = broadcast(&two_node_state(), SHORT, call).await;
        assert!(matches!(results[..], [(2, PeerResult::Failed(_))]));
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn timeouts_are_not_retried() {
        let n = Arc::new(AtomicUsize::new(0));
        let call = fake(n.clone(), |_| Duration::from_secs(60), |_| Ok(1));
        let results = broadcast(&two_node_state(), SHORT, call).await;
        assert!(matches!(results[..], [(2, PeerResult::TimedOut)]));
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_stays_within_the_deadline() {
        let n = Arc::new(AtomicUsize::new(0));
        // First attempt fails after 250 ms; the retry would hang.
        let call = fake(
            n.clone(),
            |a| if a == 0 { Duration::from_millis(250) } else { Duration::from_secs(60) },
            |a| if a == 0 { Err(Status::unavailable("blip")) } else { Ok(1) },
        );
        let start = tokio::time::Instant::now();
        let results = broadcast(&two_node_state(), SHORT, call).await;
        assert!(matches!(results[..], [(2, PeerResult::TimedOut)]));
        assert_eq!(n.load(Ordering::SeqCst), 2);
        assert_eq!(start.elapsed(), SHORT, "the retry only gets the time that was left");
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
