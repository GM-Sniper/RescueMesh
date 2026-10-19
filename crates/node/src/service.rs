//! The gRPC entry point: tonic calls these methods for every incoming request.
//! Each handler stays thin (unwrap the message, count it, later check `muted`)
//! and hands the real work to a `handle_*` function: this is the dispatch point
//! where other modules plug in.

use std::sync::Arc;
use std::time::Duration;

use common::pb::node_server::Node;
use common::pb::{
    FaultAck, FaultCommand, HeartbeatReply, HeartbeatRequest, SubmitReply, SubmitRequest,
};
use tonic::{Request, Response, Status};

use crate::state::NodeState;

pub struct NodeService {
    /// Shared with the background tasks (heartbeat loop, detector ticker).
    state: Arc<NodeState>,
}

impl NodeService {
    pub fn new(state: Arc<NodeState>) -> Self {
        NodeService { state }
    }

    /// Task 4's gate. Muted: hold the request until the mute ends, then drop it.
    /// Held, not refused: a crashed machine never answers "I'm down"; callers only
    /// notice when their own deadline runs out (and then cancel, which ends the wait).
    async fn gate(&self) -> Result<(), Status> {
        if !self.state.is_muted() {
            return Ok(());
        }
        self.state.wait_while_muted().await;
        Err(Status::unavailable("dropped: node was muted"))
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        self.gate().await?; // muted: arriving requests are held
        let hb = req.into_inner();
        // debug, not info: 2 peers x 2 per second would flood the log. Show with RUST_LOG=debug.
        tracing::debug!(from = hb.from_node, seq = hb.seq, "heartbeat received");
        // A peer's own heartbeat is proof it is alive (and how a returning node
        // announces itself). Clients (0) and unknown IDs are ignored by the detector.
        self.state.heard_from(hb.from_node);
        let reply = HeartbeatReply {
            node_id: self.state.id,
            seq: hb.seq,
        };
        self.gate().await?; // muted meanwhile: the reply never leaves
        Ok(Response::new(reply))
    }

    async fn submit(&self, req: Request<SubmitRequest>) -> Result<Response<SubmitReply>, Status> {
        self.gate().await?; // before counting: held requests are not work
        let _guard = self.state.start_request(); // P3's queue counter, until this returns
        let reply = handle_submit(&self.state, req.into_inner()).await?;
        // Muted mid-request: drop the reply. The client retries with the same
        // request_id and gets the stored plan (duplicate = true).
        self.gate().await?;
        Ok(reply)
    }

    async fn inject_fault(&self, req: Request<FaultCommand>) -> Result<Response<FaultAck>, Status> {
        self.gate().await?; // a muted node ignores new fault orders too
        // No gate before replying: this handler mutes the node itself and must still ack.
        handle_inject_fault(&self.state, req.into_inner()).await
    }
}

/// Task 7 (Tue): check request_id -> incident_from_pb -> store.snapshot() -> plan()
/// -> store.apply() -> reply.
async fn handle_submit(
    _state: &NodeState,
    _req: SubmitRequest,
) -> Result<Response<SubmitReply>, Status> {
    Err(Status::unimplemented("Submit is wired in task 7"))
}

/// Longest allowed silence: the course caps an injected fault at 20 s.
const MAX_MUTE_MS: u64 = 20_000;

/// Task 4: check the fault order, go silent for duration_ms, and ack.
/// Refusals are FaultAcks (accepted = false + reason), not gRPC errors, so the
/// injector only has to read one kind of answer.
async fn handle_inject_fault(
    state: &NodeState,
    cmd: FaultCommand,
) -> Result<Response<FaultAck>, Status> {
    let refusal = if cmd.target_node != state.id {
        Some(format!("this is node {}, not node {}", state.id, cmd.target_node))
    } else if cmd.duration_ms == 0 {
        Some("duration must be at least 1 ms".to_string())
    } else if cmd.duration_ms > MAX_MUTE_MS {
        Some(format!("duration over {} s", MAX_MUTE_MS / 1000))
    } else {
        None
    };
    if let Some(reason) = refusal {
        tracing::warn!(fault_id = %cmd.fault_id, %reason, "fault order refused");
        return Ok(Response::new(FaultAck {
            fault_id: cmd.fault_id,
            accepted: false,
            reason,
        }));
    }

    let duration = Duration::from_millis(cmd.duration_ms);
    state.mute_for(duration);
    tracing::info!(
        fault_id = %cmd.fault_id,
        duration_ms = cmd.duration_ms,
        issuer = cmd.issuer_node,
        term = cmd.term,
        "muted"
    );
    // Log line only: the node wakes up by itself when muted_until passes. No overlap is
    // possible, because a muted node holds (ignores) new fault orders at the gate.
    let fault_id = cmd.fault_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        tracing::info!(%fault_id, "mute ended");
    });

    // Sent even though we are now muted: this request passed the gate before the mute.
    Ok(Response::new(FaultAck {
        fault_id: cmd.fault_id,
        accepted: true,
        reason: String::new(),
    }))
}

#[cfg(test)]
mod tests {
    use common::config::ClusterConfig;

    use super::*;
    use crate::peers::Peers;

    fn service() -> NodeService {
        let cfg = ClusterConfig::parse("[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n").unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        NodeService::new(Arc::new(NodeState::new(1, cfg, peers)))
    }

    #[tokio::test]
    async fn heartbeat_replies_with_own_id_and_echoes_seq() {
        let svc = service();
        let reply = svc
            .heartbeat(Request::new(HeartbeatRequest { from_node: 2, seq: 7 }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_id, 1);
        assert_eq!(reply.seq, 7);
    }

    #[tokio::test]
    async fn submit_is_a_stub_and_leaves_counter_at_zero() {
        let svc = service();
        let err = svc
            .submit(Request::new(SubmitRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert_eq!(svc.state.in_flight(), 0);
    }

    // ---- Task 4: mute hook. `start_paused` = fake clock that jumps ahead whenever
    // everything is waiting, so these 10 s scenarios finish in milliseconds.

    use tokio::time::{Instant, timeout};

    fn order(target_node: u32, duration_ms: u64) -> Request<FaultCommand> {
        Request::new(FaultCommand {
            fault_id: "f1".into(),
            target_node,
            duration_ms,
            issuer_node: 0,
            term: 0,
        })
    }

    fn ping() -> Request<HeartbeatRequest> {
        Request::new(HeartbeatRequest { from_node: 2, seq: 1 })
    }

    #[tokio::test(start_paused = true)]
    async fn fault_order_is_accepted_and_mutes() {
        let svc = service();
        let ack = svc.inject_fault(order(1, 10_000)).await.unwrap().into_inner();
        assert!(ack.accepted);
        assert_eq!(ack.fault_id, "f1");
        assert!(svc.state.is_muted());
    }

    #[tokio::test(start_paused = true)]
    async fn refuses_bad_orders() {
        for (target, ms) in [(2, 10_000), (1, 0), (1, 20_001)] {
            let svc = service();
            let ack = svc.inject_fault(order(target, ms)).await.unwrap().into_inner();
            assert!(!ack.accepted, "target {target}, {ms} ms should be refused");
            assert!(!ack.reason.is_empty());
            assert!(!svc.state.is_muted());
        }
        let svc = service();
        let ack = svc.inject_fault(order(1, 20_000)).await.unwrap().into_inner();
        assert!(ack.accepted, "exactly 20 s is allowed");
    }

    #[tokio::test(start_paused = true)]
    async fn muted_heartbeat_gets_no_early_answer() {
        let svc = service();
        svc.inject_fault(order(1, 10_000)).await.unwrap();
        // No answer at all within 9 s: no quick error.
        assert!(timeout(Duration::from_secs(9), svc.heartbeat(ping())).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn muted_heartbeat_is_dropped_when_mute_ends() {
        let svc = service();
        svc.inject_fault(order(1, 10_000)).await.unwrap();
        let start = Instant::now();
        let err = svc.heartbeat(ping()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(start.elapsed() >= Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn answers_normally_after_mute() {
        let svc = service();
        svc.inject_fault(order(1, 10_000)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10_001)).await;
        assert!(!svc.state.is_muted());
        let reply = svc.heartbeat(ping()).await.unwrap().into_inner();
        assert_eq!(reply.node_id, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn muted_node_ignores_new_fault_orders() {
        let svc = service();
        svc.inject_fault(order(1, 10_000)).await.unwrap();
        let second = timeout(Duration::from_secs(5), svc.inject_fault(order(1, 1_000))).await;
        assert!(second.is_err(), "a muted node must not answer a new fault order");
    }

    // ---- Task 5c: an incoming heartbeat counts as hearing from its sender.

    #[tokio::test(start_paused = true)]
    async fn incoming_heartbeat_brings_peer_back_up() {
        let cfg = ClusterConfig::parse(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:50052\"\n",
        )
        .unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        let svc = NodeService::new(Arc::new(NodeState::new(1, cfg, peers)));

        tokio::time::sleep(Duration::from_millis(1500)).await; // node 2 silent for 1.5 s
        assert_eq!(svc.state.check_peers(), vec![crate::failure_detector::PeerEvent::PeerDown(2)]);

        svc.heartbeat(ping()).await.unwrap(); // node 2's own heartbeat arrives
        assert_eq!(svc.state.check_peers(), vec![crate::failure_detector::PeerEvent::PeerUp(2)]);
    }

    // ---- Task 4f: the same mute, end to end over a real network, with the real clock.

    #[tokio::test]
    async fn real_network_mute_round_trip() {
        use common::pb::node_client::NodeClient;
        use common::pb::node_server::NodeServer;

        // 1. A real server on a free port, in the background.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr: std::net::SocketAddr = ([127, 0, 0, 1], port).into();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(NodeServer::new(service()))
                .serve(addr),
        );

        // 2. A real client (retry briefly while the server starts), normal heartbeat.
        let mut client = loop {
            match NodeClient::connect(format!("http://{addr}")).await {
                Ok(c) => break c,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        assert_eq!(client.heartbeat(ping()).await.unwrap().into_inner().node_id, 1);

        // 3. Mute for 500 ms; the ack still comes back over the network.
        let ack = client.inject_fault(order(1, 500)).await.unwrap().into_inner();
        assert!(ack.accepted);

        // 4. Heartbeat with a 200 ms deadline: silence, not a quick error.
        let muted = timeout(Duration::from_millis(200), client.heartbeat(ping())).await;
        assert!(muted.is_err(), "muted node must not answer at all, got {muted:?}");

        // 5. After the mute, it answers normally again.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let reply = client.heartbeat(ping()).await.unwrap().into_inner();
        assert_eq!(reply.node_id, 1);
    }
}
