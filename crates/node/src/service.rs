//! The gRPC entry point: tonic calls these methods for every incoming request.
//! Each handler stays thin (unwrap the message, count it, later check `muted`)
//! and hands the real work to a `handle_*` function: this is the dispatch point
//! where other modules plug in.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::conversions::{incident_from_pb, plan_to_pb};
use common::domain::Plan;
use common::pb::node_server::Node;
use common::pb::{
    FaultAck, FaultCommand, HeartbeatReply, HeartbeatRequest, SubmitReply, SubmitRequest,
    submit_request,
};
use common::planner;
use tonic::{Request, Response, Status};

use crate::state::NodeState;
use crate::store::ApplyError;

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
        // Logged under node::heartbeat, so RUST_LOG=node::service=debug shows only requests.
        tracing::debug!(target: "node::heartbeat", from = hb.from_node, seq = hb.seq, "heartbeat received");
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
        let guard = self.state.start_request(); // P3's queue counter and service time
        let result = handle_submit(&self.state, req.into_inner()).await;
        // Stop counting and timing before the reply gate: time held by a mute is not
        // service time, and counting it would make the node look overloaded (and lose
        // elections) right after it unmutes.
        drop(guard);
        let reply = result?;
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

/// How many times to plan again when another request takes a planned resource
/// between our snapshot and our write.
const MAX_PLAN_ATTEMPTS: usize = 3;

/// Task 7: one client request, in the order from P4's notes (docs/p4-changes.md):
/// lookup_request -> incident_from_pb -> snapshot -> plan -> try_apply -> reply.
async fn handle_submit(
    state: &NodeState,
    req: SubmitRequest,
) -> Result<Response<SubmitReply>, Status> {
    // 1. Every request needs its ID (idempotency) and an incident (the only kind in Step A).
    let request_id = req.request_id;
    if request_id.is_empty() {
        return Err(Status::invalid_argument("request_id is empty"));
    }
    let Some(submit_request::Kind::Incident(incident)) = req.kind else {
        return Err(Status::invalid_argument("expected an incident"));
    };
    let reply = |plan: &Plan, duplicate: bool| {
        Response::new(SubmitReply {
            request_id: request_id.clone(),
            executor_node: state.id,
            plan: Some(plan_to_pb(plan)),
            duplicate,
        })
    };

    // 2. Already handled: the same answer again, never a second plan.
    if let Some(stored) = state.store.lookup_request(&request_id) {
        return Ok(reply(&stored, true));
    }

    // 3. P2's conversion checks the incident (category, severity 1-10, location...).
    let incident = incident_from_pb(incident).map_err(Status::invalid_argument)?;
    let weights = state.config.weights;
    let planner_config = state.config.planner;
    // 4. Snapshot -> plan -> try_apply. try_apply re-checks under the store's lock, so a
    //    resource taken by another request in between is caught, and we plan again.
    for attempt in 1..=MAX_PLAN_ATTEMPTS {
        let now = now_unix_ms();
        let snapshot = state.store.snapshot();
        let to_plan = incident.clone();
        // The planner is CPU work: run it off the async threads so it can never
        // delay heartbeats (which would cause false PeerDowns).
        let plan = tokio::task::spawn_blocking(move || {
            planner::plan_with_workload(&to_plan, &snapshot, &weights, now, &planner_config)
        })
        .await
        .map_err(|e| Status::internal(format!("planner failed: {e}")))?;

        match state
            .store
            .try_apply(&request_id, incident.clone(), plan, now)
        {
            Ok(applied) => {
                tracing::debug!(
                    %request_id,
                    assigned = applied.plan.assignments.len(),
                    duplicate = applied.duplicate,
                    attempt,
                    "submit applied"
                );
                return Ok(reply(&applied.plan, applied.duplicate));
            }
            Err(ApplyError::ResourceTaken { resource_id }) => {
                tracing::debug!(%request_id, attempt, resource_id, "resource taken meanwhile; planning again");
            }
            Err(ApplyError::UnknownResource { resource_id }) => {
                return Err(Status::internal(format!(
                    "plan names resource {resource_id}, which the store does not have"
                )));
            }
        }
    }
    // Conflicts every time: the client may retry with the same request_id.
    // warn: the client only sees "error", so the node's log must say why.
    tracing::warn!(%request_id, "submit aborted: resources kept changing during planning");
    Err(Status::aborted(format!(
        "resources kept changing during planning ({MAX_PLAN_ATTEMPTS} attempts); retry"
    )))
}

/// The node's wall clock in Unix milliseconds (the planner's `now`, the log entry's timestamp).
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Longest allowed silence: the course caps an injected fault at 20 s.
const MAX_MUTE_MS: u64 = 20_000;

/// Task 4: check the fault order, go silent for duration_ms, and ack.
/// Refusals are FaultAcks (accepted = false + reason), not gRPC errors, so the
/// injector only has to read one kind of answer.
async fn handle_inject_fault(
    state: &Arc<NodeState>, // Arc: the "mute ended" task keeps its own handle
    cmd: FaultCommand,
) -> Result<Response<FaultAck>, Status> {
    let refusal = if cmd.target_node != state.id {
        Some(format!(
            "this is node {}, not node {}",
            state.id, cmd.target_node
        ))
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
    if let Some(log) = &state.fault_log {
        log.mute_start(&cmd.fault_id, cmd.duration_ms);
    }
    // Logging only: the node wakes up by itself when muted_until passes. No overlap is
    // possible, because a muted node holds (ignores) new fault orders at the gate.
    let fault_id = cmd.fault_id.clone();
    let state = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        tracing::info!(%fault_id, "mute ended");
        if let Some(log) = &state.fault_log {
            log.mute_end(&fault_id);
        }
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
            .heartbeat(Request::new(HeartbeatRequest {
                from_node: 2,
                seq: 7,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_id, 1);
        assert_eq!(reply.seq, 7);
    }

    #[tokio::test]
    async fn empty_submit_is_rejected_and_leaves_counter_at_zero() {
        let svc = service();
        let err = svc
            .submit(Request::new(SubmitRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(svc.state.in_flight(), 0);
    }

    // ---- Task 7b: Submit against the real store (10 default resources) and P2's
    // planner, with real incidents from P2's seeded generator. No network.

    use common::conversions::incident_to_pb;
    use common::generators::generate_incident;

    fn submit_req(request_id: &str, seed: u64) -> Request<SubmitRequest> {
        Request::new(SubmitRequest {
            request_id: request_id.into(),
            kind: Some(submit_request::Kind::Incident(incident_to_pb(
                &generate_incident(seed),
            ))),
        })
    }

    #[tokio::test]
    async fn valid_submit_returns_a_plan() {
        let svc = service();
        let reply = svc.submit(submit_req("r1", 7)).await.unwrap().into_inner();
        assert_eq!(reply.request_id, "r1");
        assert_eq!(reply.executor_node, 1);
        assert!(!reply.duplicate);
        let plan = reply.plan.expect("a plan");
        assert_eq!(plan.incident_id, generate_incident(7).id);
        assert_eq!(
            plan.assignments.len(),
            1,
            "a free resource exists, so one is assigned"
        );
        assert_eq!(svc.state.store.applied().get(&1), Some(&1)); // one log entry
    }

    #[tokio::test]
    async fn same_request_twice_gives_the_same_plan_once() {
        let svc = service();
        let first = svc.submit(submit_req("r1", 7)).await.unwrap().into_inner();
        let again = svc.submit(submit_req("r1", 7)).await.unwrap().into_inner();
        assert!(again.duplicate);
        assert_eq!(again.plan, first.plan);
        assert_eq!(svc.state.store.applied().get(&1), Some(&1)); // still one log entry
        assert_eq!(svc.state.store.snapshot().assignments.len(), 1);
    }

    #[tokio::test]
    async fn bad_inputs_are_rejected_and_store_nothing() {
        let svc = service();
        let mut bad_incident = incident_to_pb(&generate_incident(7));
        bad_incident.severity = 0;
        let cases = [
            submit_req("", 7), // empty request_id
            Request::new(SubmitRequest {
                request_id: "r2".into(),
                kind: None,
            }),
            Request::new(SubmitRequest {
                request_id: "r3".into(),
                kind: Some(submit_request::Kind::Incident(bad_incident)),
            }),
        ];
        for req in cases {
            let err = svc.submit(req).await.unwrap_err();
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "{}",
                err.message()
            );
        }
        assert!(svc.state.store.applied().is_empty());
        assert_eq!(svc.state.in_flight(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_requests_never_double_book() {
        let svc = Arc::new(service());
        // 20 requests at once for 10 resources: they collide, and the handler re-plans.
        let tasks: Vec<_> = (1..=20u64)
            .map(|i| {
                let svc = svc.clone();
                tokio::spawn(async move { svc.submit(submit_req(&format!("r{i}"), i)).await })
            })
            .collect();
        let mut aborted = Vec::new();
        for (i, task) in (1..=20u64).zip(tasks) {
            match task.await.unwrap() {
                Ok(_) => {}
                Err(s) if s.code() == tonic::Code::Aborted => aborted.push(i),
                Err(s) => panic!("request r{i}: unexpected error {s:?}"),
            }
        }
        // Retrying with the same request_id succeeds once the rush is over.
        for i in aborted {
            svc.submit(submit_req(&format!("r{i}"), i)).await.unwrap();
        }

        let snap = svc.state.store.snapshot();
        let mut resources: Vec<u64> = snap.assignments.iter().map(|a| a.resource_id).collect();
        let total = resources.len();
        resources.sort();
        resources.dedup();
        assert_eq!(
            resources.len(),
            total,
            "a resource was given to two incidents"
        );
        assert_eq!(total, 10, "all 10 resources end up assigned, once each");
        assert_eq!(svc.state.store.applied().get(&1), Some(&20)); // every request logged once
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
        Request::new(HeartbeatRequest {
            from_node: 2,
            seq: 1,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn fault_order_is_accepted_and_mutes() {
        let svc = service();
        let ack = svc
            .inject_fault(order(1, 10_000))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);
        assert_eq!(ack.fault_id, "f1");
        assert!(svc.state.is_muted());
    }

    #[tokio::test(start_paused = true)]
    async fn refuses_bad_orders() {
        for (target, ms) in [(2, 10_000), (1, 0), (1, 20_001)] {
            let svc = service();
            let ack = svc
                .inject_fault(order(target, ms))
                .await
                .unwrap()
                .into_inner();
            assert!(!ack.accepted, "target {target}, {ms} ms should be refused");
            assert!(!ack.reason.is_empty());
            assert!(!svc.state.is_muted());
        }
        let svc = service();
        let ack = svc
            .inject_fault(order(1, 20_000))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted, "exactly 20 s is allowed");
    }

    #[tokio::test(start_paused = true)]
    async fn muted_heartbeat_gets_no_early_answer() {
        let svc = service();
        svc.inject_fault(order(1, 10_000)).await.unwrap();
        // No answer at all within 9 s: no quick error.
        assert!(
            timeout(Duration::from_secs(9), svc.heartbeat(ping()))
                .await
                .is_err()
        );
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
        assert!(
            second.is_err(),
            "a muted node must not answer a new fault order"
        );
    }

    // ---- Task 7d: the mute hook writes P5's fault CSV.

    #[tokio::test(start_paused = true)]
    async fn mute_writes_start_and_end_rows() {
        let path = std::env::temp_dir()
            .join(format!("rescuemesh-test-{}-mute-rows", std::process::id()))
            .join("faults.csv");
        let _ = std::fs::remove_file(&path);
        let cfg = ClusterConfig::parse("[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n").unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        let log = crate::fault_log::FaultLog::open(&path, 1, "t").unwrap();
        let svc = NodeService::new(Arc::new(NodeState::new(1, cfg, peers).with_fault_log(log)));

        svc.inject_fault(order(1, 10_000)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10_001)).await; // lets the "mute ended" task run
        let events: Vec<String> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .skip(1) // header
            .map(|l| l.split(',').take(6).collect::<Vec<_>>().join(","))
            .collect();
        assert_eq!(events, ["t,1,MUTE_START,f1,,10000", "t,1,MUTE_END,f1,,"]);
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
        assert_eq!(
            svc.state.check_peers(),
            vec![crate::failure_detector::PeerEvent::PeerDown(2)]
        );

        svc.heartbeat(ping()).await.unwrap(); // node 2's own heartbeat arrives
        assert_eq!(
            svc.state.check_peers(),
            vec![crate::failure_detector::PeerEvent::PeerUp(2)]
        );
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
        assert_eq!(
            client.heartbeat(ping()).await.unwrap().into_inner().node_id,
            1
        );

        // 3. Mute for 500 ms; the ack still comes back over the network.
        let ack = client
            .inject_fault(order(1, 500))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);

        // 4. Heartbeat with a 200 ms deadline: silence, not a quick error.
        let muted = timeout(Duration::from_millis(200), client.heartbeat(ping())).await;
        assert!(
            muted.is_err(),
            "muted node must not answer at all, got {muted:?}"
        );

        // 5. After the mute, it answers normally again.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let reply = client.heartbeat(ping()).await.unwrap().into_inner();
        assert_eq!(reply.node_id, 1);
    }
}
