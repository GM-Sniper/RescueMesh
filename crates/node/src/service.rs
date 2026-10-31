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
    Bid, BidRequest, Coordinator, CoordinatorAck, FaultAck, FaultCommand, HeartbeatReply,
    HeartbeatRequest, SubmitReply, SubmitRequest, submit_request,
};
use common::planner;
use common::shared_state::{CommitOutcome, StateUnavailable};
use tokio::sync::watch;
use tonic::{Request, Response, Status};

use crate::election;
use crate::state::{NodeState, SubmitResult};

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
        self.gate().await?; // held while muted; not counted as work
        // The work runs in its own task (it counts itself in P3's queue and service time).
        // If the client stops waiting, only this wait ends: the work still finishes and
        // commits, and the client's retry gets the stored plan.
        let reply = wait_for(start_or_join(&self.state, req.into_inner())).await?;
        // Muted mid-request: drop the reply. The client retries with the same
        // request_id and gets the stored plan (duplicate = true).
        self.gate().await?;
        Ok(Response::new(reply))
    }

    async fn inject_fault(&self, req: Request<FaultCommand>) -> Result<Response<FaultAck>, Status> {
        self.gate().await?; // a muted node ignores new fault orders too
        // No gate before replying: this handler mutes the node itself and must still ack.
        handle_inject_fault(&self.state, req.into_inner()).await
    }

    // P3: a peer's bid for one request; the reply carries ours (see election.rs).
    async fn exchange_bid(&self, req: Request<BidRequest>) -> Result<Response<Bid>, Status> {
        self.gate().await?;
        let call = req.into_inner();
        let request = call.request.clone();
        let round_of = |id: &str| self.state.election.lock().unwrap().round(id);
        let before = request.as_ref().and_then(|r| round_of(&r.request_id));
        let reply = election::handle_exchange_bid(&self.state, call)?;
        // First we hear of this request (the client's copy is late or lost), or the peer
        // moved us to a newer round: run Submit for it too. Otherwise, with the lowest
        // bid, we would be everyone's winner but never plan. The reply has no client
        // to go to; a client retry finds the committed plan. If the client's copy is already
        // being worked on here, this joins that work instead of planning a second time.
        if let Some(request) = request
            && (before.is_none() || round_of(&request.request_id) > before)
        {
            start_or_join(&self.state, request); // nobody waits for this result
        }
        self.gate().await?; // muted meanwhile: the reply never leaves
        Ok(Response::new(reply))
    }

    // P3: a peer announces it won an election; we check it against ours (see election.rs).
    async fn announce_winner(
        &self,
        req: Request<Coordinator>,
    ) -> Result<Response<CoordinatorAck>, Status> {
        self.gate().await?;
        let ack = election::handle_announce_winner(&self.state, req.into_inner())?;
        self.gate().await?; // muted meanwhile: the reply never leaves
        Ok(Response::new(ack))
    }
}

/// Starts the Submit work for `req` in its own task, or joins the work already running
/// for the same request_id. Returns where the result will arrive.
///
/// Own task (Step C, found in the slow-request case): work run inside the client's call
/// is dropped when the client stops waiting, so a plan slower than the client's deadline
/// was never committed, and every retry started it again. The table makes a retry, or a
/// peer's bid, wait for the work already running instead of planning twice.
fn start_or_join(state: &Arc<NodeState>, req: SubmitRequest) -> watch::Receiver<Option<SubmitResult>> {
    let mut running = state.in_progress.lock().unwrap();
    if let Some(result) = running.get(&req.request_id) {
        return result.clone(); // already being worked on: wait for that
    }
    let (send, result) = watch::channel(None);
    running.insert(req.request_id.clone(), result.clone());
    drop(running); // unlock before starting the task
    let state = state.clone();
    tokio::spawn(async move {
        // Leaves the table however the task ends (result, error or panic), so no
        // request_id can be stuck "in progress" with nobody working on it.
        let _entry = InProgressEntry { state: &state, request_id: req.request_id.clone() };
        let request_id = req.request_id.clone();
        let started = std::time::Instant::now();
        if let Some(log) = &state.request_log {
            log.start(&request_id); // for P6: a START with no END = unfinished when the node died
        }
        let counted = state.start_request(); // real work: counts in our load (P3)
        let outcome = handle_submit(&state, req).await.map(Response::into_inner);
        drop(counted); // stop counting before anyone sees the result
        if let Err(status) = &outcome {
            tracing::debug!(%request_id, %status, "submit failed");
        }
        if let Some(log) = &state.request_log {
            log.end(&request_id, &outcome, started.elapsed());
        }
        let _ = send.send(Some(outcome)); // an error here only means nobody is waiting any more
    });
    result
}

/// Waits for the result `start_or_join` promised.
async fn wait_for(mut result: watch::Receiver<Option<SubmitResult>>) -> SubmitResult {
    match result.wait_for(Option::is_some).await {
        Ok(done) => done.clone().expect("wait_for only returns once there is a result"),
        // The task ended without sending: it panicked.
        Err(_) => Err(Status::internal("submit stopped without a result")),
    }
}

/// Removes a request from `in_progress` when dropped.
struct InProgressEntry<'a> {
    state: &'a NodeState,
    request_id: String,
}

impl Drop for InProgressEntry<'_> {
    fn drop(&mut self) {
        self.state.in_progress.lock().unwrap().remove(&self.request_id);
    }
}

/// How many times to plan again when another request takes a planned resource
/// between our snapshot and our write.
const MAX_PLAN_ATTEMPTS: usize = 3;

/// Task 7: one client request, in the order from P4's notes (docs/p4-changes.md):
/// lookup_request -> incident_from_pb -> election -> snapshot -> plan -> commit -> reply.
async fn handle_submit(
    state: &Arc<NodeState>, // Arc: the election and the win announcement keep their own handle
    req: SubmitRequest,
) -> Result<Response<SubmitReply>, Status> {
    // 1. Every request needs its ID (idempotency) and an incident (the only kind in Step A).
    let request_id = req.request_id.clone();
    if request_id.is_empty() {
        return Err(Status::invalid_argument("request_id is empty"));
    }
    // Cloned, not moved: the election sends the whole request along with our bid.
    let Some(submit_request::Kind::Incident(incident)) = req.kind.clone() else {
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
    if let Some(stored) = state.store.lookup_request(&request_id).await.map_err(unavailable)? {
        return Ok(reply(&stored, true));
    }

    // 3. P2's conversion checks the incident (category, severity 1-10, location...).
    let incident = incident_from_pb(incident).map_err(Status::invalid_argument)?;
    let weights = state.config.weights;
    let planner_config = state.config.planner;

    // 4. Election (P3): every node that got this request bids, and only the winner plans.
    match election::elect(state, &req).await {
        Some(winner) if winner == state.id => {}
        // Lost: name the winner and send no plan (Q2). "No plan" is not an empty plan;
        // the client keeps the reply that has one.
        Some(winner) => {
            return Ok(Response::new(SubmitReply {
                request_id,
                executor_node: winner,
                plan: None,
                duplicate: false,
            }));
        }
        // No bid was ready: no node can reach the shared state now. The client retries.
        None => return Err(Status::unavailable("no node can take this request now; retry")),
    }
    // Won: tell the peers before planning, so a rival that also thinks it won can step
    // aside early (design doc step 6). In the background: the acks carry nothing we need,
    // so the client's reply never waits for them.
    let round = state.election.lock().unwrap().round(&request_id).unwrap_or(0);
    let (announcer, id) = (state.clone(), request_id.clone());
    tokio::spawn(async move { election::announce_winner(&announcer, &id, round).await });

    // 5. Snapshot -> plan -> commit. commit re-checks that every planned resource is still
    //    free, so a resource taken by another request in between is caught, and we plan again.
    for attempt in 1..=MAX_PLAN_ATTEMPTS {
        let now = now_unix_ms();
        let snapshot = state.store.snapshot().await.map_err(unavailable)?;
        let to_plan = incident.clone();
        // The planner is CPU work: run it off the async threads so it can never
        // delay heartbeats (which would cause false PeerDowns).
        let plan = tokio::task::spawn_blocking(move || {
            planner::plan_with_workload(&to_plan, &snapshot, &weights, now, &planner_config)
        })
        .await
        .map_err(|e| Status::internal(format!("planner failed: {e}")))?;

        match state.store.commit(&request_id, incident.clone(), plan.clone()).await {
            CommitOutcome::Committed => {
                tracing::debug!(
                    %request_id,
                    assigned = plan.assignments.len(),
                    attempt,
                    "submit committed"
                );
                return Ok(reply(&plan, false));
            }
            // Another request (maybe on another node) committed this request_id first:
            // its plan is the answer, ours is discarded.
            CommitOutcome::AlreadyCommitted(stored) => {
                tracing::debug!(%request_id, attempt, "submit already committed; returning stored plan");
                return Ok(reply(&stored, true));
            }
            CommitOutcome::ResourceTaken { resource_id } => {
                tracing::debug!(%request_id, attempt, resource_id, "resource taken meanwhile; planning again");
            }
            CommitOutcome::StateUnavailable(why) => return Err(unavailable(why)),
        }
    }
    // Conflicts every time: the client may retry with the same request_id.
    // warn: the client only sees "error", so the node's log must say why.
    tracing::warn!(%request_id, "submit aborted: resources kept changing during planning");
    Err(Status::aborted(format!(
        "resources kept changing during planning ({MAX_PLAN_ATTEMPTS} attempts); retry"
    )))
}

/// A shared-state failure as a gRPC error. `Unavailable` tells the client the same
/// request_id can be retried: commit is create-if-absent, so a retry cannot double-book.
fn unavailable(why: StateUnavailable) -> Status {
    tracing::warn!(%why, "shared state unavailable");
    Status::unavailable(why.to_string())
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
pub(crate) async fn handle_inject_fault(
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
        assert_eq!(plan.assignments.len(), 1, "a free resource exists, so one is assigned");
        assert_eq!(svc.state.store.snapshot().await.unwrap().incidents.len(), 1);
    }

    #[tokio::test]
    async fn same_request_twice_gives_the_same_plan_once() {
        let svc = service();
        let first = svc.submit(submit_req("r1", 7)).await.unwrap().into_inner();
        let again = svc.submit(submit_req("r1", 7)).await.unwrap().into_inner();
        assert!(again.duplicate);
        assert_eq!(again.plan, first.plan);
        let snapshot = svc.state.store.snapshot().await.unwrap();
        assert_eq!(snapshot.incidents.len(), 1); // still one record
        assert_eq!(snapshot.assignments.len(), 1);
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
        assert!(svc.state.store.snapshot().await.unwrap().incidents.is_empty());
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

        let snap = svc.state.store.snapshot().await.unwrap();
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
        assert_eq!(snap.incidents.len(), 20, "every request is stored once");
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

    // ---- Step C: Submit runs in its own task (found in the slow-request case).

    /// One node whose plans take a while (P2's workload knob), so a caller can give up mid-plan.
    fn slow_service() -> NodeService {
        let cfg = ClusterConfig::parse(
            "[[nodes]]
id = 1
addr = \"127.0.0.1:50051\"
[planner]
iterations = 20000
",
        )
        .unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        NodeService::new(Arc::new(NodeState::new(1, cfg, peers)))
    }

    /// Waits (up to 10 s) until no Submit is in progress on this node.
    async fn wait_until_idle(svc: &NodeService) {
        for _ in 0..1000 {
            if svc.state.in_progress.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("a request stayed in progress");
    }

    #[tokio::test]
    async fn slow_plan_is_saved_even_if_the_caller_stops_waiting() {
        let svc = slow_service();
        // The caller gives up mid-plan, like a client whose attempt deadline passed.
        let gave_up = tokio::time::timeout(Duration::from_millis(20), svc.submit(submit_req("s1", 1))).await;
        assert!(gave_up.is_err(), "the plan should take longer than 20 ms");
        // Before the fix, the plan was dropped here and never committed.
        wait_until_idle(&svc).await;
        let stored = svc.state.store.lookup_request("s1").await.unwrap().expect("plan was committed");
        // The retry gets that plan at once.
        let retry = svc.submit(submit_req("s1", 1)).await.unwrap().into_inner();
        assert!(retry.duplicate);
        assert_eq!(retry.plan, Some(plan_to_pb(&stored)));
        assert_eq!(svc.state.in_flight(), 0);
    }

    #[tokio::test]
    async fn same_request_arriving_twice_at_once_is_planned_once() {
        let svc = slow_service();
        // E.g. the client's copy and a peer's bid, or a retry while the first plan runs.
        let (a, b) = tokio::join!(svc.submit(submit_req("s2", 2)), svc.submit(submit_req("s2", 2)));
        let (a, b) = (a.unwrap().into_inner(), b.unwrap().into_inner());
        // One piece of work, one result for both. Planning twice would make the
        // second commit fail and come back as a duplicate.
        assert_eq!(a, b);
        assert!(!a.duplicate, "planned only once");
    }

    #[tokio::test]
    async fn finished_requests_leave_the_in_progress_table() {
        let svc = service();
        svc.submit(submit_req("s3", 3)).await.unwrap();
        let mut bad = submit_req("s4", 4).into_inner();
        if let Some(submit_request::Kind::Incident(incident)) = bad.kind.as_mut() {
            incident.severity = 99; // rejected by P2's checks
        }
        assert!(svc.submit(Request::new(bad)).await.is_err());
        wait_until_idle(&svc).await; // an error must not leave the request stuck "in progress"
        assert_eq!(svc.state.in_flight(), 0);
    }

    #[tokio::test]
    async fn request_log_has_one_start_and_end_per_piece_of_work() {
        use crate::request_log::RequestLog;
        let mut path = std::env::temp_dir();
        path.push(format!("rescuemesh-test-{}-svc-requests.csv", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let cfg = ClusterConfig::parse("[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n").unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        let state = NodeState::new(1, cfg, peers).with_request_log(RequestLog::open(&path, 1, "t").unwrap());
        let svc = NodeService::new(Arc::new(state));

        // Two copies at once (client + retry) are one piece of work: one START, one END.
        let (a, b) = tokio::join!(svc.submit(submit_req("q1", 1)), svc.submit(submit_req("q1", 1)));
        a.unwrap();
        b.unwrap();
        // A later retry is new work that finds the stored plan.
        svc.submit(submit_req("q1", 1)).await.unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        let events: Vec<String> = text
            .lines()
            .skip(1) // header
            .map(|l| {
                let c: Vec<&str> = l.split(',').collect();
                format!("{} {} {}", c[2], c[3], c[4])
            })
            .collect();
        assert_eq!(events, ["q1 START ", "q1 END committed", "q1 START ", "q1 END duplicate"]);
        let _ = std::fs::remove_file(&path);
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

    // ---- Step B task 4: the election in Submit. 3 real nodes on localhost share one
    // store, standing in for the DFS: with a store each, a second plan would go unnoticed.

    use common::pb::node_client::NodeClient;
    use common::pb::node_server::NodeServer;
    use common::NodeId;
    use common::shared_state::SharedState;
    use tonic::transport::Channel;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    fn shared_store() -> Arc<dyn SharedState> {
        Arc::new(crate::store::InMemoryStore::with_default_resources())
    }

    /// Starts 3 real nodes that all use `store`; returns each one's state and a client to it.
    async fn three_nodes(store: Arc<dyn SharedState>) -> Vec<(Arc<NodeState>, NodeClient<Channel>)> {
        three_nodes_with_slow_node(store, None).await
    }

    /// Like `three_nodes`, but `slow = Some((id, iterations))` gives that one node a heavier
    /// planner, as `--planner-iterations` does: the "artificially loaded node" (P3).
    async fn three_nodes_with_slow_node(
        store: Arc<dyn SharedState>,
        slow: Option<(NodeId, usize)>,
    ) -> Vec<(Arc<NodeState>, NodeClient<Channel>)> {
        let cfg = ClusterConfig::parse(&format!(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:{}\"\n",
            free_port(),
            free_port(),
            free_port()
        ))
        .unwrap();
        let mut nodes = Vec::new();
        for id in 1..=3 {
            let peers = Peers::new(&cfg, id).unwrap();
            let mut node_cfg = cfg.clone();
            if let Some((slow_id, iterations)) = slow
                && slow_id == id
            {
                node_cfg.planner.iterations = iterations;
            }
            let state = Arc::new(NodeState::new(id, node_cfg, peers).with_store(store.clone()));
            let addr: std::net::SocketAddr = cfg.node(id).unwrap().addr.parse().unwrap();
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(NodeServer::new(NodeService::new(state.clone())))
                    .serve(addr),
            );
            let client = loop {
                match NodeClient::connect(format!("http://{addr}")).await {
                    Ok(c) => break c,
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            };
            nodes.push((state, client));
        }
        nodes
    }

    #[tokio::test]
    async fn request_sent_to_all_three_gets_exactly_one_plan() {
        let store = shared_store();
        let nodes = three_nodes(store.clone()).await;
        // The client multicasts: the same request_id to every node at once.
        let [mut c1, mut c2, mut c3] = [0, 1, 2].map(|i| nodes[i].1.clone());
        let (r1, r2, r3) = tokio::join!(
            c1.submit(submit_req("e1", 7)),
            c2.submit(submit_req("e1", 7)),
            c3.submit(submit_req("e1", 7)),
        );
        let replies = [r1, r2, r3].map(|r| r.unwrap().into_inner());

        let planned: Vec<&SubmitReply> = replies.iter().filter(|r| r.plan.is_some()).collect();
        assert_eq!(planned.len(), 1, "exactly one node plans: {replies:?}");
        let winner = planned[0].executor_node;
        assert!(replies.iter().all(|r| r.executor_node == winner), "same winner everywhere: {replies:?}");
        assert!(replies.iter().all(|r| !r.duplicate));
        assert_eq!(store.snapshot().await.unwrap().incidents.len(), 1, "stored once");
    }

    #[tokio::test]
    async fn retry_to_a_node_that_lost_returns_the_same_plan() {
        let nodes = three_nodes(shared_store()).await;
        let [mut c1, mut c2, mut c3] = [0, 1, 2].map(|i| nodes[i].1.clone());
        let (r1, r2, r3) = tokio::join!(
            c1.submit(submit_req("e2", 7)),
            c2.submit(submit_req("e2", 7)),
            c3.submit(submit_req("e2", 7)),
        );
        let replies = [r1, r2, r3].map(|r| r.unwrap().into_inner());
        let first = replies.iter().find(|r| r.plan.is_some()).expect("one plan");

        let loser = if first.executor_node == 1 { 2 } else { 1 };
        let mut client = nodes[loser - 1].1.clone();
        let again = client.submit(submit_req("e2", 7)).await.unwrap().into_inner();
        assert!(again.duplicate, "answered from the store, not planned again");
        assert_eq!(again.plan, first.plan);
    }

    #[tokio::test]
    async fn with_one_node_muted_the_other_two_still_give_one_plan() {
        let nodes = three_nodes(shared_store()).await;
        nodes[2].0.mute_for(Duration::from_secs(5)); // node 3 goes silent
        // Node 3's copy from the client would be held by its mute: only 1 and 2 answer.
        let [mut c1, mut c2] = [0, 1].map(|i| nodes[i].1.clone());
        let (r1, r2) = tokio::join!(c1.submit(submit_req("e3", 7)), c2.submit(submit_req("e3", 7)));
        let replies = [r1, r2].map(|r| r.unwrap().into_inner());
        assert_eq!(replies.iter().filter(|r| r.plan.is_some()).count(), 1, "{replies:?}");
        assert!(replies.iter().all(|r| r.executor_node != 3), "a muted node cannot win");
    }

    #[tokio::test]
    async fn winner_that_only_got_a_peers_bid_still_plans() {
        let store = shared_store();
        let nodes = three_nodes(store.clone()).await;
        // Only node 1 gets the client's copy. It is busy with it (queue 1), so nodes 2 and
        // 3 bid a lower load, and node 2 wins (tie with 3) from the bid call alone.
        let mut c1 = nodes[0].1.clone();
        let first = c1.submit(submit_req("e4", 7)).await.unwrap().into_inner();
        assert_eq!(first.executor_node, 2, "{first:?}");
        assert!(first.plan.is_none());

        // Node 2 plans in the background; the client's retry gets that plan.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let stored = loop {
            if let Some(plan) = store.lookup_request("e4").await.unwrap() {
                break plan;
            }
            assert!(tokio::time::Instant::now() < deadline, "node 2 never planned");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let again = c1.submit(submit_req("e4", 7)).await.unwrap().into_inner();
        assert!(again.duplicate);
        assert_eq!(again.plan, Some(plan_to_pb(&stored)));
    }

    // P3's load-imbalance test: node 1 plans much slower, so its service time (part of
    // the load score) goes up each time it wins, and the other two take most requests.
    // Node 1 is the hardest one to make lose: it wins every tie (lowest ID), and with
    // nobody slow it takes about 13 of 30. Only service time decides here: requests go
    // one at a time, so no node has a queue, and CPU stays 0 (tests don't start the
    // sampler). Loose check, not exact shares: timing differs between machines. The
    // measured shares come from live runs (docs/p1-design-log.md, section 12).
    #[tokio::test]
    async fn slow_node_wins_fewer_elections() {
        let nodes = three_nodes_with_slow_node(shared_store(), Some((1, SLOW_ITERATIONS))).await;
        let mut wins = [0usize; 3];
        for i in 0..30 {
            let id = format!("li{i}");
            let [mut c1, mut c2, mut c3] = [0, 1, 2].map(|n| nodes[n].1.clone());
            let (r1, r2, r3) = tokio::join!(
                c1.submit(submit_req(&id, i)),
                c2.submit(submit_req(&id, i)),
                c3.submit(submit_req(&id, i)),
            );
            let replies = [r1, r2, r3].map(|r| r.unwrap().into_inner());
            let winner = replies.iter().find(|r| r.plan.is_some()).expect("one node plans").executor_node;
            assert!(replies.iter().all(|r| r.executor_node == winner), "same winner everywhere: {replies:?}");
            wins[winner as usize - 1] += 1;
        }
        // At most half a fair share (10 of 30). Runs so far gave node 1 one or two wins.
        assert!(wins[0] <= 5, "wins per node: {wins:?}");
    }

    /// Enough planner work that node 1's plans clearly take longer than the others'.
    const SLOW_ITERATIONS: usize = 30_000;

    #[tokio::test]
    async fn invalid_request_starts_no_election() {
        let svc = service();
        let mut bad = incident_to_pb(&generate_incident(7));
        bad.severity = 0;
        let req = Request::new(SubmitRequest {
            request_id: "bad".into(),
            kind: Some(submit_request::Kind::Incident(bad)),
        });
        let err = svc.submit(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(svc.state.election.lock().unwrap().round("bad").is_none(), "no election for it");
    }
}
