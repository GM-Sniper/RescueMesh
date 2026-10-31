//! Client middleware (owner: P6).
//!
//! `ClientMiddleware` sends one incident and measures how long the reply takes.
//! The demo (`src/main.rs`) and the load generator (`crates/loadgen`) both use it,
//! so every request in every experiment is sent and timed the same way.
//!
//! Step A: `connect` = one fixed node, no retries.
//! Step B (P1, agreed with P6): `connect_all` = every node in the config. Each request goes
//! to all of them at once, the first reply with a plan wins, and a request with no plan
//! before the attempt deadline is retried with the same request_id (`RetryPolicy`).

use std::sync::Arc; // shared ownership: the late-plan counter is shared with background tasks
use std::sync::atomic::{AtomicU64, Ordering}; // a counter many tasks can add to without a lock
use std::time::{Duration, Instant}; // Duration = a length of time; Instant = a stopwatch reading

use common::NodeId; // node IDs are u32; 0 means "not a node"
use common::pb::{self, node_client::NodeClient, submit_request}; // code generated from proto/rescuemesh.proto
use tokio::task::JoinSet; // a group of background tasks we can collect as each one finishes
use tonic::transport::{Channel, Endpoint}; // Endpoint = an address + settings; Channel = the open connection
use uuid::Uuid; // random unique IDs

/// The node a client talks to, read from the cluster config that the nodes use too
/// (`config/nodes.toml` on one laptop, `config/nodes.lab.toml` on the 3 real machines).
/// `override_addr` (the `--node` flag) wins if given, so a quick test can still name any address.
pub fn target_addr(config_path: &str, target: NodeId, override_addr: Option<&str>) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(addr) = override_addr {
        return Ok(addr.to_string()); // --node given: use it as is (e.g. the mock server on another port)
    }
    let config = common::config::ClusterConfig::load(config_path)?; // same loader and checks as the nodes (REPLACE-ME addresses are refused)
    let node = config.node(target).ok_or_else(|| format!("node {target} is not listed in {config_path}"))?;
    Ok(node.addr.clone()) // e.g. "10.0.0.12:50051"
}

/// How long to wait for a reply before giving up, if the caller doesn't choose.
/// A muted node hangs instead of answering, so without a limit the client would wait forever.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// When to give up on an attempt and retry (Step B). Final values: P1's design log.
#[derive(Debug, Clone, Copy)] // lets us print and copy it
pub struct RetryPolicy {
    pub attempt_timeout: Duration, // how long one attempt waits for a plan: the client's failure detection
    pub attempts: u32,             // tries in total, the first one included (1 = never retry)
}

impl Default for RetryPolicy {
    /// 2 s per attempt: longer than the nodes' suspicion time (1.5 s) + bid timeout (0.3 s)
    /// + planning, so when a winner dies, the retry arrives after the others treat it as down.
    fn default() -> Self {
        RetryPolicy { attempt_timeout: Duration::from_secs(2), attempts: 3 }
    }
}

/// How one request ended. Written to the `status` column of the CSV.
#[derive(Debug, Clone, Copy, PartialEq, Eq)] // lets us print, copy and compare it
pub enum SubmitStatus {
    Ok,      // the node replied with a plan
    Timeout, // no reply before the timeout
    Error,   // the node or network returned an error, or the reply was wrong
}

impl SubmitStatus {
    /// Short text for the CSV and logs: "ok", "timeout" or "error".
    pub fn as_str(self) -> &'static str {
        match self {
            SubmitStatus::Ok => "ok",
            SubmitStatus::Timeout => "timeout",
            SubmitStatus::Error => "error",
        }
    }
}

/// Everything we learned from one request. One of these becomes one CSV row on Monday.
#[derive(Debug, Clone)]
pub struct SubmitOutcome {
    pub request_id: String,            // the Uuid we generated for this request
    pub latency_us: u64,               // send-to-reply time in microseconds (1 ms = 1000 us)
    pub status: SubmitStatus,          // ok / timeout / error
    pub executor_node: Option<NodeId>, // which node ran the planner; None if the request failed
    pub plan: Option<pb::Plan>,        // the plan the node returned; None if the request failed
    pub duplicate: bool,               // true = the node had already seen this request_id and returned the stored plan
    pub error: Option<String>,         // why it failed; None if status is Ok
    pub attempts: u32,                 // tries this request used (1 = answered without a retry)
    pub no_plan_replies: u32,          // replies with no plan: nodes that lost the election, naming the winner
}

/// Connections to the nodes plus the rules for talking to them.
/// Cloning is cheap (the connections are shared), so many tasks can each hold a copy.
#[derive(Clone)]
pub struct ClientMiddleware {
    nodes: Vec<(NodeId, NodeClient<Channel>)>, // every node a request goes to, with its ID (0 = unknown)
    policy: RetryPolicy,                       // per-attempt time limit and number of attempts
    late_plans: Arc<AtomicU64>,                // plans that arrived after we already had one (see `late_plans`)
}

impl ClientMiddleware {
    /// Connects to the node at `addr` (for example "127.0.0.1:50051"): one node, no retries.
    /// Fails if the node can't be reached within `timeout`.
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Self, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))? // gRPC needs a URL, so add "http://"
            .connect_timeout(timeout); // don't wait forever if the node is down at startup
        let channel = endpoint.connect().await?; // open the connection now, so a wrong address fails here and not on the first request
        Ok(Self {
            nodes: vec![(0, NodeClient::new(channel))], // one node; its ID is not known here
            policy: RetryPolicy { attempt_timeout: timeout, attempts: 1 }, // Step A behaviour: one try
            late_plans: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Step B: every node in the cluster config (the same file the nodes read).
    /// Connects lazily (on the first request), so a node that is down at startup
    /// is just one failed call, not a client that cannot start. Call it inside the tokio runtime.
    pub fn connect_all(config_path: &str, policy: RetryPolicy) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = common::config::ClusterConfig::load(config_path)?; // same loader and checks as the nodes
        let nodes: Vec<(NodeId, &str)> = config.nodes.iter().map(|n| (n.id, n.addr.as_str())).collect();
        Ok(Self::for_nodes(&nodes, policy)?)
    }

    /// Like `connect_all`, from a list of (node ID, address). Used by `connect_all` and the tests.
    pub fn for_nodes(nodes: &[(NodeId, &str)], policy: RetryPolicy) -> Result<Self, tonic::transport::Error> {
        let mut clients = Vec::new();
        for &(id, addr) in nodes {
            let endpoint = Endpoint::from_shared(format!("http://{addr}"))? // gRPC needs a URL, so add "http://"
                .connect_timeout(policy.attempt_timeout); // a down node costs at most one attempt
            clients.push((id, NodeClient::new(endpoint.connect_lazy()))); // connects on first use
        }
        Ok(Self { nodes: clients, policy, late_plans: Arc::new(AtomicU64::new(0)) })
    }

    /// Plans that arrived after the request already had one: a split decision, or a slow
    /// winner finishing late. Counted for the whole run (the request's own outcome was
    /// already returned when they arrive).
    pub fn late_plans(&self) -> u64 {
        self.late_plans.load(Ordering::Relaxed) // Relaxed: a statistic, nothing else depends on its order
    }

    /// Sends `incident` with a brand-new request_id. Use this for every new request.
    pub async fn submit(&self, incident: pb::Incident) -> SubmitOutcome {
        let request_id = Uuid::new_v4().to_string(); // the client creates the ID, never the server
        self.submit_with_id(request_id, incident).await
    }

    /// Sends `incident` under an existing request_id. Use this only to retry a request:
    /// the node recognizes the ID and returns the stored plan (`duplicate = true`) instead of planning twice.
    ///
    /// Step B: the request goes to every node at once, and the first reply that has a plan
    /// is the answer (even an empty plan: "nothing was free"). A reply with no plan comes
    /// from a node that lost the election; it names the winner, and we keep waiting.
    ///
    /// Never panics or returns Err: failures come back as `status = Timeout` or `Error`,
    /// so a 1000-request run counts its failures instead of stopping at the first one.
    pub async fn submit_with_id(&self, request_id: String, incident: pb::Incident) -> SubmitOutcome {
        let message = pb::SubmitRequest {
            request_id: request_id.clone(),                              // keep a copy to check the reply against
            kind: Some(submit_request::Kind::Incident(incident)),        // the proto's `oneof kind`; Step A only sends incidents
        };
        let start = Instant::now(); // start the stopwatch right before the first send
        let mut no_plan_replies = 0; // losers' replies seen so far, all attempts together
        let mut silent = false; // a node gave no answer in the last attempt: decides Timeout vs Error
        let mut last_error = String::from("no node returned a plan"); // why it failed, if it does

        for attempt in 1..=self.policy.attempts {
            if attempt > 1 {
                // A short random pause (0-50 ms), so many clients retrying together don't all hit the nodes at once.
                let pause_ms = Uuid::new_v4().as_bytes()[0] as u64 % 51; // a random byte; avoids a new dependency
                tokio::time::sleep(Duration::from_millis(pause_ms)).await;
            }
            silent = false; // only the last attempt decides the final status
            let deadline = tokio::time::Instant::now() + self.policy.attempt_timeout; // one deadline for the whole attempt
            let mut calls = self.send_to_all(&message, deadline); // every node at once, so nobody waits on the slowest
            while let Some(joined) = calls.join_next().await { // answers in the order they arrive
                let Ok((node_id, result)) = joined else { continue }; // a call task panicked: nothing to read
                match result {
                    // This node gave no answer before the deadline (muted, crashed, or just slow).
                    Err(_elapsed) => {
                        silent = true;
                        last_error = format!("node {node_id}: no reply before the deadline");
                    }
                    // This node (or the network to it) answered with a gRPC error.
                    Ok(Err(status)) => last_error = format!("node {node_id}: {:?}: {}", status.code(), status.message()),
                    // This node answered normally.
                    Ok(Ok(response)) => {
                        let reply = response.into_inner(); // strip the gRPC wrapper, keep the SubmitReply
                        if reply.request_id != request_id {
                            // A reply for some other request would make the measurement meaningless.
                            last_error = format!("node {node_id}: reply is for request {}", reply.request_id);
                            continue;
                        }
                        let Some(plan) = reply.plan else {
                            no_plan_replies += 1; // a loser naming the winner: keep waiting for the winner's plan
                            continue;
                        };
                        // The first plan is the answer. Any later plan is counted, not used.
                        self.count_late_plans(calls);
                        return SubmitOutcome {
                            request_id,
                            latency_us: micros_since(start), // up to the reply WITH a plan (P6's Q2 rule)
                            status: SubmitStatus::Ok,
                            executor_node: Some(reply.executor_node), // the node that ran the planner
                            plan: Some(plan),
                            duplicate: reply.duplicate,
                            error: None,
                            attempts: attempt,
                            no_plan_replies,
                        };
                    }
                }
            }
            // No plan in this attempt. Wait out its deadline before retrying, even if every node
            // already answered: if the winner just died, the others still call it alive for up to
            // 1.5 s, and a retry before then would only hear "no plan" again.
            if attempt < self.policy.attempts {
                tokio::time::sleep_until(deadline).await;
            }
        }
        // No attempt brought a plan.
        let status = if silent { SubmitStatus::Timeout } else { SubmitStatus::Error };
        let mut outcome = SubmitOutcome::failed(request_id, micros_since(start), status, last_error);
        outcome.attempts = self.policy.attempts;
        outcome.no_plan_replies = no_plan_replies;
        outcome
    }

    /// Starts the same request on every node at once, each in its own task, all with one deadline.
    fn send_to_all(&self, message: &pb::SubmitRequest, deadline: tokio::time::Instant) -> JoinSet<Answer> {
        let mut calls = JoinSet::new();
        for (id, node) in &self.nodes {
            let (id, mut node, message) = (*id, node.clone(), message.clone()); // each task gets its own copies; the connection is shared
            calls.spawn(async move { (id, tokio::time::timeout_at(deadline, node.submit(message)).await) });
        }
        calls
    }

    /// Keeps listening to the calls still running (at most until their deadline) and counts
    /// any plan they bring, since this request already has one (see `late_plans`).
    fn count_late_plans(&self, mut calls: JoinSet<Answer>) {
        let late = self.late_plans.clone();
        tokio::spawn(async move {
            while let Some(joined) = calls.join_next().await {
                if let Ok((_, Ok(Ok(response)))) = joined
                    && response.get_ref().plan.is_some()
                {
                    late.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
}

/// One node's answer to one attempt: its ID, then either "deadline passed" or the gRPC result.
type Answer = (NodeId, Result<Result<tonic::Response<pb::SubmitReply>, tonic::Status>, tokio::time::error::Elapsed>);

/// Microseconds since `start` (1 ms = 1000 us). Instant ignores wall-clock changes.
fn micros_since(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}

impl SubmitOutcome {
    /// Did the plan actually assign a resource?
    /// - Some(true):  yes, at least one resource was assigned
    /// - Some(false): the node planned, but nothing was free (empty plan; P4: resubmit with a NEW request_id)
    /// - None:        no plan at all (the request failed, or in Step B this node wasn't the executor)
    pub fn assigned(&self) -> Option<bool> {
        self.plan.as_ref().map(|plan| !plan.assignments.is_empty())
    }

    /// How the request ended, for the CSV's `outcome` column (Step C, P6's reliability %):
    /// - "committed": a plan came back that was made and saved for this request
    /// - "duplicate": a plan came back, but it was already saved for this request_id. Usually a
    ///   retry found it, but with shared state (PR #33) a slower node can also answer the
    ///   first attempt from the store. Either way it is one saved plan, never two: a success
    /// - "failed":    no plan (timeout or error)
    pub fn outcome(&self) -> &'static str {
        match (self.status, self.duplicate) {
            (SubmitStatus::Ok, false) => "committed",
            (SubmitStatus::Ok, true) => "duplicate",
            _ => "failed",
        }
    }

    /// Builds the outcome for a request that got no usable reply.
    fn failed(request_id: String, latency_us: u64, status: SubmitStatus, error: String) -> Self {
        SubmitOutcome {
            request_id,
            latency_us, // still recorded: how long we waited before failing is useful in the plots
            status,
            executor_node: None,
            plan: None,
            duplicate: false,
            error: Some(error),
            attempts: 1,
            no_plan_replies: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU32;

    use common::pb::node_server::{Node, NodeServer};
    use tonic::transport::server::TcpIncoming;
    use tonic::{Request, Response, Status};

    use super::*;

    /// What a fake node does with the n-th Submit it receives (n = 1, 2, ...).
    #[derive(Clone, Copy, Debug)]
    enum Act {
        Plan { after_ms: u64, duplicate: bool }, // a winner: an (empty) plan after a delay
        NoPlan { winner: NodeId },               // a loser: no plan, names the winner
        Fail,                                    // an error at once
        Hang,                                    // never answers: a muted node, or a winner that died
    }

    /// A fake node: answers Submit as `act` says, and records every request_id it gets.
    struct Fake {
        id: NodeId,
        act: fn(u32) -> Act,
        calls: AtomicU32,
        seen: Arc<Mutex<Vec<String>>>,
    }

    #[tonic::async_trait]
    impl Node for Fake {
        async fn submit(&self, req: Request<pb::SubmitRequest>) -> Result<Response<pb::SubmitReply>, Status> {
            let request_id = req.into_inner().request_id;
            self.seen.lock().unwrap().push(request_id.clone());
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let reply = |plan, executor_node, duplicate| {
                Ok(Response::new(pb::SubmitReply { request_id: request_id.clone(), executor_node, plan, duplicate }))
            };
            match (self.act)(n) {
                Act::Plan { after_ms, duplicate } => {
                    tokio::time::sleep(Duration::from_millis(after_ms)).await;
                    reply(Some(pb::Plan::default()), self.id, duplicate)
                }
                Act::NoPlan { winner } => reply(None, winner, false),
                Act::Fail => Err(Status::unavailable("fake node fails")),
                Act::Hang => std::future::pending().await,
            }
        }

        async fn heartbeat(&self, _: Request<pb::HeartbeatRequest>) -> Result<Response<pb::HeartbeatReply>, Status> {
            Err(Status::unimplemented("fake"))
        }

        async fn inject_fault(&self, _: Request<pb::FaultCommand>) -> Result<Response<pb::FaultAck>, Status> {
            Err(Status::unimplemented("fake"))
        }

        async fn exchange_bid(&self, _: Request<pb::BidRequest>) -> Result<Response<pb::Bid>, Status> {
            Err(Status::unimplemented("fake"))
        }

        async fn announce_winner(&self, _: Request<pb::Coordinator>) -> Result<Response<pb::CoordinatorAck>, Status> {
            Err(Status::unimplemented("fake"))
        }
    }

    fn free_addr() -> String {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        format!("127.0.0.1:{port}")
    }

    /// Starts fake nodes 1, 2, 3... on localhost; returns their (id, address) and what each saw.
    async fn start(acts: &[fn(u32) -> Act]) -> (Vec<(NodeId, String)>, Vec<Arc<Mutex<Vec<String>>>>) {
        start_at(1, acts).await
    }

    /// Like `start`, numbering the fakes from `first_id`.
    async fn start_at(first_id: NodeId, acts: &[fn(u32) -> Act]) -> (Vec<(NodeId, String)>, Vec<Arc<Mutex<Vec<String>>>>) {
        let (mut nodes, mut seen) = (Vec::new(), Vec::new());
        for (i, &act) in acts.iter().enumerate() {
            let id = first_id + i as NodeId;
            let log = Arc::new(Mutex::new(Vec::new()));
            let fake = Fake { id, act, calls: AtomicU32::new(0), seen: log.clone() };
            // Listening before the server task starts: on Windows, connecting to a port that
            // is not listening yet takes ~2 s to fail, so polling for it would be slow.
            let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = incoming.local_addr().unwrap().to_string();
            tokio::spawn(tonic::transport::Server::builder().add_service(NodeServer::new(fake)).serve_with_incoming(incoming));
            nodes.push((id, addr));
            seen.push(log);
        }
        (nodes, seen)
    }

    fn client(nodes: &[(NodeId, String)], policy: RetryPolicy) -> ClientMiddleware {
        let list: Vec<(NodeId, &str)> = nodes.iter().map(|(id, addr)| (*id, addr.as_str())).collect();
        ClientMiddleware::for_nodes(&list, policy).unwrap()
    }

    /// Short times, so the tests run fast.
    const FAST: RetryPolicy = RetryPolicy { attempt_timeout: Duration::from_millis(300), attempts: 3 };

    #[tokio::test]
    async fn the_reply_with_a_plan_is_the_answer() {
        let (nodes, _) = start(&[
            |_| Act::Plan { after_ms: 0, duplicate: false },
            |_| Act::NoPlan { winner: 1 },
            |_| Act::NoPlan { winner: 1 },
        ])
        .await;
        let out = client(&nodes, FAST).submit(pb::Incident::default()).await;
        assert_eq!(out.status, SubmitStatus::Ok, "{out:?}");
        assert_eq!(out.executor_node, Some(1));
        assert_eq!(out.assigned(), Some(false), "an empty plan still counts as the plan");
        assert_eq!(out.attempts, 1);
    }

    // Step B plan, multicast check: the calls run at the same time, the first plan wins,
    // later plans are ignored (and counted), and nothing waits on the slowest node.
    #[tokio::test]
    async fn calls_run_in_parallel_and_the_first_plan_wins() {
        let (nodes, _) = start(&[
            |_| Act::Plan { after_ms: 1000, duplicate: false }, // slow
            |_| Act::Plan { after_ms: 20, duplicate: false },   // fast
            |_| Act::Hang,                                      // silent
        ])
        .await;
        let policy = RetryPolicy { attempt_timeout: Duration::from_secs(2), attempts: 1 };
        let client = client(&nodes, policy);
        let out = client.submit(pb::Incident::default()).await;
        assert_eq!(out.executor_node, Some(2), "{out:?}");
        assert!(out.latency_us < 300_000, "waited for a slower node: {} us", out.latency_us);

        tokio::time::sleep(Duration::from_millis(1200)).await; // the slow plan arrives meanwhile
        assert_eq!(client.late_plans(), 1, "the later plan is counted, not used");
    }

    #[tokio::test]
    async fn a_retry_sends_the_same_request_id_to_every_node() {
        let (nodes, seen) = start(&[
            |n| if n == 1 { Act::NoPlan { winner: 3 } } else { Act::Plan { after_ms: 0, duplicate: true } },
            |_| Act::NoPlan { winner: 3 },
            |_| Act::NoPlan { winner: 3 },
        ])
        .await;
        let out = client(&nodes, FAST).submit(pb::Incident::default()).await;
        assert_eq!(out.status, SubmitStatus::Ok, "{out:?}");
        assert_eq!(out.attempts, 2);
        for log in &seen {
            assert_eq!(*log.lock().unwrap(), vec![out.request_id.clone(); 2], "same ID, both attempts");
        }
    }

    #[tokio::test]
    async fn no_plan_in_any_attempt_is_a_timeout() {
        let (nodes, _) = start(&[|_| Act::Hang, |_| Act::Hang, |_| Act::Hang]).await;
        let started = Instant::now();
        let out = client(&nodes, FAST).submit(pb::Incident::default()).await;
        assert_eq!(out.status, SubmitStatus::Timeout, "{out:?}");
        assert_eq!(out.attempts, 3);
        assert!(started.elapsed() >= 3 * FAST.attempt_timeout, "gave up early: {:?}", started.elapsed());
    }

    #[tokio::test]
    async fn fast_failures_still_wait_out_each_attempt() {
        let (nodes, _) = start(&[|_| Act::Fail, |_| Act::Fail, |_| Act::Fail]).await;
        let started = Instant::now();
        let out = client(&nodes, FAST).submit(pb::Incident::default()).await;
        assert_eq!(out.status, SubmitStatus::Error, "every node answered, none with a plan: {out:?}");
        // Not 3 tries in a few ms: each retry waits for the attempt deadline first.
        assert!(started.elapsed() >= 2 * FAST.attempt_timeout, "retried too fast: {:?}", started.elapsed());
    }

    // ---- Step B plan: the three client failure cases. Each ends in a retry with the same
    // request_id that a node which is up answers with a plan.

    #[tokio::test]
    async fn network_cut_send_fails_and_the_retry_gets_the_stored_plan() {
        // Node 1 (the winner) is cut off: nothing answers at its address, so sending fails.
        // It had already committed; on the retry, node 2 finds that plan in the shared state.
        let (mut nodes, _) = start_at(2, &[
            |n| if n == 1 { Act::NoPlan { winner: 1 } } else { Act::Plan { after_ms: 0, duplicate: true } },
            |_| Act::NoPlan { winner: 1 },
        ])
        .await;
        nodes.insert(0, (1, free_addr()));
        let out = client(&nodes, FAST).submit(pb::Incident::default()).await;
        assert_eq!(out.status, SubmitStatus::Ok, "{out:?}");
        assert_eq!((out.attempts, out.executor_node, out.duplicate), (2, Some(2), true));
    }

    #[tokio::test]
    async fn winner_took_the_request_then_died_and_the_retry_is_re_elected() {
        // Node 1 won and went silent before replying. The others name it until they
        // suspect it; the retry's election skips it, and node 2 plans.
        let (nodes, seen) = start(&[
            |_| Act::Hang,
            |n| if n == 1 { Act::NoPlan { winner: 1 } } else { Act::Plan { after_ms: 0, duplicate: false } },
            |n| if n == 1 { Act::NoPlan { winner: 1 } } else { Act::NoPlan { winner: 2 } },
        ])
        .await;
        let out = client(&nodes, FAST).submit(pb::Incident::default()).await;
        assert_eq!(out.status, SubmitStatus::Ok, "{out:?}");
        assert_eq!((out.attempts, out.executor_node, out.duplicate), (2, Some(2), false));
        assert_eq!(seen[0].lock().unwrap().len(), 2, "the retry still goes to node 1 too");
    }

    #[tokio::test]
    async fn slow_request_hits_the_deadline_and_the_retry_gets_the_stored_plan() {
        // Node 1 wins but its plan takes longer than the attempt deadline. By the retry it
        // has committed, so it answers at once with the stored plan.
        let (nodes, _) = start(&[
            |n| if n == 1 { Act::Plan { after_ms: 500, duplicate: false } } else { Act::Plan { after_ms: 0, duplicate: true } },
            |_| Act::NoPlan { winner: 1 },
            |_| Act::NoPlan { winner: 1 },
        ])
        .await;
        let out = client(&nodes, FAST).submit(pb::Incident::default()).await;
        assert_eq!(out.status, SubmitStatus::Ok, "{out:?}");
        assert_eq!((out.attempts, out.executor_node, out.duplicate), (2, Some(1), true));
    }

    #[test]
    fn outcome_is_committed_duplicate_or_failed() {
        let mut out = SubmitOutcome::failed("r1".into(), 0, SubmitStatus::Timeout, "no reply".into());
        assert_eq!(out.outcome(), "failed");
        out.status = SubmitStatus::Error;
        assert_eq!(out.outcome(), "failed");
        out.status = SubmitStatus::Ok;
        assert_eq!(out.outcome(), "committed");
        out.duplicate = true;
        assert_eq!(out.outcome(), "duplicate", "a retry that found the stored plan");
    }
}
