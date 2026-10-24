//! Client middleware (owner: P6).
//!
//! `ClientMiddleware` sends one incident to one fixed node and measures how long
//! the reply takes. The demo (`src/main.rs`) and the load generator (`crates/loadgen`)
//! both use it, so every request in every experiment is sent and timed the same way.
//!
//! Step A: one fixed node, no node selection, no automatic retries.

use std::time::{Duration, Instant}; // Duration = a length of time; Instant = a stopwatch reading

use common::NodeId; // node IDs are u32; 0 means "not a node"
use common::pb::{self, node_client::NodeClient, submit_request}; // code generated from proto/rescuemesh.proto
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
}

/// A connection to one node plus the rules for talking to it.
/// Cloning is cheap (the connection is shared), so many tasks can each hold a copy.
#[derive(Clone)]
pub struct ClientMiddleware {
    node: NodeClient<Channel>, // generated gRPC client for the `Node` service
    timeout: Duration,         // per-request time limit
}

impl ClientMiddleware {
    /// Connects to the node at `addr` (for example "127.0.0.1:50051").
    /// Fails if the node can't be reached within `timeout`.
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Self, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))? // gRPC needs a URL, so add "http://"
            .connect_timeout(timeout); // don't wait forever if the node is down at startup
        let channel = endpoint.connect().await?; // open the connection now, so a wrong address fails here and not on the first request
        Ok(Self {
            node: NodeClient::new(channel), // wrap the connection in the generated client
            timeout,
        })
    }

    /// Sends `incident` with a brand-new request_id. Use this for every new request.
    pub async fn submit(&self, incident: pb::Incident) -> SubmitOutcome {
        let request_id = Uuid::new_v4().to_string(); // the client creates the ID, never the server
        self.submit_with_id(request_id, incident).await
    }

    /// Sends `incident` under an existing request_id. Use this only to retry a request:
    /// the node recognizes the ID and returns the stored plan (`duplicate = true`) instead of planning twice.
    ///
    /// Never panics or returns Err: failures come back as `status = Timeout` or `Error`,
    /// so a 1000-request run counts its failures instead of stopping at the first one.
    pub async fn submit_with_id(&self, request_id: String, incident: pb::Incident) -> SubmitOutcome {
        let message = pb::SubmitRequest {
            request_id: request_id.clone(),                              // keep a copy to check the reply against
            kind: Some(submit_request::Kind::Incident(incident)),        // the proto's `oneof kind`; Step A only sends incidents
        };
        let mut node = self.node.clone(); // generated calls need `&mut`; cloning shares the same connection, so it's cheap

        let start = Instant::now(); // start the stopwatch right before sending
        let result = tokio::time::timeout(self.timeout, node.submit(message)).await; // send and wait, but at most `timeout`
        let latency_us = start.elapsed().as_micros() as u64; // stop the stopwatch; Instant ignores wall-clock changes

        match result {
            // The time limit ran out before any reply arrived.
            Err(_elapsed) => SubmitOutcome::failed(request_id, latency_us, SubmitStatus::Timeout, "no reply before timeout".into()),

            // The node (or the network) answered with a gRPC error.
            Ok(Err(status)) => SubmitOutcome::failed(request_id, latency_us, SubmitStatus::Error, format!("{:?}: {}", status.code(), status.message())),

            // The node answered normally.
            Ok(Ok(response)) => {
                let reply = response.into_inner(); // strip the gRPC wrapper, keep the SubmitReply
                if reply.request_id != request_id {
                    // A reply for some other request would make the measurement meaningless.
                    return SubmitOutcome::failed(request_id, latency_us, SubmitStatus::Error, format!("reply is for request {}", reply.request_id));
                }
                SubmitOutcome {
                    request_id,
                    latency_us,
                    status: SubmitStatus::Ok,
                    executor_node: Some(reply.executor_node), // the node that ran the planner
                    plan: reply.plan,                         // already an Option in the generated code
                    duplicate: reply.duplicate,
                    error: None,
                }
            }
        }
    }
}

impl SubmitOutcome {
    /// Did the plan actually assign a resource?
    /// - Some(true):  yes, at least one resource was assigned
    /// - Some(false): the node planned, but nothing was free (empty plan; P4: resubmit with a NEW request_id)
    /// - None:        no plan at all (the request failed, or in Step B this node wasn't the executor)
    pub fn assigned(&self) -> Option<bool> {
        self.plan.as_ref().map(|plan| !plan.assignments.is_empty())
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
        }
    }
}
