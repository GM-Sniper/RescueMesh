//! Mock node (owner: P6): a fake server for testing the client until P1's real node
//! can answer `Submit` (Tue Oct 20). It checks the incident like the real node will,
//! but plans nothing: it echoes the request_id back with a plan that assigns no resources.
//!
//! Run:  cargo run -p client --bin mock_server -- --port 50051 --id 1 --delay-ms 2

use std::collections::HashSet; // a set of strings, for remembering request_ids
use std::net::SocketAddr; // an IP address + port to listen on
use std::sync::Mutex; // lets several requests safely share the `seen` set
use std::time::Duration;

use common::NodeId;
use common::conversions::incident_from_pb; // P2: proto Incident -> domain Incident, with validation
use common::pb::{self, node_server::{Node, NodeServer}, submit_request}; // `Node` = the trait a server must implement
use tonic::{Request, Response, Status, transport::Server}; // gRPC request/reply wrappers and the server itself

/// The fake node's state.
struct MockNode {
    id: NodeId,                 // reported as `executor_node` in every reply
    delay: Duration,            // pretend planning takes this long, so latencies aren't all ~0
    seen: Mutex<HashSet<String>>, // request_ids already answered, to test duplicates like the real store will
}

#[tonic::async_trait] // needed because the generated `Node` trait has async methods
impl Node for MockNode {
    /// The only RPC the mock really implements.
    async fn submit(&self, request: Request<pb::SubmitRequest>) -> Result<Response<pb::SubmitReply>, Status> {
        let req = request.into_inner(); // strip the gRPC wrapper, keep the SubmitRequest

        if req.request_id.is_empty() {
            return Err(Status::invalid_argument("request_id is empty")); // every request must carry an ID
        }
        let Some(submit_request::Kind::Incident(incident)) = req.kind else {
            return Err(Status::invalid_argument("expected an incident")); // Step A only accepts incidents
        };
        // Same check the real node will do: P2's converter rejects bad incidents
        // (no location, severity outside 1..10, category UNSPECIFIED, ...).
        let incident = incident_from_pb(incident).map_err(Status::invalid_argument)?;

        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await; // fake work; `.await` lets other requests run meanwhile
        }

        // `insert` returns false if the ID was already in the set, so that's a retry.
        let duplicate = !self.seen.lock().unwrap().insert(req.request_id.clone());
        println!("node {} answered {} for incident {} (duplicate: {duplicate})", self.id, req.request_id, incident.id);

        Ok(Response::new(pb::SubmitReply {
            request_id: req.request_id, // echo the ID so the client can match reply to request
            executor_node: self.id,     // "I ran the planner"
            plan: Some(pb::Plan {
                incident_id: incident.id, // which incident this plan is for
                priority: 0.0,            // the mock doesn't run P2's planner, so no real priority
                assignments: vec![],      // ...and no resources assigned
            }),
            duplicate,
        }))
    }

    /// Not needed by the client; heartbeats are P1's.
    async fn heartbeat(&self, _request: Request<pb::HeartbeatRequest>) -> Result<Response<pb::HeartbeatReply>, Status> {
        Err(Status::unimplemented("mock server has no heartbeat"))
    }

    /// Not needed by the client; fault injection is P5's.
    async fn inject_fault(&self, _request: Request<pb::FaultCommand>) -> Result<Response<pb::FaultAck>, Status> {
        Err(Status::unimplemented("mock server has no fault injection"))
    }

    /// Not needed by the client; the election is P3's.
    async fn exchange_bid(&self, _request: Request<pb::BidRequest>) -> Result<Response<pb::Bid>, Status> {
        Err(Status::unimplemented("mock server has no election"))
    }

    /// Not needed by the client; the election is P3's.
    async fn announce_winner(&self, _request: Request<pb::Coordinator>) -> Result<Response<pb::CoordinatorAck>, Status> {
        Err(Status::unimplemented("mock server has no election"))
    }
}

#[tokio::main] // starts the async runtime, then runs `main` inside it
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = arg("--port", 50051); // which port to listen on
    let id: NodeId = arg("--id", 1); // which node this pretends to be
    let delay_ms: u64 = arg("--delay-ms", 0); // fake planning time per request

    // 127.0.0.1 = this laptop only. The real node binds 0.0.0.0 to accept other machines.
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let node = MockNode {
        id,
        delay: Duration::from_millis(delay_ms),
        seen: Mutex::new(HashSet::new()),
    };

    println!("mock node {id} listening on {addr} (delay {delay_ms} ms)");
    Server::builder()
        .add_service(NodeServer::new(node)) // plug our `Node` implementation into the gRPC server
        .serve(addr)                        // listen until the program is stopped (Ctrl+C)
        .await?;
    Ok(())
}

/// Reads `--name value` from the command line, or returns `default` if it's missing.
fn arg<T: std::str::FromStr>(name: &str, default: T) -> T {
    let args: Vec<String> = std::env::args().collect(); // every word typed after the program name
    args.iter()
        .position(|a| a == name)                // where is "--name"?
        .and_then(|i| args.get(i + 1))          // the word right after it is the value
        .map(|v| v.parse().unwrap_or_else(|_| panic!("bad value for {name}: {v}")))
        .unwrap_or(default)
}
