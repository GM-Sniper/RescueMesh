//! Client demo (owner: P6): sends a few incidents to one node and prints each latency,
//! then resends the first one to check that the node spots the duplicate.
//!
//! Run (with the mock server already running in another terminal):
//!   cargo run -p client -- --node 127.0.0.1:50051 --count 5 --seed 1

use client::{ClientMiddleware, DEFAULT_TIMEOUT, SubmitOutcome}; // our library from src/lib.rs
use common::conversions::incident_to_pb; // P2: domain Incident -> proto Incident
use common::generators::generate_incident; // P2: same seed always gives the same incident

#[tokio::main] // starts the async runtime, then runs `main` inside it
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_addr: String = arg("--node", "127.0.0.1:50051".to_string()); // the one fixed node (Step A)
    let count: u64 = arg("--count", 5); // how many requests to send
    let seed: u64 = arg("--seed", 1); // first incident's seed; request i uses seed + i, so runs are repeatable

    let client = ClientMiddleware::connect(&node_addr, DEFAULT_TIMEOUT).await?; // fails here if no node is listening
    println!("connected to {node_addr}");

    let mut first = None; // remember the first request_id and incident for the duplicate test
    for i in 0..count {
        let incident = incident_to_pb(&generate_incident(seed + i)); // a realistic random incident, as proto
        let outcome = client.submit(incident.clone()).await; // clone: we may resend the first one below
        print_outcome(&outcome);
        first.get_or_insert((outcome.request_id, incident)); // keeps only the first one
    }

    // Retry test: same request_id and incident again, so the node should answer with duplicate = true.
    if let Some((id, incident)) = first {
        println!("resending {id} to test duplicate detection:");
        let outcome = client.submit_with_id(id, incident).await;
        print_outcome(&outcome);
    }
    Ok(())
}

/// Prints one line per request, e.g. "ok      3f2a... node 1   842 us  duplicate=false".
fn print_outcome(o: &SubmitOutcome) {
    let node = o.executor_node.map_or("-".to_string(), |n| n.to_string()); // "-" if the request failed
    println!(
        "{:<7} {} node {:<2} {:>7} us  duplicate={}{}",
        o.status.as_str(),
        o.request_id,
        node,
        o.latency_us,
        o.duplicate,
        o.error.as_ref().map_or(String::new(), |e| format!("  error: {e}")), // only shown on failure
    );
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
