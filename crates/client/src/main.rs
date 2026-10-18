//! Client demo (owner: P6): sends a few incidents to one node and prints each latency,
//! then resends the first one to check that the node spots the duplicate.
//!
//! Run (with the mock server already running in another terminal):
//!   cargo run -p client -- --node 127.0.0.1:50051 --count 5

use client::{ClientMiddleware, DEFAULT_TIMEOUT, SubmitOutcome}; // our library from src/lib.rs
use common::pb;

#[tokio::main] // starts the async runtime, then runs `main` inside it
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_addr: String = arg("--node", "127.0.0.1:50051".to_string()); // the one fixed node (Step A)
    let count: u32 = arg("--count", 5); // how many requests to send

    let client = ClientMiddleware::connect(&node_addr, DEFAULT_TIMEOUT).await?; // fails here if no node is listening
    println!("connected to {node_addr}");

    let mut first_id = None; // remember the first request_id for the duplicate test
    for _ in 0..count {
        let outcome = client.submit(pb::Incident {}).await; // empty incident until P2 adds Incident's fields
        print_outcome(&outcome);
        first_id.get_or_insert(outcome.request_id); // keeps only the first ID
    }

    // Retry test: same request_id again, so the node should answer with duplicate = true.
    if let Some(id) = first_id {
        println!("resending {id} to test duplicate detection:");
        let outcome = client.submit_with_id(id, pb::Incident {}).await;
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
