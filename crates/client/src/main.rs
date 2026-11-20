//! Client demo (owner: P6): sends a few incidents and prints each latency and winner,
//! then resends the first one to check that the nodes spot the duplicate.
//!
//! Run (with the nodes already running):
//!   cargo run -p client -- --count 5 --seed 1                               (multicast to the 3 nodes in config/nodes.toml)
//!   cargo run -p client -- --config config/nodes.lab.toml                   (multicast to the 3 real machines)
//!   cargo run -p client -- --target 2                                       (Step A style: node 2 only, one try)
//!   cargo run -p client -- --node 127.0.0.1:6000                            (Step A style: any address, e.g. the mock)
//! At the end it resolves every incident it sent, so their resources are free again; --no-resolve keeps them claimed.
//! --resolve-only (with the same --seed and --count) only resolves the incidents such a run sent.

use client::{ClientMiddleware, DEFAULT_TIMEOUT, RetryPolicy, SubmitOutcome}; // our library from src/lib.rs
use common::conversions::incident_to_pb; // P2: domain Incident -> proto Incident
use common::generators::generate_incident; // P2: same seed always gives the same incident

#[tokio::main] // starts the async runtime, then runs `main` inside it
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config: String = arg("--config", "config/nodes.toml".to_string()); // same file the nodes read
    let target: u32 = arg("--target", 0); // 0 = not given: multicast to every node
    let node_override: String = arg("--node", String::new()); // optional: any address; empty = not given
    let count: u64 = arg("--count", 5); // how many requests to send
    let seed: u64 = arg("--seed", 1); // first incident's seed; request i uses seed + i, so runs are repeatable

    let client = if target == 0 && node_override.is_empty() {
        // Step B: every node in the config; the first reply with a plan wins, retries keep the request_id.
        println!("multicast to every node in {config}");
        ClientMiddleware::connect_all(&config, RetryPolicy::default())?
    } else {
        // Step A style: one node, one try. --node wins over --target.
        let node_override = (!node_override.is_empty()).then_some(node_override.as_str());
        let node_addr = client::target_addr(&config, target.max(1), node_override)?;
        let client = ClientMiddleware::connect(&node_addr, DEFAULT_TIMEOUT).await?; // fails here if no node is listening
        println!("connected to {node_addr}");
        client
    };

    // --resolve-only: send nothing, only resolve the incidents a run with the same --seed and
    // --count sent (frees resources an older run, or one run with --no-resolve, left claimed).
    if std::env::args().any(|a| a == "--resolve-only") {
        let mut ok = 0;
        for i in 0..count {
            ok += client.resolve(generate_incident(seed + i).id).await.ok as usize;
        }
        println!("resolved {ok} of {count} incidents");
        return Ok(());
    }

    let keep = std::env::args().any(|a| a == "--no-resolve"); // leave the incidents' resources claimed
    let mut first = None; // remember the first request_id and incident for the duplicate test
    let mut sent = Vec::new(); // incident ids, resolved at the end
    for i in 0..count {
        let generated = generate_incident(seed + i); // a realistic random incident
        sent.push(generated.id);
        let incident = incident_to_pb(&generated); // as proto
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

    // Give the resources back, like a finished mission; otherwise they stay claimed for good.
    if !keep {
        let mut ok = 0;
        for &id in &sent {
            ok += client.resolve(id).await.ok as usize;
        }
        println!("resolved {ok} of {} incidents (their resources are free again)", sent.len());
    }
    Ok(())
}

/// Prints one line per request, e.g. "ok      3f2a... node 1   842 us  duplicate=false".
fn print_outcome(o: &SubmitOutcome) {
    let node = o.executor_node.map_or("-".to_string(), |n| n.to_string()); // "-" if the request failed
    println!(
        "{:<7} {} node {:<2} {:>7} us  duplicate={} attempts={} no-plan replies={}{}",
        o.status.as_str(),
        o.request_id,
        node, // multicast: the election winner
        o.latency_us,
        o.duplicate,
        o.attempts,
        o.no_plan_replies, // losers naming the winner

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
