//! Manual fault injector (P5). Tells one node to go silent for a while.
//!
//!   cargo run -p injector -- --node 2 --duration 10s
//!
//! What it does: reads the node's address from the config file, sends it one
//! FaultCommand, waits for the FaultAck, prints the result and exits.
//!
//! What it does NOT do: tell the other nodes. They must notice the silence on
//! their own, through missing heartbeat replies (the failure detector).
//! The node, not this program, writes the fault times to its CSV, because only
//! the node knows when the silence really started and ended.

use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, bail};
use clap::Parser;
use common::NodeId;
use common::config::ClusterConfig;
use common::pb::FaultCommand;
use common::pb::node_client::NodeClient;
use tonic::transport::Endpoint;

/// Longest fault the spec allows ("up to 20 seconds").
const MAX_DURATION: Duration = Duration::from_secs(20);

/// Tell one RescueMesh node to go silent for a while.
#[derive(Parser)]
struct Args {
    /// Which node goes silent.
    #[arg(long)]
    node: NodeId,
    /// How long it stays silent: "10s", "500ms", or a plain number of seconds. At most 20s.
    #[arg(long, value_parser = parse_duration)]
    duration: Duration,
    /// Cluster config file (the same one the nodes use).
    #[arg(long, default_value = "config/nodes.toml")]
    config: String,
    /// How long to wait for the node's FaultAck before giving up, e.g. "2s".
    #[arg(long, default_value = "2s", value_parser = parse_duration)]
    wait: Duration,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    // 1. Check the request before sending anything.
    check_duration(args.duration)?;
    let config = ClusterConfig::load(&args.config)?;
    let target = config
        .node(args.node)
        .with_context(|| format!("node {} is not listed in {}", args.node, args.config))?;
    if args.duration < config.timing.suspect_after() {
        // Allowed on purpose (short faults are part of the experiments), but say so.
        println!(
            "note: {} ms is shorter than suspect_after_ms ({} ms); the other nodes will probably not notice",
            args.duration.as_millis(),
            config.timing.suspect_after_ms
        );
    }

    // 2. Build the order. A fresh random ID, so a retried order is recognised as the same one.
    let cmd = FaultCommand {
        fault_id: new_fault_id(),
        target_node: args.node,
        duration_ms: args.duration.as_millis() as u64,
        issuer_node: 0, // 0 = not a node: the manual CLI
        term: 0,        // no election in Step A
    };
    let fault_id = cmd.fault_id.clone();

    // 3. Connect. Fails fast if nothing is listening at that address.
    let addr = format!("http://{}", target.addr);
    let channel = Endpoint::from_shared(addr.clone())?
        .connect_timeout(config.timing.connect_timeout())
        .connect()
        .await
        .with_context(|| format!("could not connect to node {} at {}", args.node, target.addr))?;
    let mut client = NodeClient::new(channel);

    // 4. Send and wait at most `--wait` for the reply.
    //    A node that is already silent never answers, so without this we would hang forever.
    let reply = match tokio::time::timeout(args.wait, client.inject_fault(cmd)).await {
        Err(_) => bail!(
            "node {} did not answer within {} ms (is it already silent?). fault_id {fault_id}",
            args.node,
            args.wait.as_millis()
        ),
        Ok(Err(status)) if status.code() == tonic::Code::Unimplemented => bail!(
            "node {} does not handle InjectFault yet (P1 task 4: mute hook)",
            args.node
        ),
        Ok(Err(status)) => bail!("node {} returned an error: {status}", args.node),
        Ok(Ok(reply)) => reply.into_inner(),
    };

    // 5. Print what happened.
    if reply.fault_id != fault_id {
        bail!(
            "node {} answered a different order ({} instead of {fault_id})",
            args.node,
            reply.fault_id
        );
    }
    if reply.accepted {
        println!(
            "node {} accepted fault {fault_id}: silent for {} ms",
            args.node,
            args.duration.as_millis()
        );
        Ok(())
    } else {
        bail!("node {} refused fault {fault_id}: {}", args.node, reply.reason)
    }
}

/// "10s" -> 10 s, "500ms" -> 500 ms, "10" -> 10 s.
fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let bad = || format!("{text:?} is not a duration; use e.g. 10s, 500ms or 10");
    if let Some(ms) = text.strip_suffix("ms") {
        ms.trim().parse::<u64>().map(Duration::from_millis).map_err(|_| bad())
    } else if let Some(s) = text.strip_suffix('s') {
        s.trim().parse::<u64>().map(Duration::from_secs).map_err(|_| bad())
    } else {
        text.parse::<u64>().map(Duration::from_secs).map_err(|_| bad())
    }
}

/// Zero does nothing; over 20 s is outside what the spec allows.
fn check_duration(d: Duration) -> anyhow::Result<()> {
    if d.is_zero() {
        bail!("duration must be more than 0");
    }
    if d > MAX_DURATION {
        bail!("duration must be at most {} s", MAX_DURATION.as_secs());
    }
    Ok(())
}

/// A random UUID (version 4) as text, e.g. "3f2b6c1e-8a4d-4c2e-9b1a-0d5e7f3a2c11".
/// Built from `rand` (already in the workspace). Switch to the `uuid` crate once
/// P6's branch adds it to the workspace, so the whole project uses one generator.
fn new_fault_id() -> String {
    let mut b: [u8; 16] = rand::random();
    b[6] = (b[6] & 0x0f) | 0x40; // version 4 = random
    b[8] = (b[8] & 0x3f) | 0x80; // standard variant
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("7").unwrap(), Duration::from_secs(7));
        assert!(parse_duration("ten").is_err());
        assert!(parse_duration("-1s").is_err());
    }

    #[test]
    fn rejects_zero_and_over_20s() {
        assert!(check_duration(Duration::ZERO).is_err());
        assert!(check_duration(Duration::from_secs(21)).is_err());
        assert!(check_duration(Duration::from_secs(20)).is_ok());
        assert!(check_duration(Duration::from_millis(1)).is_ok());
    }

    #[test]
    fn fault_ids_look_like_uuids_and_differ() {
        let a = new_fault_id();
        let b = new_fault_id();
        assert_eq!(a.len(), 36);
        assert_eq!(a.chars().filter(|c| *c == '-').count(), 4);
        assert_eq!(&a[14..15], "4"); // version 4
        assert_ne!(a, b);
    }
}