//! RescueMesh cloud node. The same program runs on all 3 machines; only --id differs.
//! Run from the repo root:  cargo run -p node -- --id 1 [--config config/nodes.toml]

// P5's module, used through NodeState. Left unedited; some of its helpers
// (e.g. timeout()) are not called by anyone yet.
#[allow(dead_code)]
mod failure_detector;
// Task 6. No caller in Step A: the election and replication use it in Step B.
#[allow(dead_code)]
mod broadcast;
// P3: election rule. No caller yet: the bid exchange is the next step.
#[allow(dead_code)]
mod election;
mod election_log;
mod fault_log;
// P5: automatic fault injector (Step B). Does nothing unless [faults] enabled = true.
mod fault_round;
mod heartbeat;
mod load;
// P6: NODE_METRICS LOG (ONE CSV ROW PER SECOND: QUEUE, CPU, SERVICE TIME, LOAD SCORE).
// NOTE FOR P1 / STEPA/P1: P6 ADDED ONLY THIS `mod` LINE AND THE `node_metrics::spawn` CALL IN STEP 6
// BELOW. NOTHING ELSE IN THIS FILE CHANGED. PLEASE KEEP BOTH LINES WHEN YOU MERGE MAIN INTO STEPA/P1.
mod node_metrics;
mod peers;
// P3: rejoin readiness rule after a mute (Step C).
mod rejoin;
mod service;
mod state;
// P4: in-memory implementation of the shared-state interface (common::shared_state).
mod store;
mod mute;
mod gated_store;
mod s3_store;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::Parser;
use common::NodeId;
use common::config::{ClusterConfig, StateBackend};
use common::pb::node_server::NodeServer;
use tracing_subscriber::EnvFilter;

use crate::election_log::ElectionLog;
use crate::fault_log::FaultLog;
use crate::peers::Peers;
use crate::service::NodeService;
use crate::state::NodeState;

/// RescueMesh cloud node.
#[derive(Parser)]
struct Args {
    /// This node's ID; must be listed in the config file.
    #[arg(long)]
    id: NodeId,
    /// Path to the cluster config file.
    #[arg(long, default_value = "config/nodes.toml")]
    config: String,
    /// Run name written in every fault-log row; pass loadgen's --run-id to join the files.
    /// Default: run-<unix seconds>, like loadgen.
    #[arg(long)]
    run_id: Option<String>,
    /// Extra planner work on this node only: replaces `[planner] iterations` from the
    /// shared config, to make it a slower machine in load-balancing tests. At the default
    /// sizes one iteration is about 0.7 us of CPU, so 30000 is about 20 ms per plan.
    #[arg(long)]
    planner_iterations: Option<usize>,
}

/// `--planner-iterations`: replaces the config's `[planner] iterations` for this node.
/// Refuses 0, like the config check does.
fn apply_planner_iterations(config: &mut ClusterConfig, iterations: Option<usize>) -> anyhow::Result<()> {
    if let Some(n) = iterations {
        anyhow::ensure!(n > 0, "--planner-iterations must be at least 1");
        config.planner.iterations = n;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Read --id and --config (clap prints a usage message if they are wrong).
    let args = Args::parse();

    // 2. Logging: "info" by default; RUST_LOG=debug shows every heartbeat.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // 3. Load and check the config (plus this node's own planner load), then find ourselves in it.
    let mut config = ClusterConfig::load(&args.config)?;
    apply_planner_iterations(&mut config, args.planner_iterations)?;
    let me = config
        .node(args.id)
        .with_context(|| format!("node {} is not listed in {}", args.id, args.config))?;
    // The config check already confirmed addr is ip:port; we only need our port.
    let port = me.addr.parse::<SocketAddr>()?.port();
    // 0.0.0.0 = accept connections from other machines, not just this one.
    let listen = SocketAddr::from(([0, 0, 0, 0], port));

    // 4. One lazy connection per peer (dials nothing yet).
    let peers = Peers::new(&config, args.id)?;
    let peer_ids: Vec<NodeId> = peers.ids().collect();

    // 5. Shared state, given to the service and the background tasks, with P5's
    //    fault CSV (appended to across restarts; /logs is git-ignored).
    let run_id = args.run_id.unwrap_or_else(|| {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        format!("run-{secs}")
    });
    let log_path = format!("logs/faults-node{}.csv", args.id);
    let fault_log = FaultLog::open(&log_path, args.id, &run_id)
        .with_context(|| format!("cannot open fault log {log_path}"))?;
    // P3: election CSV, same run_id so P6 can join it with the other logs.
    let election_log_path = format!("logs/elections-node{}.csv", args.id);
    let election_log = ElectionLog::open(&election_log_path, args.id, &run_id)
        .with_context(|| format!("cannot open election log {election_log_path}"))?;
    // P5, Step C: the shared-state backend. "memory" (default) keeps P4's per-node store;
    // "s3" connects to the SeaweedFS S3 gateway named in [shared_state] (see s3_store.rs).
    let backend = config.shared_state.clone();
    let mut node_state = NodeState::new(args.id, config, peers)
        .with_fault_log(fault_log)
        .with_election_log(election_log);
    if backend.backend == StateBackend::S3 {
        let s3 = s3_store::S3Store::connect(&backend, store::default_resources())
            .await
            .with_context(|| format!("cannot reach the shared state at {}/{}", backend.endpoint, backend.bucket))?;
        node_state = node_state.with_store(Arc::new(s3));
    }
    tracing::info!(backend = ?backend.backend, endpoint = %backend.endpoint, bucket = %backend.bucket, "shared state");
    let state = Arc::new(node_state);

    // 6. Background tasks: CPU sampling for the load score (P3), heartbeats to
    //    each peer, and the detector ticker.
    load::spawn_sampler(state.clone());
    heartbeat::spawn_senders(state.clone());
    heartbeat::spawn_ticker(state.clone());
    fault_round::spawn(state.clone());
    // P3: a (re)started node has missed what happened while it was down, like a muted one,
    // so it bids again only once the rejoin rule passes (rejoin.rs).
    state.set_ready(false);
    let rejoining = state.clone();
    tokio::spawn(async move { rejoin::rejoin(&rejoining, "startup").await });
    // P6 (SEE NOTE AT THE TOP, P1): ONE ROW PER SECOND TO logs/node_metrics-node<ID>.CSV
    node_metrics::spawn(state.clone(), format!("logs/node_metrics-node{}.csv", args.id))
        .context("cannot open the node_metrics log")?;

    // 7. Open the doors: serve the Node service until Ctrl+C.
    let planner_iterations = state.config.planner.iterations; // in the log, so every run says which node was loaded
    tracing::info!(id = args.id, %listen, peers = ?peer_ids, %run_id, fault_log = %log_path, planner_iterations, "node listening");
    tonic::transport::Server::builder()
        .add_service(NodeServer::new(NodeService::new(state)))
        .serve(listen)
        .await
        .with_context(|| format!("server on {listen} stopped"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_iterations_flag_replaces_the_config_value() {
        let mut cfg = ClusterConfig::parse(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:50052\"\n",
        )
        .unwrap();
        let shared = cfg.planner.iterations;
        apply_planner_iterations(&mut cfg, None).unwrap();
        assert_eq!(cfg.planner.iterations, shared, "no flag: the config's value stays");
        apply_planner_iterations(&mut cfg, Some(30_000)).unwrap();
        assert_eq!(cfg.planner.iterations, 30_000);
        assert!(apply_planner_iterations(&mut cfg, Some(0)).is_err(), "0 is refused");
    }
}
