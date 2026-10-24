//! RescueMesh cloud node. The same program runs on all 3 machines; only --id differs.
//! Run from the repo root:  cargo run -p node -- --id 1 [--config config/nodes.toml]

// P5's module, used through NodeState. Left unedited; some of its helpers
// (e.g. timeout()) are not called by anyone yet.
#[allow(dead_code)]
mod failure_detector;
// Task 6. No caller in Step A: the election and replication use it in Step B.
#[allow(dead_code)]
mod broadcast;
mod fault_log;
mod heartbeat;
mod load;
// P6: NODE_METRICS LOG (ONE CSV ROW PER SECOND: QUEUE, CPU, SERVICE TIME, LOAD SCORE).
// NOTE FOR P1 / STEPA/P1: P6 ADDED ONLY THIS `mod` LINE AND THE `node_metrics::spawn` CALL IN STEP 6
// BELOW. NOTHING ELSE IN THIS FILE CHANGED. PLEASE KEEP BOTH LINES WHEN YOU MERGE MAIN INTO STEPA/P1.
mod node_metrics;
mod peers;
mod service;
mod state;
// P4: store and change log. Used by the Submit handler; the replication functions
// (apply_remote, entries_since, applied) are not called until Step B.
#[allow(dead_code)]
mod store;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::Parser;
use common::NodeId;
use common::config::ClusterConfig;
use common::pb::node_server::NodeServer;
use tracing_subscriber::EnvFilter;

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

    // 3. Load and check the config, then find ourselves in it.
    let config = ClusterConfig::load(&args.config)?;
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
    let state = Arc::new(NodeState::new(args.id, config, peers).with_fault_log(fault_log));

    // 6. Background tasks: CPU sampling for the load score (P3), heartbeats to
    //    each peer, and the detector ticker.
    load::spawn_sampler(state.clone());
    heartbeat::spawn_senders(state.clone());
    heartbeat::spawn_ticker(state.clone());
    // P6 (SEE NOTE AT THE TOP, P1): ONE ROW PER SECOND TO logs/node_metrics-node<ID>.CSV
    node_metrics::spawn(state.clone(), format!("logs/node_metrics-node{}.csv", args.id))
        .context("cannot open the node_metrics log")?;

    // 7. Open the doors: serve the Node service until Ctrl+C.
    tracing::info!(id = args.id, %listen, peers = ?peer_ids, %run_id, fault_log = %log_path, "node listening");
    tonic::transport::Server::builder()
        .add_service(NodeServer::new(NodeService::new(state)))
        .serve(listen)
        .await
        .with_context(|| format!("server on {listen} stopped"))?;
    Ok(())
}
