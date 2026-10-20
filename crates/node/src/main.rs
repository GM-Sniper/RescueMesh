//! RescueMesh cloud node. The same program runs on all 3 machines; only --id differs.
//! Run from the repo root:  cargo run -p node -- --id 1 [--config config/nodes.toml]

// P5's module, used through NodeState. Left unedited; some of its helpers
// (e.g. timeout()) are not called by anyone yet.
#[allow(dead_code)]
mod failure_detector;
// P5: fault CSV. Not called yet: P1 wires it into the mute hook and the detector ticker.
#[allow(dead_code)]
mod fault_log;
mod heartbeat;
mod load;
mod peers;
mod service;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use common::NodeId;
use common::config::ClusterConfig;
use common::pb::node_server::NodeServer;
use tracing_subscriber::EnvFilter;

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

    // 5. Shared state, given to the service and the background tasks.
    let state = Arc::new(NodeState::new(args.id, config, peers));

    // 6. Background tasks: CPU sampling for the load score (P3), heartbeats to
    //    each peer, and the detector ticker.
    load::spawn_sampler(state.clone());
    heartbeat::spawn_senders(state.clone());
    heartbeat::spawn_ticker(state.clone());

    // 7. Open the doors: serve the Node service until Ctrl+C.
    tracing::info!(id = args.id, %listen, peers = ?peer_ids, "node listening");
    tonic::transport::Server::builder()
        .add_service(NodeServer::new(NodeService::new(state)))
        .serve(listen)
        .await
        .with_context(|| format!("server on {listen} stopped"))?;
    Ok(())
}
