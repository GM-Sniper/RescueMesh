//! Experiment runner (owner: P6): one command runs one experiment from a config file and puts
//! everything it produced in one folder.
//!
//!   cargo build --release
//!   cargo run --release -p loadgen --bin experiment -- experiments/ramp-local.toml
//!
//! Local mode (`start_nodes = true`): starts every node of the cluster file on this machine, runs the
//! load generator, stops the nodes, runs the stats script, and gathers everything in results/<run_id>/:
//!
//!   experiment.toml                  copy of the config that was used
//!   <run_id>.csv, <run_id>.meta.txt  the load generator's output
//!   summary.txt                      the stats script's output
//!   node<N>.log                      each node's console output
//!   logs/faults-node<N>.csv          each node's fault log (P5)
//!   logs/node_metrics-node<N>.csv    each node's load, once per second (P6)
//!
//! Lab mode (`start_nodes = false`): the nodes already run on the 3 machines. The runner tells you how
//! to start them with the right --run-id, waits for Enter, runs the load, and lists the node files to
//! copy into the run folder. Copying them automatically needs a shared folder or ssh: not done yet.

use std::env::consts::EXE_SUFFIX; // ".exe" on Windows, "" on Linux and macOS
use std::fs::{self, File};
use std::io::BufRead;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio}; // start the node, loadgen and stats programs
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use clap::Parser;
use common::config::ClusterConfig; // P1's config loader: the same node list the nodes and clients use
use serde::Deserialize;

/// Command line: just the experiment file.
#[derive(Parser)]
#[command(about = "Run one RescueMesh experiment from a config file and gather all its CSVs in one folder")]
struct Args {
    /// Experiment config (TOML), e.g. experiments/ramp-local.toml
    config: PathBuf,
}

/// One experiment, as written in the config file. See experiments/*.toml for commented examples.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)] // a typo in the file is an error, not a silently ignored setting
struct Experiment {
    name: String,          // the run is called <name>-<unix seconds>
    cluster: PathBuf,      // node addresses: config/nodes.toml (one laptop) or config/nodes.lab.toml
    #[serde(default)]
    start_nodes: bool,     // true = start every node on this machine; false = they already run (lab)
    #[serde(default = "default_target")]
    target: u32,           // which node the load generator sends to (until P1's multicast)
    #[serde(default = "default_seed")]
    seed: u64,             // incident seed; same seed = same incidents
    #[serde(default)]
    stages: Vec<u64>,      // ramp: clients per stage, e.g. [1, 2, 4, 8]; empty = fixed mode
    #[serde(default = "default_stage_secs")]
    stage_secs: f64,       // ramp: seconds per stage
    clients: Option<u64>,  // fixed mode only (loadgen's default if missing)
    requests: Option<u64>, // fixed mode only
    rate: Option<f64>,     // fixed mode only
}

fn default_target() -> u32 { 1 }
fn default_seed() -> u64 { 1 }
fn default_stage_secs() -> f64 { 30.0 }

/// The nodes this runner started. Dropping it stops them, so they never outlive the experiment,
/// even when a step fails half-way.
struct Nodes(Vec<Child>);

impl Drop for Nodes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill(); // already exited is fine
            let _ = child.wait(); // reap it, so no zombie process is left behind
        }
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let text = fs::read_to_string(&args.config).with_context(|| format!("can't read {}", args.config.display()))?;
    let exp: Experiment = toml::from_str(&text).with_context(|| format!("bad experiment file {}", args.config.display()))?;
    if !exp.stages.is_empty() && (exp.clients.is_some() || exp.requests.is_some() || exp.rate.is_some()) {
        bail!("use either `stages` (ramp) or `clients`/`requests`/`rate` (fixed), not both");
    }

    // The cluster file, as an absolute path: the nodes run inside the run folder, so relative paths would break.
    let cluster_path = std::path::absolute(&exp.cluster)?;
    let cluster = ClusterConfig::load(&cluster_path)?; // same checks as the nodes: REPLACE-ME addresses fail here
    cluster.node(exp.target).with_context(|| format!("target node {} is not in {}", exp.target, cluster_path.display()))?;

    // One folder per run: results/<name>-<unix seconds>/
    let run_id = format!("{}-{}", exp.name, unix_secs());
    let out = std::path::absolute(Path::new("results").join(&run_id))?;
    fs::create_dir_all(&out)?;
    fs::write(out.join("experiment.toml"), &text)?; // keep the exact config next to its results

    // The other programs are built next to this one (target/release or target/debug).
    let bin_dir = std::env::current_exe()?.parent().context("no folder for this program")?.to_path_buf();
    let bin = |name: &str| bin_dir.join(format!("{name}{EXE_SUFFIX}"));
    for name in ["node", "loadgen", "stats"] {
        if !bin(name).exists() {
            bail!("{} not found: run `cargo build --release` first (it builds every program)", bin(name).display());
        }
    }

    println!("experiment {run_id}: results go to {}", out.display());

    // 1. Nodes: start them here (local) or have the user start them (lab).
    let nodes = if exp.start_nodes {
        Some(start_nodes(&bin("node"), &cluster, &cluster_path, &run_id, &out)?)
    } else {
        wait_for_lab_nodes(&cluster, &cluster_path, &run_id)?;
        None
    };

    // 2. Load: the load generator writes <run_id>.csv and .meta.txt straight into the run folder.
    let mut load = Command::new(bin("loadgen"));
    load.arg("--config").arg(&cluster_path).arg("--out").arg(&out);
    load.args(["--target", &exp.target.to_string(), "--seed", &exp.seed.to_string(), "--run-id", &run_id]);
    if exp.stages.is_empty() {
        // Fixed mode: pass only what the file sets; loadgen's defaults cover the rest.
        if let Some(c) = exp.clients { load.args(["--clients", &c.to_string()]); }
        if let Some(r) = exp.requests { load.args(["--requests", &r.to_string()]); }
        if let Some(r) = exp.rate { load.args(["--rate", &r.to_string()]); }
    } else {
        let stages = exp.stages.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",");
        load.args(["--stages", &stages, "--stage-secs", &exp.stage_secs.to_string()]);
    }
    let load_status = load.status().context("could not start loadgen")?; // its progress shows in this terminal

    drop(nodes); // 3. Stop the nodes we started (no-op in lab mode), before anything else can fail
    if !load_status.success() {
        bail!("loadgen failed ({load_status}); see the messages above and the node<N>.log files in {}", out.display());
    }

    // 4. Stats: print them and keep a copy next to the CSV.
    let csv = out.join(format!("{run_id}.csv"));
    let stats = Command::new(bin("stats")).arg(&csv).output().context("could not start stats")?;
    if !stats.status.success() {
        bail!("stats failed: {}", String::from_utf8_lossy(&stats.stderr));
    }
    fs::write(out.join("summary.txt"), &stats.stdout)?;
    print!("{}", String::from_utf8_lossy(&stats.stdout));

    // 5. Tell the user where everything is.
    if exp.start_nodes {
        println!("all files are in {}", out.display());
    } else {
        println!("load results are in {}. Copy these from each node machine into it:", out.display());
        for node in &cluster.nodes {
            println!("  node {}: logs/faults-node{}.csv and logs/node_metrics-node{}.csv -> {}", node.id, node.id, node.id, out.join(format!("node{}", node.id)).display());
        }
    }
    Ok(())
}

/// Starts every node of the cluster file on this machine and waits until all of them accept connections.
fn start_nodes(node_bin: &Path, cluster: &ClusterConfig, cluster_path: &Path, run_id: &str, out: &Path) -> anyhow::Result<Nodes> {
    // A port that already answers belongs to some other program (an old node, the mock server):
    // the experiment would silently measure that instead of the nodes it starts.
    for node in &cluster.nodes {
        let addr: SocketAddr = node.addr.parse()?;
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            bail!("something is already listening on {addr} (node {}): stop it first", node.id);
        }
    }

    let mut nodes = Nodes(Vec::new());
    for node in &cluster.nodes {
        let log = File::create(out.join(format!("node{}.log", node.id)))?; // the node's console output
        let child = Command::new(node_bin)
            .args(["--id", &node.id.to_string(), "--run-id", run_id]) // run_id joins its fault log with the CSV
            .arg("--config").arg(cluster_path)
            .current_dir(out) // the node writes logs/... relative to here, so each run gets its own copies
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()
            .with_context(|| format!("could not start node {}", node.id))?;
        nodes.0.push(child); // pushed right away: if a later step fails, Drop still stops this node
    }

    for (child, node) in nodes.0.iter_mut().zip(&cluster.nodes) {
        let addr: SocketAddr = node.addr.parse()?;
        wait_until_listening(child, addr, Duration::from_secs(15))
            .with_context(|| format!("node {} did not start; see node{}.log in {}", node.id, node.id, out.display()))?;
    }
    println!("started {} nodes", cluster.nodes.len());
    Ok(nodes)
}

/// Polls `addr` until it accepts a connection, the node exits, or `limit` passes.
fn wait_until_listening(child: &mut Child, addr: SocketAddr, limit: Duration) -> anyhow::Result<()> {
    let start = Instant::now();
    loop {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return Ok(()); // the node's server is up
        }
        if let Some(status) = child.try_wait()? {
            bail!("it exited early ({status})"); // e.g. a bad config or a locked log file
        }
        if start.elapsed() > limit {
            bail!("nothing listening on {addr} after {limit:?}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Lab mode: print how to start each node with this run's --run-id, then wait for Enter.
fn wait_for_lab_nodes(cluster: &ClusterConfig, cluster_path: &Path, run_id: &str) -> anyhow::Result<()> {
    println!("lab mode: start each node on its own machine with");
    for node in &cluster.nodes {
        println!("  node {} ({}): cargo run --release -p node -- --id {} --config {} --run-id {run_id}", node.id, node.addr, node.id, cluster_path.display());
    }
    println!("then press Enter here to start the load");
    std::io::stdin().lock().read_line(&mut String::new())?;
    Ok(())
}

/// Current time in whole seconds since 1970, for the run name.
fn unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
