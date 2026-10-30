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
//!   elections-summary.txt            election metrics and win share (from the nodes' election logs)
//!   reliability.txt                  success %, failed, retried, lost per scenario (no failure, node down, cut)
//!   logs/faults-runner.csv           network cuts the runner caused (`[[faults]] kind = "cut"`)
//!   node<N>.log                      each node's console output
//!   logs/faults-node<N>.csv          each node's fault log (P5)
//!   logs/node_metrics-node<N>.csv    each node's load, once per second (P6)
//!   logs/elections-node<N>.csv       each node's view of every election (P3)
//!
//! Lab mode (`start_nodes = false`): the nodes already run on the 3 machines. The runner tells you how
//! to start them with the right --run-id, waits for Enter, runs the load, and lists the node files to
//! copy into the run folder. Copying them automatically needs a shared folder or ssh: not done yet.

use std::collections::BTreeMap; // node ID -> planner iterations
use std::env::consts::EXE_SUFFIX; // ".exe" on Windows, "" on Linux and macOS
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, Write};
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
    target: Option<u32>,   // Step A style: send to this one node only; missing = multicast to every node
    attempts: Option<u32>, // multicast: tries per request (loadgen's default if missing)
    attempt_timeout_ms: Option<u64>, // multicast: how long one attempt waits for a plan
    #[serde(default)]
    planner_iterations: BTreeMap<String, usize>, // local mode: make some nodes slower, e.g. { "2" = 30000 } (P2's knob)
    #[serde(default = "default_true")]
    shared_state: bool,    // TEMPORARY until the DFS: all nodes share one set of resources (false = one private copy per node)
    shared_state_dir: Option<PathBuf>, // lab mode: a folder all 3 machines reach (network share); local mode uses one per run
    #[serde(default = "default_seed")]
    seed: u64,             // incident seed; same seed = same incidents
    #[serde(default)]
    stages: Vec<u64>,      // ramp: clients per stage, e.g. [1, 2, 4, 8]; empty = fixed mode
    #[serde(default = "default_stage_secs")]
    stage_secs: f64,       // ramp: seconds per stage
    clients: Option<u64>,  // fixed mode only (loadgen's default if missing)
    requests: Option<u64>, // fixed mode only
    rate: Option<f64>,     // fixed mode only
    #[serde(default)]
    faults: Vec<FaultStep>, // reliability scenarios: failures caused on a timer while the load runs
}

/// One failure to cause during the run, counted from when the load starts. In the file:
///   [[faults]]
///   kind = "mute"     # "mute" = one node down (P5's injector: it goes silent, then wakes itself)
///   node = 2          # "cut"  = network cut (the runner stops the node's program, then restarts it)
///   at_secs = 10
///   duration_secs = 8
#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct FaultStep {
    kind: FaultKind,
    node: u32,
    at_secs: f64,
    duration_secs: f64,
}

#[derive(Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
enum FaultKind {
    Mute, // the node hangs: calls to it get no answer until the deadline
    Cut,  // the node is gone: calls to it fail at once (connection refused)
}

fn default_seed() -> u64 { 1 }
fn default_true() -> bool { true }
fn default_stage_secs() -> f64 { 30.0 }

/// The nodes this runner started, with their IDs. Dropping it stops them, so they never outlive
/// the experiment, even when a step fails half-way.
struct Nodes(Vec<(u32, Child)>);

impl Drop for Nodes {
    fn drop(&mut self) {
        for (_, child) in &mut self.0 {
            let _ = child.kill(); // already exited is fine
            let _ = child.wait(); // reap it, so no zombie process is left behind
        }
    }
}

/// Everything needed to start one node, so a "cut" node can be started again the same way.
struct NodeLauncher<'a> {
    node_bin: PathBuf,
    cluster: &'a ClusterConfig,
    cluster_path: &'a Path,
    run_id: &'a str,
    out: &'a Path,
    planner_iterations: &'a BTreeMap<String, usize>,
    shared_dir: Option<&'a Path>,
}

impl NodeLauncher<'_> {
    /// Starts node `id` (its console output is appended to node<N>.log) without waiting for it.
    fn spawn(&self, id: u32) -> anyhow::Result<Child> {
        let log = OpenOptions::new().create(true).append(true).open(self.out.join(format!("node{id}.log")))?;
        let mut cmd = Command::new(&self.node_bin);
        cmd.args(["--id", &id.to_string(), "--run-id", self.run_id]) // run_id joins its logs with the CSV
            .arg("--config").arg(self.cluster_path);
        if let Some(dir) = self.shared_dir {
            cmd.arg("--shared-state-dir").arg(dir); // every node gets the SAME folder: one set of resources
        }
        if let Some(n) = self.planner_iterations.get(&id.to_string()) {
            cmd.args(["--planner-iterations", &n.to_string()]); // P2's knob: a slower node, for load-balancing tests
        }
        cmd.current_dir(self.out) // the node writes logs/... relative to here, so each run gets its own copies
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()
            .with_context(|| format!("could not start node {id}"))
    }

    /// Waits until node `id` accepts connections.
    fn wait_ready(&self, id: u32, child: &mut Child) -> anyhow::Result<()> {
        let addr: SocketAddr = self.cluster.node(id).context("unknown node")?.addr.parse()?;
        wait_until_listening(child, addr, Duration::from_secs(15))
            .with_context(|| format!("node {id} did not start; see node{id}.log in {}", self.out.display()))
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
    if let Some(target) = exp.target {
        cluster.node(target).with_context(|| format!("target node {target} is not in {}", cluster_path.display()))?;
    }
    for id in exp.planner_iterations.keys() {
        let known = id.parse::<u32>().ok().and_then(|n| cluster.node(n)).is_some();
        if !known {
            bail!("planner_iterations names node \"{id}\", which is not in {}", cluster_path.display());
        }
    }
    if !exp.planner_iterations.is_empty() && !exp.start_nodes {
        bail!("planner_iterations only works with start_nodes = true; in the lab, pass --planner-iterations to that node by hand");
    }
    for f in &exp.faults {
        cluster.node(f.node).with_context(|| format!("a fault names node {}, which is not in {}", f.node, cluster_path.display()))?;
        if f.at_secs < 0.0 || f.duration_secs <= 0.0 {
            bail!("fault on node {}: at_secs must be 0 or more and duration_secs more than 0", f.node);
        }
        if f.kind == FaultKind::Mute && f.duration_secs > 20.0 {
            bail!("fault on node {}: a mute lasts at most 20 s (the spec's limit, enforced by the injector)", f.node);
        }
        if f.kind == FaultKind::Cut && !exp.start_nodes {
            bail!("a \"cut\" stops and restarts the node's program, so it needs start_nodes = true");
        }
    }

    // One folder per run: results/<name>-<unix seconds>/
    let run_id = format!("{}-{}", exp.name, unix_secs());
    let out = std::path::absolute(Path::new("results").join(&run_id))?;
    fs::create_dir_all(&out)?;
    fs::write(out.join("experiment.toml"), &text)?; // keep the exact config next to its results

    // The other programs are built next to this one (target/release or target/debug).
    let bin_dir = std::env::current_exe()?.parent().context("no folder for this program")?.to_path_buf();
    let bin = |name: &str| bin_dir.join(format!("{name}{EXE_SUFFIX}"));
    for name in ["node", "loadgen", "stats", "elections", "reliability", "inject"] {
        if !bin(name).exists() {
            bail!("{} not found: run `cargo build --release` first (it builds every program)", bin(name).display());
        }
    }

    println!("experiment {run_id}: results go to {}", out.display());

    // 1. Nodes: start them here (local) or have the user start them (lab).
    // Shared state (TEMPORARY until the DFS): one folder for all nodes, so the cluster has ONE set of
    // resources. Local mode: a fresh folder inside this run's folder, so every run starts with all
    // resources free. Lab mode: the network folder from the file (it must be empty for a fresh start).
    let shared_dir: Option<PathBuf> = match (exp.shared_state, exp.start_nodes, &exp.shared_state_dir) {
        (false, _, _) => None,
        (true, true, _) => Some(out.join("shared-state")),
        (true, false, Some(dir)) => Some(dir.clone()),
        (true, false, None) => bail!(
            "lab mode needs `shared_state_dir` (a folder all 3 machines reach, e.g. a network share) \
             so the nodes share one set of resources; or set `shared_state = false`"
        ),
    };

    let launcher = NodeLauncher {
        node_bin: bin("node"),
        cluster: &cluster,
        cluster_path: &cluster_path,
        run_id: &run_id,
        out: &out,
        planner_iterations: &exp.planner_iterations,
        shared_dir: shared_dir.as_deref(),
    };
    let mut nodes = if exp.start_nodes {
        Some(start_nodes(&launcher)?)
    } else {
        wait_for_lab_nodes(&cluster, &cluster_path, &run_id, shared_dir.as_deref())?;
        None
    };

    // 2. Load: the load generator writes <run_id>.csv and .meta.txt straight into the run folder.
    let mut load = Command::new(bin("loadgen"));
    load.arg("--config").arg(&cluster_path).arg("--out").arg(&out);
    load.args(["--seed", &exp.seed.to_string(), "--run-id", &run_id]);
    if let Some(t) = exp.target { load.args(["--target", &t.to_string()]); } // one node; otherwise multicast
    if let Some(a) = exp.attempts { load.args(["--attempts", &a.to_string()]); }
    if let Some(ms) = exp.attempt_timeout_ms { load.args(["--attempt-timeout-ms", &ms.to_string()]); }
    if exp.stages.is_empty() {
        // Fixed mode: pass only what the file sets; loadgen's defaults cover the rest.
        if let Some(c) = exp.clients { load.args(["--clients", &c.to_string()]); }
        if let Some(r) = exp.requests { load.args(["--requests", &r.to_string()]); }
        if let Some(r) = exp.rate { load.args(["--rate", &r.to_string()]); }
    } else {
        let stages = exp.stages.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",");
        load.args(["--stages", &stages, "--stage-secs", &exp.stage_secs.to_string()]);
    }
    let mut load_child = load.spawn().context("could not start loadgen")?; // its progress shows in this terminal
    let t0 = Instant::now(); // fault times count from here

    // 2b. Faults (reliability scenarios), on schedule while the load runs.
    let fault_result = run_faults(&exp.faults, t0, &launcher, &mut nodes, &bin("inject"));
    let load_status = load_child.wait()?;
    fault_result?;

    // 3. Stop the nodes we started (no-op in lab mode). First wait a moment: a node writes each
    // election row one bid timeout (300 ms) after the decision, so stopping at once would lose
    // the last elections of the run (Farida's review of PR #24).
    if nodes.is_some() {
        std::thread::sleep(Duration::from_secs(1));
    }
    drop(nodes);
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

    // 4b. Election metrics (P3's logs), when the nodes wrote them here (local mode).
    let election_logs: Vec<PathBuf> = cluster.nodes.iter()
        .map(|n| out.join("logs").join(format!("elections-node{}.csv", n.id)))
        .filter(|p| p.exists())
        .collect();
    if !election_logs.is_empty() {
        let report = Command::new(bin("elections")).args(&election_logs).output().context("could not start elections")?;
        if !report.status.success() {
            bail!("elections failed: {}", String::from_utf8_lossy(&report.stderr));
        }
        fs::write(out.join("elections-summary.txt"), &report.stdout)?;
        print!("{}", String::from_utf8_lossy(&report.stdout));
    }

    // 4b2. Reliability per scenario, from the request CSV and every fault log in logs/ (local mode;
    //      in lab mode the nodes' fault logs are on the other machines, so only the CSV is used).
    let mut fault_logs: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = fs::read_dir(out.join("logs")) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("faults-") && name.ends_with(".csv") {
                fault_logs.push(entry.path()); // faults-node<N>.csv (P5) and faults-runner.csv (cuts)
            }
        }
    }
    fault_logs.sort();
    let mut reliability_cmd = Command::new(bin("reliability"));
    reliability_cmd.arg(&csv);
    if !fault_logs.is_empty() {
        reliability_cmd.arg("--faults").args(&fault_logs);
    }
    let reliability = reliability_cmd.output().context("could not start reliability")?;
    if !reliability.status.success() {
        bail!("reliability failed: {}", String::from_utf8_lossy(&reliability.stderr));
    }
    fs::write(out.join("reliability.txt"), &reliability.stdout)?;
    print!("{}", String::from_utf8_lossy(&reliability.stdout));

    // 4c. Safeguards: say so when this run's results can't be trusted, and keep the warnings in the folder.
    let csv_text = fs::read_to_string(&csv)?;
    let mut election_texts = Vec::new();
    for path in &election_logs {
        election_texts.push(fs::read_to_string(path)?);
    }
    let warnings = check_run(&csv_text, &election_texts);
    if !warnings.is_empty() {
        for w in &warnings {
            println!("WARNING: {w}");
        }
        fs::write(out.join("WARNINGS.txt"), warnings.join("\n") + "\n")?; // a later reader of the folder sees them too
    }

    // 5. Tell the user where everything is.
    if exp.start_nodes {
        println!("all files are in {}", out.display());
    } else {
        println!("load results are in {}. Copy these from each node machine into it:", out.display());
        for node in &cluster.nodes {
            println!("  node {}: logs/faults-node{}.csv, logs/node_metrics-node{}.csv, logs/elections-node{}.csv -> {}", node.id, node.id, node.id, node.id, out.join(format!("node{}", node.id)).display());
        }
    }
    Ok(())
}

/// Starts every node of the cluster file on this machine and waits until all of them accept connections.
fn start_nodes(launcher: &NodeLauncher) -> anyhow::Result<Nodes> {
    // A port that already answers belongs to some other program (an old node, the mock server):
    // the experiment would silently measure that instead of the nodes it starts.
    for node in &launcher.cluster.nodes {
        let addr: SocketAddr = node.addr.parse()?;
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            bail!("something is already listening on {addr} (node {}): stop it first", node.id);
        }
    }

    let mut nodes = Nodes(Vec::new());
    for node in &launcher.cluster.nodes {
        let _ = fs::remove_file(launcher.out.join(format!("node{}.log", node.id))); // a fresh log per run
        if let Some(n) = launcher.planner_iterations.get(&node.id.to_string()) {
            println!("node {} runs with --planner-iterations {n}", node.id);
        }
        let child = launcher.spawn(node.id)?;
        nodes.0.push((node.id, child)); // pushed right away: if a later step fails, Drop still stops this node
    }
    for (id, child) in &mut nodes.0 {
        launcher.wait_ready(*id, child)?;
    }
    println!("started {} nodes", launcher.cluster.nodes.len());
    Ok(nodes)
}

/// Causes the experiment's faults on schedule, counted from `t0` (when the load started).
/// - mute: runs P5's injector; the node logs MUTE_START / MUTE_END itself (logs/faults-node<N>.csv).
/// - cut: stops the node's program, then starts it again; the runner logs CUT_START / CUT_END to
///   logs/faults-runner.csv, in the same format as P5's fault log, so the reliability script reads both.
fn run_faults(faults: &[FaultStep], t0: Instant, launcher: &NodeLauncher, nodes: &mut Option<Nodes>, inject_bin: &Path) -> anyhow::Result<()> {
    if faults.is_empty() {
        return Ok(());
    }
    enum Action { Mute, CutStart, CutEnd }
    let mut timeline: Vec<(f64, Action, usize, &FaultStep)> = Vec::new(); // usize: the fault's number, so start and end share one fault_id
    for (i, f) in faults.iter().enumerate() {
        match f.kind {
            FaultKind::Mute => timeline.push((f.at_secs, Action::Mute, i, f)),
            FaultKind::Cut => {
                timeline.push((f.at_secs, Action::CutStart, i, f));
                timeline.push((f.at_secs + f.duration_secs, Action::CutEnd, i, f));
            }
        }
    }
    timeline.sort_by(|a, b| a.0.total_cmp(&b.0)); // run them in time order

    let log_path = launcher.out.join("logs").join("faults-runner.csv");
    fs::create_dir_all(log_path.parent().expect("logs folder"))?;
    let mut log = File::create(&log_path)?;
    writeln!(log, "run_id,node_id,event,fault_id,peer_id,duration_ms,wall_ts_us")?; // P5's fault-log columns

    for (at, action, n, f) in timeline {
        let due = t0 + Duration::from_secs_f64(at);
        std::thread::sleep(due.saturating_duration_since(Instant::now())); // wait until this step is due
        let duration_ms = (f.duration_secs * 1000.0) as u64;
        match action {
            Action::Mute => {
                println!("fault: muting node {} for {} s", f.node, f.duration_secs);
                // --wait 5s: a busy node can take more than the injector's default 2 s to ack.
                let status = Command::new(inject_bin)
                    .args(["--node", &f.node.to_string(), "--duration", &format!("{duration_ms}ms"), "--wait", "5s"])
                    .arg("--config").arg(launcher.cluster_path)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .context("could not start the injector")?;
                if !status.success() {
                    // No ack is not proof of no mute: the node logs MUTE_START itself, and the
                    // reliability script uses that log, not this message.
                    println!("note: the injector got no ack from node {} ({status}); check MUTE_START in logs/faults-node{}.csv", f.node, f.node);
                }
            }
            Action::CutStart => {
                println!("fault: cutting node {} off for {} s", f.node, f.duration_secs);
                let child = node_child(nodes, f.node)?;
                let _ = child.kill(); // its connections close at once: callers get "connection refused"
                let _ = child.wait();
                writeln!(log, "{},{},CUT_START,cut-{n},,{duration_ms},{}", launcher.run_id, f.node, wall_ts_us())?;
            }
            Action::CutEnd => {
                let mut child = launcher.spawn(f.node)?; // the same node again, with the same shared state
                launcher.wait_ready(f.node, &mut child)?;
                *node_child(nodes, f.node)? = child;
                writeln!(log, "{},{},CUT_END,cut-{n},,,{}", launcher.run_id, f.node, wall_ts_us())?;
                println!("fault: node {} is back", f.node);
            }
        }
        log.flush()?;
    }
    Ok(())
}

/// The running program of node `id`.
fn node_child(nodes: &mut Option<Nodes>, id: u32) -> anyhow::Result<&mut Child> {
    let nodes = nodes.as_mut().context("a cut needs start_nodes = true")?;
    nodes.0.iter_mut().find(|(n, _)| *n == id).map(|(_, c)| c).with_context(|| format!("node {id} was not started"))
}

/// Wall-clock microseconds since 1970, like P5's fault log, so the files can be joined by time.
fn wall_ts_us() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros()).unwrap_or(0)
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
fn wait_for_lab_nodes(cluster: &ClusterConfig, cluster_path: &Path, run_id: &str, shared_dir: Option<&Path>) -> anyhow::Result<()> {
    let shared = shared_dir.map_or(String::new(), |d| format!(" --shared-state-dir {}", d.display()));
    println!("lab mode: start each node on its own machine with");
    for node in &cluster.nodes {
        println!("  node {} ({}): cargo run --release -p node -- --id {} --config {} --run-id {run_id}{shared}", node.id, node.addr, node.id, cluster_path.display());
    }
    if let Some(dir) = shared_dir {
        println!("  (empty {} first, so the run starts with all resources free)", dir.display());
    }
    println!("then press Enter here to start the load");
    std::io::stdin().lock().read_line(&mut String::new())?;
    Ok(())
}

/// Checks that make a run's results untrustworthy. Returns one message per problem found (empty = OK).
/// `csv` is the load generator's CSV; `election_logs` are the nodes' election logs (empty in lab mode).
fn check_run(csv: &str, election_logs: &[String]) -> Vec<String> {
    let mut warnings = Vec::new();
    let sent = column_values(csv, "request_id"); // every request this run sent

    // A. Foreign traffic: the nodes handled requests this run never sent, so another client was
    //    sending to the same nodes at the same time. Its requests use up resources and mix into
    //    every number from the nodes (win share, election duration, load).
    let mut foreign = std::collections::BTreeSet::new();
    for log in election_logs {
        for id in column_values(log, "request_id") {
            if !sent.contains(&id) {
                foreign.insert(id);
            }
        }
    }
    if !foreign.is_empty() {
        warnings.push(format!(
            "the nodes handled {} requests that are not in this run's CSV: another client was sending to the \
             same nodes at the same time, so the results are mixed. Stop the other client and rerun.",
            foreign.len()
        ));
    }

    // B. Nothing assigned: requests got plans, but not one plan assigned a resource. Freshly started
    //    nodes always have free starting resources, so something used them up first (often foreign traffic).
    let statuses = column(csv, "status");
    let assigned = column(csv, "assigned");
    if let (Some(statuses), Some(assigned)) = (statuses, assigned) {
        let ok = statuses.iter().filter(|s| *s == "ok").count();
        let got_resource = assigned.iter().filter(|a| *a == "true").count();
        if ok > 0 && got_resource == 0 {
            warnings.push(format!(
                "none of the {ok} ok requests got a resource, but fresh nodes start with free resources. \
                 Check that nothing else was running against the nodes, then rerun."
            ));
        }
    }
    warnings
}

/// The values of one CSV column, by header name; None if the column is missing.
/// Our CSVs never have commas inside a field, so a plain split is enough.
fn column(text: &str, name: &str) -> Option<Vec<String>> {
    let mut lines = text.lines();
    let i = lines.next()?.split(',').position(|c| c == name)?;
    Some(lines.filter(|l| !l.trim().is_empty()).filter_map(|l| l.split(',').nth(i).map(str::to_string)).collect())
}

/// Like `column`, as a set (empty if the column is missing).
fn column_values(text: &str, name: &str) -> std::collections::BTreeSet<String> {
    column(text, name).unwrap_or_default().into_iter().collect()
}

/// Current time in whole seconds since 1970, for the run name.
fn unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CSV_HEADER: &str = "run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned,stage,attempts,no_plan_replies\n";
    const ELECTION_HEADER: &str = "run_id,node_id,request_id,round,bids,winner,duration_us,waited_full_t,late_bids,split_decision,bid_calls_sent,wall_ts_us\n";

    fn csv(rows: &[(&str, &str, &str)]) -> String {
        let mut text = CSV_HEADER.to_string();
        for (id, status, assigned) in rows {
            text += &format!("r,1,1,{id},1000,1,{status},{assigned},1,1,2\n");
        }
        text
    }

    fn election_log(ids: &[&str]) -> String {
        let mut text = ELECTION_HEADER.to_string();
        for id in ids {
            text += &format!("r,1,{id},0,1:0.1,1,100,false,0,0,2,1\n");
        }
        text
    }

    #[test]
    fn a_clean_run_has_no_warnings() {
        let run = csv(&[("a", "ok", "true"), ("b", "ok", "false")]);
        assert!(check_run(&run, &[election_log(&["a", "b"])]).is_empty());
    }

    #[test]
    fn requests_from_another_client_are_reported() {
        let run = csv(&[("a", "ok", "true")]);
        let warnings = check_run(&run, &[election_log(&["a", "x", "y"]), election_log(&["y"])]);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("2 requests"), "{warnings:?}"); // x and y, each counted once
    }

    #[test]
    fn no_resource_at_all_is_reported() {
        let run = csv(&[("a", "ok", "false"), ("b", "ok", "false")]);
        let warnings = check_run(&run, &[]);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("none of the 2 ok requests"), "{warnings:?}");
    }

    #[test]
    fn a_run_with_only_failures_is_not_called_unassigned() {
        let run = csv(&[("a", "timeout", ""), ("b", "error", "")]);
        assert!(check_run(&run, &[]).is_empty()); // nothing succeeded, so "no resource" says nothing
    }
}
