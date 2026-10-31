//! Load generator (owner: P6): many fake clients send requests at the same time,
//! and every result is saved as one CSV row for the stats script.
//!
//! Step B (default): multicast. Every request goes to every node in the config file (the same file
//! the nodes use); the nodes elect who plans, the first reply with a plan wins, and a request with no
//! plan before the attempt deadline is retried with the same request_id (P1's ClientMiddleware).
//!   cargo run -p loadgen -- --clients 10 --requests 1000 --seed 1 --rate 100                     (3 nodes in config/nodes.toml)
//!   cargo run --release -p loadgen -- --config config/nodes.lab.toml --requests 1000            (the 3 real machines)
//!   cargo run -p loadgen -- --attempts 5 --attempt-timeout-ms 3000                               (retry settings)
//!
//! Step A style: one node only, one try per request, if --target or --node is given.
//!   cargo run -p loadgen -- --target 1 --requests 100                                            (node 1 in the config)
//!   cargo run -p loadgen -- --node 127.0.0.1:6000 --requests 100                                  (any address, e.g. the mock)
//!
//! Ramp mode (Step B): the load grows in stages, e.g. 1, 2, 4 then 8 clients for 30 s each.
//! Every row gets a `stage` column, so the stats script can show latency per stage:
//!   cargo run --release -p loadgen -- --stages 1,2,4,8 --stage-secs 30
//!
//! Output, in the `results/` folder:
//!   <run_id>.csv       one row per request
//!   <run_id>.meta.txt  how the run was made (flags, seed, git commit) and its totals

use std::fs::{self, File}; // create the results folder and files
use std::io::{BufWriter, Write}; // BufWriter collects writes in memory and saves them in big chunks (faster)
use std::path::{Path, PathBuf}; // file paths
use std::sync::Arc; // lets several tasks share one value
use std::sync::atomic::{AtomicU64, Ordering}; // a counter that many tasks can increase safely without a lock
use std::sync::mpsc; // a channel: tasks drop rows in one end, the CSV writer takes them out the other
use std::time::{Duration, SystemTime, UNIX_EPOCH}; // durations and the wall clock (for CSV timestamps)

use clap::Parser; // turns the `Args` struct below into command-line flags
use client::{ClientMiddleware, DEFAULT_TIMEOUT, RetryPolicy, SubmitStatus}; // our client from crates/client
use common::conversions::incident_to_pb; // P2: domain Incident -> proto Incident
use common::generators::generate_incident; // P2: same seed always gives the same incident
use tokio::time::Instant; // stopwatch that works with tokio's sleep_until

/// Command-line flags. clap reads them, checks them and prints `--help` for free.
#[derive(Parser, Debug)]
#[command(about = "RescueMesh load generator: many clients, one CSV row per request")]
struct Args {
    /// Cluster config with the nodes' addresses (the same file the nodes use).
    /// On the 3 real machines: config/nodes.lab.toml
    #[arg(long, default_value = "config/nodes.toml")]
    config: String,

    /// Step A style: send to this one node of the config only, one try per request.
    /// Without --target or --node, every request goes to every node (multicast).
    #[arg(long, conflicts_with = "node")]
    target: Option<u32>,

    /// Step A style: send to this one address only (e.g. the mock server: 127.0.0.1:6000).
    #[arg(long)]
    node: Option<String>,

    /// Multicast: tries per request, the first one included (1 = never retry).
    #[arg(long, default_value_t = RetryPolicy::default().attempts, value_parser = clap::value_parser!(u32).range(1..))]
    attempts: u32,

    /// Multicast: how long one attempt waits for a plan before retrying, in milliseconds.
    #[arg(long, default_value_t = RetryPolicy::default().attempt_timeout.as_millis() as u64)]
    attempt_timeout_ms: u64,

    /// How many fake clients send at the same time.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    clients: u64,

    /// Total number of requests, shared between all clients.
    #[arg(long, default_value_t = 1000)]
    requests: u64,

    /// Seed for P2's generator: request n sends generate_incident(seed + n). Same seed = same run.
    #[arg(long, default_value_t = 1)]
    seed: u64,

    /// Target requests per second across all clients. 0 = as fast as possible.
    #[arg(long, default_value_t = 0.0)]
    rate: f64,

    /// Ramp mode: number of clients in each stage, e.g. "1,2,4,8". Each stage lasts --stage-secs, and every
    /// client sends its next request as soon as the previous one returns. Replaces --clients, --requests and --rate.
    /// Without --stages, the run is a single stage of --clients clients sending --requests requests (Step A mode).
    #[arg(long, value_delimiter = ',', conflicts_with_all = ["clients", "requests", "rate"])]
    stages: Vec<u64>,

    /// Ramp mode: how long each stage lasts, in seconds.
    #[arg(long, default_value_t = 30.0)]
    stage_secs: f64,

    /// Folder for the CSV and metadata files.
    #[arg(long, default_value = "results")]
    out: PathBuf,

    /// Name for this run. Default: "run-" + the current unix time in seconds.
    #[arg(long)]
    run_id: Option<String>,
}

/// One finished request, on its way to the CSV writer.
struct Row {
    timestamp_ms: u64,          // wall-clock time the request was sent (unix ms)
    client_id: u64,             // which fake client sent it (1..=clients)
    request_id: String,         // the Uuid ClientMiddleware generated
    latency_us: u64,            // send-to-reply time measured by ClientMiddleware
    executor_node: Option<u32>, // node that answered; None if it failed
    status: SubmitStatus,       // ok / timeout / error
    assigned: Option<bool>,     // true = a resource was assigned, false = empty plan (nothing free), None = no plan
    stage: u32,                 // which stage sent it (1, 2, ...); always 1 without --stages
    attempts: u32,              // tries this request used (1 = no retry); multicast retries with the same request_id
    no_plan_replies: u32,       // replies with no plan: nodes that lost the election, naming the winner
    outcome: &'static str,      // committed / duplicate / failed (P1, Step C: for the reliability %)
}

/// Everything one client task needs. One per client per stage.
struct ClientJob {
    client: ClientMiddleware,   // this client's connection (cheap clone, shared with other stages)
    client_id: u64,             // 1..=clients in this stage
    stage: u32,                 // written into every row this client sends
    next: Arc<AtomicU64>,       // shared request counter: request n sends generate_incident(seed + n)
    tx: mpsc::Sender<Row>,      // where finished rows go (the CSV writer)
    seed: u64,
    max_requests: Option<u64>,  // fixed mode: stop once this many requests have been taken
    stage_end: Option<Instant>, // ramp mode: stop sending new requests at this time
    interval: Option<Duration>, // --rate: request n is due at start + n × interval
    start: Instant,             // when the run started (for --rate)
}

impl ClientJob {
    /// Sends requests until the job's limit (request count or stage end) is reached.
    async fn run(self) {
        loop {
            if self.stage_end.is_some_and(|end| Instant::now() >= end) {
                break; // ramp mode: this stage is over. A request already in flight still finishes and is recorded
            }
            let n = self.next.fetch_add(1, Ordering::Relaxed); // take the next request number (0, 1, 2, ...)
            if self.max_requests.is_some_and(|max| n >= max) {
                break; // fixed mode: all requests are taken, so this client is done
            }
            if let Some(gap) = self.interval {
                // Request n is due at start + n × gap, which spreads requests evenly at --rate per second.
                // If the node is slower than that, requests start late and the achieved rate drops (shown at the end).
                tokio::time::sleep_until(self.start + gap.mul_f64(n as f64)).await;
            }
            let incident = incident_to_pb(&generate_incident(self.seed + n)); // same seed + n = same incident every run
            let timestamp_ms = unix_ms(); // when this request was sent
            let outcome = self.client.submit(incident).await; // send, wait, time it (never panics)
            let assigned = outcome.assigned(); // read before outcome's fields are moved into the row
            let result = outcome.outcome(); // same: committed / duplicate / failed
            let row = Row {
                timestamp_ms,
                client_id: self.client_id,
                request_id: outcome.request_id,
                latency_us: outcome.latency_us,
                executor_node: outcome.executor_node, // multicast: the election winner
                status: outcome.status,
                assigned,
                stage: self.stage,
                attempts: outcome.attempts,
                no_plan_replies: outcome.no_plan_replies,
                outcome: result,
            };
            if self.tx.send(row).is_err() {
                break; // the writer stopped (disk error): no point sending more
            }
        }
    }
}

/// How many requests ended each way. Filled in by the CSV writer as it saves rows.
#[derive(Default)]
struct Counts {
    ok: u64,
    timeout: u64,
    error: u64,
    unassigned: u64, // ok requests whose plan was empty (nothing was free)
    retried: u64,    // requests that needed more than one attempt
}

#[tokio::main] // starts the async runtime, then runs `main` inside it
async fn main() -> anyhow::Result<()> {
    let args = Args::parse(); // read the flags (exits with a help message if they're wrong)
    let run_id = args.run_id.clone().unwrap_or_else(|| format!("run-{}", unix_ms() / 1000));
    fs::create_dir_all(&args.out)?; // make results/ if it doesn't exist yet
    let csv_path = args.out.join(format!("{run_id}.csv"));
    let meta_path = args.out.join(format!("{run_id}.meta.txt"));
    // Where requests go: one node if --target or --node is given (Step A style), else every node in --config.
    let single = args.target.is_some() || args.node.is_some();
    let policy = RetryPolicy { attempts: args.attempts, attempt_timeout: Duration::from_millis(args.attempt_timeout_ms) };
    let destination = if single {
        client::target_addr(&args.config, args.target.unwrap_or(1), args.node.as_deref()).map_err(|e| anyhow::anyhow!(e))?
    } else {
        format!("all nodes in {}", args.config)
    };

    // The stages to run. Ramp mode: one per --stages value. Fixed mode: a single stage of --clients clients.
    let ramp = !args.stages.is_empty();
    let stages: Vec<u64> = if ramp { args.stages.clone() } else { vec![args.clients] };
    if stages.contains(&0) {
        anyhow::bail!("every stage needs at least 1 client (got --stages {:?})", args.stages);
    }
    if ramp && args.stage_secs <= 0.0 {
        anyhow::bail!("--stage-secs must be more than 0");
    }
    let stage_len = Duration::from_secs_f64(args.stage_secs);
    let max_clients = *stages.iter().max().expect("at least one stage"); // the biggest stage sets how many connections we need

    // One ClientMiddleware per client, each with its own connections, like separate users;
    // a stage with k clients uses the first k.
    // - single node: connects now, so a wrong address fails here and setup isn't counted as latency;
    // - multicast: P1's connect_all connects lazily, on each client's first request, so a node that is
    //   down at startup costs one failed call instead of stopping the run. That first request per client
    //   includes connection setup (about a millisecond on a LAN).
    let mut clients = Vec::new();
    for _ in 0..max_clients {
        clients.push(if single {
            ClientMiddleware::connect(&destination, DEFAULT_TIMEOUT).await?
        } else {
            ClientMiddleware::connect_all(&args.config, policy).map_err(|e| anyhow::anyhow!(e))?
        });
    }
    let how = if single { format!("to {destination}, one try per request") } else {
        format!("multicast to {destination}, {} attempts of {} ms", policy.attempts, args.attempt_timeout_ms)
    };
    if ramp {
        println!("{max_clients} clients, {how}; ramp {stages:?} clients, {} s per stage", args.stage_secs);
    } else {
        println!("{max_clients} clients, {how}; sending {} requests...", args.requests);
    }

    // The CSV writer runs on its own thread, so slow disk writes never delay a request.
    let (tx, rx) = mpsc::channel::<Row>(); // tx = where tasks drop rows, rx = where the writer picks them up
    let writer = std::thread::spawn({
        let (csv_path, run_id) = (csv_path.clone(), run_id.clone()); // the thread needs its own copies
        move || write_csv(&csv_path, &run_id, rx)
    });

    let next = Arc::new(AtomicU64::new(0)); // number of the next request to send; shared by all clients and stages
    let interval = (args.rate > 0.0).then(|| Duration::from_secs_f64(1.0 / args.rate)); // gap between requests, if --rate is set
    let started_at_ms = unix_ms(); // wall-clock start, for the metadata file
    let start = Instant::now(); // stopwatch for the whole run

    // Run the stages one after another. Within a stage, one tokio task per client, all at the same time.
    for (s, &stage_clients) in stages.iter().enumerate() {
        let stage = s as u32 + 1; // stages are numbered from 1
        let stage_end = ramp.then(|| Instant::now() + stage_len); // ramp mode: when this stage stops sending
        if ramp {
            println!("stage {stage}/{}: {stage_clients} clients for {} s", stages.len(), args.stage_secs);
        }
        let mut tasks = Vec::new();
        for (i, client) in clients.iter().take(stage_clients as usize).enumerate() {
            let job = ClientJob {
                client: client.clone(),
                client_id: i as u64 + 1, // client IDs start at 1
                stage,
                next: next.clone(),
                tx: tx.clone(),
                seed: args.seed,
                max_requests: (!ramp).then_some(args.requests),
                stage_end,
                interval,
                start,
            };
            tasks.push(tokio::spawn(job.run()));
        }
        for task in tasks {
            task.await?; // the next stage starts only when every client of this one is done
        }
    }
    drop(tx); // drop main's copy; the writer finishes once every task has dropped theirs too
    let elapsed = start.elapsed(); // total run time
    let counts = writer.join().expect("CSV writer thread panicked")?; // wait for the last rows to be saved

    // Late plans: a second plan for a request that already had one (a split decision, or a slow winner).
    // P1's client counts them in the background until each attempt's deadline, so wait that long first.
    let late_plans = if single { 0 } else {
        tokio::time::sleep(policy.attempt_timeout).await;
        clients.iter().map(|c| c.late_plans()).sum::<u64>()
    };

    // Summary. The stats script computes the full numbers from the CSV; this is a quick check.
    let secs = elapsed.as_secs_f64();
    let throughput = counts.ok as f64 / secs; // successful requests per second
    println!(
        "done in {secs:.2} s: {} ok ({} with nothing assigned, {} retried), {} timeout, {} error, {late_plans} late plans, {throughput:.1} ok/s",
        counts.ok, counts.unassigned, counts.retried, counts.timeout, counts.error
    );

    // The metadata file records how to repeat this exact run.
    let meta = format!(
        "run_id: {run_id}\n\
         started_at_ms: {started_at_ms}\n\
         command: {}\n\
         mode: {}\nsend: {}\nnode: {}\nattempts: {}\nattempt_timeout_ms: {}\n\
         stages: {}\nstage_secs: {}\nclients: {}\nrequests: {}\nseed: {}\nrate: {}\n\
         git_commit: {}\n\
         duration_s: {secs:.3}\n\
         ok: {}\nunassigned: {}\nretried: {}\ntimeout: {}\nerror: {}\nlate_plans: {late_plans}\n\
         throughput_ok_per_s: {throughput:.1}\n",
        std::env::args().collect::<Vec<_>>().join(" "), // the exact command that was typed
        if ramp { "ramp" } else { "fixed" },
        if single { "single" } else { "multicast" },
        destination,
        if single { "1".to_string() } else { policy.attempts.to_string() },
        if single { DEFAULT_TIMEOUT.as_millis() as u64 } else { args.attempt_timeout_ms },
        stages.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(","), // clients per stage, e.g. "1,2,4,8"
        if ramp { args.stage_secs.to_string() } else { "-".to_string() },
        max_clients,
        if ramp { "-".to_string() } else { args.requests.to_string() }, // ramp mode runs by time, not by count
        args.seed, args.rate,
        git_commit(),
        counts.ok, counts.unassigned, counts.retried, counts.timeout, counts.error,
    );
    fs::write(&meta_path, meta)?;
    println!("wrote {} and {}", csv_path.display(), meta_path.display());
    Ok(())
}

/// The CSV writer: takes rows off the channel and writes them until every sender is gone.
fn write_csv(path: &Path, run_id: &str, rx: mpsc::Receiver<Row>) -> std::io::Result<Counts> {
    let mut w = BufWriter::new(File::create(path)?); // buffered, so we don't hit the disk on every row
    writeln!(w, "run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned,stage,attempts,no_plan_replies,outcome")?; // header row
    let mut counts = Counts::default();
    for row in rx {
        // This loop ends by itself once all tasks are done and the channel is closed.
        match row.status {
            SubmitStatus::Ok => counts.ok += 1,
            SubmitStatus::Timeout => counts.timeout += 1,
            SubmitStatus::Error => counts.error += 1,
        }
        if row.assigned == Some(false) {
            counts.unassigned += 1; // the node planned but nothing was free
        }
        if row.attempts > 1 {
            counts.retried += 1; // needed a retry (the winner died, the network cut, or the plan was slow)
        }
        let node = row.executor_node.map_or(String::new(), |n| n.to_string()); // empty cell if no node answered
        let assigned = row.assigned.map_or(String::new(), |a| a.to_string()); // "true", "false", or empty if no plan
        writeln!(
            w,
            "{run_id},{},{},{},{},{node},{},{assigned},{},{},{},{}",
            row.timestamp_ms, row.client_id, row.request_id, row.latency_us, row.status.as_str(), row.stage,
            row.attempts, row.no_plan_replies, row.outcome
        )?;
    }
    w.flush()?; // save whatever is still in the buffer
    Ok(counts)
}

/// Current wall-clock time in unix milliseconds. Used for timestamps only, never to measure latency.
fn unix_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// The current git commit (e.g. "b41acd2", or "b41acd2-dirty" if there are uncommitted changes).
fn git_commit() -> String {
    std::process::Command::new("git")
        .args(["describe", "--always", "--dirty"])
        .output()
        .ok()
        .filter(|out| out.status.success()) // git missing or not a repo -> "unknown"
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
