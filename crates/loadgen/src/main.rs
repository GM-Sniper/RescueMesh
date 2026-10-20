//! Load generator (owner: P6): many fake clients send requests to one node at the same time,
//! and every result is saved as one CSV row for the stats script.
//!
//! Run (with a node or the mock server already running):
//!   cargo run -p loadgen -- --clients 10 --requests 1000 --seed 1 --rate 100
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
use client::{ClientMiddleware, DEFAULT_TIMEOUT, SubmitStatus}; // our client from crates/client
use common::conversions::incident_to_pb; // P2: domain Incident -> proto Incident
use common::generators::generate_incident; // P2: same seed always gives the same incident
use tokio::time::Instant; // stopwatch that works with tokio's sleep_until

/// Command-line flags. clap reads them, checks them and prints `--help` for free.
#[derive(Parser, Debug)]
#[command(about = "RescueMesh load generator: many clients, one CSV row per request")]
struct Args {
    /// Node to send to (Step A: one fixed node).
    #[arg(long, default_value = "127.0.0.1:50051")]
    node: String,

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
}

/// How many requests ended each way. Filled in by the CSV writer as it saves rows.
#[derive(Default)]
struct Counts {
    ok: u64,
    timeout: u64,
    error: u64,
}

#[tokio::main] // starts the async runtime, then runs `main` inside it
async fn main() -> anyhow::Result<()> {
    let args = Args::parse(); // read the flags (exits with a help message if they're wrong)
    let run_id = args.run_id.clone().unwrap_or_else(|| format!("run-{}", unix_ms() / 1000));
    fs::create_dir_all(&args.out)?; // make results/ if it doesn't exist yet
    let csv_path = args.out.join(format!("{run_id}.csv"));
    let meta_path = args.out.join(format!("{run_id}.meta.txt"));

    // Connect every client BEFORE the clock starts, so connection setup isn't counted as request latency.
    // Each client gets its own connection, like separate users would.
    let mut clients = Vec::new();
    for _ in 0..args.clients {
        clients.push(ClientMiddleware::connect(&args.node, DEFAULT_TIMEOUT).await?);
    }
    println!("{} clients connected to {}, sending {} requests...", args.clients, args.node, args.requests);

    // The CSV writer runs on its own thread, so slow disk writes never delay a request.
    let (tx, rx) = mpsc::channel::<Row>(); // tx = where tasks drop rows, rx = where the writer picks them up
    let writer = std::thread::spawn({
        let (csv_path, run_id) = (csv_path.clone(), run_id.clone()); // the thread needs its own copies
        move || write_csv(&csv_path, &run_id, rx)
    });

    let next = Arc::new(AtomicU64::new(0)); // number of the next request to send; shared by all clients
    let interval = (args.rate > 0.0).then(|| Duration::from_secs_f64(1.0 / args.rate)); // gap between requests, if --rate is set
    let started_at_ms = unix_ms(); // wall-clock start, for the metadata file
    let start = Instant::now(); // stopwatch for the whole run

    // Start one tokio task per client. They all run at the same time.
    let mut tasks = Vec::new();
    for (i, client) in clients.into_iter().enumerate() {
        let client_id = i as u64 + 1; // client IDs start at 1
        let (next, tx) = (next.clone(), tx.clone()); // each task gets its own handle to the counter and channel
        let (total, seed) = (args.requests, args.seed);
        tasks.push(tokio::spawn(async move {
            loop {
                let n = next.fetch_add(1, Ordering::Relaxed); // take the next request number (0, 1, 2, ...)
                if n >= total {
                    break; // all requests are taken: this client is done
                }
                if let Some(gap) = interval {
                    // Request n is due at start + n × gap, which spreads requests evenly at --rate per second.
                    // If the node is slower than that, requests start late and the achieved rate drops (shown at the end).
                    tokio::time::sleep_until(start + gap.mul_f64(n as f64)).await;
                }
                let incident = incident_to_pb(&generate_incident(seed + n)); // same seed + n = same incident every run
                let timestamp_ms = unix_ms(); // when this request was sent
                let outcome = client.submit(incident).await; // send, wait, time it (never panics)
                let row = Row {
                    timestamp_ms,
                    client_id,
                    request_id: outcome.request_id,
                    latency_us: outcome.latency_us,
                    executor_node: outcome.executor_node,
                    status: outcome.status,
                };
                if tx.send(row).is_err() {
                    break; // the writer stopped (disk error): no point sending more
                }
            }
        }));
    }
    drop(tx); // drop main's copy; the writer finishes once every task has dropped theirs too

    for task in tasks {
        task.await?; // wait for every client to finish
    }
    let elapsed = start.elapsed(); // total run time
    let counts = writer.join().expect("CSV writer thread panicked")?; // wait for the last rows to be saved

    // Summary. The stats script (Tue) computes the full numbers from the CSV; this is a quick check.
    let secs = elapsed.as_secs_f64();
    let throughput = counts.ok as f64 / secs; // successful requests per second
    println!(
        "done in {secs:.2} s: {} ok, {} timeout, {} error, {throughput:.1} ok/s",
        counts.ok, counts.timeout, counts.error
    );

    // The metadata file records how to repeat this exact run.
    let meta = format!(
        "run_id: {run_id}\n\
         started_at_ms: {started_at_ms}\n\
         command: {}\n\
         node: {}\nclients: {}\nrequests: {}\nseed: {}\nrate: {}\n\
         git_commit: {}\n\
         duration_s: {secs:.3}\n\
         ok: {}\ntimeout: {}\nerror: {}\n\
         throughput_ok_per_s: {throughput:.1}\n",
        std::env::args().collect::<Vec<_>>().join(" "), // the exact command that was typed
        args.node, args.clients, args.requests, args.seed, args.rate,
        git_commit(),
        counts.ok, counts.timeout, counts.error,
    );
    fs::write(&meta_path, meta)?;
    println!("wrote {} and {}", csv_path.display(), meta_path.display());
    Ok(())
}

/// The CSV writer: takes rows off the channel and writes them until every sender is gone.
fn write_csv(path: &Path, run_id: &str, rx: mpsc::Receiver<Row>) -> std::io::Result<Counts> {
    let mut w = BufWriter::new(File::create(path)?); // buffered, so we don't hit the disk on every row
    writeln!(w, "run_id,timestamp,client_id,request_id,latency_us,executor_node,status")?; // header row
    let mut counts = Counts::default();
    for row in rx {
        // This loop ends by itself once all tasks are done and the channel is closed.
        match row.status {
            SubmitStatus::Ok => counts.ok += 1,
            SubmitStatus::Timeout => counts.timeout += 1,
            SubmitStatus::Error => counts.error += 1,
        }
        let node = row.executor_node.map_or(String::new(), |n| n.to_string()); // empty cell if no node answered
        writeln!(
            w,
            "{run_id},{},{},{},{},{node},{}",
            row.timestamp_ms, row.client_id, row.request_id, row.latency_us, row.status.as_str()
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
