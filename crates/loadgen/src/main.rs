// Load generator (owner: P6): many fake clients send requests at the same time,
// and every result is saved as one CSV row for the stats script.
//
// Step B (default): multicast. Every request goes to every node in the config file (the same file
// the nodes use); the nodes elect who plans, the first reply with a plan wins, and a request with no
// plan before the attempt deadline is retried with the same request_id (P1's ClientMiddleware).
//   cargo run -p loadgen -- --clients 10 --requests 1000 --seed 1 --rate 100
//   cargo run --release -p loadgen -- --config config/nodes.lab.toml --requests 1000
//   cargo run -p loadgen -- --attempts 5 --attempt-timeout-ms 3000
//
// Step A style: one node only, one try per request, if --target or --node is given.
//   cargo run -p loadgen -- --target 1 --requests 100
//   cargo run -p loadgen -- --node 127.0.0.1:6000 --requests 100
//
// Ramp mode (Step B): the load grows in stages, e.g. 1, 2, 4 then 8 clients for 30 s each.
// Every row gets a `stage` column, so the stats script can show latency per stage:
//   cargo run --release -p loadgen -- --stages 1,2,4,8 --stage-secs 30
//
// Output, in the `results/` folder:
//   <run_id>.csv       one row per request
//   <run_id>.meta.txt  how the run was made (flags, seed, git commit) and its totals

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use client::{ClientMiddleware, DEFAULT_TIMEOUT, RetryPolicy, SubmitStatus};
use common::conversions::incident_to_pb;
use common::generators::generate_incident;
use tokio::time::Instant;

/// Command-line flags. clap reads them, checks them and prints `--help` for free.
#[derive(Parser, Debug)]
#[command(about = "RescueMesh load generator: many clients, one row per request")]
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

    /// Resolve assigned incidents after a deterministic mission delay (milliseconds).
    /// 0 = resolution disabled. Otherwise, delays range from 1 to this value.
    #[arg(long, default_value_t = 0)]
    resolve_after_ms: u64,

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
/// One finished request, on its way to the CSV writer.
struct Row {
    /// Wall-clock time when the request was sent, in Unix milliseconds.
    timestamp_ms: u64,
    /// ID of the simulated client that sent the request.
    client_id: u64,
    /// Unique request ID, reused across retries.
    request_id: String,
    /// Time taken by the submit operation, in microseconds.
    latency_us: u64,
    /// Node that executed the plan, if one was returned.
    executor_node: Option<u32>,
    /// Final status returned by the client.
    status: SubmitStatus,
    /// Whether the returned plan assigned a resource.
    assigned: Option<bool>,
    /// Ramp-test stage number (1 for a fixed run).
    stage: u32,
    /// Number of attempts used for this request.
    attempts: u32,
    /// Number of replies that did not contain a plan.
    no_plan_replies: u32,
    /// Short result label used by the stats script.
    outcome: &'static str,
}

/// Everything one client task needs. One per client per stage.
struct ClientJob {
    client: ClientMiddleware,
    client_id: u64,
    stage: u32,
    next: Arc<AtomicU64>,
    tx: mpsc::Sender<Row>,
    seed: u64,
    resolve_after_ms: u64,
    max_requests: Option<u64>,
    stage_end: Option<Instant>,
    interval: Option<Duration>,
    start: Instant,
    /// Becomes true on Ctrl+C / SIGTERM: send nothing new, resolve what is pending right away.
    stop: tokio::sync::watch::Receiver<bool>,
}

/// Resolves one incident, retrying for up to ~15 s. A resolve is what gives the incident's
/// resources back: a confirmed claim never expires, so a resolve given up on holds its
/// resources for good. During a failover (a node just died or just came back) every node can
/// be slow for several seconds, longer than the client's own 3 attempts, so keep trying.
async fn resolve_until_done(client: &ClientMiddleware, incident_id: u64) -> bool {
    let mut last = None;
    for round in 0..4u64 {
        if round > 0 {
            tokio::time::sleep(Duration::from_millis(500 * (1 << round))).await;
        }
        let outcome = client.resolve(incident_id).await;
        if outcome.ok {
            return true;
        }
        last = outcome.error;
    }
    eprintln!("Failed to resolve incident {incident_id} after 4 rounds: {last:?}");
    false
}

impl ClientJob {
    /// Sends requests until the job's limit (request count or stage end) is reached.
    async fn run(self) -> Vec<tokio::task::JoinHandle<bool>> {
        // Keep handles to missions that are still waiting to be resolved.
        let mut resolution_tasks = Vec::new();

        let mut stop = self.stop.clone();
        loop {
            if *stop.borrow() || self.stage_end.is_some_and(|end| Instant::now() >= end) {
                break;
            }

            let n = self.next.fetch_add(1, Ordering::Relaxed);

            if self.max_requests.is_some_and(|max| n >= max) {
                break;
            }

            if let Some(gap) = self.interval {
                tokio::select! {
                    _ = tokio::time::sleep_until(self.start + gap.mul_f64(n as f64)) => {}
                    _ = stop.wait_for(|s| *s) => break,
                }
            }

            // Generate the incident and save its ID before converting it to protobuf.
            let generated_incident = generate_incident(self.seed + n);
            let incident_id = generated_incident.id;

            let incident = incident_to_pb(&generated_incident);
            let timestamp_ms = unix_ms();

            let outcome = self.client.submit(incident).await;

            let assigned = outcome.assigned();
            let result = outcome.outcome();

            // Start a resolution task only when resolution is enabled. Resolve when resources
            // were assigned, and ALSO when no plan came back (timeout/error): during a failover a
            // request can commit on a node while its reply is lost, and nobody else would ever
            // resolve that incident, so its resources would stay claimed for good. Resolving an
            // incident that never committed is harmless, and it stops a late commit of it.
            // Only a plan with nothing assigned (Some(false)) holds nothing and needs no resolve.
            if assigned != Some(false) && self.resolve_after_ms > 0 {
                let delay_ms = mission_delay_ms(self.seed, n, self.resolve_after_ms);

                let resolve_client = self.client.clone();
                let mut stop = self.stop.clone();

                let task = tokio::spawn(async move {
                    // On a stop, resolve at once instead of waiting out the mission delay.
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {}
                        _ = stop.wait_for(|s| *s) => {}
                    }
                    resolve_until_done(&resolve_client, incident_id).await
                });

                resolution_tasks.push(task);
            }

            let row = Row {
                timestamp_ms,
                client_id: self.client_id,
                request_id: outcome.request_id,
                latency_us: outcome.latency_us,
                executor_node: outcome.executor_node,
                status: outcome.status,
                assigned,
                stage: self.stage,
                attempts: outcome.attempts,
                no_plan_replies: outcome.no_plan_replies,
                outcome: result,
            };

            if self.tx.send(row).is_err() {
                break;
            }
        }

        // Step 4: wait for every mission started by this client to finish resolving.
        // This prevents the client job from ending while its resolution tasks are
        // still running.
        resolution_tasks
    }
}

/// How many requests ended each way. Filled in by the CSV writer as it saves rows.
#[derive(Default)]
struct Counts {
    ok: u64,
    timeout: u64,
    error: u64,
    unassigned: u64,
    retried: u64,
    resolve_sent: u64,
    resolve_ok: u64,
    resolve_failed: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let run_id = args
        .run_id
        .clone()
        .unwrap_or_else(|| format!("run-{}", unix_ms() / 1000));

    fs::create_dir_all(&args.out)?;
    let csv_path = args.out.join(format!("{run_id}.csv"));
    let meta_path = args.out.join(format!("{run_id}.meta.txt"));

    // Where requests go: one node if --target or --node is given (Step A style), else every node in the config file.
    let single = args.target.is_some() || args.node.is_some();

    let policy = RetryPolicy {
        attempts: args.attempts,
        attempt_timeout: Duration::from_millis(args.attempt_timeout_ms),
    };

    let destination = if single {
        client::target_addr(&args.config, args.target.unwrap_or(1), args.node.as_deref())
            .map_err(|e| anyhow::anyhow!(e))?
    } else {
        format!("all nodes in {}", args.config)
    };

    // The stages to run. Ramp mode: one per --stages value. Fixed mode: a single stage of --clients clients.
    let ramp = !args.stages.is_empty();

    let stages: Vec<u64> = if ramp {
        args.stages.clone()
    } else {
        vec![args.clients]
    };

    if stages.contains(&0) {
        anyhow::bail!(
            "every stage needs at least 1 client (got --stages {:?})",
            args.stages
        );
    }

    if ramp && args.stage_secs <= 0.0 {
        anyhow::bail!("--stage-secs must be more than 0");
    }

    let stage_len = Duration::from_secs_f64(args.stage_secs);
    let max_clients = *stages.iter().max().expect("at least one stage");

    // One ClientMiddleware per client, each with its own connections, like separate users.
    let mut clients = Vec::new();

    for _ in 0..max_clients {
        clients.push(if single {
            ClientMiddleware::connect(&destination, DEFAULT_TIMEOUT).await?
        } else {
            ClientMiddleware::connect_all(&args.config, policy).map_err(|e| anyhow::anyhow!(e))?
        });
    }

    let how = if single {
        format!("to {destination}, one try per request")
    } else {
        format!(
            "multicast to {destination}, {} attempts of {} ms",
            policy.attempts, args.attempt_timeout_ms
        )
    };

    if ramp {
        println!(
            "{max_clients} clients, {how}; ramp {stages:?} clients, {} s per stage",
            args.stage_secs
        );
    } else {
        println!(
            "{max_clients} clients, {how}; sending {} requests...",
            args.requests
        );
    }

    // The CSV writer runs on its own thread, so slow disk writes never delay a request.
    let (tx, rx) = mpsc::channel::<Row>();
    let writer = std::thread::spawn({
        let (csv_path, run_id) = (csv_path.clone(), run_id.clone());
        move || write_csv(&csv_path, &run_id, rx)
    });

    let next = Arc::new(AtomicU64::new(0));
    let interval = (args.rate > 0.0).then(|| Duration::from_secs_f64(1.0 / args.rate));

    // A clean stop (Ctrl+C, or SIGTERM from `kill`, which the demo panel sends): stop sending,
    // resolve every incident still waiting for its mission delay, write the totals, exit.
    // Being killed outright instead would leave those incidents' resources claimed for good.
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        wait_for_stop_signal().await;
        eprintln!("stop requested: no new requests; resolving the pending incidents, then exiting");
        let _ = stop_tx.send(true);
    });

    let started_at_ms = unix_ms();
    let start = Instant::now();
    let mut resolution_tasks: Vec<tokio::task::JoinHandle<bool>> = Vec::new();
    // Run the stages one after another. Within a stage, one tokio task per client, all at the same time.
    for (s, &stage_clients) in stages.iter().enumerate() {
        if *stop_rx.borrow() {
            break;
        }
        let stage = s as u32 + 1;
        let stage_end = ramp.then(|| Instant::now() + stage_len);

        if ramp {
            println!(
                "stage {stage}/{}: {stage_clients} clients for {} s",
                stages.len(),
                args.stage_secs
            );
        }

        let mut tasks = Vec::new();

        for (i, client) in clients.iter().take(stage_clients as usize).enumerate() {
            let job = ClientJob {
                client: client.clone(),
                client_id: i as u64 + 1,
                stage,
                next: next.clone(),
                tx: tx.clone(),
                seed: args.seed,
                resolve_after_ms: args.resolve_after_ms,
                max_requests: (!ramp).then_some(args.requests),
                stage_end,
                interval,
                start,
                stop: stop_rx.clone(),
            };

            tasks.push(tokio::spawn(job.run()));
        }

        for task in tasks {
            resolution_tasks.extend(task.await?);
        }
    }

    drop(tx);

    let elapsed = start.elapsed();

    let resolve_sent = resolution_tasks.len() as u64;
    let mut resolve_ok = 0u64;

    for task in resolution_tasks {
        match task.await {
            Ok(true) => resolve_ok += 1,
            Ok(false) | Err(_) => {}
        }
    }

    let resolve_failed = resolve_sent - resolve_ok;

    let mut counts = writer.join().expect("CSV writer thread panicked")?;
    counts.resolve_sent = resolve_sent;
    counts.resolve_ok = resolve_ok;
    counts.resolve_failed = resolve_failed;

    // Late plans: a second plan for a request that already had one (a split decision, or a slow winner).
    // P1's client counts them in the background until each attempt's deadline, so wait that long first.
    // (Skipped on a stop: whoever stopped us is waiting for the exit.)
    let late_plans = if single || *stop_rx.borrow() {
        0
    } else {
        tokio::time::sleep(policy.attempt_timeout).await;
        clients.iter().map(|c| c.late_plans()).sum::<u64>()
    };

    // Summary. The stats script computes the full numbers from the CSV; this is a quick check.
    let secs = elapsed.as_secs_f64();
    let throughput = counts.ok as f64 / secs;

    println!(
        "done in {secs:.2} s: {} ok ({} with nothing assigned, {} retried), {} timeout, {} error, {late_plans} late plans, {} resolves sent ({} successful, {} failed), {throughput:.1} ok/s",
        counts.ok,
        counts.unassigned,
        counts.retried,
        counts.timeout,
        counts.error,
        counts.resolve_sent,
        counts.resolve_ok,
        counts.resolve_failed
    );

    // The metadata file records how to repeat this exact run.
    let meta = format!(
        "run_id: {run_id}\n\
         started_at_ms: {started_at_ms}\n\
         command: {}\n\
         mode: {}\nsend: {}\nnode: {}\nattempts: {}\nattempt_timeout_ms: {}\n\
         stages: {}\nstage_secs: {}\nclients: {}\nrequests: {}\nseed: {}\nrate: {}\nresolve_after_ms: {}\n\
git_commit: {}\n\
duration_s: {secs:.3}\n\
ok: {}\nunassigned: {}\nretried: {}\ntimeout: {}\nerror: {}\nlate_plans: {late_plans}\n\
resolve_sent: {}\nresolve_ok: {}\nresolve_failed: {}\n\
throughput_ok_per_s: {throughput:.1}\n",
        std::env::args().collect::<Vec<_>>().join(" "),
        if ramp { "ramp" } else { "fixed" },
        if single { "single" } else { "multicast" },
        destination,
        if single {
            "1".to_string()
        } else {
            policy.attempts.to_string()
        },
        if single {
            DEFAULT_TIMEOUT.as_millis() as u64
        } else {
            args.attempt_timeout_ms
        },
        stages
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(","),
        if ramp {
            args.stage_secs.to_string()
        } else {
            "-".to_string()
        },
        max_clients,
        if ramp {
            "-".to_string()
        } else {
            args.requests.to_string()
        },
        args.seed,
        args.rate,
        args.resolve_after_ms,
        git_commit(),
        counts.ok,
        counts.unassigned,
        counts.retried,
        counts.timeout,
        counts.error,
        counts.resolve_sent,
        counts.resolve_ok,
        counts.resolve_failed,
    );

    fs::write(&meta_path, meta)?;
    println!("wrote {} and {}", csv_path.display(), meta_path.display());

    Ok(())
}

/// The CSV writer: takes rows off the channel and writes them until every sender is gone.
fn write_csv(path: &Path, run_id: &str, rx: mpsc::Receiver<Row>) -> std::io::Result<Counts> {
    let mut w = BufWriter::new(File::create(path)?);

    writeln!(
        w,
        "run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned,stage,attempts,no_plan_replies,outcome"
    )?;

    let mut counts = Counts::default();

    for row in rx {
        match row.status {
            SubmitStatus::Ok => counts.ok += 1,
            SubmitStatus::Timeout => counts.timeout += 1,
            SubmitStatus::Error => counts.error += 1,
        }

        if row.assigned == Some(false) {
            counts.unassigned += 1;
        }

        if row.attempts > 1 {
            counts.retried += 1;
        }

        let node = row.executor_node.map_or(String::new(), |n| n.to_string());
        let assigned = row.assigned.map_or(String::new(), |a| a.to_string());

        writeln!(
            w,
            "{run_id},{},{},{},{},{node},{},{assigned},{},{},{},{}",
            row.timestamp_ms,
            row.client_id,
            row.request_id,
            row.latency_us,
            row.status.as_str(),
            row.stage,
            row.attempts,
            row.no_plan_replies,
            row.outcome
        )?;
    }

    w.flush()?;
    Ok(counts)
}

/// Returns a deterministic mission delay between 1 and max_ms.
fn mission_delay_ms(seed: u64, request_number: u64, max_ms: u64) -> u64 {
    if max_ms == 0 {
        return 0;
    }

    let mut value = seed.wrapping_add(request_number);
    value = value.wrapping_add(0x9E3779B97F4A7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
    value ^= value >> 31;

    (value % max_ms) + 1
}

/// Returns on Ctrl+C, or on SIGTERM (what `kill <pid>` sends) on Linux/macOS.
async fn wait_for_stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Current wall-clock time in unix milliseconds. Used for timestamps only, never to measure latency.
fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The current git commit (e.g. "b41acd2", or "b41acd2-dirty" if there are uncommitted changes).
fn git_commit() -> String {
    std::process::Command::new("git")
        .args(["describe", "--always", "--dirty"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
#[cfg(test)]
mod tests {
    use super::mission_delay_ms;

    #[test]
    fn mission_delay_is_deterministic() {
        let first = mission_delay_ms(42, 10, 5000);
        let second = mission_delay_ms(42, 10, 5000);

        assert_eq!(first, second);
    }

    #[test]
    fn mission_delay_stays_within_range() {
        for request_number in 0..100 {
            let delay = mission_delay_ms(42, request_number, 5000);

            assert!((1..=5000).contains(&delay));
        }
    }

    #[test]
    fn zero_max_delay_disables_resolution_delay() {
        assert_eq!(mission_delay_ms(42, 10, 0), 0);
    }
}
