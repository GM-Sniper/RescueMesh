//! Failover, recovery and latency around each failure (owner: P6, Step C). Definitions: "P6: Failure
//! metrics (Step C)" in DESIGN_LOG.md.
//!
//!   cargo run --release -p loadgen --bin failover -- results/<run>/<run>.csv \
//!       --faults results/<run>/logs/faults-*.csv --elections results/<run>/logs/elections-*.csv
//!
//! For every failure in the fault logs (a mute from P5's injector, or a cut from the experiment runner):
//! - detection time = first PEER_DOWN for the failed node on a live node - failure start;
//! - failover time  = first answer that could only have come without the failed node - failure start;
//! - recovery time  = node ready again - node back. STAND-IN until P3 logs a readiness event:
//!                    "ready" = the first election the returning node wins after it is back;
//! - latency before, during and after the failure (task 5), each window as long as the failure.
//!
//! Times are printed from the first request sent, like the plan's examples ("muted at 10.0 s").

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::Parser;

#[derive(Parser)]
#[command(about = "Detection, failover and recovery time, and latency before/during/after each failure")]
struct Args {
    /// The load generator's request CSV.
    csv: PathBuf,

    /// Fault logs: logs/faults-node<N>.csv (P5) and logs/faults-runner.csv (the experiment runner).
    #[arg(long, num_args = 0..)]
    faults: Vec<PathBuf>,

    /// Election logs: logs/elections-node<N>.csv (P3). Needed for recovery time.
    #[arg(long, num_args = 0..)]
    elections: Vec<PathBuf>,
}

/// One request from the CSV (times in µs since 1970).
#[derive(Debug, Clone)]
struct Request {
    sent_us: u128,           // when the client sent it (first attempt)
    done_us: u128,           // sent + latency: when it got its answer, or gave up
    ok: bool,                // got a plan (outcome committed or duplicate)
    attempts: u32,           // 1 = no retry
    executor: Option<u32>,   // the node whose plan came back (empty when it failed)
    latency_us: u64,         // send-to-answer time
}

/// One failure: a mute (P5's injector) or a cut (the runner stops the node's program).
#[derive(Debug, Clone, PartialEq)]
struct Failure {
    kind: &'static str, // "mute" or "cut"
    node: u32,          // the node that failed
    start_us: u128,     // MUTE_START / CUT_START
    end_us: u128,       // MUTE_END / CUT_END, or start + planned duration if the run stopped first
    ended: bool,        // false = no _END row: the node was not back before the run stopped
}

/// What the fault logs say: the failures, and every "peer suspected" row.
#[derive(Default)]
struct FaultLog {
    failures: Vec<Failure>,
    peer_down: Vec<(u32, u128)>, // (suspected node, when) from PEER_DOWN rows written by live nodes
}

/// Results for one failure. None = not measured (the reason is printed instead of a number).
#[derive(Debug, PartialEq)]
struct Measured {
    detected_us: Option<u128>, // first PEER_DOWN for the failed node
    answer_us: Option<u128>,   // first answer that could only have come without the failed node
    ready_us: Option<u128>,    // first election the returning node won after it was back (stand-in)
}

/// Latency numbers for one window around a failure.
#[derive(Debug, Default, PartialEq)]
struct Window {
    requests: usize,       // requests in the window
    lost: usize,           // of which got no plan
    latencies_us: Vec<u64>, // latencies of the ones that got a plan, sorted
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let csv = fs::read_to_string(&args.csv).with_context(|| format!("can't read {}", args.csv.display()))?;
    let requests = parse_requests(&csv).with_context(|| format!("bad request CSV {}", args.csv.display()))?;
    let faults = parse_faults(&read_all(&args.faults)?)?;
    let wins = parse_wins(&read_all(&args.elections)?)?;
    print_report(&requests, &faults, &wins);
    Ok(())
}

/// Reads every file into a string (one string per file).
fn read_all(paths: &[PathBuf]) -> anyhow::Result<Vec<String>> {
    paths.iter().map(|p| fs::read_to_string(p).with_context(|| format!("can't read {}", p.display()))).collect()
}

/// Reads the request CSV. Columns are found by name; `attempts` is optional (older CSVs: 1 try each).
fn parse_requests(text: &str) -> anyhow::Result<Vec<Request>> {
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().context("file is empty")?.split(',').collect();
    let column = |name: &str| header.iter().position(|c| *c == name).with_context(|| format!("column `{name}` is missing"));
    let (i_ts, i_lat, i_status, i_exec) = (column("timestamp")?, column("latency_us")?, column("status")?, column("executor_node")?);
    let i_attempts = header.iter().position(|c| *c == "attempts");

    let mut requests = Vec::new();
    for (n, line) in lines.enumerate().filter(|(_, l)| !l.trim().is_empty()) {
        let line_no = n + 2; // +1 for the header, +1 because lines count from 1
        let f: Vec<&str> = line.split(',').collect();
        let field = |i: usize| f.get(i).copied().with_context(|| format!("line {line_no}: too few columns"));
        let sent_us = field(i_ts)?.parse::<u128>().with_context(|| format!("line {line_no}: bad timestamp"))? * 1000; // ms -> µs
        let latency_us: u64 = field(i_lat)?.parse().with_context(|| format!("line {line_no}: bad latency_us"))?;
        let status = field(i_status)?;
        if !matches!(status, "ok" | "timeout" | "error") {
            bail!("line {line_no}: unknown status `{status}`");
        }
        let exec = field(i_exec)?;
        requests.push(Request {
            sent_us,
            done_us: sent_us + latency_us as u128,
            ok: status == "ok", // a plan came back: committed or duplicate (P1's outcome column, derived)
            attempts: match i_attempts {
                Some(i) => field(i)?.parse().with_context(|| format!("line {line_no}: bad attempts"))?,
                None => 1,
            },
            executor: if exec.is_empty() { None } else { Some(exec.parse().with_context(|| format!("line {line_no}: bad executor_node"))?) },
            latency_us,
        });
    }
    Ok(requests)
}

/// Reads the fault logs (P5's and the runner's, same columns). A start without an end (the run
/// stopped first) lasts for its planned duration and is marked as not ended.
fn parse_faults(texts: &[String]) -> anyhow::Result<FaultLog> {
    let mut log = FaultLog::default();
    for text in texts {
        let mut lines = text.lines();
        let Some(header) = lines.next() else { continue }; // an empty file: no faults
        let header: Vec<&str> = header.split(',').collect();
        let column = |name: &str| header.iter().position(|c| *c == name).with_context(|| format!("fault log: column `{name}` is missing"));
        let (i_node, i_event, i_id, i_peer, i_dur, i_ts) =
            (column("node_id")?, column("event")?, column("fault_id")?, column("peer_id")?, column("duration_ms")?, column("wall_ts_us")?);

        let mut open: Vec<(u32, String, usize)> = Vec::new(); // (node, fault_id, index in failures) still waiting for their _END
        for line in lines.filter(|l| !l.trim().is_empty()) {
            let f: Vec<&str> = line.split(',').collect();
            let get = |i: usize| f.get(i).copied().unwrap_or("");
            let ts: u128 = get(i_ts).parse().context("fault log: bad wall_ts_us")?;
            let event = get(i_event);
            if event == "PEER_DOWN" {
                let peer: u32 = get(i_peer).parse().context("fault log: bad peer_id")?; // the suspected node
                log.peer_down.push((peer, ts));
                continue;
            }
            let kind = match event {
                "MUTE_START" | "MUTE_END" => "mute",
                "CUT_START" | "CUT_END" => "cut",
                _ => continue, // PEER_UP and anything new: not needed here
            };
            let node: u32 = get(i_node).parse().context("fault log: bad node_id")?;
            let id = get(i_id).to_string();
            if event.ends_with("_START") {
                let planned_us = get(i_dur).parse::<u128>().unwrap_or(0) * 1000; // ms -> µs
                open.push((node, id, log.failures.len()));
                log.failures.push(Failure { kind, node, start_us: ts, end_us: ts + planned_us, ended: false });
            } else if let Some(pos) = open.iter().position(|(n, i, _)| *n == node && *i == id) {
                let (_, _, idx) = open.remove(pos);
                log.failures[idx].end_us = ts; // the real end
                log.failures[idx].ended = true;
            }
        }
    }
    log.failures.sort_by_key(|f| f.start_us);
    Ok(log)
}

/// Reads P3's election logs and keeps (winner, when) for every row that has a winner.
fn parse_wins(texts: &[String]) -> anyhow::Result<Vec<(u32, u128)>> {
    let mut wins = Vec::new();
    for text in texts {
        let mut lines = text.lines();
        let Some(header) = lines.next() else { continue };
        let header: Vec<&str> = header.split(',').collect();
        let column = |name: &str| header.iter().position(|c| *c == name).with_context(|| format!("election log: column `{name}` is missing"));
        let (i_winner, i_ts) = (column("winner")?, column("wall_ts_us")?);
        for line in lines.filter(|l| !l.trim().is_empty()) {
            let f: Vec<&str> = line.split(',').collect();
            let winner = f.get(i_winner).copied().unwrap_or("");
            if winner.is_empty() {
                continue; // no bid was ready: nobody won
            }
            let ts: u128 = f.get(i_ts).copied().unwrap_or("").parse().context("election log: bad wall_ts_us")?;
            wins.push((winner.parse().context("election log: bad winner")?, ts));
        }
    }
    Ok(wins)
}

/// Detection, failover and recovery for one failure (definitions in DESIGN_LOG.md).
fn measure(f: &Failure, requests: &[Request], faults: &FaultLog, wins: &[(u32, u128)]) -> Measured {
    // Detection: the first time a live node suspected the failed one, while it was down.
    let detected_us = faults.peer_down.iter()
        .filter(|(peer, ts)| *peer == f.node && *ts >= f.start_us && *ts <= f.end_us)
        .map(|(_, ts)| *ts)
        .min();

    // Failover: the first answer that could only have come without the failed node.
    let answer_us = requests.iter()
        .filter(|r| r.ok && r.executor.is_some_and(|n| n != f.node)) // a plan, from a live node
        .filter(|r| r.sent_us >= f.start_us                           // sent after the failure started,
            || (r.done_us >= f.start_us && r.attempts > 1))           // or caught by it and saved by a retry
        .map(|r| r.done_us)
        .filter(|done| *done <= f.end_us) // must come while the node is still down
        .min();

    // Recovery (STAND-IN): the first election the node wins once it is back. Replace with P3's
    // readiness event when the node logs one.
    let ready_us = if f.ended {
        wins.iter().filter(|(winner, ts)| *winner == f.node && *ts >= f.end_us).map(|(_, ts)| *ts).min()
    } else {
        None // never came back during the run
    };

    Measured { detected_us, answer_us, ready_us }
}

/// Latency before, during and after one failure. Each side window is as long as the failure, cut
/// short so it does not reach into the previous or the next failure.
fn windows(f: &Failure, prev_end_us: Option<u128>, next_start_us: Option<u128>, requests: &[Request]) -> [Window; 3] {
    let len = f.end_us - f.start_us;
    let before_from = f.start_us.saturating_sub(len).max(prev_end_us.unwrap_or(0));
    let after_to = (f.end_us + len).min(next_start_us.unwrap_or(u128::MAX));
    let mut out: [Window; 3] = Default::default(); // [before, during, after]
    for r in requests {
        let slot = if r.sent_us <= f.end_us && r.done_us >= f.start_us {
            1 // its time overlaps the failure: during
        } else if r.sent_us >= before_from && r.done_us < f.start_us {
            0 // finished before the failure started
        } else if r.sent_us > f.end_us && r.done_us <= after_to {
            2 // sent after the node was back, and done before the next failure
        } else {
            continue; // outside all three windows
        };
        let w = &mut out[slot];
        w.requests += 1;
        if r.ok {
            w.latencies_us.push(r.latency_us);
        } else {
            w.lost += 1;
        }
    }
    for w in &mut out {
        w.latencies_us.sort_unstable();
    }
    out
}

fn print_report(requests: &[Request], faults: &FaultLog, wins: &[(u32, u128)]) {
    let Some(t0) = requests.iter().map(|r| r.sent_us).min() else {
        println!("failover: the request CSV has no requests");
        return;
    };
    let at = |us: u128| format!("{:.2} s", us.saturating_sub(t0) as f64 / 1e6); // a moment, from the first request
    let gap = |a: u128, b: u128| format!("{:.2} s", b.saturating_sub(a) as f64 / 1e6); // a duration b - a

    let fs = &faults.failures;
    println!("failover and recovery ({} failures in the fault logs; times from the first request sent):", fs.len());
    if wins.is_empty() {
        println!("  (no election logs given: recovery time can't be measured)");
    }
    let mut failovers = Vec::new(); // in µs, for the summary line
    let mut recoveries = Vec::new();
    for (i, f) in fs.iter().enumerate() {
        let m = measure(f, requests, faults, wins);
        let end_note = if f.ended { "" } else { " (not back before the run stopped)" };
        println!("failure {}: node {} {}, {} -> {} ({}){end_note}", i + 1, f.node, if f.kind == "mute" { "muted" } else { "cut" }, at(f.start_us), at(f.end_us), gap(f.start_us, f.end_us));
        match m.detected_us {
            Some(t) => println!("  detected   {:>9}   detection {:>8}   first PEER_DOWN for node {}", at(t), gap(f.start_us, t), f.node),
            None => println!("  detected   {:>9}   detection {:>8}   no PEER_DOWN for node {} while it was down", "-", "-", f.node),
        }
        match m.answer_us {
            Some(t) => {
                failovers.push(t - f.start_us);
                println!("  answered   {:>9}   failover  {:>8}   first plan that could only come without node {}", at(t), gap(f.start_us, t), f.node);
            }
            None => println!("  answered   {:>9}   failover  {:>8}   not measured: no such plan before the node was back", "-", "-"),
        }
        if f.ended {
            match m.ready_us {
                Some(t) => {
                    recoveries.push(t - f.end_us);
                    println!("  ready      {:>9}   recovery  {:>8}   STAND-IN: first election node {} won after it was back", at(t), gap(f.end_us, t), f.node);
                }
                None => println!("  ready      {:>9}   recovery  {:>8}   not measured: node {} won no election after it was back", "-", "-", f.node),
            }
        }

        // Task 5: latency before, during and after this failure.
        let prev_end = i.checked_sub(1).map(|j| fs[j].end_us);
        let next_start = fs.get(i + 1).map(|n| n.start_us);
        let [before, during, after] = windows(f, prev_end, next_start, requests);
        println!("  {:<8} {:>9} {:>6} {:>10} {:>10} {:>10}", "latency", "requests", "lost", "p50", "p95", "max");
        for (label, w) in [("before", &before), ("during", &during), ("after", &after)] {
            println!("  {:<8} {:>9} {:>6} {:>10} {:>10} {:>10}", label, w.requests, w.lost, pct(w, 50.0), pct(w, 95.0), pct(w, 100.0));
        }
    }
    if !fs.is_empty() {
        println!("summary: failover {}, recovery {}", spread(&failovers, fs.len()), spread(&recoveries, fs.iter().filter(|f| f.ended).count()));
        println!("  before = same length as the failure, just before it; during = overlaps it; after = same length, just after");
    }
}

/// Mean and max of the measured times, e.g. "mean 2.60 s, max 3.10 s (2 of 2 failures)".
fn spread(us: &[u128], out_of: usize) -> String {
    if us.is_empty() {
        return format!("not measured (0 of {out_of} failures)");
    }
    let mean = us.iter().sum::<u128>() as f64 / us.len() as f64 / 1e6;
    let max = *us.iter().max().unwrap() as f64 / 1e6;
    format!("mean {mean:.2} s, max {max:.2} s ({} of {out_of} failures)", us.len())
}

/// Nearest-rank percentile in ms, as in stats; "-" when the window has no answered request.
fn pct(w: &Window, p: f64) -> String {
    if w.latencies_us.is_empty() {
        return "-".to_string();
    }
    let rank = (p / 100.0 * w.latencies_us.len() as f64).ceil() as usize;
    format!("{:.2} ms", w.latencies_us[rank.clamp(1, w.latencies_us.len()) - 1] as f64 / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CSV: &str = "run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned,stage,attempts,no_plan_replies\n";
    const FAULTS: &str = "run_id,node_id,event,fault_id,peer_id,duration_ms,wall_ts_us\n";
    const ELECTIONS: &str = "run_id,node_id,request_id,round,bids,winner,duration_us,waited_full_t,late_bids,split_decision,bid_calls_sent,wall_ts_us\n";

    /// A request sent at `sent_ms` taking `latency_ms`, answered by `exec` ("" = no plan).
    fn row(sent_ms: u64, latency_ms: u64, exec: &str, status: &str, attempts: u32) -> String {
        format!("r,{sent_ms},1,id{sent_ms},{},{exec},{status},true,1,{attempts},0\n", latency_ms * 1000)
    }

    /// The plan's worked examples: node 2 muted at 10.0 s for 8.4 s (back at 18.4 s), suspected at
    /// 11.5 s, first request answered by a live node at 12.6 s, ready again at 20.1 s.
    fn worked_example() -> (Vec<Request>, FaultLog, Vec<(u32, u128)>) {
        let faults = format!(
            "{FAULTS}r,2,MUTE_START,f1,,8400,10000000\nr,2,MUTE_END,f1,,,18400000\n\
             r,1,PEER_DOWN,,2,,11500000\nr,3,PEER_DOWN,,2,,11600000\n"
        );
        let csv = format!(
            "{CSV}{}{}{}{}{}",
            row(0, 5, "1", "ok", 1),         // t0 = 0 s, before the failure
            row(9_999, 4, "1", "ok", 1),     // elected before the mute, done at 10.003 s: NOT failover
            row(9_000, 2_000, "2", "ok", 1), // answered by the muted node itself: NOT failover
            row(10_000, 2_600, "1", "ok", 2), // sent at the mute, retried, answered at 12.6 s: failover
            row(13_000, 5, "3", "ok", 1),    // later answers don't matter
        );
        let elections = format!(
            "{ELECTIONS}r,1,a,0,3,2,100,false,0,false,2,9000000\n\
             r,1,b,0,2,1,100,false,0,false,2,15000000\n\
             r,1,c,0,3,2,100,false,0,false,2,20100000\n\
             r,3,d,0,3,2,100,false,0,false,2,21000000\n"
        );
        (parse_requests(&csv).unwrap(), parse_faults(&[faults]).unwrap(), parse_wins(&[elections]).unwrap())
    }

    #[test]
    fn worked_examples_give_2_6_s_failover_and_1_7_s_recovery() {
        let (requests, faults, wins) = worked_example();
        assert_eq!(faults.failures.len(), 1);
        let f = &faults.failures[0];
        let m = measure(f, &requests, &faults, &wins);
        assert_eq!(m.detected_us.map(|t| t - f.start_us), Some(1_500_000), "detection 1.5 s");
        assert_eq!(m.answer_us.map(|t| t - f.start_us), Some(2_600_000), "failover 2.6 s");
        assert_eq!(m.ready_us.map(|t| t - f.end_us), Some(1_700_000), "recovery 1.7 s");
    }

    #[test]
    fn no_answer_while_down_is_not_measured_rather_than_zero() {
        let faults = parse_faults(&[format!("{FAULTS}r,2,MUTE_START,f1,,5000,10000000\nr,2,MUTE_END,f1,,,15000000\n")]).unwrap();
        // The only answer after the mute comes once the node is back.
        let requests = parse_requests(&format!("{CSV}{}", row(14_000, 2_000, "1", "ok", 2))).unwrap();
        let m = measure(&faults.failures[0], &requests, &faults, &[]);
        assert_eq!((m.answer_us, m.ready_us), (None, None));
    }

    #[test]
    fn a_node_that_never_came_back_has_no_recovery() {
        let faults = parse_faults(&[format!("{FAULTS}r,2,MUTE_START,f1,,5000,10000000\n")]).unwrap(); // run stopped first
        let f = &faults.failures[0];
        assert!(!f.ended);
        assert_eq!(f.end_us, 15_000_000, "lasts its planned duration");
        let m = measure(f, &[], &faults, &[(2, 16_000_000)]);
        assert_eq!(m.ready_us, None, "a win after the planned end doesn't count: the node never reported being back");
    }

    #[test]
    fn a_cut_from_the_runner_is_measured_like_a_mute() {
        let faults = parse_faults(&[format!("{FAULTS}r,3,CUT_START,cut-0,,8000,30000000\nr,3,CUT_END,cut-0,,,38000000\n")]).unwrap();
        let f = &faults.failures[0];
        assert_eq!((f.kind, f.node, f.ended), ("cut", 3, true));
        let m = measure(f, &[], &faults, &[(3, 38_500_000)]);
        assert_eq!(m.ready_us.map(|t| t - f.end_us), Some(500_000));
    }

    #[test]
    fn latency_is_split_into_before_during_and_after() {
        let (requests, faults, _) = worked_example();
        let [before, during, after] = windows(&faults.failures[0], None, None, &requests);
        // Failure 10.0 -> 18.4 s, so before = 1.6 -> 10.0 s and after = 18.4 -> 26.8 s.
        assert_eq!(before.requests, 0, "the 0 s request is older than the before window");
        assert_eq!(during.requests, 4, "everything overlapping 10.0 -> 18.4 s");
        assert_eq!(during.latencies_us.last(), Some(&2_600_000), "the retried request is the slowest");
        assert_eq!(after.requests, 0);
    }

    #[test]
    fn side_windows_stop_at_the_neighbouring_failures() {
        let f = Failure { kind: "mute", node: 2, start_us: 10_000_000, end_us: 20_000_000, ended: true };
        let csv = format!("{CSV}{}{}{}", row(4_000, 5, "1", "ok", 1), row(6_000, 5, "1", "ok", 1), row(24_000, 5, "1", "timeout", 3));
        let requests = parse_requests(&csv).unwrap();
        // The previous failure ended at 5 s, the next starts at 25 s.
        let [before, _, after] = windows(&f, Some(5_000_000), Some(25_000_000), &requests);
        assert_eq!(before.requests, 1, "the 4 s request belongs to the previous failure");
        assert_eq!((after.requests, after.lost), (1, 1));
    }
}
