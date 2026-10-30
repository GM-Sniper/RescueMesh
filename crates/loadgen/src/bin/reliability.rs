//! Reliability (owner: P6): how many requests succeeded, needed a retry, or were lost, split by
//! scenario: no failure, one node down, network cut.
//!
//!   cargo run --release -p loadgen --bin reliability -- results/<run>/<run>.csv --faults results/<run>/logs/faults-node1.csv ...
//!
//! Inputs:
//! - the request CSV (status, attempts, timestamp, latency_us), one row per request;
//! - the fault logs: P5's logs/faults-node<N>.csv (MUTE_START / MUTE_END, written by the muted node)
//!   and the experiment runner's logs/faults-runner.csv (CUT_START / CUT_END). Same columns.
//!
//! Definitions (Step B plan, P6 review note: "confirm what counts as lost"):
//! - success = the request got a plan (status ok), after however many attempts it took;
//! - failed  = at least one attempt got no plan: the request needed a retry, or was lost;
//! - retried = needed more than one attempt but got a plan in the end (failover worked);
//! - lost    = no plan after all attempts (status timeout or error);
//! - success % = ok / requests.
//!
//! Scenario of a request: whatever was going on while it ran, from when it was sent until its
//! reply (or until it gave up). If its time overlaps a cut it counts as "network cut"; else if it
//! overlaps a mute, "one node down"; otherwise "no failure".

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::Parser;

#[derive(Parser)]
#[command(about = "Success %, failed, retried and lost requests per scenario (no failure, one node down, network cut)")]
struct Args {
    /// The load generator's request CSV.
    csv: PathBuf,

    /// Fault logs: logs/faults-node<N>.csv (P5) and logs/faults-runner.csv (the experiment runner).
    #[arg(long, num_args = 0..)]
    faults: Vec<PathBuf>,
}

/// The three scenarios, in report order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Scenario {
    NoFailure,
    NodeDown,
    NetworkCut,
}

impl Scenario {
    fn label(self) -> &'static str {
        match self {
            Scenario::NoFailure => "no failure",
            Scenario::NodeDown => "one node down",
            Scenario::NetworkCut => "network cut",
        }
    }
}

/// A time window during which a fault was active (wall clock, µs since 1970).
#[derive(Debug, Clone, PartialEq)]
struct Window {
    kind: Scenario, // NodeDown (mute) or NetworkCut (cut)
    node: u32,
    from_us: u128,
    to_us: u128,
}

/// One request: when it ran and how it ended.
struct Request {
    from_us: u128,
    to_us: u128,
    ok: bool,
    attempts: u32,
    latency_us: u64,
}

/// Counts for one scenario.
#[derive(Default)]
struct Tally {
    requests: u64,
    ok: u64,
    retried: u64, // ok, but after more than one attempt
    lost: u64,    // no plan after all attempts
    latencies_us: Vec<u64>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let csv = fs::read_to_string(&args.csv).with_context(|| format!("can't read {}", args.csv.display()))?;
    let requests = parse_requests(&csv).with_context(|| format!("bad request CSV {}", args.csv.display()))?;
    let mut fault_texts = Vec::new();
    for path in &args.faults {
        fault_texts.push(fs::read_to_string(path).with_context(|| format!("can't read {}", path.display()))?);
    }
    let windows = parse_windows(&fault_texts)?;
    print_report(&tally(&requests, &windows), &windows);
    Ok(())
}

/// Reads the request CSV. Columns are found by name; `attempts` is optional (older CSVs: 1 try each).
fn parse_requests(text: &str) -> anyhow::Result<Vec<Request>> {
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().context("file is empty")?.split(',').collect();
    let column = |name: &str| header.iter().position(|c| *c == name).with_context(|| format!("column `{name}` is missing"));
    let (i_ts, i_lat, i_status) = (column("timestamp")?, column("latency_us")?, column("status")?);
    let i_attempts = header.iter().position(|c| *c == "attempts");

    let mut requests = Vec::new();
    for (n, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let line_no = n + 2;
        let f: Vec<&str> = line.split(',').collect();
        let field = |i: usize| f.get(i).copied().with_context(|| format!("line {line_no}: too few columns"));
        let sent_us = field(i_ts)?.parse::<u128>().with_context(|| format!("line {line_no}: bad timestamp"))? * 1000;
        let latency_us: u64 = field(i_lat)?.parse().with_context(|| format!("line {line_no}: bad latency_us"))?;
        let status = field(i_status)?;
        if !matches!(status, "ok" | "timeout" | "error") {
            bail!("line {line_no}: unknown status `{status}`");
        }
        let attempts = match i_attempts {
            Some(i) => field(i)?.parse().with_context(|| format!("line {line_no}: bad attempts"))?,
            None => 1,
        };
        requests.push(Request { from_us: sent_us, to_us: sent_us + latency_us as u128, ok: status == "ok", attempts, latency_us });
    }
    Ok(requests)
}

/// Turns fault-log rows into time windows. A start without an end (the run stopped first) lasts
/// for its planned duration; an end closes the matching start (same node and fault_id).
fn parse_windows(texts: &[String]) -> anyhow::Result<Vec<Window>> {
    let mut windows = Vec::new();
    for text in texts {
        let mut lines = text.lines();
        let Some(header) = lines.next() else { continue }; // an empty file: no faults
        let header: Vec<&str> = header.split(',').collect();
        let column = |name: &str| header.iter().position(|c| *c == name).with_context(|| format!("fault log: column `{name}` is missing"));
        let (i_node, i_event, i_id, i_dur, i_ts) = (column("node_id")?, column("event")?, column("fault_id")?, column("duration_ms")?, column("wall_ts_us")?);

        let mut open: BTreeMap<(u32, String), usize> = BTreeMap::new(); // (node, fault_id) -> index in `windows`
        for line in lines.filter(|l| !l.trim().is_empty()) {
            let f: Vec<&str> = line.split(',').collect();
            let get = |i: usize| f.get(i).copied().unwrap_or("");
            let kind = match get(i_event) {
                "MUTE_START" | "MUTE_END" => Scenario::NodeDown,
                "CUT_START" | "CUT_END" => Scenario::NetworkCut,
                _ => continue, // PEER_DOWN / PEER_UP: what other nodes noticed, not when the fault was
            };
            let node: u32 = get(i_node).parse().context("fault log: bad node_id")?;
            let ts: u128 = get(i_ts).parse().context("fault log: bad wall_ts_us")?;
            let key = (node, get(i_id).to_string());
            if get(i_event).ends_with("_START") {
                let planned_us = get(i_dur).parse::<u128>().unwrap_or(0) * 1000;
                open.insert(key, windows.len());
                windows.push(Window { kind, node, from_us: ts, to_us: ts + planned_us });
            } else if let Some(i) = open.remove(&key) {
                windows[i].to_us = ts; // the real end
            }
        }
    }
    windows.sort_by_key(|w| w.from_us);
    Ok(windows)
}

/// The scenario a request ran in: the worst fault its time overlaps.
fn scenario_of(r: &Request, windows: &[Window]) -> Scenario {
    windows
        .iter()
        .filter(|w| r.from_us <= w.to_us && w.from_us <= r.to_us) // the two time spans overlap
        .map(|w| w.kind)
        .max() // NetworkCut > NodeDown > NoFailure
        .unwrap_or(Scenario::NoFailure)
}

fn tally(requests: &[Request], windows: &[Window]) -> BTreeMap<Scenario, Tally> {
    let mut by: BTreeMap<Scenario, Tally> = BTreeMap::new();
    for r in requests {
        let t = by.entry(scenario_of(r, windows)).or_default();
        t.requests += 1;
        if r.ok {
            t.ok += 1;
            t.latencies_us.push(r.latency_us);
            if r.attempts > 1 {
                t.retried += 1; // failover worked: a later attempt got the plan
            }
        } else {
            t.lost += 1; // no plan after all attempts
        }
    }
    for t in by.values_mut() {
        t.latencies_us.sort_unstable();
    }
    by
}

fn print_report(by: &BTreeMap<Scenario, Tally>, windows: &[Window]) {
    let mutes = windows.iter().filter(|w| w.kind == Scenario::NodeDown).count();
    let cuts = windows.iter().filter(|w| w.kind == Scenario::NetworkCut).count();
    println!("reliability ({mutes} node-down and {cuts} network-cut windows in the fault logs):");
    println!("  {:<14} {:>9} {:>9} {:>9} {:>7} {:>8} {:>6} {:>10} {:>10}", "scenario", "requests", "ok", "success", "failed", "retried", "lost", "p50", "p95");
    let mut all = Tally::default();
    for (scenario, t) in by {
        print_row(scenario.label(), t);
        all.requests += t.requests;
        all.ok += t.ok;
        all.retried += t.retried;
        all.lost += t.lost;
        all.latencies_us.extend(&t.latencies_us);
    }
    all.latencies_us.sort_unstable();
    print_row("all", &all);
    println!("  failed = needed a retry or was lost; retried = got a plan after a retry; lost = no plan after all attempts");
}

fn print_row(label: &str, t: &Tally) {
    let success = if t.requests == 0 { 0.0 } else { 100.0 * t.ok as f64 / t.requests as f64 };
    let pct = |p: f64| {
        if t.latencies_us.is_empty() {
            "-".to_string()
        } else {
            let rank = (p / 100.0 * t.latencies_us.len() as f64).ceil() as usize; // nearest rank, as in stats
            format!("{:.2} ms", t.latencies_us[rank.clamp(1, t.latencies_us.len()) - 1] as f64 / 1000.0)
        }
    };
    println!(
        "  {:<14} {:>9} {:>9} {:>8.2}% {:>7} {:>8} {:>6} {:>10} {:>10}",
        label, t.requests, t.ok, success, t.retried + t.lost, t.retried, t.lost, pct(50.0), pct(95.0)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const CSV: &str = "run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned,stage,attempts,no_plan_replies\n";
    const FAULTS: &str = "run_id,node_id,event,fault_id,peer_id,duration_ms,wall_ts_us\n";

    /// A request sent at `sent_ms` (wall clock, ms) taking `latency_ms`.
    fn row(sent_ms: u64, latency_ms: u64, status: &str, attempts: u32) -> String {
        format!("r,{sent_ms},1,id{sent_ms},{},1,{status},false,1,{attempts},2\n", latency_ms * 1000)
    }

    #[test]
    fn requests_are_split_by_the_fault_they_overlap() {
        // Node 2 muted from 10 s to 20 s; node 3 cut from 30 s to 35 s (times in µs in the logs).
        let mute = format!("{FAULTS}r,2,MUTE_START,f1,,10000,10000000\nr,2,MUTE_END,f1,,,20000000\n");
        let cut = format!("{FAULTS}r,3,CUT_START,cut-0,,5000,30000000\nr,3,CUT_END,cut-0,,,35000000\n");
        let windows = parse_windows(&[mute, cut]).unwrap();
        assert_eq!(windows.len(), 2);

        let csv = format!(
            "{CSV}{}{}{}{}{}",
            row(1_000, 5, "ok", 1),       // before any fault
            row(9_000, 2_500, "ok", 2),    // sent before the mute, still running when it starts -> node down
            row(15_000, 5, "timeout", 3),  // during the mute, lost
            row(31_000, 5, "ok", 1),       // during the cut
            row(40_000, 5, "error", 3),    // after everything, lost
        );
        let by = tally(&parse_requests(&csv).unwrap(), &windows);
        let get = |s| &by[&s];
        assert_eq!((get(Scenario::NoFailure).requests, get(Scenario::NoFailure).lost), (2, 1));
        assert_eq!((get(Scenario::NodeDown).requests, get(Scenario::NodeDown).retried, get(Scenario::NodeDown).lost), (2, 1, 1));
        assert_eq!((get(Scenario::NetworkCut).requests, get(Scenario::NetworkCut).ok), (1, 1));
    }

    #[test]
    fn a_fault_with_no_end_lasts_its_planned_duration() {
        let mute = format!("{FAULTS}r,2,MUTE_START,f1,,4000,10000000\n"); // run stopped before MUTE_END
        let w = &parse_windows(&[mute]).unwrap()[0];
        assert_eq!((w.from_us, w.to_us), (10_000_000, 14_000_000));
    }

    #[test]
    fn peer_down_rows_do_not_count_as_faults() {
        let observed = format!("{FAULTS}r,1,PEER_DOWN,,2,,10000000\nr,1,PEER_UP,,2,,20000000\n");
        assert!(parse_windows(&[observed]).unwrap().is_empty());
    }

    #[test]
    fn a_cut_wins_over_a_mute_when_both_overlap() {
        let windows = vec![
            Window { kind: Scenario::NodeDown, node: 2, from_us: 0, to_us: 100 },
            Window { kind: Scenario::NetworkCut, node: 3, from_us: 50, to_us: 150 },
        ];
        let r = Request { from_us: 60, to_us: 70, ok: true, attempts: 1, latency_us: 10 };
        assert_eq!(scenario_of(&r, &windows), Scenario::NetworkCut);
    }

    #[test]
    fn older_csvs_without_attempts_still_work() {
        let csv = "run_id,timestamp,client_id,request_id,latency_us,executor_node,status\nr,1,1,a,1000,1,ok\nr,2,1,b,1000,,timeout\n";
        let by = tally(&parse_requests(csv).unwrap(), &[]);
        let t = &by[&Scenario::NoFailure];
        assert_eq!((t.requests, t.ok, t.retried, t.lost), (2, 1, 0, 1));
    }
}
