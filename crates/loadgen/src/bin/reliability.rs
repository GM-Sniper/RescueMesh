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
//! Definitions (Step B plan, P6 review note: "confirm what counts as lost"; Step C: DESIGN_LOG.md,
//! "P6: Failure metrics"):
//! - ok      = the request got a plan: P1's outcome `committed` or `duplicate` (a retry found the
//!             stored plan). CSVs without the outcome column: status ok;
//! - failed  = at least one attempt got no plan: the request needed a retry, or was lost;
//! - retried = needed more than one attempt but got a plan in the end (failover worked);
//! - lost    = no plan after all attempts (outcome `failed`, status timeout or error);
//! - reliability % = ok / requests.
//!
//! Scenario of a request: whatever was going on while it ran, from when it was sent until its
//! reply (or until it gave up). If its time overlaps a cut it counts as "network cut"; else if it
//! overlaps a mute, "one node down"; otherwise "no failure".
//!
//! Every lost request is listed with its failure case (the three cases the client handles):
//! overlaps a cut -> network cut; overlaps a mute -> winner died after taking the request;
//! no failure and timed out -> slow request; anything else -> unexplained (check the node logs).

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
    id: String,        // request_id, to list the lost ones
    from_us: u128,
    to_us: u128,
    ok: bool,          // got a plan: committed or duplicate
    duplicate: bool,   // the plan was already stored: a retry found it (P1's outcome column)
    no_resource: bool, // got a plan, but nothing was free (assigned = false)
    status: String,    // ok / timeout / error: how the last attempt ended
    attempts: u32,
    latency_us: u64,
}

/// Counts for one scenario.
#[derive(Default)]
struct Tally {
    requests: u64,
    ok: u64,
    duplicates: u64,  // ok, and the plan was found already stored
    no_resource: u64, // ok, but nothing was free
    retried: u64,     // ok, but after more than one attempt
    lost: u64,        // no plan after all attempts
    latencies_us: Vec<u64>,
}

/// The failure case a lost request is put in (DESIGN_LOG.md, "Which failure case a lost request was").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Case {
    NetworkCut,
    WinnerDied,
    SlowRequest,
    Unexplained,
}

impl Case {
    fn label(self) -> &'static str {
        match self {
            Case::NetworkCut => "network cut",
            Case::WinnerDied => "winner died after taking the request",
            Case::SlowRequest => "slow request",
            Case::Unexplained => "unexplained (check the node logs)",
        }
    }
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
    print_lost(&requests, &windows, csv.lines().next().is_some_and(|h| h.split(',').any(|c| c == "outcome")));
    Ok(())
}

/// Reads the request CSV. Columns are found by name; `attempts`, `assigned` and `outcome` are
/// optional (older CSVs: 1 try each, no resource info, outcome derived from status).
fn parse_requests(text: &str) -> anyhow::Result<Vec<Request>> {
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().context("file is empty")?.split(',').collect();
    let column = |name: &str| header.iter().position(|c| *c == name).with_context(|| format!("column `{name}` is missing"));
    let (i_ts, i_lat, i_status, i_id) = (column("timestamp")?, column("latency_us")?, column("status")?, column("request_id")?);
    let i_attempts = header.iter().position(|c| *c == "attempts");
    let i_assigned = header.iter().position(|c| *c == "assigned"); // true = a resource was given
    let i_outcome = header.iter().position(|c| *c == "outcome");   // P1's column: committed / duplicate / failed

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
        // Outcome: P1's column when the CSV has it, otherwise derived from status (plan task 9:
        // "P6 derives it from status"). The derived one can't see duplicates: they count as committed.
        let outcome = match i_outcome {
            Some(i) => field(i)?,
            None if status == "ok" => "committed",
            None => "failed",
        };
        if !matches!(outcome, "committed" | "duplicate" | "failed") {
            bail!("line {line_no}: unknown outcome `{outcome}`");
        }
        let ok = outcome != "failed";
        let assigned = match i_assigned {
            Some(i) => field(i)? == "true",
            None => true, // older CSVs: unknown, so don't report a shortage that may not exist
        };
        requests.push(Request {
            id: field(i_id)?.to_string(),
            from_us: sent_us,
            to_us: sent_us + latency_us as u128,
            ok,
            duplicate: outcome == "duplicate",
            no_resource: ok && !assigned, // answered, but every resource was taken
            status: status.to_string(),
            attempts,
            latency_us,
        });
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
            t.duplicates += r.duplicate as u64;    // a retry found the plan already stored
            t.no_resource += r.no_resource as u64; // answered, but nothing was free
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
    println!("  {:<14} {:>9} {:>9} {:>11} {:>7} {:>8} {:>6} {:>10} {:>10}", "scenario", "requests", "ok", "reliability", "failed", "retried", "lost", "p50", "p95");
    let mut all = Tally::default();
    for (scenario, t) in by {
        print_row(scenario.label(), t);
        all.requests += t.requests;
        all.ok += t.ok;
        all.duplicates += t.duplicates;
        all.no_resource += t.no_resource;
        all.retried += t.retried;
        all.lost += t.lost;
        all.latencies_us.extend(&t.latencies_us);
    }
    all.latencies_us.sort_unstable();
    print_row("all", &all);
    println!("  ok = got a plan: {} committed + {} duplicate (a retry found the stored plan)", all.ok - all.duplicates, all.duplicates);
    println!("  of the ok ones, {} got no resource (all taken): answered, so not lost", all.no_resource);
    println!("  failed = needed a retry or was lost; retried = got a plan after a retry; lost = no plan after all attempts");
}

/// The failure case of a lost request, from the faults its time overlaps and how it ended.
fn case_of(r: &Request, windows: &[Window]) -> Case {
    match scenario_of(r, windows) {
        Scenario::NetworkCut => Case::NetworkCut,
        Scenario::NodeDown => Case::WinnerDied, // the muted node had it and never answered
        Scenario::NoFailure if r.status == "timeout" => Case::SlowRequest,
        Scenario::NoFailure => Case::Unexplained, // an error with no failure going on
    }
}

/// Every lost request with its failure case ("each one needs an explanation", Step C plan).
fn print_lost(requests: &[Request], windows: &[Window], has_outcome: bool) {
    let t0 = requests.iter().map(|r| r.from_us).min().unwrap_or(0); // times from the first request sent
    let lost: Vec<(Case, &Request)> = requests.iter().filter(|r| !r.ok).map(|r| (case_of(r, windows), r)).collect();
    if !has_outcome {
        println!("  (no outcome column in this CSV: outcome derived from status, so duplicates count as committed)");
    }
    if lost.is_empty() {
        println!("lost requests: 0");
        return;
    }
    let mut by_case: BTreeMap<Case, usize> = BTreeMap::new();
    for (case, _) in &lost {
        *by_case.entry(*case).or_default() += 1;
    }
    println!("lost requests: {}, by failure case:", lost.len());
    for (case, n) in &by_case {
        println!("  {:<40} {:>6}", case.label(), n);
    }
    const SHOWN: usize = 100; // enough to explain each one; the rest are in the CSV (outcome failed)
    println!("  {:<40} {:>9} {:>8} {:>8}  {}", "request_id", "sent", "attempts", "status", "case");
    for (case, r) in lost.iter().take(SHOWN) {
        let sent = format!("{:.2} s", (r.from_us - t0) as f64 / 1e6);
        println!("  {:<40} {:>9} {:>8} {:>8}  {}", r.id, sent, r.attempts, r.status, case.label());
    }
    if lost.len() > SHOWN {
        println!("  ... and {} more (rows with no plan in the CSV)", lost.len() - SHOWN);
    }
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
        "  {:<14} {:>9} {:>9} {:>10.2}% {:>7} {:>8} {:>6} {:>10} {:>10}",
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
        let r = Request { id: "a".into(), from_us: 60, to_us: 70, ok: true, duplicate: false, no_resource: false, status: "ok".into(), attempts: 1, latency_us: 10 };
        assert_eq!(scenario_of(&r, &windows), Scenario::NetworkCut);
    }

    #[test]
    fn worked_example_9962_of_10000_is_99_62_percent_with_38_lost() {
        // The Step C plan's example, as a hand-made CSV: 10,000 requests, 38 got no plan.
        let mut csv = String::from(CSV);
        for i in 0..10_000u64 {
            let status = if i < 38 { "timeout" } else { "ok" };
            csv += &row(1_000 + i, 5, status, 1);
        }
        let requests = parse_requests(&csv).unwrap();
        let by = tally(&requests, &[]);
        let t = &by[&Scenario::NoFailure];
        assert_eq!((t.requests, t.ok, t.lost), (10_000, 9_962, 38));
        let reliability = 100.0 * t.ok as f64 / t.requests as f64;
        assert!((reliability - 99.62).abs() < 1e-9, "got {reliability}");
        // With no failure going on, a timeout is a slow request.
        assert!(requests.iter().filter(|r| !r.ok).all(|r| case_of(r, &[]) == Case::SlowRequest));
    }

    #[test]
    fn outcome_column_counts_duplicates_as_ok_and_failed_as_lost() {
        let csv = "run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned,stage,attempts,no_plan_replies,outcome\n\
                   r,1,1,a,1000,1,ok,true,1,1,0,committed\n\
                   r,2,1,b,1000,2,ok,true,1,2,0,duplicate\n\
                   r,3,1,c,1000,3,ok,false,1,1,0,committed\n\
                   r,4,1,d,1000,,timeout,false,1,3,0,failed\n";
        let by = tally(&parse_requests(csv).unwrap(), &[]);
        let t = &by[&Scenario::NoFailure];
        assert_eq!((t.requests, t.ok, t.duplicates, t.no_resource, t.lost), (4, 3, 1, 1, 1));
    }

    #[test]
    fn a_lost_request_gets_the_case_of_the_failure_it_ran_in() {
        let windows = vec![
            Window { kind: Scenario::NodeDown, node: 2, from_us: 10_000_000, to_us: 20_000_000 },
            Window { kind: Scenario::NetworkCut, node: 3, from_us: 30_000_000, to_us: 38_000_000 },
        ];
        let csv = format!(
            "{CSV}{}{}{}{}",
            row(12_000, 6_000, "timeout", 3), // during the mute
            row(31_000, 10, "error", 3),      // during the cut
            row(50_000, 6_000, "timeout", 3), // no failure: slow
            row(60_000, 10, "error", 3),      // no failure, fast error: unexplained
        );
        let cases: Vec<Case> = parse_requests(&csv).unwrap().iter().map(|r| case_of(r, &windows)).collect();
        assert_eq!(cases, [Case::WinnerDied, Case::NetworkCut, Case::SlowRequest, Case::Unexplained]);
    }

    #[test]
    fn older_csvs_without_attempts_still_work() {
        let csv = "run_id,timestamp,client_id,request_id,latency_us,executor_node,status\nr,1,1,a,1000,1,ok\nr,2,1,b,1000,,timeout\n";
        let by = tally(&parse_requests(csv).unwrap(), &[]);
        let t = &by[&Scenario::NoFailure];
        assert_eq!((t.requests, t.ok, t.retried, t.lost), (2, 1, 0, 1));
    }
}
