//! Summary script (owner: P6): turns a loadgen CSV into a few numbers:
//! request counts, mean / p50 / p95 / p99 latency, and throughput.
//!
//! Run:
//!   cargo run -p loadgen --bin stats -- results/run-123.csv
//!   cargo run -p loadgen --bin stats -- results/a.csv results/b.csv   (several runs, one after another)

use std::collections::{BTreeMap, BTreeSet}; // a map and a set that keep their keys sorted (node 1, node 2, node 3)
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, bail}; // Context adds a readable message to an error; bail! returns an error right away
use clap::Parser; // reads the file names from the command line

#[derive(Parser)]
#[command(about = "Summarize loadgen CSVs: counts, mean/p50/p95/p99 latency, throughput")]
struct Args {
    /// One or more CSV files written by loadgen.
    #[arg(required = true)]
    csv: Vec<PathBuf>,
}

/// Everything we count while reading one CSV (or one stage of it).
#[derive(Default)]
struct Summary {
    total: u64,                      // every row
    ok: u64,                         // status = ok
    timeout: u64,                    // status = timeout
    error: u64,                      // status = error
    has_assigned: bool,              // does this CSV have the `assigned` column? (older ones don't)
    unassigned: u64,                 // ok rows whose plan was empty (nothing was free)
    by_node: BTreeMap<String, u64>,  // ok rows per executor_node
    latencies_us: Vec<u64>,          // latencies of ok rows only, sorted fastest first
    duration_s: f64,                 // first request sent -> last reply received
    first_send_us: Option<u64>,      // earliest send time seen so far (unix µs)
    last_reply_us: u64,              // latest reply time seen so far (send time + latency)
    clients: BTreeSet<String>,       // client_ids seen, so a stage can say how many clients it had
    stages: BTreeMap<u32, Summary>,  // ramp runs: the same numbers per stage (empty for CSVs without a `stage` column)
}

/// The fields of one CSV row that the stats use.
struct Rec<'a> {
    ts_ms: u64,       // when the request was sent (unix ms)
    latency_us: u64,  // how long it took
    status: &'a str,  // "ok", "timeout" or "error" (checked before a Rec is built)
    node: &'a str,    // executor_node
    client: &'a str,  // client_id
    unassigned: bool, // assigned = false: the plan was empty
}

impl Summary {
    /// Counts one row.
    fn add(&mut self, r: &Rec) {
        self.total += 1;
        // Run duration: from the first request sent to the last reply received (send time + its latency).
        let sent_us = r.ts_ms * 1000;
        self.first_send_us = Some(self.first_send_us.map_or(sent_us, |first| first.min(sent_us)));
        self.last_reply_us = self.last_reply_us.max(sent_us + r.latency_us);
        self.clients.insert(r.client.to_string());
        match r.status {
            "ok" => {
                self.ok += 1;
                self.latencies_us.push(r.latency_us); // only successful requests count toward latency
                *self.by_node.entry(r.node.to_string()).or_default() += 1; // which node answered
            }
            "timeout" => self.timeout += 1,
            _ => self.error += 1, // the only other status summarize() lets through
        }
        if r.unassigned {
            self.unassigned += 1; // the node planned but nothing was free
        }
    }

    /// Called once every row has been added: sorts latencies and works out the duration.
    fn finish(&mut self) {
        self.latencies_us.sort_unstable(); // percentiles need the values in order
        if let Some(first) = self.first_send_us {
            self.duration_s = (self.last_reply_us - first) as f64 / 1_000_000.0; // microseconds -> seconds
        }
        for stage in self.stages.values_mut() {
            stage.finish();
        }
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    for path in &args.csv {
        let text = fs::read_to_string(path).with_context(|| format!("can't read {}", path.display()))?; // whole file into memory
        let summary = summarize(&text).with_context(|| format!("bad CSV {}", path.display()))?;
        println!("== {}", path.display());
        print_summary(&summary);
    }
    Ok(())
}

/// Reads the CSV text and counts everything. Columns are found by name, so adding a column later doesn't break this.
fn summarize(text: &str) -> anyhow::Result<Summary> {
    let mut lines = text.lines(); // one item per row
    let header: Vec<&str> = lines.next().context("file is empty")?.split(',').collect(); // first row = column names
    let column = |name: &str| header.iter().position(|c| *c == name).with_context(|| format!("column `{name}` is missing"));
    let i_ts = column("timestamp")?; // where each column is, counting from 0
    let i_latency = column("latency_us")?;
    let i_node = column("executor_node")?;
    let i_status = column("status")?;
    let i_client = column("client_id")?;
    let i_assigned = header.iter().position(|c| *c == "assigned"); // optional: added Oct 21
    let i_stage = header.iter().position(|c| *c == "stage"); // optional: ramp runs, added Oct 26

    let mut s = Summary { has_assigned: i_assigned.is_some(), ..Default::default() };

    for (n, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue; // skip blank lines (e.g. at the end of the file)
        }
        let line_no = n + 2; // +1 because enumerate starts at 0, +1 for the header: matches the line number in an editor
        let fields: Vec<&str> = line.split(',').collect(); // our fields never contain commas, so a plain split is enough
        let field = |i: usize| fields.get(i).copied().with_context(|| format!("line {line_no}: too few columns"));

        let status = field(i_status)?;
        if !matches!(status, "ok" | "timeout" | "error") {
            bail!("line {line_no}: unknown status `{status}`");
        }
        let rec = Rec {
            ts_ms: field(i_ts)?.parse().with_context(|| format!("line {line_no}: bad timestamp"))?,
            latency_us: field(i_latency)?.parse().with_context(|| format!("line {line_no}: bad latency_us"))?,
            status,
            node: field(i_node)?,
            client: field(i_client)?,
            unassigned: i_assigned.is_some_and(|i| fields.get(i) == Some(&"false")),
        };
        s.add(&rec); // the whole run
        if let Some(i) = i_stage {
            let stage: u32 = field(i)?.parse().with_context(|| format!("line {line_no}: bad stage"))?;
            s.stages.entry(stage).or_default().add(&rec); // and its stage
        }
    }

    s.finish();
    Ok(s)
}

/// Prints the summary in a form you can paste into the report.
fn print_summary(s: &Summary) {
    println!("requests:    {}  ({} ok, {} timeout, {} error)", s.total, s.ok, s.timeout, s.error);
    if s.has_assigned {
        println!("assigned:    {} got a resource, {} got nothing (empty plan)", s.ok - s.unassigned, s.unassigned);
    }

    if s.latencies_us.is_empty() {
        println!("latency:     n/a (no ok requests)");
        return; // nothing else makes sense without successful requests
    }
    let lat = &s.latencies_us;
    println!("latency (ok requests only):");
    println!("  mean       {}", ms(mean(lat)));
    println!("  p50        {}", ms(percentile(lat, 50.0) as f64)); // typical request
    println!("  p95        {}", ms(percentile(lat, 95.0) as f64)); // slow tail: only 5% were slower
    println!("  p99        {}", ms(percentile(lat, 99.0) as f64)); // worst cases: only 1% were slower
    println!("  min / max  {} / {}", ms(lat[0] as f64), ms(lat[lat.len() - 1] as f64));

    if s.duration_s > 0.0 {
        println!("throughput:  {:.1} ok requests/s over {:.2} s", s.ok as f64 / s.duration_s, s.duration_s);
    }
    let nodes: Vec<String> = s.by_node.iter().map(|(node, count)| format!("node {node}: {count}")).collect();
    println!("by node:     {}", nodes.join(", "));

    if s.stages.len() > 1 {
        print_stages(&s.stages); // ramp run: show how latency and throughput change as clients are added
    }
}

/// One line per ramp stage, so the effect of adding clients is visible at a glance.
fn print_stages(stages: &BTreeMap<u32, Summary>) {
    println!("per stage (latency of ok requests):");
    println!("  {:>5}  {:>7}  {:>8}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}  {:>8}", "stage", "clients", "requests", "ok", "mean", "p50", "p95", "p99", "ok/s");
    for (stage, st) in stages {
        let lat = &st.latencies_us;
        let pct = |p: f64| if lat.is_empty() { "-".to_string() } else { ms(percentile(lat, p) as f64) }; // "-" if nothing succeeded
        let mean_ms = if lat.is_empty() { "-".to_string() } else { ms(mean(lat)) };
        let ok_per_s = if st.duration_s > 0.0 { st.ok as f64 / st.duration_s } else { 0.0 };
        println!(
            "  {:>5}  {:>7}  {:>8}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}  {:>8.1}",
            stage, st.clients.len(), st.total, st.ok, mean_ms, pct(50.0), pct(95.0), pct(99.0), ok_per_s
        );
    }
}

/// Average of the values.
fn mean(values: &[u64]) -> f64 {
    values.iter().sum::<u64>() as f64 / values.len() as f64
}

/// Nearest-rank percentile, the method in the Step A plan: the value at position ceil(p% × n), counting from 1.
/// `sorted` must be sorted and not empty.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize; // e.g. p95 of 10 values -> rank 10
    sorted[rank.clamp(1, sorted.len()) - 1] // -1 because Rust counts from 0
}

/// Formats microseconds as milliseconds, e.g. 14845 -> "14.85 ms".
fn ms(us: f64) -> String {
    format!("{:.2} ms", us / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example from the Step A plan: 10 latencies (ms) 12, 13, 13, 14, 15, 15, 16, 18, 25, 90.
    /// Expected: mean 23.1, median 15, p95 90.
    #[test]
    fn plan_worked_example() {
        let mut csv = String::from("run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned\n");
        for (i, latency_ms) in [12, 13, 13, 14, 15, 15, 16, 18, 25, 90].into_iter().enumerate() {
            csv += &format!("t,{},1,id{i},{},1,ok,true\n", 1000 + i, latency_ms * 1000);
        }
        let s = summarize(&csv).unwrap();
        assert_eq!(s.ok, 10);
        assert!((mean(&s.latencies_us) - 23_100.0).abs() < 1e-9); // 23.1 ms
        assert_eq!(percentile(&s.latencies_us, 50.0), 15_000); // 15 ms
        assert_eq!(percentile(&s.latencies_us, 95.0), 90_000); // 90 ms
    }

    #[test]
    fn failures_are_counted_but_not_timed() {
        let csv = "run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned\n\
                   t,1000,1,a,2000,1,ok,false\n\
                   t,1000,2,b,5000000,,timeout,\n\
                   t,1001,3,c,300,,error,\n";
        let s = summarize(csv).unwrap();
        assert_eq!((s.ok, s.timeout, s.error, s.unassigned), (1, 1, 1, 1));
        assert_eq!(s.latencies_us, vec![2000]); // the 5 s timeout doesn't distort the latency numbers
    }

    #[test]
    fn ramp_runs_are_split_by_stage() {
        let csv = "run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned,stage\n\
                   t,1000,1,a,1000,1,ok,true,1\n\
                   t,1001,1,b,1000,1,ok,true,1\n\
                   t,2000,1,c,3000,1,ok,true,2\n\
                   t,2000,2,d,5000,1,ok,false,2\n\
                   t,2001,2,e,5000000,,timeout,,2\n";
        let s = summarize(csv).unwrap();
        assert_eq!(s.total, 5); // the whole run still adds up
        assert_eq!(s.stages.len(), 2);
        let (one, two) = (&s.stages[&1], &s.stages[&2]);
        assert_eq!((one.clients.len(), one.total, one.ok), (1, 2, 2)); // stage 1: 1 client, 2 requests
        assert_eq!((two.clients.len(), two.total, two.ok, two.timeout), (2, 3, 2, 1)); // stage 2: 2 clients
        assert_eq!(two.latencies_us, vec![3000, 5000]); // per-stage latency, timeout left out
    }
}
