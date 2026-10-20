//! Summary script (owner: P6): turns a loadgen CSV into a few numbers:
//! request counts, mean / p50 / p95 / p99 latency, and throughput.
//!
//! Run:
//!   cargo run -p loadgen --bin stats -- results/run-123.csv
//!   cargo run -p loadgen --bin stats -- results/a.csv results/b.csv   (several runs, one after another)

use std::collections::BTreeMap; // a map that keeps its keys sorted (node 1, node 2, node 3)
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

/// Everything we count while reading one CSV.
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
    let i_assigned = header.iter().position(|c| *c == "assigned"); // optional

    let mut s = Summary { has_assigned: i_assigned.is_some(), ..Default::default() };
    let mut start_us = u64::MAX; // earliest send time seen so far
    let mut end_us = 0; // latest reply time seen so far

    for (n, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue; // skip blank lines (e.g. at the end of the file)
        }
        let line_no = n + 2; // +1 because enumerate starts at 0, +1 for the header: matches the line number in an editor
        let fields: Vec<&str> = line.split(',').collect(); // our fields never contain commas, so a plain split is enough
        let field = |i: usize| fields.get(i).copied().with_context(|| format!("line {line_no}: too few columns"));

        let ts_ms: u64 = field(i_ts)?.parse().with_context(|| format!("line {line_no}: bad timestamp"))?;
        let latency_us: u64 = field(i_latency)?.parse().with_context(|| format!("line {line_no}: bad latency_us"))?;
        s.total += 1;

        // Run duration: from the first request sent to the last reply received (send time + its latency).
        start_us = start_us.min(ts_ms * 1000);
        end_us = end_us.max(ts_ms * 1000 + latency_us);

        match field(i_status)? {
            "ok" => {
                s.ok += 1;
                s.latencies_us.push(latency_us); // only successful requests count toward latency
                *s.by_node.entry(field(i_node)?.to_string()).or_default() += 1; // which node answered
            }
            "timeout" => s.timeout += 1,
            "error" => s.error += 1,
            other => bail!("line {line_no}: unknown status `{other}`"),
        }
        if i_assigned.is_some_and(|i| fields.get(i) == Some(&"false")) {
            s.unassigned += 1; // the node planned but nothing was free
        }
    }

    s.latencies_us.sort_unstable(); // percentiles need the values in order
    if s.total > 0 {
        s.duration_s = (end_us - start_us) as f64 / 1_000_000.0; // microseconds -> seconds
    }
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
}
