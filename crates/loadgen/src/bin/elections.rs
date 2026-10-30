//! Election metrics (owner: P6): reads the nodes' election logs (P3's `logs/elections-node<N>.csv`)
//! and prints how elections behaved and how the work was shared between nodes.
//!
//!   cargo run --release -p loadgen --bin elections -- results/<run_id>/logs/elections-node1.csv results/<run_id>/logs/elections-node2.csv results/<run_id>/logs/elections-node3.csv
//!   cargo run --release -p loadgen --bin elections -- --window-secs 5 <files...>
//!
//! Every node logs its own view of every election, so one election appears once per node.
//! An election is identified by (request_id, round): round 0 is the first election for a request,
//! round 1+ a re-election after the winner went down.
//!
//! Printed:
//! - elections: how many, how many were re-elections, splits, late bids
//! - duration (per node view): mean / p50 / p95 / p99, and how often a node waited the full bid timeout
//! - messages: bid calls sent per election, all nodes together (3 nodes x 2 peers = 6, plus broadcast retries)
//! - load distribution: wins and share per node, overall and per time window

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::Parser;

#[derive(Parser)]
#[command(about = "Summarize the nodes' election logs: duration, splits, messages and win share per node")]
struct Args {
    /// Width of each time window for the win-share table, in seconds.
    #[arg(long, default_value_t = 10.0)]
    window_secs: f64,

    /// The nodes' election logs (logs/elections-node<N>.csv), one per node.
    #[arg(required = true)]
    csv: Vec<PathBuf>,
}

/// One row of an election log: one node's view of one election.
#[derive(Debug, Clone)]
struct ElectionRow {
    node_id: u32,          // the node that wrote the row
    request_id: String,
    round: u32,
    bids: usize,           // how many bids this node held (own included)
    winner: Option<u32>,   // empty in the file = no bid was ready
    duration_us: u64,      // election start -> decision, on this node
    waited_full_t: bool,   // waited the whole bid timeout
    late_bids: u32,        // bids that came after the decision
    splits: u32,           // announcements from a different winner
    bid_calls_sent: u32,   // bid messages this node sent, retries included
    wall_ts_us: u128,      // when this node decided
}

/// One election, merged from every node's row about it.
struct Election {
    winner: Option<u32>,   // the winner most nodes reported (ties: lowest node ID)
    disagreed: bool,       // the nodes reported different winners
    first_ts_us: u128,     // the earliest decision among the nodes
    bid_calls_sent: u32,   // all nodes together
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.window_secs <= 0.0 {
        bail!("--window-secs must be more than 0");
    }
    let mut rows = Vec::new();
    for path in &args.csv {
        let text = fs::read_to_string(path).with_context(|| format!("can't read {}", path.display()))?;
        rows.extend(parse(&text).with_context(|| format!("bad election log {}", path.display()))?);
    }
    if rows.is_empty() {
        println!("no elections recorded (only headers): is the election wired into the Submit handler yet?");
        return Ok(());
    }
    print_report(&rows, args.window_secs);
    Ok(())
}

/// Reads one election log. Columns are found by name, so a column added later doesn't break this.
fn parse(text: &str) -> anyhow::Result<Vec<ElectionRow>> {
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().context("file is empty")?.split(',').collect();
    let column = |name: &str| header.iter().position(|c| *c == name).with_context(|| format!("column `{name}` is missing"));
    let (i_node, i_req, i_round, i_bids, i_winner) = (column("node_id")?, column("request_id")?, column("round")?, column("bids")?, column("winner")?);
    let (i_dur, i_full, i_late, i_split) = (column("duration_us")?, column("waited_full_t")?, column("late_bids")?, column("split_decision")?);
    let (i_calls, i_ts) = (column("bid_calls_sent")?, column("wall_ts_us")?);

    let mut rows = Vec::new();
    for (n, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let line_no = n + 2; // matches the line number in an editor (header is line 1)
        // P3 quotes a field only if it contains a comma; run_id and request_id (a Uuid) never do.
        let fields: Vec<&str> = line.split(',').collect();
        let field = |i: usize| fields.get(i).copied().with_context(|| format!("line {line_no}: too few columns"));
        let num = |i: usize, what: &str| -> anyhow::Result<u64> {
            field(i)?.parse().with_context(|| format!("line {line_no}: bad {what}"))
        };
        let bids = field(i_bids)?;
        let winner = field(i_winner)?;
        rows.push(ElectionRow {
            node_id: num(i_node, "node_id")? as u32,
            request_id: field(i_req)?.to_string(),
            round: num(i_round, "round")? as u32,
            bids: if bids.is_empty() { 0 } else { bids.split(';').count() }, // "1:0.2;2:0.45" -> 2 bids
            winner: if winner.is_empty() { None } else { Some(winner.parse().with_context(|| format!("line {line_no}: bad winner"))?) },
            duration_us: num(i_dur, "duration_us")?,
            waited_full_t: field(i_full)? == "true",
            late_bids: num(i_late, "late_bids")? as u32,
            splits: num(i_split, "split_decision")? as u32,
            bid_calls_sent: num(i_calls, "bid_calls_sent")? as u32,
            wall_ts_us: field(i_ts)?.parse().with_context(|| format!("line {line_no}: bad wall_ts_us"))?,
        });
    }
    Ok(rows)
}

/// Groups the nodes' rows into elections, keyed by (request_id, round).
fn merge(rows: &[ElectionRow]) -> BTreeMap<(String, u32), Election> {
    let mut by_key: BTreeMap<(String, u32), Vec<&ElectionRow>> = BTreeMap::new();
    for r in rows {
        by_key.entry((r.request_id.clone(), r.round)).or_default().push(r);
    }
    by_key
        .into_iter()
        .map(|(key, views)| {
            // The winner most nodes reported; ties go to the lowest node ID, like the election rule.
            let mut votes: BTreeMap<u32, usize> = BTreeMap::new();
            for v in &views {
                if let Some(w) = v.winner {
                    *votes.entry(w).or_default() += 1;
                }
            }
            let winner = votes.iter().max_by(|a, b| a.1.cmp(b.1).then(b.0.cmp(a.0))).map(|(w, _)| *w);
            let election = Election {
                winner,
                disagreed: votes.len() > 1, // two nodes named different winners
                first_ts_us: views.iter().map(|v| v.wall_ts_us).min().unwrap_or(0),
                bid_calls_sent: views.iter().map(|v| v.bid_calls_sent).sum(),
            };
            (key, election)
        })
        .collect()
}

fn print_report(rows: &[ElectionRow], window_secs: f64) {
    let elections = merge(rows);
    let nodes: BTreeSet<u32> = rows.iter().map(|r| r.node_id).collect(); // nodes that wrote a log
    let reelections = elections.keys().filter(|(_, round)| *round > 0).count();
    let disagreed = elections.values().filter(|e| e.disagreed).count();
    let splits: u32 = rows.iter().map(|r| r.splits).sum();
    let late: u32 = rows.iter().map(|r| r.late_bids).sum();
    let no_winner = elections.values().filter(|e| e.winner.is_none()).count();

    println!("elections:   {} ({} re-elections after a winner went down), seen by nodes {:?}", elections.len(), reelections, nodes);
    println!("disagreement: {disagreed} elections where nodes named different winners, {splits} split announcements, {late} late bids, {no_winner} with no winner");

    // Duration, per node view (each node times its own election).
    let mut durations: Vec<u64> = rows.iter().map(|r| r.duration_us).collect();
    durations.sort_unstable();
    let full_t = rows.iter().filter(|r| r.waited_full_t).count();
    println!("duration (per node view, {} views):", rows.len());
    println!("  mean {}  p50 {}  p95 {}  p99 {}  max {}",
        ms(mean(&durations)), ms(percentile(&durations, 50.0) as f64), ms(percentile(&durations, 95.0) as f64),
        ms(percentile(&durations, 99.0) as f64), ms(durations[durations.len() - 1] as f64));
    println!("  waited the full bid timeout: {} of {} ({:.1}%)", full_t, rows.len(), pct(full_t, rows.len()));
    let avg_bids = rows.iter().map(|r| r.bids).sum::<usize>() as f64 / rows.len() as f64;
    println!("  bids held per decision: {avg_bids:.2} on average");

    // Messages: all nodes' bid calls for one election, added up.
    let calls: Vec<u64> = elections.values().map(|e| e.bid_calls_sent as u64).collect();
    let mut sorted_calls = calls.clone();
    sorted_calls.sort_unstable();
    println!("messages: {:.2} bid calls per election on average (min {}, max {}); 3 nodes x 2 peers = 6 without retries",
        mean(&calls), sorted_calls[0], sorted_calls[sorted_calls.len() - 1]);

    print_win_share(&elections, window_secs);
}

/// Wins and share per node, overall and per time window (the plan's load-distribution task).
fn print_win_share(elections: &BTreeMap<(String, u32), Election>, window_secs: f64) {
    let decided: Vec<&Election> = elections.values().filter(|e| e.winner.is_some()).collect();
    if decided.is_empty() {
        return;
    }
    let winners: BTreeSet<u32> = decided.iter().filter_map(|e| e.winner).collect(); // table columns
    let even = 100.0 / winners.len().max(1) as f64;

    // Overall, as in the plan's worked example: wins / total, compared with an even split.
    let total = decided.len();
    println!("win share ({total} decided elections, even split = {even:.1}%):");
    for node in &winners {
        let wins = decided.iter().filter(|e| e.winner == Some(*node)).count();
        let share = pct(wins, total);
        println!("  node {node}: {wins} wins = {share:.1}% ({:+.1} points vs even)", share - even);
    }

    // Per time window, counted from the first election.
    let start = decided.iter().map(|e| e.first_ts_us).min().unwrap_or(0);
    let width_us = (window_secs * 1_000_000.0) as u128;
    let mut windows: BTreeMap<u128, Vec<u32>> = BTreeMap::new(); // window number -> winners in it
    for e in &decided {
        windows.entry((e.first_ts_us - start) / width_us).or_default().push(e.winner.unwrap_or(0));
    }
    println!("win share per {window_secs} s window:");
    let head: Vec<String> = winners.iter().map(|n| format!("{:>8}", format!("node {n}"))).collect();
    println!("  {:>13}  {:>9}  {}", "window", "elections", head.join("  "));
    for (w, in_window) in &windows {
        let from = *w as f64 * window_secs;
        let label = format!("{from:.0}-{:.0} s", from + window_secs);
        let shares: Vec<String> = winners
            .iter()
            .map(|n| format!("{:>7.1}%", pct(in_window.iter().filter(|x| **x == *n).count(), in_window.len())))
            .collect();
        println!("  {:>13}  {:>9}  {}", label, in_window.len(), shares.join("  "));
    }
}

fn pct(part: usize, whole: usize) -> f64 {
    if whole == 0 { 0.0 } else { 100.0 * part as f64 / whole as f64 }
}

fn mean(values: &[u64]) -> f64 {
    values.iter().sum::<u64>() as f64 / values.len() as f64
}

/// Nearest-rank percentile, the same method as the stats script. `sorted` must be sorted and not empty.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// Microseconds as milliseconds, e.g. 2840 -> "2.84 ms".
fn ms(us: f64) -> String {
    format!("{:.2} ms", us / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "run_id,node_id,request_id,round,bids,winner,duration_us,waited_full_t,late_bids,split_decision,bid_calls_sent,wall_ts_us\n";

    /// P3's own example row (election_log.rs test) parses to the right values.
    #[test]
    fn reads_p3s_format() {
        let csv = format!("{HEADER}run7,2,r7,0,1:0.2;2:0.45;3:0.31,1,2840,false,1,0,2,1761480000123456\n");
        let rows = parse(&csv).unwrap();
        let r = &rows[0];
        assert_eq!((r.node_id, r.request_id.as_str(), r.round, r.bids, r.winner), (2, "r7", 0, 3, Some(1)));
        assert_eq!((r.duration_us, r.waited_full_t, r.late_bids, r.splits, r.bid_calls_sent), (2840, false, 1, 0, 2));
    }

    /// Each election is logged by every node; merged, it counts once, with all nodes' messages added up.
    #[test]
    fn one_election_seen_by_three_nodes_counts_once() {
        let mut csv = HEADER.to_string();
        for node in 1..=3 {
            csv += &format!("r,{node},req1,0,1:0.2;2:0.4;3:0.3,1,1000,false,0,0,2,{}\n", 1_000_000 + node);
        }
        let elections = merge(&parse(&csv).unwrap());
        assert_eq!(elections.len(), 1);
        let e = &elections[&("req1".to_string(), 0)];
        assert_eq!((e.winner, e.disagreed, e.bid_calls_sent), (Some(1), false, 6)); // 3 nodes x 2 peers = 6
    }

    /// A split: two nodes name node 1, one names node 2. The majority winner is kept and the split is flagged.
    #[test]
    fn disagreement_is_flagged_and_majority_wins() {
        let csv = format!("{HEADER}r,1,q,0,,1,1,false,0,1,2,1\nr,2,q,0,,2,1,false,0,1,2,1\nr,3,q,0,,1,1,false,0,0,2,1\n");
        let e = &merge(&parse(&csv).unwrap())[&("q".to_string(), 0)];
        assert_eq!((e.winner, e.disagreed), (Some(1), true));
    }

    /// The plan's worked example: over 100 elections node 1 wins 40, node 2 35, node 3 25.
    #[test]
    fn plan_win_share_example() {
        let mut csv = HEADER.to_string();
        for i in 0..100 {
            let winner = if i < 40 { 1 } else if i < 75 { 2 } else { 3 };
            csv += &format!("r,1,req{i},0,,{winner},1000,false,0,0,2,{}\n", 1_000_000 + i);
        }
        let elections = merge(&parse(&csv).unwrap());
        let wins = |n| elections.values().filter(|e| e.winner == Some(n)).count();
        assert_eq!((wins(1), wins(2), wins(3)), (40, 35, 25));
        assert!((pct(wins(1), 100) - 40.0).abs() < 1e-9);
    }

    #[test]
    fn reelections_are_separate_elections() {
        let csv = format!("{HEADER}r,1,q,0,,2,1,true,0,0,2,1\nr,1,q,1,,1,1,false,0,0,1,2\n");
        let elections = merge(&parse(&csv).unwrap());
        assert_eq!(elections.len(), 2); // round 0 (winner 2 went down) and round 1 (node 1 took over)
        assert_eq!(elections[&("q".to_string(), 1)].winner, Some(1));
    }
}
