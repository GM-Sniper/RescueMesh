//! Election log (P3): one CSV row per election, for P6's election metrics
//! (docs/p3-election-design.md, Section 8).
//!
//! Each node writes its own file, e.g. `logs/elections-node2.csv`. Rows:
//!
//!   run_id,node_id,request_id,round,bids,winner,duration_us,waited_full_t,late_bids,split_decision,bid_calls_sent,wall_ts_us
//!
//! | column           | meaning                                                              |
//! |------------------|----------------------------------------------------------------------|
//! | `bids`           | every bid this node held, own included, as `node:score` pairs: `1:0.2;2:0.45` |
//! | `winner`         | the decided node; empty if no bid was ready                          |
//! | `duration_us`    | election start → decision                                            |
//! | `waited_full_t`  | the decision waited the whole bid timeout (a silent peer not yet marked down) |
//! | `late_bids`      | bids that arrived after the decision                                 |
//! | `split_decision` | announcements from a different winner (design doc Section 5)        |
//! | `bid_calls_sent` | bid calls this node sent, `broadcast()` retries included             |
//!
//! The row is written one bid timeout after the decision, so late bids and split
//! announcements (which by definition come after it) are counted.
//! `wall_ts_us` is the wall clock at the decision, like P5's fault log, so rows can
//! be joined by time with the other nodes' files.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use common::NodeId;

use crate::election::Bid;

const HEADER: &str = "run_id,node_id,request_id,round,bids,winner,duration_us,waited_full_t,late_bids,split_decision,bid_calls_sent,wall_ts_us";

/// What one election looked like from this node.
#[derive(Debug, Clone, PartialEq)]
pub struct ElectionRow {
    pub request_id: String,
    pub round: u32,
    /// Own bid included, smallest node ID first.
    pub bids: Vec<Bid>,
    pub winner: Option<NodeId>,
    pub duration: Duration,
    pub waited_full_t: bool,
    pub late_bids: u32,
    pub splits: u32,
    pub bid_calls_sent: u32,
    pub wall_ts_us: u128,
}

pub struct ElectionLog {
    run_id: String,
    node_id: NodeId,
    /// Mutex: elections for different requests finish at the same moment.
    file: Mutex<File>,
}

impl ElectionLog {
    /// Opens `path` for appending (creates it and its folder if needed). Writes the
    /// header only if the file is new or empty, like P5's fault log.
    pub fn open(path: impl AsRef<Path>, node_id: NodeId, run_id: &str) -> std::io::Result<Self> {
        let path = path.as_ref();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if file.metadata()?.len() == 0 {
            writeln!(file, "{HEADER}")?;
        }
        Ok(ElectionLog {
            run_id: run_id.to_string(),
            node_id,
            file: Mutex::new(file),
        })
    }

    pub fn write(&self, row: &ElectionRow) {
        let line = format_row(&self.run_id, self.node_id, row);
        // A logging failure must never stop the node, so it is reported and ignored.
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Err(e) = file.write_all(line.as_bytes()).and_then(|()| file.flush()) {
            tracing::warn!(error = %e, "could not write to the election log");
        }
    }
}

/// One CSV line, newline included. Scores are written in full (`{}`), so the
/// values in the file are exactly the ones the nodes compared.
fn format_row(run_id: &str, node_id: NodeId, row: &ElectionRow) -> String {
    let bids = row
        .bids
        .iter()
        .map(|b| format!("{}:{}", b.node_id, b.load_score))
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "{},{node_id},{},{},{bids},{},{},{},{},{},{},{}\n",
        csv_field(run_id),
        csv_field(&row.request_id),
        row.round,
        row.winner.map(|w| w.to_string()).unwrap_or_default(),
        row.duration.as_micros(),
        row.waited_full_t,
        row.late_bids,
        row.splits,
        row.bid_calls_sent,
        row.wall_ts_us,
    )
}

/// Quotes a field if it contains a comma, quote or newline (standard CSV rule).
fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// A fresh file path in the system temp folder, unique per test.
    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("rescuemesh-test-{}-{name}", std::process::id()));
        p.push("elections.csv");
        let _ = std::fs::remove_file(&p);
        p
    }

    fn row() -> ElectionRow {
        let bid = |node_id, load_score| Bid {
            node_id,
            load_score,
            ready: true,
        };
        ElectionRow {
            request_id: "r7".to_string(),
            round: 0,
            bids: vec![bid(1, 0.2), bid(2, 0.45), bid(3, 0.31)],
            winner: Some(1),
            duration: Duration::from_micros(2840),
            waited_full_t: false,
            late_bids: 1,
            splits: 0,
            bid_calls_sent: 2,
            wall_ts_us: 1_761_480_000_123_456,
        }
    }

    #[test]
    fn row_has_the_design_doc_format() {
        assert_eq!(
            format_row("run7", 2, &row()),
            "run7,2,r7,0,1:0.2;2:0.45;3:0.31,1,2840,false,1,0,2,1761480000123456\n"
        );
        assert_eq!(HEADER.split(',').count(), 12);
    }

    #[test]
    fn no_winner_leaves_the_column_empty() {
        let no_winner = ElectionRow {
            winner: None,
            ..row()
        };
        let cols: Vec<String> = format_row("run7", 2, &no_winner)
            .trim_end()
            .split(',')
            .map(String::from)
            .collect();
        assert_eq!(cols.len(), 12);
        assert_eq!(cols[5], "");
    }

    #[test]
    fn writes_the_header_once_then_rows() {
        let p = temp_path("rows");
        ElectionLog::open(&p, 2, "run7").unwrap().write(&row());
        ElectionLog::open(&p, 2, "run7").unwrap().write(&row());
        let lines: Vec<String> = std::fs::read_to_string(&p)
            .unwrap()
            .lines()
            .map(String::from)
            .collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], HEADER);
        assert_eq!(lines[1], lines[2]);
    }
}
