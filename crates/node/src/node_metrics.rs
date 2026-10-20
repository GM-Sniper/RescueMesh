//! node_metrics log (P6): once per second, each node writes down how busy it is, so
//! latency spikes in loadgen's CSV can be lined up with what the node was going through.
//!
//! Each node writes its own file, e.g. `logs/node_metrics-node2.csv`. Rows:
//!
//!   node_id,wall_ts_us,queue_len,cpu_pct,service_ms,load_score
//!
//! | column       | meaning                                                         |
//! |--------------|-----------------------------------------------------------------|
//! | `queue_len`  | requests this node is working on right now                      |
//! | `cpu_pct`    | whole-machine CPU use, 0-100                                    |
//! | `service_ms` | average time per request (EWMA), 0 before the first request    |
//! | `load_score` | P3's single "how busy am I" number: 0 = idle, 1 = fully loaded |
//!
//! The values come from P3's `NodeState::load_report()` (see load.rs); this file only writes them.
//! `wall_ts_us` is the wall clock in microseconds since 1970, like P5's fault log, so rows can be
//! joined by time with other nodes' files and with loadgen's CSV (its `timestamp` is in ms).

use std::fs::{File, OpenOptions}; // open the log file for appending
use std::io::Write; // write_all and flush
use std::path::Path;
use std::sync::Arc; // the thread shares NodeState with the rest of the node
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH}; // Instant = schedule, SystemTime = timestamp

use common::NodeId;
use common::load::LoadReport; // P3: queue_len, cpu_pct, service_ms, load_score

use crate::state::NodeState;

const HEADER: &str = "node_id,wall_ts_us,queue_len,cpu_pct,service_ms,load_score";
const INTERVAL: Duration = Duration::from_secs(1); // one row per second (Step A plan)

/// Opens the log at `path` and starts a thread that writes one row per second until the node stops.
/// Opening happens here, before the thread starts, so a bad path stops the node at startup
/// instead of failing silently later.
///
/// A plain thread, not a tokio task, for the same reason as P3's sampler: if the planner keeps the
/// async runtime busy, a tokio task would write late, exactly when the node is most loaded.
pub fn spawn(state: Arc<NodeState>, path: impl AsRef<Path>) -> std::io::Result<()> {
    let mut file = open(path.as_ref())?;
    std::thread::Builder::new()
        .name("node-metrics".into()) // shows up by name in debuggers and crash reports
        .spawn(move || {
            let mut next = Instant::now() + INTERVAL; // when the next row is due
            loop {
                std::thread::sleep(next.saturating_duration_since(Instant::now())); // wait until it's due
                // Fixed schedule: rows stay 1 s apart even if a write took a while.
                // If we fell far behind (e.g. the laptop slept), restart the schedule instead of catching up.
                next = (next + INTERVAL).max(Instant::now());
                let line = row(state.id, wall_ts_us(), &state.load_report()); // P3's numbers, right now
                // One write per row, then flush: a crash loses at most the row in progress.
                // A logging failure must never stop the node, so it is reported and ignored.
                if let Err(e) = file.write_all(line.as_bytes()).and_then(|()| file.flush()) {
                    tracing::warn!(error = %e, "could not write to the node_metrics log");
                }
            }
        })?;
    Ok(())
}

/// Opens `path` for appending (creates it and its folder if needed). Writes the header only if the
/// file is new or empty, so a restarted node adds rows to the same file instead of repeating the header.
fn open(path: &Path) -> std::io::Result<File> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?; // e.g. make logs/ on first run
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if file.metadata()?.len() == 0 {
        writeln!(file, "{HEADER}")?;
    }
    Ok(file)
}

/// One CSV row (with its newline). Rounded so the file stays readable: CPU to 0.1 %,
/// service time to 0.01 ms, load score to 3 decimals.
fn row(node_id: NodeId, wall_ts_us: u128, r: &LoadReport) -> String {
    format!(
        "{node_id},{wall_ts_us},{},{:.1},{:.2},{:.3}\n",
        r.queue_len, r.cpu_pct, r.service_ms, r.load_score
    )
}

/// Wall-clock time in microseconds since 1970. Used to join files by time, never to measure durations.
fn wall_ts_us() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// A fresh file path in the system temp folder, unique per test.
    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("rescuemesh-test-{}-{name}", std::process::id()));
        p.push("node_metrics.csv");
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn row_has_every_column_rounded() {
        let r = LoadReport { queue_len: 4, cpu_pct: 50.04, service_ms: 20.0, load_score: 0.39 };
        assert_eq!(row(2, 1_700_000_000_000_000, &r), "2,1700000000000000,4,50.0,20.00,0.390\n");
        assert_eq!(row(2, 1, &r).trim_end().split(',').count(), HEADER.split(',').count());
    }

    #[test]
    fn reopening_does_not_repeat_the_header() {
        let p = temp_path("reopen");
        let r = LoadReport { queue_len: 0, cpu_pct: 0.0, service_ms: 0.0, load_score: 0.0 };
        open(&p).unwrap().write_all(row(1, 1, &r).as_bytes()).unwrap(); // first start
        open(&p).unwrap().write_all(row(1, 2, &r).as_bytes()).unwrap(); // restart
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(text.lines().filter(|l| *l == HEADER).count(), 1);
        assert_eq!(text.lines().count(), 3); // header + 2 rows
    }
}
