//! Fault log (P5): one CSV row per fault event, so P6 can line failures up
//! against latency plots.
//!
//! Each node writes its own file, e.g. `logs/faults-node2.csv`. Rows:
//!
//!   run_id,node_id,event,fault_id,peer_id,duration_ms,wall_ts_us
//!
//! | event        | Written by                       | fault_id | peer_id | duration_ms |
//! |--------------|----------------------------------|----------|---------|-------------|
//! | `MUTE_START` | the muted node, when it goes silent | set   | empty   | set         |
//! | `MUTE_END`   | the muted node, when it wakes up    | set   | empty   | empty       |
//! | `PEER_DOWN`  | each other node, when its detector suspects a peer | empty | set | empty |
//! | `PEER_UP`    | each other node, when that peer is heard again     | empty | set | empty |
//!
//! `PEER_DOWN` / `PEER_UP` have no fault_id on purpose: the other nodes must not
//! know about the fault, only notice the silence. Join the files by time.
//!
//! `wall_ts_us` is the wall clock (microseconds since 1970), because rows from
//! different machines are compared. Keep the machines' clocks synced (chrony/NTP).
//!
//! Who calls what (not wired yet):
//!   - P1's mute hook (task 4): `mute_start` after sending the FaultAck, `mute_end` when the timer ends.
//!   - The detector ticker (task 5): `peer_down` / `peer_up` for each event from `tick()`.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use common::NodeId;

const HEADER: &str = "run_id,node_id,event,fault_id,peer_id,duration_ms,wall_ts_us";

pub struct FaultLog {
    run_id: String,
    node_id: NodeId,
    /// Mutex: the mute handler and the detector ticker may write at the same moment.
    file: Mutex<File>,
}

impl FaultLog {
    /// Opens `path` for appending (creates it and its folder if needed).
    /// Writes the header only if the file is new or empty, so restarts add rows
    /// to the same file instead of repeating the header.
    pub fn open(path: impl AsRef<Path>, node_id: NodeId, run_id: &str) -> std::io::Result<Self> {
        let path = path.as_ref();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if file.metadata()?.len() == 0 {
            writeln!(file, "{HEADER}")?;
        }
        Ok(FaultLog {
            run_id: run_id.to_string(),
            node_id,
            file: Mutex::new(file),
        })
    }

    /// This node just went silent because of `fault_id`, for `duration_ms`.
    pub fn mute_start(&self, fault_id: &str, duration_ms: u64) {
        self.write("MUTE_START", fault_id, None, Some(duration_ms));
    }

    /// This node's silence for `fault_id` just ended.
    pub fn mute_end(&self, fault_id: &str) {
        self.write("MUTE_END", fault_id, None, None);
    }

    /// The failure detector just suspected `peer`.
    pub fn peer_down(&self, peer: NodeId) {
        self.write("PEER_DOWN", "", Some(peer), None);
    }

    /// The failure detector just heard from `peer` again.
    pub fn peer_up(&self, peer: NodeId) {
        self.write("PEER_UP", "", Some(peer), None);
    }

    fn write(&self, event: &str, fault_id: &str, peer: Option<NodeId>, duration_ms: Option<u64>) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0);
        let line = format!(
            "{},{},{event},{},{},{},{ts}\n",
            csv_field(&self.run_id),
            self.node_id,
            csv_field(fault_id),
            peer.map(|p| p.to_string()).unwrap_or_default(),
            duration_ms.map(|d| d.to_string()).unwrap_or_default(),
        );
        // One write_all per row, then flush: a crash loses at most the row in progress.
        // A logging failure must never stop the node, so it is reported and ignored.
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Err(e) = file.write_all(line.as_bytes()).and_then(|()| file.flush()) {
            tracing::warn!(error = %e, "could not write to the fault log");
        }
    }
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
        p.push("faults.csv");
        let _ = std::fs::remove_file(&p);
        p
    }

    fn read_lines(p: &Path) -> Vec<String> {
        std::fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(String::from)
            .collect()
    }

    #[test]
    fn writes_header_then_one_row_per_event() {
        let p = temp_path("rows");
        let log = FaultLog::open(&p, 2, "run-7").unwrap();
        log.mute_start("abc", 10_000);
        log.mute_end("abc");
        log.peer_down(3);
        log.peer_up(3);

        let lines = read_lines(&p);
        assert_eq!(lines[0], HEADER);
        assert_eq!(lines.len(), 5);
        let cols = |i: usize| lines[i].split(',').map(String::from).collect::<Vec<_>>();
        assert_eq!(
            cols(1)[..6],
            ["run-7", "2", "MUTE_START", "abc", "", "10000"]
        );
        assert_eq!(cols(2)[..6], ["run-7", "2", "MUTE_END", "abc", "", ""]);
        assert_eq!(cols(3)[..6], ["run-7", "2", "PEER_DOWN", "", "3", ""]);
        assert_eq!(cols(4)[..6], ["run-7", "2", "PEER_UP", "", "3", ""]);
        // Every row has all 7 columns and a real timestamp.
        for i in 1..5 {
            assert_eq!(cols(i).len(), 7);
            assert!(cols(i)[6].parse::<u128>().unwrap() > 0);
        }
    }

    #[test]
    fn reopening_does_not_repeat_the_header() {
        let p = temp_path("reopen");
        FaultLog::open(&p, 1, "r").unwrap().peer_down(2);
        FaultLog::open(&p, 1, "r").unwrap().peer_up(2);
        let lines = read_lines(&p);
        assert_eq!(lines.iter().filter(|l| *l == HEADER).count(), 1);
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn timestamps_never_go_backwards_within_a_run() {
        let p = temp_path("order");
        let log = FaultLog::open(&p, 1, "r").unwrap();
        log.mute_start("x", 1000);
        log.mute_end("x");
        let lines = read_lines(&p);
        let ts = |i: usize| {
            lines[i]
                .rsplit(',')
                .next()
                .unwrap()
                .parse::<u128>()
                .unwrap()
        };
        assert!(ts(2) >= ts(1));
    }

    #[test]
    fn quotes_fields_with_commas() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");
    }
}
