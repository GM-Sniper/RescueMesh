//! Request log (P1, Step C): one row when this node starts working on a request and one
//! when it stops, for P6's measurements only. Nothing in the node reads it back.
//!
//! Each node writes its own file, e.g. `logs/requests-node2.csv`. Rows:
//!
//!   run_id,node_id,request_id,event,detail,duration_us,wall_ts_us
//!
//! | column        | meaning                                                                 |
//! |---------------|-------------------------------------------------------------------------|
//! | `event`       | `START` (the work began), `END` (it stopped), or `SNAPSHOT` / `COMMIT` (one shared-state call, see below) |
//! | `detail`      | END: `committed` (we planned and saved it), `duplicate` (the stored plan was returned), `lost` (another node won the election), or `error:<code>` (e.g. `error:unavailable` when muted). SNAPSHOT / COMMIT: how the call ended (`ok`, `committed`, `resource_taken`, ...) |
//! | `duration_us` | END: START to END. SNAPSHOT / COMMIT: how long that one call took       |
//! | `wall_ts_us`  | wall clock, like P5's fault log, so rows join by time with the other files |
//!
//! How P6 reads it, for a request the client had to retry:
//! - no START on a node: the request never reached it (network cut);
//! - START and no END: the node was working on it when it died (node took it, then failed).
//!   A switched-off node can't write at that moment, but its START is already on disk;
//! - END later than the client's 2 s deadline: the node was alive but slow (slow request).
//!
//! The rows come from the Submit task in `service.rs` (`start_or_join`), so a retry or a
//! peer's bid that joins work already running writes no second START.
//!
//! SNAPSHOT and COMMIT rows (P1, Step D: "state cost under load") come from `handle_submit`,
//! one per call, only on the node that won the election. A request planned again after
//! `resource_taken` has several of each. Their `wall_ts_us` places them in a ramp stage.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::NodeId;

use crate::state::SubmitResult;

const HEADER: &str = "run_id,node_id,request_id,event,detail,duration_us,wall_ts_us";

pub struct RequestLog {
    run_id: String,
    node_id: NodeId,
    /// Mutex: many requests start and end at the same moment.
    file: Mutex<File>,
}

impl RequestLog {
    /// Opens `path` for appending (creates it and its folder if needed). Writes the
    /// header only if the file is new or empty, like the fault and election logs.
    pub fn open(path: impl AsRef<Path>, node_id: NodeId, run_id: &str) -> std::io::Result<Self> {
        let path = path.as_ref();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if file.metadata()?.len() == 0 {
            writeln!(file, "{HEADER}")?;
        }
        Ok(RequestLog {
            run_id: run_id.to_string(),
            node_id,
            file: Mutex::new(file),
        })
    }

    pub fn start(&self, request_id: &str) {
        self.write(request_id, "START", "", None);
    }

    pub fn end(&self, request_id: &str, outcome: &SubmitResult, took: Duration) {
        self.write(request_id, "END", &end_detail(outcome, self.node_id), Some(took));
    }

    /// One shared-state call inside the work: `event` is `SNAPSHOT` or `COMMIT`.
    pub fn step(&self, request_id: &str, event: &str, detail: &str, took: Duration) {
        self.write(request_id, event, detail, Some(took));
    }

    fn write(&self, request_id: &str, event: &str, detail: &str, took: Option<Duration>) {
        let line = format_row(&self.run_id, self.node_id, request_id, event, detail, took, wall_us());
        // A logging failure must never stop the node, so it is reported and ignored.
        let mut file = self.file.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Err(e) = file.write_all(line.as_bytes()).and_then(|()| file.flush()) {
            tracing::warn!(error = %e, "could not write to the request log");
        }
    }
}

/// What the END row says about how the work ended.
fn end_detail(outcome: &SubmitResult, own_id: NodeId) -> String {
    match outcome {
        Ok(reply) if reply.plan.is_none() || reply.executor_node != own_id => "lost".to_string(),
        Ok(reply) if reply.duplicate => "duplicate".to_string(),
        Ok(_) => "committed".to_string(),
        Err(status) => format!("error:{}", code_name(status.code())),
    }
}

/// Lower-case, one word: `Code::Unavailable` -> `unavailable`.
fn code_name(code: tonic::Code) -> String {
    format!("{code:?}").to_lowercase()
}

/// One CSV line, newline included.
fn format_row(
    run_id: &str,
    node_id: NodeId,
    request_id: &str,
    event: &str,
    detail: &str,
    took: Option<Duration>,
    wall_ts_us: u128,
) -> String {
    format!(
        "{},{node_id},{},{event},{detail},{},{wall_ts_us}\n",
        csv_field(run_id),
        csv_field(request_id),
        took.map(|d| d.as_micros().to_string()).unwrap_or_default(),
    )
}

fn wall_us() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros()).unwrap_or(0)
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

    use common::pb::{Plan, SubmitReply};
    use tonic::Status;

    use super::*;

    /// A fresh file path in the system temp folder, unique per test.
    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("rescuemesh-test-{}-{name}", std::process::id()));
        p.push("requests.csv");
        let _ = std::fs::remove_file(&p);
        p
    }

    fn reply(executor_node: NodeId, plan: bool, duplicate: bool) -> SubmitResult {
        Ok(SubmitReply {
            request_id: "r1".into(),
            executor_node,
            plan: plan.then(Plan::default),
            duplicate,
        })
    }

    #[test]
    fn rows_have_the_documented_format() {
        assert_eq!(
            format_row("run7", 2, "r1", "START", "", None, 1_761_480_000_123_456),
            "run7,2,r1,START,,,1761480000123456\n"
        );
        assert_eq!(
            format_row("run7", 2, "r1", "END", "committed", Some(Duration::from_micros(2840)), 5),
            "run7,2,r1,END,committed,2840,5\n"
        );
        assert_eq!(HEADER.split(',').count(), 7);
    }

    #[test]
    fn end_detail_names_how_the_work_ended() {
        assert_eq!(end_detail(&reply(2, true, false), 2), "committed");
        assert_eq!(end_detail(&reply(2, true, true), 2), "duplicate");
        assert_eq!(end_detail(&reply(3, false, false), 2), "lost");
        assert_eq!(
            end_detail(&Err(Status::unavailable("node is muted")), 2),
            "error:unavailable"
        );
        assert_eq!(end_detail(&Err(Status::aborted("x")), 2), "error:aborted");
    }

    #[test]
    fn header_is_written_once_and_rows_are_appended() {
        let path = temp_path("append");
        {
            let log = RequestLog::open(&path, 2, "run7").unwrap();
            log.start("r1");
            log.end("r1", &reply(2, true, false), Duration::from_millis(3));
        }
        // Reopened, e.g. the node restarted after a power-off: no second header.
        RequestLog::open(&path, 2, "run7").unwrap().start("r2");
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0], HEADER);
        assert!(lines[1].starts_with("run7,2,r1,START,,,"));
        assert!(lines[2].starts_with("run7,2,r1,END,committed,3000,"));
        assert!(lines[3].starts_with("run7,2,r2,START,,,"));
    }

    #[test]
    fn step_rows_carry_the_call_and_its_duration() {
        let path = temp_path("step");
        let log = RequestLog::open(&path, 2, "run7").unwrap();
        log.step("r1", "SNAPSHOT", "ok", Duration::from_micros(4120));
        log.step("r1", "COMMIT", "committed", Duration::from_micros(9830));
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[1].starts_with("run7,2,r1,SNAPSHOT,ok,4120,"));
        assert!(lines[2].starts_with("run7,2,r1,COMMIT,committed,9830,"));
    }

    #[test]
    fn a_request_id_with_a_comma_stays_one_field() {
        let line = format_row("run7", 2, "a,b", "START", "", None, 1);
        assert_eq!(line, "run7,2,\"a,b\",START,,,1\n");
    }
}
