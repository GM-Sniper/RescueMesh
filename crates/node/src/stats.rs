//! Demo branch: counters for the Submit path, reported as one readable summary
//! instead of a log line per request.
//!
//! Every second, if anything changed, the node logs one summary line and rewrites
//! `logs/submit-stats-node<N>.txt` (key=value lines) for the demo script to read.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::state::NodeState;

#[derive(Default)]
pub struct SubmitStats {
    /// New requests that got a resource.
    pub assigned: AtomicU64,
    /// New requests answered with an empty plan: every suitable resource was busy.
    pub none_free: AtomicU64,
    /// Requests whose ID was already handled: the stored plan was sent again.
    pub repeats: AtomicU64,
    /// Lost races: a planned resource was taken before our write, so we planned again.
    pub replans: AtomicU64,
    /// Requests that lost the race on every attempt and got `aborted`.
    pub aborted: AtomicU64,
    /// Resources freed when their mission ended.
    pub released: AtomicU64,
}

impl SubmitStats {
    pub fn add(counter: &AtomicU64) {
        // Relaxed: plain statistics; nothing else depends on their ordering.
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

/// Starts the once-a-second summary. Runs until the node stops.
pub fn spawn_reporter(state: Arc<NodeState>) {
    let path = format!("logs/submit-stats-node{}.txt", state.id);
    tokio::spawn(async move {
        let mut last = String::new();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let s = &state.stats;
            let (assigned, none_free) = (SubmitStats::get(&s.assigned), SubmitStats::get(&s.none_free));
            let (repeats, replans) = (SubmitStats::get(&s.repeats), SubmitStats::get(&s.replans));
            let (aborted, released) = (SubmitStats::get(&s.aborted), SubmitStats::get(&s.released));
            let snapshot = state.store.snapshot();
            let (busy, total) = (snapshot.assignments.len(), snapshot.resources.len());

            let file = format!(
                "assigned={assigned}\nnone_free={none_free}\nrepeats={repeats}\nreplans={replans}\n\
                 aborted={aborted}\nreleased={released}\nbusy={busy}\ntotal={total}\n"
            );
            if file == last || assigned + none_free + repeats + aborted == 0 {
                continue; // nothing new, or no request yet: stay quiet
            }
            tracing::info!(
                "requests: {} new ({assigned} got a resource, {none_free} none free), {repeats} repeats \
                 | races: {replans} re-planned, {aborted} gave up \
                 | resources: {busy}/{total} busy, {released} freed after their mission",
                assigned + none_free
            );
            if let Err(e) = std::fs::write(&path, &file) {
                tracing::warn!(error = %e, %path, "could not write the stats file");
            }
            last = file;
        }
    });
}
