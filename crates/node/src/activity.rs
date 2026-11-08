//! Live activity line for the node's log: every few seconds, one INFO line that says what the
//! node did (requests in, elections won, plans saved, retries, resolves), e.g.
//!
//!   activity (5 s): 312 requests · elected for 104 · planned 101 · 6 re-planned · 2 retried by the client · resolved 98 · peers 2/2
//!
//! Expected events (a resource taken by another request meanwhile, a claim given back by the
//! sweeper) are only counted here, at debug level per event, so the console is not a wall of
//! WARN lines for things that are normal under load. Real problems stay WARN.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::state::NodeState;

/// One counter per kind of event, reset after each summary line.
pub struct Counters {
    /// Incident requests that reached this node (from a client, or a peer's bid).
    pub received: AtomicU64,
    pub won: AtomicU64,
    pub lost: AtomicU64,
    /// Plans saved by this node.
    pub planned: AtomicU64,
    /// Requests answered with the plan already saved (a client retry or a duplicate).
    pub duplicates: AtomicU64,
    /// A planned resource was taken by another request meanwhile: planned again.
    pub replanned: AtomicU64,
    /// Still taken after every re-plan: the client retries the same request.
    pub collisions: AtomicU64,
    pub resolved: AtomicU64,
    /// Resources the sweeper gave back.
    pub swept: AtomicU64,
    /// Shared-state calls that failed (the client retries).
    pub storage_errors: AtomicU64,
}

pub static COUNTERS: Counters = Counters {
    received: AtomicU64::new(0),
    won: AtomicU64::new(0),
    lost: AtomicU64::new(0),
    planned: AtomicU64::new(0),
    duplicates: AtomicU64::new(0),
    replanned: AtomicU64::new(0),
    collisions: AtomicU64::new(0),
    resolved: AtomicU64::new(0),
    swept: AtomicU64::new(0),
    storage_errors: AtomicU64::new(0),
};

pub fn add(counter: &AtomicU64) {
    counter.fetch_add(1, Relaxed);
}

pub fn add_n(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Relaxed);
}

/// The first storage error in a 5 s window is logged at WARN, later ones only counted
/// (the summary line gives the number). During a power cut there can be hundreds.
pub fn first_storage_error_in_window() -> bool {
    static LAST_WARN_MS: AtomicU64 = AtomicU64::new(0);
    add(&COUNTERS.storage_errors);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
    let last = LAST_WARN_MS.load(Relaxed);
    now.saturating_sub(last) >= 5000 && LAST_WARN_MS.compare_exchange(last, now, Relaxed, Relaxed).is_ok()
}

/// Logs the summary every `every`; while idle, a short "idle" line every 30 s instead.
pub fn spawn(state: Arc<NodeState>, every: Duration) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.tick().await; // the first tick is immediate
        let mut quiet = Duration::ZERO;
        loop {
            tick.tick().await;
            let c = &COUNTERS;
            let take = |x: &AtomicU64| x.swap(0, Relaxed);
            let (received, won, lost, planned) = (take(&c.received), take(&c.won), take(&c.lost), take(&c.planned));
            let (dups, replanned, collisions) = (take(&c.duplicates), take(&c.replanned), take(&c.collisions));
            let (resolved, swept, errors) = (take(&c.resolved), take(&c.swept), take(&c.storage_errors));
            let peers = format!("peers {}/{}", state.alive_peers().len(), state.config.nodes.len().saturating_sub(1));
            if state.is_muted() {
                continue; // the mute itself is logged; a muted node does no work
            }
            if received + resolved + swept + errors == 0 {
                quiet += every;
                if quiet >= Duration::from_secs(30) {
                    quiet = Duration::ZERO;
                    tracing::info!("idle · {peers}{}", if state.is_ready() { "" } else { " · catching up" });
                }
                continue;
            }
            quiet = Duration::ZERO;
            // Not "lost N": a lost election just means another node took the request, and in a
            // console "lost" reads as lost requests. `lost` stays counted for the debug log.
            tracing::debug!(lost, "elections lost to a peer");
            let mut line = format!("{received} requests · elected for {won} · planned {planned}");
            for (n, part) in [
                (dups, format!("{dups} answered from the store")),
                (replanned, format!("{replanned} re-planned")),
                (collisions, format!("{collisions} retried by the client")),
                (resolved, format!("resolved {resolved}")),
                (swept, format!("{swept} resources returned by the sweeper")),
                (errors, format!("{errors} storage errors (retried)")),
            ] {
                if n > 0 {
                    line.push_str(" · ");
                    line.push_str(&part);
                }
            }
            tracing::info!("activity ({} s): {line} · {peers}", every.as_secs());
        }
    });
}
