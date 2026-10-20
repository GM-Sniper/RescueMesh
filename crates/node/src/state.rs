//! State shared by every request handler and background task of one node.
//! Shared as `Arc<NodeState>`: cheap to clone, all clones point to the same struct.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use common::NodeId;
use common::config::ClusterConfig;
use common::load::LoadReport;

use crate::load::LoadCollector;
use crate::peers::Peers;
use crate::store::Store;

#[allow(dead_code)] // config and peers are first read by the heartbeat loop (task 5)
pub struct NodeState {
    pub id: NodeId,
    pub config: ClusterConfig,
    /// One client per other node (see peers.rs).
    pub peers: Peers,
    /// Requests currently being processed. P3's load collector reads this as "queue length".
    in_flight: AtomicUsize,
    /// P3: CPU and service-time samples behind this node's load score (see load.rs).
    pub load: LoadCollector,
    /// P4: incidents, resources, assignments and the change log (see store.rs).
    pub store: Store,
    // Added later: muted (task 4), failure detector (task 5), store (P4).
}

impl NodeState {
    pub fn new(id: NodeId, config: ClusterConfig, peers: Peers) -> Self {
        let load = LoadCollector::new(config.load);
        NodeState {
            id,
            config,
            peers,
            in_flight: AtomicUsize::new(0),
            load,
            store: Store::with_default_resources(id),
        }
    }

    /// P3's hook: how many requests this node is working on right now.
    pub fn in_flight(&self) -> usize {
        // Relaxed: the counter is a statistic; no other data depends on its ordering.
        self.in_flight.load(Ordering::Relaxed)
    }

    /// P3: this node's load right now; `load_score` is what it bids in Step B.
    /// The queue length is read live, so two requests arriving within one CPU
    /// sample still see different scores.
    pub fn load_report(&self) -> LoadReport {
        self.load.current(self.in_flight())
    }

    /// Call at the start of a request handler. Counts the request until the guard is dropped.
    pub fn start_request(&self) -> InFlightGuard<'_> {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlightGuard {
            state: self,
            started: Instant::now(),
        }
    }
}

/// Decrements `in_flight` when it goes out of scope, however the handler ends
/// (success, early error return, or the request being cancelled), and gives the
/// request's duration to the load collector's service-time average.
pub struct InFlightGuard<'a> {
    state: &'a NodeState,
    started: Instant,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.state.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.state.load.record_service_time(self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> NodeState {
        let cfg = ClusterConfig::parse(
            r#"
            [[nodes]]
            id = 1
            addr = "127.0.0.1:50051"
            "#,
        )
        .unwrap();
        let peers = Peers::new(&cfg, 1).unwrap(); // one node, so no peers and no channels
        NodeState::new(1, cfg, peers)
    }

    #[test]
    fn counts_requests_while_guards_live() {
        let s = state();
        assert_eq!(s.in_flight(), 0);
        let a = s.start_request();
        let b = s.start_request();
        assert_eq!(s.in_flight(), 2);
        drop(a);
        assert_eq!(s.in_flight(), 1);
        drop(b);
        assert_eq!(s.in_flight(), 0);
    }

    #[test]
    fn early_return_still_decrements() {
        fn handler(s: &NodeState, fail: bool) -> Result<(), ()> {
            let _guard = s.start_request();
            if fail {
                return Err(()); // early exit: the guard is still dropped
            }
            Ok(())
        }
        let s = state();
        assert!(handler(&s, true).is_err());
        assert!(handler(&s, false).is_ok());
        assert_eq!(s.in_flight(), 0);
    }

    #[test]
    fn load_report_follows_the_queue_and_records_service_time() {
        let s = state();
        assert_eq!(s.load_report().load_score, 0.0);
        let g = s.start_request();
        // 1 of queue_max 10, weight 0.5 -> 0.05; no CPU sample or finished request yet.
        assert_eq!(s.load_report().queue_len, 1);
        assert!((s.load_report().load_score - 0.05).abs() < 1e-9);
        std::thread::sleep(std::time::Duration::from_millis(5));
        drop(g);
        let r = s.load_report();
        assert_eq!(r.queue_len, 0);
        assert!(
            r.service_ms >= 5.0,
            "service time not recorded: {}",
            r.service_ms
        );
    }
}
