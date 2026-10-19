//! State shared by every request handler and background task of one node.
//! Shared as `Arc<NodeState>`: cheap to clone, all clones point to the same struct.

use std::sync::atomic::{AtomicUsize, Ordering};

use common::NodeId;
use common::config::ClusterConfig;

pub struct NodeState {
    pub id: NodeId,
    pub config: ClusterConfig,
    /// Requests currently being processed. P3's load collector reads this as "queue length".
    in_flight: AtomicUsize,
    // Added later: peers (3d), muted (task 4), failure detector (task 5), store (P4).
}

impl NodeState {
    pub fn new(id: NodeId, config: ClusterConfig) -> Self {
        NodeState {
            id,
            config,
            in_flight: AtomicUsize::new(0),
        }
    }

    /// P3's hook: how many requests this node is working on right now.
    pub fn in_flight(&self) -> usize {
        // Relaxed: the counter is a statistic; no other data depends on its ordering.
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Call at the start of a request handler. Counts the request until the guard is dropped.
    pub fn start_request(&self) -> InFlightGuard<'_> {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlightGuard { state: self }
    }
}

/// Decrements `in_flight` when it goes out of scope, however the handler ends
/// (success, early error return, or the request being cancelled).
pub struct InFlightGuard<'a> {
    state: &'a NodeState,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.state.in_flight.fetch_sub(1, Ordering::Relaxed);
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
        NodeState::new(1, cfg)
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
}
