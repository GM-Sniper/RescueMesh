//! State shared by every request handler and background task of one node.
//! Shared as `Arc<NodeState>`: cheap to clone, all clones point to the same struct.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::NodeId;
use common::config::ClusterConfig;
// tokio's Instant follows tokio's fake clock in tests, so a 10 s mute test runs instantly.
use tokio::time::{Instant, sleep_until};

use crate::peers::Peers;

#[allow(dead_code)] // config and peers are first read by the heartbeat loop (task 5)
pub struct NodeState {
    pub id: NodeId,
    pub config: ClusterConfig,
    /// One client per other node (see peers.rs).
    pub peers: Peers,
    /// Requests currently being processed. P3's load collector reads this as "queue length".
    in_flight: AtomicUsize,
    /// When the current mute ends; None = never muted (task 4).
    /// An end time instead of an on/off flag: nothing has to switch it back off,
    /// and overlapping mutes just keep the later end.
    muted_until: Mutex<Option<Instant>>,
    // Added later: failure detector (task 5), store (P4).
}

impl NodeState {
    pub fn new(id: NodeId, config: ClusterConfig, peers: Peers) -> Self {
        NodeState {
            id,
            config,
            peers,
            in_flight: AtomicUsize::new(0),
            muted_until: Mutex::new(None),
        }
    }

    /// P3's hook: how many requests this node is working on right now.
    #[allow(dead_code)] // read by P3's load collector once it is wired in
    pub fn in_flight(&self) -> usize {
        // Relaxed: the counter is a statistic; no other data depends on its ordering.
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Call at the start of a request handler. Counts the request until the guard is dropped.
    pub fn start_request(&self) -> InFlightGuard<'_> {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlightGuard { state: self }
    }

    /// Go silent for `d` from now. An existing longer mute is kept, never shortened.
    pub fn mute_for(&self, d: Duration) {
        let until = Instant::now() + d;
        let mut current = self.muted_until.lock().unwrap();
        if current.is_none_or(|c| until > c) {
            *current = Some(until);
        }
    }

    /// True while a mute is running. Nobody switches it off: once the end time
    /// has passed, it simply no longer applies.
    pub fn is_muted(&self) -> bool {
        matches!(*self.muted_until.lock().unwrap(), Some(u) if Instant::now() < u)
    }

    /// Returns at once if not muted; otherwise sleeps until the mute ends.
    pub async fn wait_while_muted(&self) {
        loop {
            // Copy the end time out so the lock is released before sleeping.
            let until = *self.muted_until.lock().unwrap();
            match until {
                Some(u) if Instant::now() < u => sleep_until(u).await,
                _ => return,
            }
            // Loop: if the mute was extended while we slept, sleep again.
        }
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
}
