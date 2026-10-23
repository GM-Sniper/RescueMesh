//! State shared by every request handler and background task of one node.
//! Shared as `Arc<NodeState>`: cheap to clone, all clones point to the same struct.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::NodeId;
use common::config::ClusterConfig;
use common::load::LoadReport;
use common::shared_state::SharedState;
use common::pb::node_client::NodeClient;
use tonic::transport::Channel;
// tokio's Instant follows tokio's fake clock in tests, so a 10 s mute test runs instantly.
// Also used by the in-flight guard to time requests (P3's service-time average).
use tokio::time::{Instant, sleep_until};

use crate::failure_detector::{FailureDetector, PeerEvent};
use crate::fault_log::FaultLog;
use crate::load::LoadCollector;
use crate::peers::Peers;
use crate::store::InMemoryStore;

pub struct NodeState {
    pub id: NodeId,
    pub config: ClusterConfig,
    /// One client per other node (see peers.rs).
    pub peers: Peers,
    /// Requests currently being processed. P3's load collector reads this as "queue length".
    in_flight: AtomicUsize,
    /// P3: CPU and service-time samples behind this node's load score (see load.rs).
    pub load: LoadCollector,
    /// P4: incidents, resources and assignments, behind the shared-state interface.
    /// `dyn` so the in-memory store can later be swapped for the DFS-backed one (see store.rs).
    pub store: Arc<dyn SharedState>,
    /// When the current mute ends; None = never muted (task 4).
    /// An end time instead of an on/off flag: nothing has to switch it back off,
    /// and overlapping mutes just keep the later end.
    muted_until: Mutex<Option<Instant>>,
    /// P5's failure detector: who we currently believe is alive (task 5).
    /// Private: used only through heard_from / check_peers / alive_peers, so the
    /// time always comes from the same clock and the lock is held only briefly.
    detector: Mutex<FailureDetector>,
    /// P5's fault CSV (MUTE_START/END, PEER_DOWN/UP). None in tests, so they write no files.
    pub fault_log: Option<FaultLog>,
}

impl NodeState {
    pub fn new(id: NodeId, config: ClusterConfig, peers: Peers) -> Self {
        let load = LoadCollector::new(config.load);
        // Every peer starts "alive" and gets the full suspect_after to send its first heartbeat.
        let detector = FailureDetector::new(peers.ids(), config.timing.suspect_after(), now_std());
        NodeState {
            id,
            config,
            peers,
            in_flight: AtomicUsize::new(0),
            load,
            store: Arc::new(InMemoryStore::with_default_resources()),
            muted_until: Mutex::new(None),
            detector: Mutex::new(detector),
            fault_log: None,
        }
    }

    /// Turns on the fault CSV (main does this; tests leave it off).
    pub fn with_fault_log(mut self, log: FaultLog) -> Self {
        self.fault_log = Some(log);
        self
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

    /// The one way to get a client for an outgoing call. None while muted:
    /// a silent node sends nothing (no heartbeats, no broadcasts), like a crashed one.
    pub fn peer_for_sending(&self, peer: NodeId) -> Option<NodeClient<Channel>> {
        if self.is_muted() {
            return None;
        }
        self.peers.client(peer)
    }

    /// "We just heard from `peer`": a reply to our heartbeat, or a heartbeat it sent us.
    pub fn heard_from(&self, peer: NodeId) {
        self.detector.lock().unwrap().record(peer, now_std());
    }

    /// Re-checks every peer; returns only the changes (PeerDown / PeerUp).
    pub fn check_peers(&self) -> Vec<PeerEvent> {
        self.detector.lock().unwrap().tick(now_std())
    }

    /// Throws the old detector away and starts a fresh one: every peer alive, full
    /// suspect_after each. Used on unmute, so a mute behaves like a reboot with a
    /// clean view instead of logging false PeerDowns for the silence we caused.
    pub fn reset_detector(&self) {
        let fresh = FailureDetector::new(
            self.peers.ids(),
            self.config.timing.suspect_after(),
            now_std(),
        );
        *self.detector.lock().unwrap() = fresh;
    }

    /// Peers we currently believe are alive, smallest ID first.
    #[allow(dead_code)] // used by broadcast (task 6) and the election (Step B)
    pub fn alive_peers(&self) -> Vec<NodeId> {
        self.detector.lock().unwrap().alive_peers()
    }
}

/// The current time on tokio's clock, in the standard form P5's detector expects.
/// In fake-clock tests this is the fake time, so the detector and the mute stay in sync.
fn now_std() -> std::time::Instant {
    Instant::now().into_std()
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

    #[tokio::test(start_paused = true)] // fake clock: sleeps jump ahead instantly
    async fn mute_is_never_shortened() {
        let s = state();
        s.mute_for(Duration::from_secs(10));
        s.mute_for(Duration::from_secs(2)); // shorter order: ignored
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(s.is_muted());
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert!(!s.is_muted());
    }

    #[tokio::test(start_paused = true)]
    async fn no_outgoing_calls_while_muted() {
        let cfg = ClusterConfig::parse(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:50052\"\n",
        )
        .unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        let s = NodeState::new(1, cfg, peers);
        assert!(s.peer_for_sending(2).is_some());
        s.mute_for(Duration::from_secs(1));
        assert!(s.peer_for_sending(2).is_none());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(s.peer_for_sending(2).is_some());
    }

    // ---- Task 5a: P5's detector inside the state, on the fake clock.

    fn three_node_state() -> NodeState {
        let cfg = ClusterConfig::parse(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:50052\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:50053\"\n",
        )
        .unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        NodeState::new(1, cfg, peers) // suspect_after = 1500 ms (default)
    }

    #[tokio::test(start_paused = true)]
    async fn peers_heard_regularly_stay_alive() {
        let s = three_node_state();
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            s.heard_from(2);
            s.heard_from(3);
            assert!(s.check_peers().is_empty());
        }
        assert_eq!(s.alive_peers(), vec![2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn silence_gives_peer_down_then_heard_gives_peer_up() {
        let s = three_node_state();
        tokio::time::sleep(Duration::from_millis(1000)).await;
        s.heard_from(3); // only node 3 keeps talking
        tokio::time::sleep(Duration::from_millis(500)).await; // 1.5 s of fake time
        assert_eq!(s.check_peers(), vec![PeerEvent::PeerDown(2)]);
        assert_eq!(s.alive_peers(), vec![3]);

        s.heard_from(2);
        assert_eq!(s.check_peers(), vec![PeerEvent::PeerUp(2)]);
        assert_eq!(s.alive_peers(), vec![2, 3]);
    }
}
