//! P3: the rejoin readiness rule (Step C).
//!
//! A node coming back from a mute has missed everything that happened while it was
//! silent, and its load score is 0 (it did nothing), so it would win every election
//! straight away. Instead it is not ready from the start of the mute until it has:
//!
//! 1. dropped its in-flight work: the election decisions from before the mute;
//! 2. reloaded the shared state (P5's `reload()`): proof it can reach the state again;
//! 3. heard from at least one peer since it woke up: proof it is back on the network.
//!
//! Until then its bids carry `ready = false`, so it takes part but cannot win.
//! When the rule passes it writes a `READY` row to the fault log; P6 measures
//! recovery time from `MUTE_END` to `READY`. A freshly started node runs the same rule
//! (`main.rs`), with `startup` as the fault_id.

use std::time::Duration;

use tokio::time::{Instant, sleep};

use crate::election::ElectionTable;
use crate::state::NodeState;

/// How often the rule re-checks a condition that has not passed yet.
const CHECK_EVERY: Duration = Duration::from_millis(50);

/// Runs the rule after the mute for `fault_id` ended (or at startup), then marks the node ready.
pub async fn rejoin(state: &NodeState, fault_id: &str) {
    let woke = Instant::now();

    // 1. Decisions from before the mute are stale: a request we had won may have been
    //    re-elected without us. Dropping them makes a retry run a fresh election.
    //    (Requests held by the mute gate are already dropped by the gate.)
    *state.election.lock().unwrap() = ElectionTable::new();

    // 2. P5's reload. Nodes keep no copy of the state, they read it per request, so a
    //    successful reload is what matters: it shows the state is reachable.
    while let Err(why) = state.store.reload().await {
        tracing::warn!(%why, "rejoin: shared state not reachable yet");
        sleep(CHECK_EVERY).await;
    }

    // 3. A peer heard since we woke. A node with no peers has nobody to hear.
    let has_peers = state.peers.ids().next().is_some();
    while has_peers && !state.heard_any_since(woke) {
        sleep(CHECK_EVERY).await;
    }

    // Muted again meanwhile: that mute's own rejoin will mark us ready.
    if state.is_muted() {
        return;
    }
    state.set_ready(true);
    tracing::info!(%fault_id, took_ms = woke.elapsed().as_millis() as u64, "rejoin: ready");
    if let Some(log) = &state.fault_log {
        log.ready(fault_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;
    use common::config::ClusterConfig;
    use common::domain::{Incident, IncidentId, Plan, Snapshot};
    use common::shared_state::{CommitOutcome, SharedState, StateUnavailable};

    use super::*;
    use crate::peers::Peers;
    use crate::store::InMemoryStore;

    fn node(config: &str) -> Arc<NodeState> {
        let cfg = ClusterConfig::parse(config).unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        let state = NodeState::new(1, cfg, peers);
        state.set_ready(false); // as the mute hook leaves it
        Arc::new(state)
    }

    fn three_nodes() -> Arc<NodeState> {
        node(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:50052\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:50053\"\n",
        )
    }

    fn start(state: &Arc<NodeState>) -> tokio::task::JoinHandle<()> {
        let state = state.clone();
        tokio::spawn(async move { rejoin(&state, "f1").await })
    }

    #[tokio::test(start_paused = true)]
    async fn ready_only_after_a_peer_is_heard() {
        let s = three_nodes();
        let task = start(&s);
        sleep(Duration::from_secs(1)).await;
        assert!(!s.is_ready(), "no peer heard yet");

        s.heard_from(2);
        task.await.unwrap();
        assert!(s.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_heard_before_waking_does_not_count() {
        let s = three_nodes();
        s.heard_from(2);
        sleep(Duration::from_millis(10)).await;
        let task = start(&s);
        sleep(Duration::from_secs(1)).await;
        assert!(!s.is_ready());
        s.heard_from(3);
        task.await.unwrap();
        assert!(s.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_is_not_a_peer() {
        let s = three_nodes();
        let task = start(&s);
        s.heard_from(0); // clients send node id 0
        sleep(Duration::from_secs(1)).await;
        assert!(!s.is_ready());
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn old_election_decisions_are_dropped() {
        let s = three_nodes();
        {
            let mut table = s.election.lock().unwrap();
            table.my_bid("r7", || crate::election::Bid {
                node_id: 1,
                load_score: 0.0,
                ready: true,
            });
            assert_eq!(table.decide("r7"), Some(1));
        }
        let task = start(&s);
        s.heard_from(2);
        task.await.unwrap();
        assert_eq!(s.election.lock().unwrap().winner("r7"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_without_peers_is_ready_at_once() {
        let s = node("[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n");
        rejoin(&s, "f1").await;
        assert!(s.is_ready());
    }

    /// The in-memory store, unreachable for its first `fail_first` snapshots.
    struct FlakyStore {
        inner: InMemoryStore,
        fail_first: u32,
        calls: AtomicU32,
    }

    #[async_trait]
    impl SharedState for FlakyStore {
        async fn snapshot(&self) -> Result<Snapshot, StateUnavailable> {
            if self.calls.fetch_add(1, Ordering::SeqCst) < self.fail_first {
                return Err(StateUnavailable {
                    reason: "test: unreachable".into(),
                });
            }
            self.inner.snapshot().await
        }
        async fn commit(&self, request_id: &str, incident: Incident, plan: Plan) -> CommitOutcome {
            self.inner.commit(request_id, incident, plan).await
        }
        async fn lookup_request(&self, request_id: &str) -> Result<Option<Plan>, StateUnavailable> {
            self.inner.lookup_request(request_id).await
        }
        async fn resolve(
            &self,
            request_id: &str,
            incident_id: IncidentId,
        ) -> Result<(), StateUnavailable> {
            self.inner.resolve(request_id, incident_id).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn waits_until_the_shared_state_can_be_read() {
        let cfg = ClusterConfig::parse("[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n").unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        let store = Arc::new(FlakyStore {
            inner: InMemoryStore::with_default_resources(),
            fail_first: 3,
            calls: AtomicU32::new(0),
        });
        let s = NodeState::new(1, cfg, peers).with_store(store.clone());
        s.set_ready(false);

        let start = Instant::now();
        rejoin(&s, "f1").await;
        assert!(s.is_ready());
        assert_eq!(store.calls.load(Ordering::SeqCst), 4);
        assert!(start.elapsed() >= CHECK_EVERY * 3);
    }
}
