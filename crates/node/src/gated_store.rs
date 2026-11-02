//! P5, Step C: "a muted node also loses its access to the shared state".
//!
//! The mute hook (P1, PR #10) already holds every incoming call and stops every outgoing
//! one. What it did not stop: a request handler that was already running when the mute
//! started. That handler could still read the state and commit a plan while its node is
//! supposed to be dead, which a crashed node can never do. This wrapper closes that gap.
//!
//! Every `SharedState` call goes through the gate. While the node is muted the call does not
//! reach the backend and returns `StateUnavailable`. The handler already maps that to a gRPC
//! `Unavailable`, the client retries with the same `request_id`, and a live node answers.
//!
//! # Decisions
//! - **Fail at once, do not wait for the mute to end.** Waiting would let the handler commit
//!   a plan made from a snapshot that is up to 20 s old, after the client has probably
//!   retried elsewhere. Failing behaves like a crash: the work is simply lost.
//! - **Checked before the call, not after.** If a mute starts while a write is already on the
//!   wire, the write may land. That is the same as a node crashing mid-write, and
//!   create-if-absent on `request_id` makes it safe: the client's retry gets
//!   `AlreadyCommitted` with that plan.
//! - **One wrapper for every backend.** It sits in front of `Arc<dyn SharedState>`, so the
//!   in-memory store and the DFS-backed store get exactly the same rule.

use std::sync::Arc;

use async_trait::async_trait;
use common::domain::{Incident, IncidentId, Plan, Snapshot};
use common::shared_state::{CommitOutcome, SharedState, StateUnavailable};

use crate::mute::MuteSwitch;

/// Wraps a backend so that a muted node cannot reach it.
pub struct MuteGatedStore {
    inner: Arc<dyn SharedState>,
    mute: Arc<MuteSwitch>,
}

impl MuteGatedStore {
    pub fn new(inner: Arc<dyn SharedState>, mute: Arc<MuteSwitch>) -> Self {
        MuteGatedStore { inner, mute }
    }

    fn check(&self) -> Result<(), StateUnavailable> {
        if self.mute.is_muted() {
            return Err(StateUnavailable {
                reason: "node is muted (fault injection)".into(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl SharedState for MuteGatedStore {
    async fn snapshot(&self) -> Result<Snapshot, StateUnavailable> {
        self.check()?;
        self.inner.snapshot().await
    }

    async fn commit(&self, request_id: &str, incident: Incident, plan: Plan) -> CommitOutcome {
        if let Err(why) = self.check() {
            return CommitOutcome::StateUnavailable(why);
        }
        self.inner.commit(request_id, incident, plan).await
    }

    async fn lookup_request(&self, request_id: &str) -> Result<Option<Plan>, StateUnavailable> {
        self.check()?;
        self.inner.lookup_request(request_id).await
    }

    async fn resolve(&self, request_id: &str, incident_id: IncidentId) -> Result<(), StateUnavailable> {
        self.check()?;
        self.inner.resolve(request_id, incident_id).await
    }

    async fn reload(&self) -> Result<Snapshot, StateUnavailable> {
        self.check()?;
        self.inner.reload().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common::domain::*;

    use super::*;
    use crate::store::InMemoryStore;

    fn incident(id: u64) -> Incident {
        Incident {
            id,
            owner: 1,
            category: Category::Medical,
            location: Zone::new(1, 1),
            severity: 5,
            timestamp: 1_000,
            status: IncidentStatus::Open,
            ttl_secs: 600,
        }
    }

    fn empty_plan(incident_id: u64) -> Plan {
        Plan {
            incident_id,
            priority: 1.0,
            assignments: vec![],
        }
    }

    fn gated() -> (MuteGatedStore, Arc<InMemoryStore>, Arc<MuteSwitch>) {
        let inner = Arc::new(InMemoryStore::with_default_resources());
        let mute = Arc::new(MuteSwitch::default());
        (MuteGatedStore::new(inner.clone(), mute.clone()), inner, mute)
    }

    #[tokio::test]
    async fn not_muted_passes_every_call_through() {
        let (store, inner, _mute) = gated();
        assert_eq!(
            store.commit("r1", incident(1), empty_plan(1)).await,
            CommitOutcome::Committed
        );
        assert!(store.lookup_request("r1").await.unwrap().is_some());
        assert_eq!(store.snapshot().await.unwrap().incidents.len(), 1);
        assert_eq!(store.reload().await.unwrap().incidents.len(), 1);
        assert!(inner.lookup_request("r1").await.unwrap().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn muted_node_reaches_nothing_and_writes_nothing() {
        let (store, inner, mute) = gated();
        mute.mute_for(Duration::from_secs(10));

        assert!(matches!(
            store.commit("r1", incident(1), empty_plan(1)).await,
            CommitOutcome::StateUnavailable(_)
        ));
        assert!(store.snapshot().await.is_err());
        assert!(store.lookup_request("r1").await.is_err());
        assert!(store.reload().await.is_err(), "rejoin must not pass while muted");
        // Nothing reached the backend.
        assert!(inner.lookup_request("r1").await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn access_comes_back_when_the_mute_ends() {
        let (store, _inner, mute) = gated();
        mute.mute_for(Duration::from_secs(10));
        assert!(store.reload().await.is_err());

        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(store.reload().await.is_ok());
        assert_eq!(
            store.commit("r1", incident(1), empty_plan(1)).await,
            CommitOutcome::Committed
        );
    }
}
