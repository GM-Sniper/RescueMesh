//! P1, Phase 1 fix: serves the pending queue.
//!
//! When no resource is free, a Submit no longer stores an empty plan as the request's final
//! answer: the incident joins the pending queue (`SharedState::pending`) instead. This task,
//! running on every node, gives queued incidents a resource as soon as one is free:
//!
//! 1. once a second, read the oldest queued incidents (at most `BATCH`);
//! 2. drop the ones whose time-to-live has passed, or that already have a plan (a client
//!    retry or another node's dispatcher got there first);
//! 3. for the oldest still waiting: snapshot, plan, commit under the client's own
//!    `request_id`, so a retry of that request finds the plan; then drop it from the queue;
//! 4. stop the round at the first incident that still finds nothing free: the queue is
//!    served oldest first, so a younger incident never jumps ahead of an older one.
//!
//! All three nodes run it. Serving the same incident at the same time is safe because the
//! commit is the same one a client request uses: one plan per `request_id` (create-if-absent)
//! and one holder per resource (claims). The loser gets `AlreadyCommitted` and just drops the
//! entry. A muted node does nothing (its store calls fail at the mute gate), and a node that
//! is not ready yet (rejoin rule, rejoin.rs) waits, like it does for elections.

use std::sync::Arc;
use std::time::Duration;

use common::planner;
use common::shared_state::{CommitOutcome, StateUnavailable};

use crate::activity;
use crate::state::NodeState;

/// Queued incidents looked at per round. Small: a round stops at the first one that finds
/// nothing free, and a long queue only means resources are scarce.
const BATCH: usize = 20;

/// How often each node looks at the queue.
const EVERY: Duration = Duration::from_secs(1);

/// What one round did, for the activity line and the tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Round {
    /// Queued incidents this node gave a resource to.
    pub assigned: usize,
    /// Dropped because their time-to-live had passed.
    pub expired: usize,
    /// Dropped because they already had a plan, or were resolved meanwhile.
    pub settled: usize,
}

pub fn spawn(state: Arc<NodeState>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(EVERY);
        loop {
            tick.tick().await;
            if state.is_muted() || !state.is_ready() {
                continue;
            }
            match run_once(&state, now_ms()).await {
                Ok(round) => {
                    activity::add_n(&activity::COUNTERS.dispatched, round.assigned as u64);
                    activity::add_n(&activity::COUNTERS.expired, round.expired as u64);
                }
                // Normal during a storage outage; the next round tries again.
                Err(e) => tracing::debug!(%e, "dispatcher: shared state not reachable this round"),
            }
        }
    });
}

/// One pass over the head of the queue (see the module docs). `now_ms` decides expiry.
pub async fn run_once(state: &Arc<NodeState>, now_ms: u64) -> Result<Round, StateUnavailable> {
    let store = &state.store;
    let mut round = Round::default();
    for entry in store.pending(BATCH).await? {
        if entry.expired(now_ms) {
            tracing::info!(request_id = %entry.request_id, incident = entry.incident.id,
                waited_ms = now_ms.saturating_sub(entry.since_ms), "queued incident dropped: its time-to-live ran out");
            store.drop_pending(&entry).await?;
            round.expired += 1;
            continue;
        }
        if store.lookup_request(&entry.request_id).await?.is_some() {
            store.drop_pending(&entry).await?;
            round.settled += 1;
            continue;
        }

        let snapshot = store.snapshot().await?;
        let (incident, weights, planner_config) =
            (entry.incident.clone(), state.config.weights, state.config.planner);
        // CPU work: off the async threads, like the Submit handler, so heartbeats never wait.
        let plan = tokio::task::spawn_blocking(move || {
            planner::plan_with_workload(&incident, &snapshot, &weights, now_ms, &planner_config)
        })
        .await
        .map_err(|e| StateUnavailable { reason: format!("planner failed: {e}") })?;
        if plan.assignments.is_empty() {
            break; // still nothing free for the oldest: the younger ones wait too
        }

        match store.commit(&entry.request_id, entry.incident.clone(), plan).await {
            CommitOutcome::Committed => {
                tracing::debug!(request_id = %entry.request_id, waited_ms = now_ms.saturating_sub(entry.since_ms),
                    "queued incident assigned");
                store.drop_pending(&entry).await?;
                round.assigned += 1;
            }
            CommitOutcome::AlreadyCommitted(_) | CommitOutcome::IncidentResolved => {
                store.drop_pending(&entry).await?;
                round.settled += 1;
            }
            // Another request took the resource meanwhile (or, impossible with a non-empty
            // plan, queued again): try again next round, still oldest first.
            CommitOutcome::ResourceTaken { .. } | CommitOutcome::Pending => break,
            CommitOutcome::StateUnavailable(e) => return Err(e),
        }
    }
    Ok(round)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use common::config::ClusterConfig;
    use common::domain::{Incident, Plan};
    use common::generators::{generate_incident, generate_snapshot};
    use common::shared_state::SharedState;

    use super::*;
    use crate::peers::Peers;
    use crate::store::InMemoryStore;

    const NOW: u64 = 1_000_000;

    /// One node with its own in-memory store holding `resources` resources.
    fn node(resources: usize) -> (Arc<NodeState>, Arc<InMemoryStore>) {
        let cfg = ClusterConfig::parse("[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n").unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        let store = Arc::new(InMemoryStore::new(generate_snapshot(42, 0, resources).resources));
        let state = NodeState::new(1, cfg, peers).with_store(store.clone());
        (Arc::new(state), store)
    }

    /// A generated incident (fixed 2023 timestamp) that may wait `ttl_secs` in the queue.
    fn incident(seed: u64, ttl_secs: u64) -> Incident {
        Incident { ttl_secs, ..generate_incident(seed) }
    }

    fn empty(incident: &Incident) -> Plan {
        Plan { incident_id: incident.id, priority: 1.0, assignments: vec![] }
    }

    /// Plans and commits `request` like the Submit handler does (whatever is free).
    async fn submit(state: &Arc<NodeState>, request: &str, incident: Incident) -> CommitOutcome {
        let snapshot = state.store.snapshot().await.unwrap();
        let plan = planner::plan_with_workload(&incident, &snapshot, &state.config.weights, NOW, &state.config.planner);
        state.store.commit(request, incident, plan).await
    }

    #[tokio::test]
    async fn a_queued_incident_gets_the_resource_freed_by_a_resolve() {
        let (state, store) = node(1);
        let (first, second) = (incident(1, 600), incident(2, 600));
        assert_eq!(submit(&state, "r1", first.clone()).await, CommitOutcome::Committed);
        // The only resource is taken: r2 is queued, not answered with a stored empty plan.
        assert_eq!(submit(&state, "r2", second.clone()).await, CommitOutcome::Pending);
        assert_eq!(store.lookup_request("r2").await.unwrap(), None);

        // Nothing free yet: the round changes nothing.
        assert_eq!(run_once(&state, NOW).await.unwrap(), Round::default());
        assert_eq!(store.pending(10).await.unwrap().len(), 1);

        // r1's incident is resolved: its resource goes to the oldest waiting incident.
        store.resolve("res-1", first.id).await.unwrap();
        let round = run_once(&state, NOW).await.unwrap();
        assert_eq!(round.assigned, 1);
        let plan = store.lookup_request("r2").await.unwrap().expect("r2 now has a plan");
        assert_eq!(plan.assignments.len(), 1);
        assert!(store.pending(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_oldest_is_served_first_and_younger_ones_keep_waiting() {
        let (state, store) = node(1);
        let holder = incident(1, 600);
        assert_eq!(submit(&state, "r1", holder.clone()).await, CommitOutcome::Committed);
        for (n, seed) in [("old", 2), ("young", 3)] {
            assert_eq!(submit(&state, n, incident(seed, 600)).await, CommitOutcome::Pending);
        }
        store.resolve("res-1", holder.id).await.unwrap();

        assert_eq!(run_once(&state, NOW).await.unwrap().assigned, 1);
        assert!(store.lookup_request("old").await.unwrap().is_some());
        assert!(store.lookup_request("young").await.unwrap().is_none());
        let left = store.pending(10).await.unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].request_id, "young");
    }

    #[tokio::test]
    async fn an_expired_incident_is_dropped_not_served() {
        let (state, store) = node(1);
        assert_eq!(submit(&state, "holder", incident(1, 600)).await, CommitOutcome::Committed);
        let short = incident(2, 5); // may wait 5 s in the queue
        assert_eq!(store.commit("r1", short.clone(), empty(&short)).await, CommitOutcome::Pending);
        let since = store.pending(1).await.unwrap()[0].since_ms;

        // Its 2023 timestamp does not matter: only the time spent in the queue counts.
        assert_eq!(run_once(&state, since + 4_999).await.unwrap(), Round::default());
        let round = run_once(&state, since + 5_000).await.unwrap();
        assert_eq!(round, Round { expired: 1, ..Round::default() });
        assert!(store.pending(10).await.unwrap().is_empty());
        assert!(store.lookup_request("r1").await.unwrap().is_none(), "an expired incident gets no plan");
    }

    #[tokio::test]
    async fn an_entry_that_already_has_a_plan_is_only_dropped() {
        let (state, store) = node(2);
        let late = incident(2, 600);
        assert_eq!(store.commit("r1", late.clone(), empty(&late)).await, CommitOutcome::Pending);
        // A client retry planned it meanwhile (a resource was free by then).
        assert_eq!(submit(&state, "r1", late).await, CommitOutcome::Committed);

        let round = run_once(&state, NOW).await.unwrap();
        assert_eq!(round, Round { settled: 1, ..Round::default() });
        assert!(store.pending(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_muted_node_does_not_touch_the_queue() {
        let (state, store) = node(1);
        let waiting = incident(2, 600);
        assert_eq!(store.commit("r1", waiting.clone(), empty(&waiting)).await, CommitOutcome::Pending);
        state.mute_for(Duration::from_secs(10));
        assert!(run_once(&state, NOW).await.is_err());
        assert_eq!(store.pending(10).await.unwrap().len(), 1);
    }
}
