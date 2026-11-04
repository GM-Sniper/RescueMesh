//! P4: the in-memory implementation of `common::shared_state::SharedState`.
//!
//! It is the stand-in behind the shared-state interface until a distributed-file-system backend
//! exists (DESIGN_LOG.md, "P4: Shared state in a distributed file system"). Each node has its
//! own copy, so unlike the real backend it does NOT share anything between nodes; it exists so
//! handlers, tests and the election can be written against the final interface today.
//!
//! The old change log, `applied` map, `entries_since` and replication path were removed when
//! the design moved to a DFS. Three ideas were kept:
//! - `request_id` is the dedup key (`plans_by_request`).
//! - No clocks decide anything.
//! - `commit` re-checks inside the write lock that every planned resource is still free,
//!   because `snapshot()` returns a copy that may be stale.
//!
//! The lock is a plain `std::sync::RwLock` and is never held across an `.await`, so it cannot
//! stall the async runtime.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;
use common::domain::*;
use common::generators::generate_snapshot;
use common::shared_state::{CommitOutcome, SharedState, StateUnavailable};

// TODO(P4): remove once clients can offer resources; make configurable only if experiments
// need a different resource count.
// Every node must start with the same resources, so these are the same constants everywhere.
const DEFAULT_RESOURCE_SEED: u64 = 42;
const DEFAULT_RESOURCE_COUNT: usize = 10;

/// The starting resources every node (and the S3 backend's first writer) uses.
pub fn default_resources() -> Vec<Resource> {
    generate_snapshot(DEFAULT_RESOURCE_SEED, 0, DEFAULT_RESOURCE_COUNT).resources
}

#[derive(Default)]
struct Contents {
    incidents: BTreeMap<IncidentId, Incident>,
    resources: BTreeMap<ResourceId, Resource>,
    /// Keyed by resource id: a resource has at most one assignment.
    assignments: BTreeMap<ResourceId, Assignment>,
    /// The one stored plan per client request. This is the create-if-absent record.
    plans_by_request: HashMap<String, Plan>,
    /// Resolve request IDs are idempotent and cannot be reused for another incident.
    resolutions_by_request: HashMap<String, IncidentId>,
    /// Incident IDs that have been resolved, including IDs unknown when resolved.
    resolved_incidents: HashSet<IncidentId>,
}

/// Shared state kept in this process's memory. Implements [`SharedState`].
///
/// Why one `RwLock` around everything: `commit` must check and write several maps as one
/// atomic step, and a single lock is the simplest way to guarantee that.
pub struct InMemoryStore {
    contents: RwLock<Contents>,
}

impl InMemoryStore {
    /// A store that starts with exactly `resources` and nothing else.
    pub fn new(resources: Vec<Resource>) -> Self {
        let contents = Contents {
            resources: resources.into_iter().map(|r| (r.id, r)).collect(),
            ..Contents::default()
        };
        InMemoryStore {
            contents: RwLock::new(contents),
        }
    }

    /// Starting resources from fixed constants, identical on every node.
    pub fn with_default_resources() -> Self {
        InMemoryStore::new(default_resources())
    }

    // A panic while holding the lock cannot leave `Contents` half-written (all checks come
    // before any write), so a poisoned lock is still safe to use.
    fn read(&self) -> RwLockReadGuard<'_, Contents> {
        self.contents.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, Contents> {
        self.contents.write().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl SharedState for InMemoryStore {
    async fn snapshot(&self) -> Result<Snapshot, StateUnavailable> {
        let contents = self.read();
        Ok(Snapshot {
            incidents: contents.incidents.values().cloned().collect(),
            resources: contents.resources.values().cloned().collect(),
            assignments: contents.assignments.values().cloned().collect(),
        })
    }

    async fn commit(&self, request_id: &str, mut incident: Incident, plan: Plan) -> CommitOutcome {
        let mut contents = self.write();

        // Create-if-absent: two retries of one request can both get past `lookup_request`,
        // so the check must be repeated here, inside the lock.
        if let Some(existing) = contents.plans_by_request.get(request_id) {
            return CommitOutcome::AlreadyCommitted(existing.clone());
        }

        // A resolved incident must never receive new resource assignments.
        if contents.resolved_incidents.contains(&incident.id) {
            return CommitOutcome::IncidentResolved;
        }

        // Every planned resource must exist, be unassigned, and appear only once in the plan.
        // An unknown resource is reported as "taken" too: from the caller's side the cure is
        // the same (snapshot again and re-plan). Nothing is written before all checks pass.
        let mut seen_in_this_plan = HashSet::new();
        for assignment in &plan.assignments {
            let resource_id = assignment.resource_id;
            let is_free = contents.resources.contains_key(&resource_id)
                && !contents.assignments.contains_key(&resource_id);
            if !is_free || !seen_in_this_plan.insert(resource_id) {
                return CommitOutcome::ResourceTaken { resource_id };
            }
        }

        // An empty plan (nothing was free) is stored too, so a retry gets the same empty plan.
        // TODO(P4): decide whether to queue such an incident and re-plan when resources free up.
        incident.status = if plan.assignments.is_empty() {
            IncidentStatus::Open
        } else {
            IncidentStatus::Assigned
        };

        for assignment in &plan.assignments {
            contents
                .assignments
                .insert(assignment.resource_id, assignment.clone());
        }
        contents.incidents.insert(incident.id, incident);
        contents
            .plans_by_request
            .insert(request_id.to_string(), plan);
        CommitOutcome::Committed
    }

    async fn resolve(
        &self,
        request_id: &str,
        incident_id: IncidentId,
    ) -> Result<(), StateUnavailable> {
        let mut contents = self.write();

        // Repeating the same resolve request is safe.
        if let Some(&previous_id) = contents.resolutions_by_request.get(request_id) {
            if previous_id == incident_id {
                return Ok(());
            }

            return Err(StateUnavailable {
                reason: format!(
                    "resolve request ID {request_id} was already used for incident {previous_id}"
                ),
            });
        }

        // Mark the incident as resolved if it exists.
        // An unknown incident is a successful no-op.
        contents.resolved_incidents.insert(incident_id);

        if let Some(incident) = contents.incidents.get_mut(&incident_id) {
            incident.status = IncidentStatus::Resolved;
        }

        // Release only resources assigned to this incident.
        contents
            .assignments
            .retain(|_, assignment| assignment.incident_id != incident_id);

        // Remember the request so retries are safe and request IDs cannot be reused.
        contents
            .resolutions_by_request
            .insert(request_id.to_string(), incident_id);

        Ok(())
    }

    async fn lookup_request(&self, request_id: &str) -> Result<Option<Plan>, StateUnavailable> {
        Ok(self.read().plans_by_request.get(request_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn resource(id: u64) -> Resource {
        Resource {
            id,
            rtype: ResourceType::Ambulance,
            quantity: 1,
            available: true,
            location: Zone::new(0, 0),
            lease_expiry: 0,
        }
    }

    fn store_with_resources(count: u64) -> InMemoryStore {
        InMemoryStore::new((1..=count).map(resource).collect())
    }

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

    /// A plan for `incident_id` assigning the given resources.
    fn plan_assigning(incident_id: u64, resource_ids: &[u64]) -> Plan {
        Plan {
            incident_id,
            priority: 1.0,
            assignments: resource_ids
                .iter()
                .map(|&resource_id| Assignment {
                    incident_id,
                    resource_id,
                    score: 0.5,
                })
                .collect(),
        }
    }

    async fn commit(
        store: &InMemoryStore,
        request_id: &str,
        incident_id: u64,
        resource_ids: &[u64],
    ) -> CommitOutcome {
        store
            .commit(
                request_id,
                incident(incident_id),
                plan_assigning(incident_id, resource_ids),
            )
            .await
    }

    #[test]
    fn default_resources_are_the_same_on_every_node() {
        let first = InMemoryStore::with_default_resources();
        let second = InMemoryStore::with_default_resources();

        assert_eq!(first.read().resources.len(), DEFAULT_RESOURCE_COUNT);
        assert_eq!(
            first.read().resources.values().collect::<Vec<_>>(),
            second.read().resources.values().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn same_request_committed_twice_leaves_one_record_and_returns_already_committed() {
        let store = store_with_resources(3);

        assert_eq!(
            commit(&store, "r1", 10, &[1]).await,
            CommitOutcome::Committed
        );

        let before = store.snapshot().await.unwrap();

        // A retry may even have been planned differently; the stored plan still wins.
        let second = commit(&store, "r1", 10, &[2]).await;

        assert_eq!(
            second,
            CommitOutcome::AlreadyCommitted(plan_assigning(10, &[1]))
        );

        let after = store.snapshot().await.unwrap();
        assert_eq!(after, before, "the second commit must not change the state");
        assert_eq!(after.incidents.len(), 1);
        assert_eq!(after.assignments.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_commits_of_one_request_id_give_exactly_one_committed() {
        let store = Arc::new(store_with_resources(8));

        let tasks: Vec<_> = (1..=8u64)
            .map(|resource_id| {
                let store = store.clone();
                // Each task plans a DIFFERENT resource for the same request, as nodes
                // that planned independently would.
                tokio::spawn(async move { commit(&store, "same", 10, &[resource_id]).await })
            })
            .collect();

        let mut committed = 0;
        let mut already_committed = 0;

        for task in tasks {
            match task.await.unwrap() {
                CommitOutcome::Committed => committed += 1,
                CommitOutcome::AlreadyCommitted(_) => already_committed += 1,
                other => panic!("unexpected outcome {other:?}"),
            }
        }

        assert_eq!(committed, 1);
        assert_eq!(already_committed, 7);
        assert_eq!(store.snapshot().await.unwrap().assignments.len(), 1);
    }

    #[tokio::test]
    async fn two_requests_planning_the_same_free_resource_only_one_gets_it() {
        let store = store_with_resources(3);

        assert_eq!(
            commit(&store, "first", 10, &[1]).await,
            CommitOutcome::Committed
        );

        // "second" planned from a snapshot taken before "first" committed; resource 2 is
        // valid but must not be written when resource 1 is taken.
        let outcome = commit(&store, "second", 11, &[2, 1]).await;

        assert_eq!(outcome, CommitOutcome::ResourceTaken { resource_id: 1 });

        let snapshot = store.snapshot().await.unwrap();
        assert_eq!(snapshot.assignments.len(), 1);
        assert_eq!(snapshot.incidents.len(), 1);
        assert_eq!(store.lookup_request("second").await.unwrap(), None);

        // After planning again with a free resource, the same request succeeds.
        assert_eq!(
            commit(&store, "second", 11, &[2]).await,
            CommitOutcome::Committed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_requests_for_one_resource_give_exactly_one_winner() {
        let store = Arc::new(store_with_resources(1));

        let tasks: Vec<_> = (1..=6u64)
            .map(|number| {
                let store = store.clone();
                tokio::spawn(
                    async move { commit(&store, &format!("r{number}"), number, &[1]).await },
                )
            })
            .collect();

        let mut committed = 0;
        let mut taken = 0;

        for task in tasks {
            match task.await.unwrap() {
                CommitOutcome::Committed => committed += 1,
                CommitOutcome::ResourceTaken { resource_id: 1 } => taken += 1,
                other => panic!("unexpected outcome {other:?}"),
            }
        }

        assert_eq!((committed, taken), (1, 5));
    }

    #[tokio::test]
    async fn an_unknown_resource_or_one_named_twice_is_reported_as_taken_and_writes_nothing() {
        let store = store_with_resources(3);
        let empty = store.snapshot().await.unwrap();

        assert_eq!(
            commit(&store, "r1", 10, &[1, 77]).await,
            CommitOutcome::ResourceTaken { resource_id: 77 }
        );

        assert_eq!(
            commit(&store, "r2", 11, &[2, 2]).await,
            CommitOutcome::ResourceTaken { resource_id: 2 }
        );

        assert_eq!(store.snapshot().await.unwrap(), empty);
    }

    #[tokio::test]
    async fn lookup_request_finds_committed_plans_and_returns_none_otherwise() {
        let store = store_with_resources(3);

        assert_eq!(store.lookup_request("r1").await.unwrap(), None);

        commit(&store, "r1", 10, &[1]).await;

        assert_eq!(
            store.lookup_request("r1").await.unwrap(),
            Some(plan_assigning(10, &[1]))
        );

        assert_eq!(store.lookup_request("other").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_request_with_no_free_resource_is_stored_as_open_with_an_empty_plan() {
        let store = store_with_resources(3);

        assert_eq!(
            commit(&store, "r1", 10, &[]).await,
            CommitOutcome::Committed
        );

        let snapshot = store.snapshot().await.unwrap();

        assert_eq!(snapshot.incidents.len(), 1);
        assert_eq!(snapshot.incidents[0].status, IncidentStatus::Open);
        assert!(snapshot.assignments.is_empty());

        assert_eq!(
            store.lookup_request("r1").await.unwrap(),
            Some(plan_assigning(10, &[]))
        );

        // A retry gets the same empty plan, not a fresh attempt.
        assert_eq!(
            commit(&store, "r1", 10, &[1]).await,
            CommitOutcome::AlreadyCommitted(plan_assigning(10, &[]))
        );
    }

    #[tokio::test]
    async fn committing_with_resources_marks_the_incident_assigned() {
        let store = store_with_resources(3);

        commit(&store, "r1", 10, &[2]).await;

        let snapshot = store.snapshot().await.unwrap();

        assert_eq!(snapshot.incidents[0].status, IncidentStatus::Assigned);
        assert_eq!(snapshot.assignments[0].resource_id, 2);
    }

    #[tokio::test]
    async fn snapshot_lists_are_sorted_by_id() {
        let store = store_with_resources(4);

        commit(&store, "a", 30, &[4]).await;
        commit(&store, "b", 10, &[1]).await;
        commit(&store, "c", 20, &[3]).await;

        let snapshot = store.snapshot().await.unwrap();

        let incident_ids: Vec<_> = snapshot.incidents.iter().map(|i| i.id).collect();
        let resource_ids: Vec<_> = snapshot.resources.iter().map(|r| r.id).collect();
        let assigned_ids: Vec<_> = snapshot.assignments.iter().map(|a| a.resource_id).collect();

        assert_eq!(incident_ids, [10, 20, 30]);
        assert_eq!(resource_ids, [1, 2, 3, 4]);
        assert_eq!(assigned_ids, [1, 3, 4]);
    }

    #[tokio::test]
    async fn resolving_incident_releases_its_resources_only() {
        let store = store_with_resources(3);

        commit(&store, "r1", 10, &[1, 2]).await;
        commit(&store, "r2", 20, &[3]).await;

        store.resolve("resolve-1", 10).await.unwrap();

        let snapshot = store.snapshot().await.unwrap();

        let resolved = snapshot
            .incidents
            .iter()
            .find(|incident| incident.id == 10)
            .unwrap();

        assert_eq!(resolved.status, IncidentStatus::Resolved);
        assert_eq!(snapshot.assignments.len(), 1);
        assert_eq!(snapshot.assignments[0].incident_id, 20);
        assert_eq!(snapshot.assignments[0].resource_id, 3);
    }

    #[tokio::test]
    async fn resolving_the_same_incident_twice_is_safe() {
        let store = store_with_resources(2);

        commit(&store, "r1", 10, &[1]).await;

        store.resolve("resolve-1", 10).await.unwrap();
        store.resolve("resolve-1", 10).await.unwrap();

        let snapshot = store.snapshot().await.unwrap();

        assert_eq!(snapshot.assignments.len(), 0);
        assert_eq!(snapshot.incidents[0].status, IncidentStatus::Resolved);
    }

    #[tokio::test]
    async fn resolve_request_id_cannot_be_reused_for_another_incident() {
        let store = store_with_resources(2);

        commit(&store, "r1", 10, &[1]).await;
        commit(&store, "r2", 20, &[2]).await;

        store.resolve("resolve-1", 10).await.unwrap();

        let result = store.resolve("resolve-1", 20).await;

        assert!(result.is_err());

        let snapshot = store.snapshot().await.unwrap();

        assert_eq!(snapshot.incidents[1].status, IncidentStatus::Assigned);
        assert_eq!(snapshot.assignments.len(), 1);
        assert_eq!(snapshot.assignments[0].incident_id, 20);
    }

    #[tokio::test]
    async fn released_resource_can_be_assigned_to_another_incident() {
        let store = store_with_resources(1);

        commit(&store, "r1", 10, &[1]).await;
        store.resolve("resolve-1", 10).await.unwrap();

        assert_eq!(
            commit(&store, "r2", 20, &[1]).await,
            CommitOutcome::Committed
        );

        let snapshot = store.snapshot().await.unwrap();

        assert_eq!(snapshot.assignments.len(), 1);
        assert_eq!(snapshot.assignments[0].incident_id, 20);
        assert_eq!(snapshot.assignments[0].resource_id, 1);
    }

    #[tokio::test]
    async fn resolved_incident_cannot_be_committed_again() {
        let store = store_with_resources(2);

        commit(&store, "r1", 10, &[1]).await;
        store.resolve("resolve-1", 10).await.unwrap();

        let result = commit(&store, "r2", 10, &[2]).await;

        assert_eq!(result, CommitOutcome::IncidentResolved);

        let snapshot = store.snapshot().await.unwrap();

        assert_eq!(snapshot.assignments.len(), 0);
        assert_eq!(snapshot.incidents[0].status, IncidentStatus::Resolved);
    }

    #[tokio::test]
    async fn resolving_unknown_incident_succeeds_without_releasing_resources() {
        let store = store_with_resources(2);

        commit(&store, "r1", 10, &[1]).await;

        let result = store.resolve("resolve-unknown", 999).await;

        assert!(result.is_ok());
        // A later commit for this incident must be rejected.
        let later_commit = store
            .commit("r2", incident(999), plan_assigning(999, &[2]))
            .await;

        assert_eq!(later_commit, CommitOutcome::IncidentResolved);

        let snapshot = store.snapshot().await.unwrap();

        assert_eq!(snapshot.assignments.len(), 1);
        assert_eq!(snapshot.assignments[0].incident_id, 10);
        assert_eq!(snapshot.assignments[0].resource_id, 1);
    }
}
