//! P4: the in-memory implementation of `common::shared_state::SharedState`.
//!
//! It is the stand-in behind the shared-state interface until a distributed-file-system backend
//! exists (DESIGN_LOG.md, "P4: Shared state in a distributed file system"). Each node has its
//! own copy, so unlike the real backend it does NOT share anything between nodes; it exists so
//! handlers, tests and the election can be written against the final interface today.
//!
//! The old change log, `applied` map, `entries_since` and replication path were removed when
//! the design moved to a DFS. Three ideas were kept:
//! - `request_id` is the dedup key (`records_by_request`).
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
use common::NodeId;
use common::shared_state::{
    CommitOutcome, MarkCompletedOutcome, ReleaseOutcome, RequestRecord, RequestStatus, SharedState,
    StateUnavailable,
};

// TODO(P4): remove once clients can offer resources; make configurable only if experiments
// need a different resource count.
// Every node must start with the same resources, so these are the same constants everywhere.
const DEFAULT_RESOURCE_SEED: u64 = 42;
const DEFAULT_RESOURCE_COUNT: usize = 10;

#[derive(Default)]
struct Contents {
    incidents: BTreeMap<IncidentId, Incident>,
    resources: BTreeMap<ResourceId, Resource>,
    /// Keyed by resource id: a resource has at most one assignment.
    assignments: BTreeMap<ResourceId, Assignment>,
    /// The one stored record per client request: the create-if-absent record, with its owner
    /// and status (`Assigned`, `Completed`, `Released`).
    records_by_request: HashMap<String, RequestRecord>,
}

/// Shared state kept in this process's memory. Implements [`SharedState`].
///
/// Why one `RwLock` around everything: `commit`, `mark_completed` and `release_unfinished_request`
/// must check and write several maps as one atomic step, and a single lock is the simplest way
/// to guarantee that.
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
        let resources =
            generate_snapshot(DEFAULT_RESOURCE_SEED, 0, DEFAULT_RESOURCE_COUNT).resources;
        InMemoryStore::new(resources)
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

    async fn commit(
        &self,
        request_id: &str,
        owner_node_id: NodeId,
        mut incident: Incident,
        plan: Plan,
    ) -> CommitOutcome {
        let mut contents = self.write();

        // Create-if-absent: two retries of one request can both get past `lookup_request`,
        // so the check must be repeated here, inside the lock. A `Released` record does not
        // block the commit: it is waiting for a new owner, and this call becomes that owner.
        // Because the check and the write share one lock, exactly one concurrent committer
        // sees `Released`; the others then see `Assigned` and get `AlreadyCommitted`.
        if let Some(existing) = contents.records_by_request.get(request_id) {
            if existing.status != RequestStatus::Released {
                return CommitOutcome::AlreadyCommitted(existing.plan.clone());
            }
        }

        // Every planned resource must exist, be unassigned, and appear only once in the plan.
        // "Unknown" is reported apart from "taken": re-planning fixes a race but not a
        // resource that does not exist. Nothing is written before all checks pass.
        let mut seen_in_this_plan = HashSet::new();
        for assignment in &plan.assignments {
            let resource_id = assignment.resource_id;
            if !contents.resources.contains_key(&resource_id) {
                return CommitOutcome::UnknownResource { resource_id };
            }
            let is_free = !contents.assignments.contains_key(&resource_id);
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
        contents.records_by_request.insert(
            request_id.to_string(),
            RequestRecord {
                request_id: request_id.to_string(),
                owner_node_id,
                status: RequestStatus::Assigned,
                plan,
            },
        );
        CommitOutcome::Committed
    }

    async fn mark_completed(&self, request_id: &str, node_id: NodeId) -> MarkCompletedOutcome {
        let mut contents = self.write();
        let Some(record) = contents.records_by_request.get(request_id) else {
            return MarkCompletedOutcome::NotFound;
        };
        match (record.status, record.owner_node_id == node_id) {
            (RequestStatus::Completed, _) => return MarkCompletedOutcome::AlreadyCompleted,
            (RequestStatus::Assigned, true) => {}
            (status, _) => {
                return MarkCompletedOutcome::NotCurrentOwner {
                    current_status: status,
                    current_owner_node_id: record.owner_node_id,
                };
            }
        }
        let incident_id = record.plan.incident_id;
        contents
            .records_by_request
            .get_mut(request_id)
            .expect("record was found above under the same lock")
            .status = RequestStatus::Completed;
        // The reserved resources stay assigned after completion (see open question in
        // docs/p4-changes.md); only the incident moves on.
        if let Some(incident) = contents.incidents.get_mut(&incident_id) {
            incident.status = IncidentStatus::Resolved;
        }
        MarkCompletedOutcome::Completed
    }

    async fn list_unfinished_requests_owned_by(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<RequestRecord>, StateUnavailable> {
        let contents = self.read();
        let mut unfinished: Vec<RequestRecord> = contents
            .records_by_request
            .values()
            .filter(|r| r.owner_node_id == node_id && r.status == RequestStatus::Assigned)
            .cloned()
            .collect();
        unfinished.sort_by(|a, b| a.request_id.cmp(&b.request_id));
        Ok(unfinished)
    }

    async fn release_unfinished_request(
        &self,
        request_id: &str,
        expected_owner_node_id: NodeId,
    ) -> ReleaseOutcome {
        let mut contents = self.write();
        let Some(record) = contents.records_by_request.get(request_id) else {
            return ReleaseOutcome::NotFound;
        };
        let owner_matches = record.owner_node_id == expected_owner_node_id;
        match (record.status, owner_matches) {
            (RequestStatus::Completed, _) => return ReleaseOutcome::AlreadyCompleted,
            (RequestStatus::Released, true) => return ReleaseOutcome::AlreadyReleased,
            (RequestStatus::Assigned, true) => {}
            (status, _) => {
                return ReleaseOutcome::NotCurrentOwner {
                    current_status: status,
                    current_owner_node_id: record.owner_node_id,
                };
            }
        }
        let incident_id = record.plan.incident_id;
        let reserved: Vec<ResourceId> = record.plan.assignments.iter().map(|a| a.resource_id).collect();
        // Only remove an assignment that still belongs to this request's incident, so a
        // resource that somehow belongs to another incident is never freed by mistake.
        for resource_id in reserved {
            if contents
                .assignments
                .get(&resource_id)
                .is_some_and(|a| a.incident_id == incident_id)
            {
                contents.assignments.remove(&resource_id);
            }
        }
        if let Some(incident) = contents.incidents.get_mut(&incident_id) {
            incident.status = IncidentStatus::Open;
        }
        contents
            .records_by_request
            .get_mut(request_id)
            .expect("record was found above under the same lock")
            .status = RequestStatus::Released;
        ReleaseOutcome::Released
    }

    async fn lookup_request(&self, request_id: &str) -> Result<Option<Plan>, StateUnavailable> {
        Ok(self
            .read()
            .records_by_request
            .get(request_id)
            .filter(|r| r.status != RequestStatus::Released)
            .map(|r| r.plan.clone()))
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
                1,
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
        assert_eq!(commit(&store, "r1", 10, &[1]).await, CommitOutcome::Committed);
        let before = store.snapshot().await.unwrap();

        // A retry may even have been planned differently; the stored plan still wins.
        let second = commit(&store, "r1", 10, &[2]).await;
        assert_eq!(second, CommitOutcome::AlreadyCommitted(plan_assigning(10, &[1])));

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
        assert_eq!(commit(&store, "first", 10, &[1]).await, CommitOutcome::Committed);

        // "second" planned from a snapshot taken before "first" committed; resource 2 is
        // valid but must not be written when resource 1 is taken.
        let outcome = commit(&store, "second", 11, &[2, 1]).await;
        assert_eq!(outcome, CommitOutcome::ResourceTaken { resource_id: 1 });
        let snapshot = store.snapshot().await.unwrap();
        assert_eq!(snapshot.assignments.len(), 1);
        assert_eq!(snapshot.incidents.len(), 1);
        assert_eq!(store.lookup_request("second").await.unwrap(), None);

        // After planning again with a free resource, the same request succeeds.
        assert_eq!(commit(&store, "second", 11, &[2]).await, CommitOutcome::Committed);
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
    async fn an_unknown_resource_is_reported_as_unknown_and_a_repeated_one_as_taken_and_nothing_is_written() {
        let store = store_with_resources(3);
        let empty = store.snapshot().await.unwrap();
        assert_eq!(
            commit(&store, "r1", 10, &[1, 77]).await,
            CommitOutcome::UnknownResource { resource_id: 77 }
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
        assert_eq!(commit(&store, "r1", 10, &[]).await, CommitOutcome::Committed);

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

    // ---- assigned / completed / released lifecycle ----

    use common::shared_state::release_unfinished_requests_of_failed_node;

    async fn commit_as(
        store: &InMemoryStore,
        owner: NodeId,
        request_id: &str,
        incident_id: u64,
        resource_ids: &[u64],
    ) -> CommitOutcome {
        store
            .commit(
                request_id,
                owner,
                incident(incident_id),
                plan_assigning(incident_id, resource_ids),
            )
            .await
    }

    #[tokio::test]
    async fn completing_a_request_twice_returns_already_completed_the_second_time() {
        let store = store_with_resources(3);
        commit_as(&store, 1, "r1", 10, &[1]).await;
        assert_eq!(store.mark_completed("r1", 1).await, MarkCompletedOutcome::Completed);
        assert_eq!(store.mark_completed("r1", 1).await, MarkCompletedOutcome::AlreadyCompleted);
        assert_eq!(
            store.snapshot().await.unwrap().incidents[0].status,
            IncidentStatus::Resolved
        );
    }

    #[tokio::test]
    async fn mark_completed_by_the_wrong_node_is_rejected_and_changes_nothing() {
        let store = store_with_resources(3);
        commit_as(&store, 1, "r1", 10, &[1]).await;
        assert_eq!(
            store.mark_completed("r1", 2).await,
            MarkCompletedOutcome::NotCurrentOwner {
                current_status: RequestStatus::Assigned,
                current_owner_node_id: 1
            }
        );
        assert_eq!(store.mark_completed("missing", 1).await, MarkCompletedOutcome::NotFound);
        // Still Assigned to node 1, so node 1 can complete it.
        assert_eq!(store.mark_completed("r1", 1).await, MarkCompletedOutcome::Completed);
    }

    #[tokio::test]
    async fn a_failed_nodes_requests_are_released_and_their_resources_become_free() {
        let store = store_with_resources(4);
        commit_as(&store, 1, "r1", 10, &[1]).await;
        commit_as(&store, 1, "r2", 11, &[2]).await;
        commit_as(&store, 2, "r3", 12, &[3]).await; // another node's request: must be untouched

        let unfinished = store.list_unfinished_requests_owned_by(1).await.unwrap();
        let ids: Vec<_> = unfinished.iter().map(|r| r.request_id.as_str()).collect();
        assert_eq!(ids, ["r1", "r2"]);

        let outcomes = release_unfinished_requests_of_failed_node(&store, 1).await.unwrap();
        assert_eq!(
            outcomes,
            vec![
                ("r1".to_string(), ReleaseOutcome::Released),
                ("r2".to_string(), ReleaseOutcome::Released)
            ]
        );
        let snapshot = store.snapshot().await.unwrap();
        let assigned: Vec<_> = snapshot.assignments.iter().map(|a| a.resource_id).collect();
        assert_eq!(assigned, [3], "only node 2's resource is still reserved");
        assert!(store.list_unfinished_requests_owned_by(1).await.unwrap().is_empty());
        assert_eq!(store.lookup_request("r1").await.unwrap(), None, "released: a retry plans afresh");
        // The freed resource can be taken by someone else at once.
        assert_eq!(commit_as(&store, 2, "r4", 13, &[1]).await, CommitOutcome::Committed);
    }

    #[tokio::test]
    async fn a_released_request_committed_by_a_new_node_rejects_the_old_owners_mark_completed() {
        let store = store_with_resources(3);
        commit_as(&store, 1, "r1", 10, &[1]).await;
        store.release_unfinished_request("r1", 1).await;

        assert_eq!(commit_as(&store, 2, "r1", 10, &[2]).await, CommitOutcome::Committed);
        assert_eq!(
            store.mark_completed("r1", 1).await,
            MarkCompletedOutcome::NotCurrentOwner {
                current_status: RequestStatus::Assigned,
                current_owner_node_id: 2
            }
        );
        assert_eq!(store.mark_completed("r1", 2).await, MarkCompletedOutcome::Completed);
    }

    #[tokio::test]
    async fn releasing_the_same_request_twice_is_a_no_op() {
        let store = store_with_resources(3);
        commit_as(&store, 1, "r1", 10, &[1]).await;
        assert_eq!(store.release_unfinished_request("r1", 1).await, ReleaseOutcome::Released);
        let after_first = store.snapshot().await.unwrap();
        assert_eq!(store.release_unfinished_request("r1", 1).await, ReleaseOutcome::AlreadyReleased);
        assert_eq!(store.snapshot().await.unwrap(), after_first);
        assert_eq!(store.release_unfinished_request("missing", 1).await, ReleaseOutcome::NotFound);
    }

    #[tokio::test]
    async fn a_release_naming_the_wrong_owner_changes_nothing() {
        let store = store_with_resources(3);
        commit_as(&store, 1, "r1", 10, &[1]).await;
        let before = store.snapshot().await.unwrap();
        assert_eq!(
            store.release_unfinished_request("r1", 2).await,
            ReleaseOutcome::NotCurrentOwner {
                current_status: RequestStatus::Assigned,
                current_owner_node_id: 1
            }
        );
        assert_eq!(store.snapshot().await.unwrap(), before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_concurrent_commits_on_a_released_request_give_exactly_one_winner() {
        let store = Arc::new(store_with_resources(8));
        commit_as(&store, 1, "r1", 10, &[1]).await;
        store.release_unfinished_request("r1", 1).await;

        let tasks: Vec<_> = (2..=7u64)
            .map(|new_owner| {
                let store = store.clone();
                // Each contender plans a different free resource, as separate nodes would.
                tokio::spawn(async move { commit_as(&store, new_owner as NodeId, "r1", 10, &[new_owner]).await })
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
        assert_eq!((committed, already_committed), (1, 5));
        assert_eq!(store.snapshot().await.unwrap().assignments.len(), 1, "one reservation only");
    }

    #[tokio::test]
    async fn a_completed_request_is_never_released() {
        let store = store_with_resources(3);
        commit_as(&store, 1, "r1", 10, &[1]).await;
        store.mark_completed("r1", 1).await;
        assert_eq!(store.release_unfinished_request("r1", 1).await, ReleaseOutcome::AlreadyCompleted);
        assert!(store.list_unfinished_requests_owned_by(1).await.unwrap().is_empty());
        assert_eq!(store.snapshot().await.unwrap().assignments.len(), 1);
        // And committing over a Completed record is AlreadyCommitted, not a re-assignment.
        assert!(matches!(
            commit_as(&store, 2, "r1", 10, &[2]).await,
            CommitOutcome::AlreadyCommitted(_)
        ));
    }

    #[tokio::test]
    async fn a_returned_nodes_stale_mark_completed_after_a_release_is_rejected() {
        let store = store_with_resources(3);
        commit_as(&store, 1, "r1", 10, &[1]).await;
        // Node 1 is declared failed and its request released; it then comes back and still
        // believes it owns the request.
        store.release_unfinished_request("r1", 1).await;
        assert_eq!(
            store.mark_completed("r1", 1).await,
            MarkCompletedOutcome::NotCurrentOwner {
                current_status: RequestStatus::Released,
                current_owner_node_id: 1
            }
        );
        // Nothing changed: the request is still waiting for a new owner.
        assert_eq!(store.lookup_request("r1").await.unwrap(), None);
        assert!(store.snapshot().await.unwrap().assignments.is_empty());
    }
}
