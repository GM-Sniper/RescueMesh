//! The interface to the cluster's shared state (incidents, resources, assignments).
//!
//! Shared state is moving into a distributed file system (see DESIGN_LOG.md, "P4: Shared state
//! in a distributed file system"). Every node code against the [`SharedState`] trait defined
//! here, so the backend can be swapped (an in-memory store now, a DFS-backed store later)
//! without touching the handlers, the election or the load generator.
//!
//! Three ideas carry over from the old per-node store, and the trait is built around them:
//! 1. `request_id` is the dedup key. A client retry can land on a different node, so the
//!    stored plan is looked up by `request_id`, never by incident or by node.
//! 2. Ordering comes from the backend (a version or counter), never from wall clocks.
//! 3. The write path re-checks that every planned resource is still free, because a
//!    [`Snapshot`] is only a copy and can be stale by the time the plan is written.

use std::fmt;

use async_trait::async_trait;

use crate::NodeId;
use crate::domain::{Incident, Plan, ResourceId, Snapshot};

/// The shared state could not be reached well enough to give a trustworthy answer.
///
/// A separate type (instead of a bare string) so callers can match on "unavailable" and
/// treat it differently from a normal answer. Today it only carries a reason for the logs;
/// the in-memory store never returns it, a DFS-backed store will (network down, too few
/// replicas reachable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateUnavailable {
    /// Human-readable cause, for logs. Callers must not parse it.
    pub reason: String,
}

impl fmt::Display for StateUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shared state unavailable: {}", self.reason)
    }
}

impl std::error::Error for StateUnavailable {}

/// Where a request is in its life. Stored with the plan.
///
/// Why three states and not two: "resources reserved" and "work confirmed done" are different
/// facts. A node can fail between them, and the survivors must be able to tell a request that
/// needs a new owner (`Assigned` to a dead node) from one that must be left alone (`Completed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestStatus {
    /// Resources are reserved for the request and its owner has not yet confirmed it is done.
    Assigned,
    /// The owner confirmed the work is done. Final: never released, never re-assigned.
    Completed,
    /// The owner was declared failed, its resources were freed, and the request is waiting for
    /// a new owner. The old owner stays recorded so that a late call from it can be recognised
    /// and rejected.
    Released,
}

/// The shared-state record of one client request.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestRecord {
    /// The dedup key, as in [`SharedState::commit`].
    pub request_id: String,
    /// The node that executes the request (for a `Released` record: the node that did).
    pub owner_node_id: NodeId,
    pub status: RequestStatus,
    /// The plan that reserved the resources (kept after release, for the record).
    pub plan: Plan,
}

/// What happened to one [`SharedState::mark_completed`] call. Every case except `Completed`
/// changed nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum MarkCompletedOutcome {
    /// The record was `Assigned` to the caller and is now `Completed`.
    Completed,
    /// The record is not `Assigned` to the caller: it belongs to another node, or it was
    /// `Released` (the caller was declared failed). A node that comes back late gets this
    /// instead of overwriting the new owner's work.
    NotCurrentOwner { current_status: RequestStatus, current_owner_node_id: NodeId },
    /// The record is already `Completed`. Harmless: a retried confirmation.
    AlreadyCompleted,
    /// No record with that `request_id`.
    NotFound,
    /// The shared state could not be reached; the change may or may not have landed. Retrying
    /// is safe, because the call is conditional.
    StateUnavailable(StateUnavailable),
}

/// What happened to one [`SharedState::release_unfinished_request`] call. Every case except
/// `Released` changed nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum ReleaseOutcome {
    /// The record was `Assigned` to the expected owner: its resources are free and the status
    /// is now `Released`.
    Released,
    /// The record is already `Released` by the same owner. A second call is a no-op, so two
    /// cleaners cannot do harm.
    AlreadyReleased,
    /// The record is no longer `Assigned` to the expected owner (a new owner has taken it, or
    /// it was never that node's).
    NotCurrentOwner { current_status: RequestStatus, current_owner_node_id: NodeId },
    /// The record is `Completed`. Finished work is never released.
    AlreadyCompleted,
    /// No record with that `request_id`.
    NotFound,
    /// The shared state could not be reached. Retrying is safe, because the call is conditional.
    StateUnavailable(StateUnavailable),
}

/// What happened to one [`SharedState::commit`] call.
///
/// An enum with one variant per case (instead of `Result<Plan, String>`) so the compiler
/// forces every caller to decide what to do for each case.
#[derive(Debug, Clone, PartialEq)]
pub enum CommitOutcome {
    /// This call stored the plan, with status `Assigned` and the caller as owner. It is now the
    /// one plan for that `request_id`. Also the answer when the previous record was `Released`:
    /// the commit atomically moved it to `Assigned` with the new owner.
    /// Why separate from `AlreadyCommitted`: only the caller that got `Committed` did the work,
    /// so only it reports `duplicate = false` to the client.
    Committed,

    /// The `request_id` already had a stored plan that is `Assigned` or `Completed` (from an
    /// earlier call, a retry, or another node). Nothing was written. A `Released` record does
    /// not count: committing over it is allowed and returns `Committed`. The caller MUST reply with this plan and discard the one
    /// it computed, otherwise a client could receive two different plans for one request.
    AlreadyCommitted(Plan),

    /// A resource in the plan was not free when the write happened (taken by another request
    /// after the snapshot was copied, or named twice in one plan). Nothing was written.
    /// The caller should take a fresh snapshot, plan again and retry: this is an ordinary
    /// race, not a fault. Carries the first offending resource so the log can say which one
    /// collided.
    ResourceTaken { resource_id: ResourceId },

    /// A resource in the plan does not exist in the shared state at all. Nothing was written.
    /// Why separate from `ResourceTaken`: retrying on a fresh snapshot cannot fix this, so
    /// the caller must NOT loop. Today it means a bug (the planner was given a resource the
    /// store does not hold, or two nodes disagree about the resource list).
    UnknownResource { resource_id: ResourceId },

    /// Not enough of the shared state could be reached to decide. Nothing is known to have
    /// been written. Why not simply "Committed" or an error: the write may or may not have
    /// landed, so the caller must retry with the SAME `request_id`; the create-if-absent rule
    /// makes that safe, and the retry will come back `AlreadyCommitted` if the first one landed.
    StateUnavailable(StateUnavailable),
}

/// Shared, cluster-wide state that every node reads and writes through.
///
/// # Why every method is `async` and can fail
/// A distributed-file-system write or read is a network call, and a network call can be slow
/// or fail. If the trait were synchronous and infallible now (it would be easy, the in-memory
/// store is), P1, P3 and P6 would have to rewrite every call site when the DFS arrives.
/// Making callers handle "slow" and "unavailable" from day one costs a few `.await`s now.
///
/// # Why `#[async_trait]` instead of native `async fn` in traits
/// Nodes hold the backend as `Arc<dyn SharedState>` so the choice (in-memory or DFS) can be
/// made once at start-up from configuration. Native `async fn` in traits is not usable behind
/// `dyn`; `async_trait` is, at the cost of one boxed future per call (negligible next to a
/// network round trip).
///
/// # How to call it (the order a Submit handler follows)
/// 1. [`lookup_request`](SharedState::lookup_request): already handled? Reply with the stored plan.
/// 2. [`snapshot`](SharedState::snapshot): copy the state for the planner.
/// 3. Plan (CPU work).
/// 4. [`commit`](SharedState::commit): store the plan. Act on the [`CommitOutcome`].
#[async_trait]
pub trait SharedState: Send + Sync {
    /// A consistent copy of the state for the planner to read.
    ///
    /// It is a copy: by the time the plan is ready the real state may have moved on, which is
    /// why `commit` re-checks. Fails with [`StateUnavailable`] if the state cannot be read.
    async fn snapshot(&self) -> Result<Snapshot, StateUnavailable>;

    /// Atomic create-if-absent: store `incident` and `plan` under `request_id` unless that
    /// `request_id` already has a stored plan. Exactly one committer wins per `request_id`,
    /// even when many nodes commit at once.
    ///
    /// Why `incident` is a parameter as well as `plan`: a `Plan` only holds an incident id, and
    /// the stored state must contain the incident itself (as `Assigned`, or as `Open` when the
    /// plan has no assignments because no resource was free).
    ///
    /// Why an empty plan is allowed: "nothing was free" is a real answer. It is stored, so a
    /// retry of that request gets the same empty plan instead of a fresh attempt.
    ///
    /// `owner_node_id` is the node that will execute the plan. It is stored with the record so
    /// that, if that node fails, the others can find and release its unfinished requests.
    /// The record starts as `Assigned`. If the existing record is `Released`, this call replaces
    /// it atomically (new owner, new plan, `Assigned`), and exactly one concurrent committer wins.
    async fn commit(
        &self,
        request_id: &str,
        owner_node_id: NodeId,
        incident: Incident,
        plan: Plan,
    ) -> CommitOutcome;

    /// Confirms that `node_id` finished the request. Conditional: succeeds only if the record is
    /// still `Assigned` to `node_id`; otherwise it changes nothing and says why.
    ///
    /// Why conditional (no special "late owner" logic): a node that was declared failed and then
    /// returns still believes it owns the request. One check on the record handles every case:
    /// if the record was released or re-assigned, the late confirmation is rejected and cannot
    /// clobber the new owner's work.
    async fn mark_completed(&self, request_id: &str, node_id: NodeId) -> MarkCompletedOutcome;

    /// The requests that are `Assigned` to `node_id` (status neither `Completed` nor `Released`),
    /// sorted by `request_id`. This is what a node elected to clean up after a failed peer reads.
    async fn list_unfinished_requests_owned_by(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<RequestRecord>, StateUnavailable>;

    /// Frees every resource the request reserved and sets its status to `Released`, but only if
    /// the record is still `Assigned` to `expected_owner_node_id`. A second call for the same
    /// record is a no-op (`AlreadyReleased`). Never touches a `Completed` record.
    ///
    /// Why conditional: the election that picks the cleaner can briefly give two winners after a
    /// restart (spike 2), so two nodes may release the same request. The condition makes that
    /// harmless. It also stops a slow cleaner from releasing a request that a new owner has
    /// already taken.
    async fn release_unfinished_request(
        &self,
        request_id: &str,
        expected_owner_node_id: NodeId,
    ) -> ReleaseOutcome;

    /// The stored plan for `request_id`, or `None` if this request was never committed or its
    /// record is `Released` (waiting for a new owner, so a retry should be planned afresh).
    ///
    /// Used to answer a client retry (which may reach any node) with the original plan.
    /// `Ok(None)` means "definitely not committed"; if that cannot be known, the answer is
    /// `Err(StateUnavailable)`, never `Ok(None)`.
    async fn lookup_request(&self, request_id: &str) -> Result<Option<Plan>, StateUnavailable>;
}

/// Releases every unfinished request owned by a node that was declared failed: list them, then
/// release each one. Returns the outcome per request, so the caller can log which were released.
///
/// Who calls it: the node ELECTED to handle the failed peer's cleanup (one cleaner, to avoid
/// duplicate work). It is deliberately not wired into the failure detector (P5) or the election
/// (P3); they decide when it runs. Each release is conditional, so a second cleaner running
/// by mistake does no harm.
pub async fn release_unfinished_requests_of_failed_node(
    shared_state: &dyn SharedState,
    failed_node_id: NodeId,
) -> Result<Vec<(String, ReleaseOutcome)>, StateUnavailable> {
    let unfinished = shared_state.list_unfinished_requests_owned_by(failed_node_id).await?;
    let mut outcomes = Vec::with_capacity(unfinished.len());
    for record in unfinished {
        let outcome = shared_state
            .release_unfinished_request(&record.request_id, failed_node_id)
            .await;
        outcomes.push((record.request_id, outcome));
    }
    Ok(outcomes)
}
