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
use serde::{Deserialize, Serialize};

use crate::domain::{Incident, IncidentId, Plan, ResourceId, Snapshot};
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

/// What happened to one [`SharedState::commit`] call.
///
/// An enum with one variant per case (instead of `Result<Plan, String>`) so the compiler
/// forces every caller to decide what to do for each case.
#[derive(Debug, Clone, PartialEq)]
pub enum CommitOutcome {
    /// This call stored the plan. It is now the one plan for that `request_id`.
    /// Why separate from `AlreadyCommitted`: only the caller that got `Committed` did the work,
    /// so only it reports `duplicate = false` to the client.
    Committed,

    /// The `request_id` already had a stored plan (from an earlier call, a retry, or another
    /// node). Nothing was written. The caller MUST reply with this plan and discard the one
    /// it computed, otherwise a client could receive two different plans for one request.
    AlreadyCommitted(Plan),

    /// A resource in the plan was not free when the write happened (taken by another request
    /// after the snapshot was copied; or not a known resource; or named twice in one plan).
    /// Nothing was written. The caller should take a fresh snapshot, plan again and retry.
    /// Carries the first offending resource so the log can say which one collided.
    ResourceTaken {
        resource_id: ResourceId,
    },

    /// The plan was empty: no resource was free. Nothing was stored as this request's answer;
    /// instead the incident joined the pending queue ([`SharedState::pending`]), and a node
    /// assigns it later, oldest first, when a resource is free (P1, Phase 1 fix).
    /// Why not store the empty plan (the old rule): a stored plan is final, so every retry of
    /// that request got "nothing" back even after resources were freed, and the incident was
    /// never served.
    Pending,

    /// Not enough of the shared state could be reached to decide. Nothing is known to have
    /// been written. Why not simply "Committed" or an error: the write may or may not have
    /// landed, so the caller must retry with the SAME `request_id`; the create-if-absent rule
    /// makes that safe, and the retry will come back `AlreadyCommitted` if the first one landed.
    /// The incident was already resolved, so this submission cannot be committed.
    /// This is a final outcome, not a temporary shared-state failure.
    IncidentResolved,
    StateUnavailable(StateUnavailable),
}

/// One incident waiting in the pending queue for a resource (see [`CommitOutcome::Pending`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingIncident {
    /// The client's request: the plan made later is stored under this ID, so a retry of the
    /// same request finds it.
    pub request_id: String,
    pub incident: Incident,
    /// When the incident joined the queue (Unix ms). Orders the queue: oldest first.
    pub since_ms: u64,
}

impl PendingIncident {
    /// True once the incident has waited its time-to-live in the queue: no longer worth
    /// serving. `ttl_secs = 0` means no limit.
    ///
    /// Counted from `since_ms` (a node's clock when it was queued), not from the incident's
    /// `timestamp`: that is the reporter's clock, and generated test incidents carry fixed
    /// timestamps from 2023, which would make every one of them "expired" at once.
    pub fn expired(&self, now_ms: u64) -> bool {
        self.incident.ttl_secs > 0
            && now_ms >= self.since_ms.saturating_add(self.incident.ttl_secs.saturating_mul(1000))
    }
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
    /// An empty plan ("nothing was free") is NOT stored as the answer: the incident is added to
    /// the pending queue and the result is [`CommitOutcome::Pending`]. A retry of that request
    /// then plans again, or gets the plan a node made from the queue meanwhile.
    async fn commit(&self, request_id: &str, incident: Incident, plan: Plan) -> CommitOutcome;

    /// The stored plan for `request_id`, or `None` if this request was never committed.
    ///
    /// Used to answer a client retry (which may reach any node) with the original plan.
    /// `Ok(None)` means "definitely not committed"; if that cannot be known, the answer is
    /// `Err(StateUnavailable)`, never `Ok(None)`.
    async fn lookup_request(&self, request_id: &str) -> Result<Option<Plan>, StateUnavailable>;
    /// Resolve an incident and release every resource assigned to it.
    ///
    /// The incident ID is recorded as resolved even if the incident is
    /// unknown when this method is called. This prevents a later commit
    /// from creating an assignment for an incident that was already resolved.
    ///
    /// Repeating the same resolve request is safe. Reusing its request ID
    /// for a different incident returns an error.
    async fn resolve(
        &self,
        request_id: &str,
        incident_id: IncidentId,
    ) -> Result<(), StateUnavailable>;

    /// The incidents waiting for a resource, oldest first, at most `limit` of them.
    ///
    /// A request can appear more than once (a client retry while it was still waiting). That
    /// is harmless: the first one committed makes the others come back `AlreadyCommitted`.
    /// The default (no queue) is for test doubles; every real backend overrides it.
    async fn pending(&self, _limit: usize) -> Result<Vec<PendingIncident>, StateUnavailable> {
        Ok(Vec::new())
    }

    /// Removes one entry from the pending queue (it was assigned, resolved, or its TTL ran
    /// out). Removing an entry that is already gone is not an error.
    async fn drop_pending(&self, _entry: &PendingIncident) -> Result<(), StateUnavailable> {
        Ok(())
    }

    /// P5, Step C: called by a node coming back from a mute (P3's rejoin readiness rule)
    /// before it bids again. `Ok` means the shared state is reachable and the returned copy
    /// is current as of this call; `Err` means the node is not ready yet and must not bid.
    async fn reload(&self) -> Result<Snapshot, StateUnavailable> {
        self.snapshot().await
    }
}
