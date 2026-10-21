//! P4: the node's replicated state (incidents, resources, assignments) and its change log.
//!
//! Every state change is a `pb::LogEntry` identified by `(source_node, seq)`. The local write
//! path (`try_apply`) creates entries; the replication path (`apply_remote`) applies other
//! nodes' entries. Order comes from `(source_node, seq)`, never from clocks.
//!
//! Two dedup layers, both kept:
//! - `(source_node, seq)` makes applying a log entry idempotent (`applied` map).
//! - `request_id` makes a duplicate client request return the same stored plan (`by_request`).
//!
//! Invariant (holds after every call, because everything runs under one write lock):
//! `log[src].len() == applied[src]`, so `log[src][seq - 1]` is the entry numbered `seq`.
//! Design notes: DESIGN_LOG.md, "P4: Store and Change Log".

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use common::NodeId;
use common::conversions::{incident_from_pb, incident_to_pb, plan_from_pb, plan_to_pb};
use common::domain::*;
use common::generators::generate_snapshot;
use common::pb;

// TODO(P4): remove once clients can offer resources as log operations; make configurable only
// if experiments need a different resource count.
// Every node must start with the same resources, so these are the same constants everywhere.
const DEFAULT_RESOURCE_SEED: u64 = 42;
const DEFAULT_RESOURCE_COUNT: usize = 10;

/// Why `try_apply` wrote nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// A planned resource is already assigned (taken between `snapshot()` and `try_apply`).
    /// The handler should take a fresh snapshot, re-plan and retry.
    ResourceTaken { resource_id: ResourceId },
    /// A planned resource is not in the store. A bug, not retryable.
    UnknownResource { resource_id: ResourceId },
}

/// What `apply_remote` did with an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteOutcome {
    /// Next in line for its source: state, log, `by_request` and `applied` were updated.
    Applied,
    /// `seq <= applied[source]`: already have it, nothing changed.
    Duplicate,
    /// `seq` is beyond the next one. Nothing changed and nothing is buffered; the caller
    /// should fetch from this source starting after `expected - 1`.
    Gap { expected: u64 },
    /// The `request_id` is already stored (from `kept_origin`, first applied wins). The entry
    /// is logged and `applied[source]` advances, but the state is left alone.
    Superseded { kept_origin: NodeId },
    /// Malformed entry (no op, missing or bad incident or plan). Like `Superseded`, it is
    /// logged and `applied[source]` advances, with a warning; the state is untouched.
    Invalid(String),
}

/// Result of a successful `try_apply`.
#[derive(Debug, Clone)]
pub struct Applied {
    /// The log entry to replicate. For a duplicate request, the one written the first time.
    pub entry: pb::LogEntry,
    pub plan: Plan,
    /// True if `request_id` was already processed: nothing was written, the stored plan is returned.
    pub duplicate: bool,
}

#[derive(Default)]
struct Inner {
    incidents: BTreeMap<IncidentId, Incident>,
    resources: BTreeMap<ResourceId, Resource>,
    /// Keyed by resource id: a resource has at most one assignment.
    assignments: BTreeMap<ResourceId, Assignment>,
    /// Per source node, in seq order: `log[src][seq - 1]`.
    log: BTreeMap<NodeId, Vec<pb::LogEntry>>,
    /// Last seq applied per source node. Same shape as `FetchLogRequest.last_seq_by_source`.
    applied: BTreeMap<NodeId, u64>,
    /// request_id -> (origin node, seq of its entry, plan).
    /// The origin is kept so a later rule for "same request from two origins" can compare it.
    by_request: HashMap<String, (NodeId, u64, Plan)>,
}

impl Inner {
    fn applied_of(&self, src: NodeId) -> u64 {
        self.applied.get(&src).copied().unwrap_or(0)
    }

    /// Append `entry` to its source's log and advance `applied`. Does not touch state.
    fn log_entry(&mut self, entry: pb::LogEntry) {
        let src = entry.source_node;
        self.applied.insert(src, entry.seq);
        self.log.entry(src).or_default().push(entry);
    }

    /// Write one decided request into the state, the log and the indexes.
    fn commit(&mut self, entry: pb::LogEntry, incident: Incident, plan: Plan) {
        for a in &plan.assignments {
            // TODO(P4): a remote entry can name a resource a local entry already took (two
            // requests planned on different nodes). First writer wins here, so nodes can
            // diverge. Detect-and-reconcile is undecided; see DESIGN_LOG.md, P4.
            match self.assignments.entry(a.resource_id) {
                Entry::Vacant(slot) => {
                    slot.insert(a.clone());
                }
                Entry::Occupied(taken) => tracing::warn!(
                    resource_id = a.resource_id,
                    kept_incident = taken.get().incident_id,
                    skipped_incident = a.incident_id,
                    request_id = %entry.request_id,
                    source = entry.source_node,
                    seq = entry.seq,
                    "resource collision: resource already assigned, keeping the first assignment"
                ),
            }
        }
        self.incidents.insert(incident.id, incident);
        self.by_request
            .insert(entry.request_id.clone(), (entry.source_node, entry.seq, plan));
        self.log_entry(entry);
    }
}

pub struct Store {
    node_id: NodeId,
    inner: RwLock<Inner>,
}

impl Store {
    pub fn new(node_id: NodeId, resources: Vec<Resource>) -> Self {
        let inner = Inner {
            resources: resources.into_iter().map(|r| (r.id, r)).collect(),
            ..Inner::default()
        };
        Store {
            node_id,
            inner: RwLock::new(inner),
        }
    }

    /// Starting resources from fixed constants, identical on every node.
    pub fn with_default_resources(node_id: NodeId) -> Self {
        let resources =
            generate_snapshot(DEFAULT_RESOURCE_SEED, 0, DEFAULT_RESOURCE_COUNT).resources;
        Store::new(node_id, resources)
    }

    // A panic while holding the lock cannot leave `Inner` half-written (all checks come
    // before any write), so a poisoned lock is still safe to use.
    fn read(&self) -> RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Read-only copy of the state for the planner. Each Vec is sorted by id.
    pub fn snapshot(&self) -> Snapshot {
        let g = self.read();
        Snapshot {
            incidents: g.incidents.values().cloned().collect(),
            resources: g.resources.values().cloned().collect(),
            assignments: g.assignments.values().cloned().collect(),
        }
    }

    /// The stored plan for a request that was already processed, if any.
    pub fn lookup_request(&self, request_id: &str) -> Option<Plan> {
        self.read().by_request.get(request_id).map(|(_, _, p)| p.clone())
    }

    /// Last seq applied per source node (for `FetchLogRequest` and `ReplicateReply`).
    pub fn applied(&self) -> HashMap<NodeId, u64> {
        self.read().applied.iter().map(|(k, v)| (*k, *v)).collect()
    }

    /// Local write path: record that `incident` arrived and `plan` was decided.
    ///
    /// All checks and writes happen inside one write lock, so a plan made from an older
    /// snapshot cannot double-assign a resource, and concurrent calls get distinct seqs.
    /// On `Err` nothing was written.
    pub fn try_apply(
        &self,
        request_id: &str,
        mut incident: Incident,
        plan: Plan,
        now_ms: u64,
    ) -> Result<Applied, ApplyError> {
        let mut g = self.write();

        // Re-check inside the lock: two retries of one request can race past lookup_request.
        if let Some((origin, seq, stored)) = g.by_request.get(request_id) {
            let entry = g.log[origin][(*seq - 1) as usize].clone();
            return Ok(Applied {
                entry,
                plan: stored.clone(),
                duplicate: true,
            });
        }

        let mut seen = HashSet::new();
        for a in &plan.assignments {
            if !g.resources.contains_key(&a.resource_id) {
                return Err(ApplyError::UnknownResource {
                    resource_id: a.resource_id,
                });
            }
            if g.assignments.contains_key(&a.resource_id) || !seen.insert(a.resource_id) {
                return Err(ApplyError::ResourceTaken {
                    resource_id: a.resource_id,
                });
            }
        }

        // TODO(P4): an empty plan (nothing free) is logged as Open and stored in by_request, so
        // a retry gets the same empty plan. Decide whether to queue the incident and re-plan
        // when resources free up instead.
        incident.status = if plan.assignments.is_empty() {
            IncidentStatus::Open
        } else {
            IncidentStatus::Assigned
        };

        let seq = g.applied_of(self.node_id) + 1;
        let entry = make_entry(self.node_id, seq, request_id, now_ms, &incident, &plan);
        g.commit(entry.clone(), incident, plan.clone());
        Ok(Applied {
            entry,
            plan,
            duplicate: false,
        })
    }

    /// TEMP (demo branch only, agreed with P4, who replaces it with a proper release
    /// operation): the mission is over, so the resource is free again and the incident
    /// is resolved. Local only: not a log entry, so it does not replicate in Step B.
    /// Frees the resource only if it still serves `incident_id`; returns whether it did.
    pub fn release(&self, resource_id: ResourceId, incident_id: IncidentId) -> bool {
        let mut g = self.write();
        if !g
            .assignments
            .get(&resource_id)
            .is_some_and(|a| a.incident_id == incident_id)
        {
            return false;
        }
        g.assignments.remove(&resource_id);
        if let Some(incident) = g.incidents.get_mut(&incident_id) {
            incident.status = IncidentStatus::Resolved;
        }
        true
    }

    /// Replication path: apply an entry created by another node. Idempotent; never buffers.
    pub fn apply_remote(&self, entry: pb::LogEntry) -> RemoteOutcome {
        let mut g = self.write();
        let src = entry.source_node;
        let have = g.applied_of(src);

        if entry.seq <= have {
            return RemoteOutcome::Duplicate;
        }
        if entry.seq > have + 1 {
            return RemoteOutcome::Gap { expected: have + 1 };
        }

        let (incident, plan) = match parse_entry(&entry) {
            Ok(parsed) => parsed,
            Err(why) => {
                // Same rule as Superseded: log it and advance `applied`, or this source would
                // look like a gap forever and FetchLog would keep returning the bad entry.
                // TODO(P4): provisional. DESIGN_LOG.md, "P4: invalid in-order entries" lists the
                // alternatives (stop and alert, quarantine); revisit in Step B, when
                // replication calls apply_remote.
                tracing::warn!(
                    source = src,
                    seq = entry.seq,
                    request_id = %entry.request_id,
                    reason = %why,
                    "invalid log entry: logged and skipped, state untouched"
                );
                g.log_entry(entry);
                return RemoteOutcome::Invalid(why);
            }
        };

        // First applied wins. The entry is still logged and `applied` still advances: without
        // that, this source would look like a gap forever and `entries_since` would serve a
        // log with a hole.
        // TODO(P4): provisional. DESIGN_LOG.md, "P4: same request_id from two origins" lists
        // three alternatives to revisit before building the undo in Step B.
        if let Some((origin, _, _)) = g.by_request.get(&entry.request_id) {
            let kept_origin = *origin;
            g.log_entry(entry);
            return RemoteOutcome::Superseded { kept_origin };
        }

        g.commit(entry, incident, plan);
        RemoteOutcome::Applied
    }

    /// Everything newer than the caller's `last_seq_by_source` (a missing source means 0),
    /// ordered by (source, seq). Answers `FetchLog` directly.
    pub fn entries_since(&self, last_seq_by_source: &HashMap<NodeId, u64>) -> Vec<pb::LogEntry> {
        let g = self.read();
        let mut out = Vec::new();
        for (src, entries) in &g.log {
            let from = last_seq_by_source.get(src).copied().unwrap_or(0) as usize;
            if from < entries.len() {
                out.extend_from_slice(&entries[from..]);
            }
        }
        out
    }
}

// ---- LogEntry <-> domain wrappers (private; P2's conversions.rs is not edited) ----

fn make_entry(
    source_node: NodeId,
    seq: u64,
    request_id: &str,
    timestamp_ms: u64,
    incident: &Incident,
    plan: &Plan,
) -> pb::LogEntry {
    pb::LogEntry {
        source_node,
        seq,
        request_id: request_id.to_string(),
        timestamp_ms,
        op: Some(pb::log_entry::Op::ApplyPlan(pb::ApplyPlan {
            incident: Some(incident_to_pb(incident)),
            plan: Some(plan_to_pb(plan)),
        })),
    }
}

fn parse_entry(entry: &pb::LogEntry) -> Result<(Incident, Plan), String> {
    match &entry.op {
        Some(pb::log_entry::Op::ApplyPlan(ap)) => {
            let incident = ap
                .incident
                .clone()
                .ok_or("bad data: apply_plan has no incident")?;
            let plan = ap.plan.clone().ok_or("bad data: apply_plan has no plan")?;
            let incident = incident_from_pb(incident).map_err(|e| format!("bad data: {e}"))?;
            Ok((incident, plan_from_pb(plan)))
        }
        // An op variant added by a newer node decodes as `None` here (prost drops fields it
        // does not know), so a missing op and an op we do not understand look the same.
        None => Err("unsupported op: no op we understand".to_string()),
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

    fn store(node: NodeId, n_res: u64) -> Store {
        Store::new(node, (1..=n_res).map(resource).collect())
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
    fn plan(incident_id: u64, resources: &[u64]) -> Plan {
        Plan {
            incident_id,
            priority: 1.0,
            assignments: resources
                .iter()
                .map(|&r| Assignment {
                    incident_id,
                    resource_id: r,
                    score: 0.5,
                })
                .collect(),
        }
    }

    fn apply(s: &Store, rid: &str, inc: u64, res: &[u64]) -> Applied {
        s.try_apply(rid, incident(inc), plan(inc, res), 99).unwrap()
    }

    #[test]
    fn release_frees_the_resource_only_for_its_own_incident() {
        let s = store(1, 3);
        apply(&s, "r1", 10, &[2]);
        assert!(!s.release(2, 11), "another incident cannot free it");
        assert!(s.release(2, 10));
        assert!(!s.release(2, 10), "already free");
        let snap = s.snapshot();
        assert!(snap.assignments.is_empty());
        assert_eq!(snap.incidents[0].status, IncidentStatus::Resolved);
        // Free again: a new request can take it.
        assert_eq!(apply(&s, "r2", 12, &[2]).plan.assignments.len(), 1);
    }

    #[test]
    fn default_resources_are_the_same_on_every_node() {
        let a = Store::with_default_resources(1).snapshot();
        let b = Store::with_default_resources(2).snapshot();
        assert_eq!(a.resources.len(), DEFAULT_RESOURCE_COUNT);
        assert_eq!(a, b);
    }

    #[test]
    fn local_apply_writes_everything_and_numbers_seq_from_one() {
        let s = store(1, 3);
        let a = apply(&s, "r1", 10, &[2]);
        let b = apply(&s, "r2", 11, &[3]);
        assert_eq!((a.entry.seq, b.entry.seq), (1, 2));
        assert!(!a.duplicate);
        let snap = s.snapshot();
        assert_eq!(snap.incidents.len(), 2);
        assert_eq!(snap.incidents[0].status, IncidentStatus::Assigned);
        assert_eq!(snap.assignments.len(), 2);
        assert_eq!(s.applied().get(&1), Some(&2));
        assert_eq!(s.lookup_request("r1"), Some(plan(10, &[2])));
    }

    #[test]
    fn snapshot_is_sorted_by_id() {
        let s = store(1, 4);
        apply(&s, "a", 30, &[4]);
        apply(&s, "b", 10, &[1]);
        apply(&s, "c", 20, &[3]);
        let snap = s.snapshot();
        let inc: Vec<_> = snap.incidents.iter().map(|i| i.id).collect();
        let res: Vec<_> = snap.resources.iter().map(|r| r.id).collect();
        let asg: Vec<_> = snap.assignments.iter().map(|a| a.resource_id).collect();
        assert_eq!(inc, [10, 20, 30]);
        assert_eq!(res, [1, 2, 3, 4]);
        assert_eq!(asg, [1, 3, 4]);
    }

    #[test]
    fn duplicate_request_id_returns_same_plan_and_seq_and_writes_nothing() {
        let s = store(1, 3);
        let first = apply(&s, "r1", 10, &[1]);
        // A retry may even have been re-planned differently; the stored plan still wins.
        let again = s.try_apply("r1", incident(10), plan(10, &[2]), 500).unwrap();
        assert!(again.duplicate);
        assert_eq!(again.plan, first.plan);
        assert_eq!(again.entry.seq, first.entry.seq);
        assert_eq!(again.entry, first.entry);
        assert_eq!(s.applied().get(&1), Some(&1));
        assert_eq!(s.snapshot().assignments.len(), 1);
    }

    #[test]
    fn resource_taken_writes_nothing() {
        let s = store(1, 3);
        apply(&s, "r1", 10, &[1]);
        let before = s.snapshot();
        let err = s
            .try_apply("r2", incident(11), plan(11, &[2, 1]), 99)
            .unwrap_err();
        assert_eq!(err, ApplyError::ResourceTaken { resource_id: 1 });
        assert_eq!(s.snapshot(), before); // resource 2 was valid but must not be assigned
        assert_eq!(s.applied().get(&1), Some(&1));
        assert_eq!(s.lookup_request("r2"), None);
        // A fresh plan with a free resource now works and gets the next seq.
        assert_eq!(apply(&s, "r2", 11, &[2]).entry.seq, 2);
    }

    #[test]
    fn unknown_resource_writes_nothing() {
        let s = store(1, 3);
        let err = s
            .try_apply("r1", incident(10), plan(10, &[1, 77]), 99)
            .unwrap_err();
        assert_eq!(err, ApplyError::UnknownResource { resource_id: 77 });
        assert_eq!(s.snapshot().assignments.len(), 0);
        assert!(s.snapshot().incidents.is_empty());
        assert!(s.applied().is_empty());
        assert_eq!(s.lookup_request("r1"), None);
    }

    #[test]
    fn same_resource_twice_in_one_plan_is_taken() {
        let s = store(1, 3);
        let err = s
            .try_apply("r1", incident(10), plan(10, &[1, 1]), 99)
            .unwrap_err();
        assert_eq!(err, ApplyError::ResourceTaken { resource_id: 1 });
        assert!(s.snapshot().assignments.is_empty());
    }

    #[test]
    fn empty_plan_is_logged_as_open_and_remembered() {
        let s = store(1, 3);
        let a = apply(&s, "r1", 10, &[]);
        assert!(a.plan.assignments.is_empty());
        let snap = s.snapshot();
        assert_eq!(snap.incidents[0].status, IncidentStatus::Open);
        assert!(snap.assignments.is_empty());
        assert_eq!(s.applied().get(&1), Some(&1));
        // A retry gets the same empty plan, not a fresh attempt.
        let again = apply(&s, "r1", 10, &[1]);
        assert!(again.duplicate);
        assert!(again.plan.assignments.is_empty());
    }

    #[test]
    fn seq_is_contiguous_under_concurrent_try_apply() {
        let s = Arc::new(store(1, 8));
        let handles: Vec<_> = (1..=8u64)
            .map(|i| {
                let s = s.clone();
                std::thread::spawn(move || {
                    s.try_apply(&format!("r{i}"), incident(i), plan(i, &[i]), 99)
                        .unwrap()
                        .entry
                        .seq
                })
            })
            .collect();
        let mut seqs: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        seqs.sort();
        assert_eq!(seqs, (1..=8).collect::<Vec<_>>());
        assert_eq!(s.applied().get(&1), Some(&8));
        let log = s.entries_since(&HashMap::new());
        assert_eq!(log.iter().map(|e| e.seq).collect::<Vec<_>>(), seqs);
    }

    #[test]
    fn concurrent_requests_for_one_resource_only_one_wins() {
        let s = Arc::new(store(1, 1));
        let handles: Vec<_> = (1..=6u64)
            .map(|i| {
                let s = s.clone();
                std::thread::spawn(move || {
                    s.try_apply(&format!("r{i}"), incident(i), plan(i, &[1]), 99)
                        .is_ok()
                })
            })
            .collect();
        let wins = handles.into_iter().map(|h| h.join().unwrap()).filter(|ok| *ok);
        assert_eq!(wins.count(), 1);
        assert_eq!(s.snapshot().assignments.len(), 1);
    }

    #[test]
    fn entries_since_returns_only_newer_entries_per_source() {
        let a = store(1, 5);
        for i in 1..=3u64 {
            apply(&a, &format!("a{i}"), i, &[i]);
        }
        // Entries from node 2 arrive at a.
        let b = store(2, 5);
        let from_b = apply(&b, "b1", 50, &[5]).entry;
        assert_eq!(a.apply_remote(from_b), RemoteOutcome::Applied);

        let all = a.entries_since(&HashMap::new());
        let ids: Vec<_> = all.iter().map(|e| (e.source_node, e.seq)).collect();
        assert_eq!(ids, [(1, 1), (1, 2), (1, 3), (2, 1)]);

        let have = HashMap::from([(1, 2), (2, 1)]);
        let newer: Vec<_> = a
            .entries_since(&have)
            .iter()
            .map(|e| (e.source_node, e.seq))
            .collect();
        assert_eq!(newer, [(1, 3)]);

        let all_have = HashMap::from([(1, 3), (2, 1), (9, 4)]);
        assert!(a.entries_since(&all_have).is_empty());
        // A caller claiming more than we have gets nothing, not a panic.
        assert!(a.entries_since(&HashMap::from([(1, 50)])).iter().all(|e| e.source_node != 1));
    }

    #[test]
    fn remote_entry_applies_once_then_is_a_duplicate() {
        let src = store(1, 3);
        let entry = apply(&src, "r1", 10, &[1]).entry;

        let dst = store(2, 3);
        assert_eq!(dst.apply_remote(entry.clone()), RemoteOutcome::Applied);
        let after_first = dst.snapshot();
        assert_eq!(dst.apply_remote(entry), RemoteOutcome::Duplicate);
        assert_eq!(dst.snapshot(), after_first);
        assert_eq!(dst.applied().get(&1), Some(&1));
        assert_eq!(dst.entries_since(&HashMap::new()).len(), 1);
        assert_eq!(dst.lookup_request("r1"), Some(plan(10, &[1])));
    }

    #[test]
    fn remote_gap_changes_nothing() {
        let src = store(1, 3);
        apply(&src, "r1", 10, &[1]);
        let second = apply(&src, "r2", 11, &[2]).entry;

        let dst = store(2, 3);
        assert_eq!(
            dst.apply_remote(second.clone()),
            RemoteOutcome::Gap { expected: 1 }
        );
        assert_eq!(dst.snapshot(), store(2, 3).snapshot());
        assert!(dst.applied().is_empty());
        assert!(dst.entries_since(&HashMap::new()).is_empty());

        // Once the missing one arrives (not buffered, so re-send), the second applies too.
        let first = src.entries_since(&HashMap::new())[0].clone();
        assert_eq!(dst.apply_remote(first), RemoteOutcome::Applied);
        assert_eq!(dst.apply_remote(second), RemoteOutcome::Applied);
    }

    #[test]
    fn invalid_in_order_entries_are_logged_and_advance_applied_but_change_no_state() {
        let src = store(1, 3);
        let good = apply(&src, "r1", 10, &[1]).entry;

        let mut no_op = good.clone();
        no_op.op = None;

        let mut no_incident = good.clone();
        no_incident.op = Some(pb::log_entry::Op::ApplyPlan(pb::ApplyPlan {
            incident: None,
            plan: Some(plan_to_pb(&plan(10, &[1]))),
        }));

        let mut no_plan = good.clone();
        no_plan.op = Some(pb::log_entry::Op::ApplyPlan(pb::ApplyPlan {
            incident: Some(incident_to_pb(&incident(10))),
            plan: None,
        }));

        let mut bad_severity = incident_to_pb(&incident(10));
        bad_severity.severity = 99;
        let mut bad_data = good.clone();
        bad_data.op = Some(pb::log_entry::Op::ApplyPlan(pb::ApplyPlan {
            incident: Some(bad_severity),
            plan: Some(plan_to_pb(&plan(10, &[1]))),
        }));

        let dst = store(2, 3);
        let empty = dst.snapshot();
        // Each bad entry comes from its own source, so each one is in order (seq 1).
        let cases = [
            (no_op, "unsupported op"),
            (no_incident, "bad data"),
            (no_plan, "bad data"),
            (bad_data, "bad data"),
        ];
        for (i, (mut bad, kind)) in cases.into_iter().enumerate() {
            let source = 11 + i as u32;
            bad.source_node = source;
            match dst.apply_remote(bad.clone()) {
                RemoteOutcome::Invalid(why) => assert!(why.starts_with(kind), "{why}"),
                other => panic!("expected Invalid, got {other:?}"),
            }
            assert_eq!(dst.applied().get(&source), Some(&1));
            assert!(dst.entries_since(&HashMap::new()).contains(&bad));
        }
        assert_eq!(dst.snapshot(), empty);
        assert_eq!(dst.lookup_request("r1"), None);
        // The good entry is still accepted afterwards.
        assert_eq!(dst.apply_remote(good), RemoteOutcome::Applied);
    }

    #[test]
    fn after_an_invalid_entry_the_next_one_from_that_source_applies() {
        let src = store(1, 3);
        let first = apply(&src, "r1", 10, &[1]).entry;
        let second = apply(&src, "r2", 11, &[2]).entry;
        let mut broken = first.clone();
        broken.op = None;

        let dst = store(2, 3);
        assert!(matches!(
            dst.apply_remote(broken.clone()),
            RemoteOutcome::Invalid(_)
        ));
        assert_eq!(dst.applied().get(&1), Some(&1)); // not stuck behind a gap
        assert_eq!(dst.snapshot().assignments.len(), 0);
        // seq 2 is in order now, not a Gap.
        assert_eq!(dst.apply_remote(second), RemoteOutcome::Applied);
        assert_eq!(dst.applied().get(&1), Some(&2));
        assert_eq!(dst.lookup_request("r2"), Some(plan(11, &[2])));
        assert_eq!(dst.lookup_request("r1"), None);
        // Seeing the bad entry again is a plain duplicate, and FetchLog serves it once.
        assert_eq!(dst.apply_remote(broken.clone()), RemoteOutcome::Duplicate);
        let log = dst.entries_since(&HashMap::new());
        assert_eq!(log.len(), 2);
        assert_eq!(log[0], broken);
    }

    #[test]
    fn resource_collision_keeps_the_first_assignment() {
        // Two requests planned on different nodes picked the same free resource.
        let n1 = store(1, 3);
        let n2 = store(2, 3);
        apply(&n1, "a", 10, &[1]);
        let from_2 = apply(&n2, "b", 11, &[1]).entry;

        assert_eq!(n1.apply_remote(from_2), RemoteOutcome::Applied);
        let snap = n1.snapshot();
        assert_eq!(snap.assignments.len(), 1);
        assert_eq!(snap.assignments[0].incident_id, 10); // first writer kept
        assert_eq!(n1.applied().get(&2), Some(&1)); // the entry still counts as applied
    }

    #[test]
    fn same_request_from_two_origins_is_superseded_but_logged() {
        // Node 3 already decided "r" locally.
        let n3 = store(3, 4);
        let kept = apply(&n3, "r", 10, &[1]);

        // Node 1 independently decided the same request with another resource.
        let n1 = store(1, 4);
        let other = apply(&n1, "r", 10, &[2]).entry;

        let state_before = n3.snapshot();
        assert_eq!(
            n3.apply_remote(other.clone()),
            RemoteOutcome::Superseded { kept_origin: 3 }
        );
        assert_eq!(n3.snapshot(), state_before); // state untouched
        assert_eq!(n3.lookup_request("r"), Some(kept.plan)); // first applied wins
        assert_eq!(n3.applied().get(&1), Some(&1)); // applied advanced
        // The entry is in the log, so FetchLog serves it and later entries are not a gap.
        assert!(n3.entries_since(&HashMap::new()).contains(&other));
        let next = apply(&n1, "r2", 11, &[3]).entry;
        assert_eq!(n3.apply_remote(next), RemoteOutcome::Applied);
        // Replaying the superseded entry is now a plain duplicate.
        assert_eq!(n3.apply_remote(other), RemoteOutcome::Duplicate);
    }

    #[test]
    fn two_stores_applying_the_same_entries_converge() {
        let a = store(1, 6);
        let b = store(2, 6);
        apply(&a, "a1", 1, &[1]);
        apply(&a, "a2", 2, &[]);
        apply(&b, "b1", 3, &[4]);
        apply(&b, "b2", 4, &[5]);

        for e in a.entries_since(&HashMap::new()) {
            assert_eq!(b.apply_remote(e), RemoteOutcome::Applied);
        }
        for e in b.entries_since(&HashMap::from([(1, 2)])) {
            // a already has b's source-1 entries; it needs only b's own.
            assert_eq!(a.apply_remote(e), RemoteOutcome::Applied);
        }

        assert_eq!(a.snapshot(), b.snapshot());
        assert_eq!(a.applied(), b.applied());
        assert_eq!(a.applied(), HashMap::from([(1, 2), (2, 2)]));
        for rid in ["a1", "a2", "b1", "b2"] {
            assert_eq!(a.lookup_request(rid), b.lookup_request(rid));
            assert!(a.lookup_request(rid).is_some());
        }
        // Feeding everything again changes nothing.
        let settled = a.snapshot();
        for e in b.entries_since(&HashMap::new()) {
            assert_eq!(a.apply_remote(e), RemoteOutcome::Duplicate);
        }
        assert_eq!(a.snapshot(), settled);
    }
}
