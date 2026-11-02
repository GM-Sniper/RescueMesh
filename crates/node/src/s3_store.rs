//! P5, Step C: the shared-state backend on SeaweedFS, through its S3 gateway.
//!
//! Implements P4's `SharedState` trait, so the handler, the election and the rejoin rule do
//! not change. Chosen because P4's spike (Oct 23) found the S3 gateway is the only SeaweedFS
//! front door with a real create-if-absent (8 of 8 rounds across two gateways), and P5
//! re-tested it on Oct 31 (10 of 10 rounds, 24 racing creates, exactly one winner) together
//! with compare-and-swap through `If-Match` (24 racing swaps, exactly one winner).
//! Full reasoning: DESIGN_LOG.md, "P5: Shared-state backend".
//!
//! # Objects in the bucket
//! | Key                    | Body (JSON)                                      | Written how              |
//! |------------------------|--------------------------------------------------|--------------------------|
//! | `resources/<id>`       | `Resource`                                       | create-if-absent, once   |
//! | `claims/<resource_id>` | `Claim { request_id, assignment, expires_at_ms, confirmed }` | create-if-absent, or swap with `If-Match` |
//! | `requests/<request_id>`| `RequestRecord { incident, plan }`               | create-if-absent: **the commit point** |
//!
//! # Commit, step by step
//! 1. `requests/<id>` exists: `AlreadyCommitted` with that plan.
//! 2. Claim every planned resource, lowest id first. A claim is a lease: it says "request X
//!    holds this resource until `expires_at_ms`, unless X's record gets written first".
//!    A claim already there is taken over (with `If-Match`, so two nodes cannot both take it)
//!    only if it is ours (same `request_id`, an earlier attempt of the same request), or it
//!    has expired and its request never committed with this resource. Otherwise the resource
//!    is taken: release what we claimed and return `ResourceTaken`.
//! 3. Create `requests/<id>`. Won: mark the claims confirmed (best effort) and return
//!    `Committed`. Lost (another node committed this request_id meanwhile): release the claims
//!    its plan does not use and return `AlreadyCommitted` with its plan.
//!
//! # Why claims are leases
//! A node can be muted or crash between step 2 and step 3. Its claims would then hold
//! resources forever for a request that never committed. With a lease they come free after
//! `lease_ms`. A client retry of the same request on another node does not even have to wait:
//! it may reuse a claim with its own `request_id` at once.
//!
//! # Clocks
//! Expiry compares the claimer's wall clock (`expires_at_ms`) with the reader's. That is the
//! one place wall clocks matter. It is safe as long as the clock offset between machines (P1
//! measures it in Step D) is far below `lease_ms` (10 s), and the commit gives up if half the
//! lease has passed before step 3, so a claim cannot expire under a commit that is still going.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use common::config::SharedStateConfig;
use common::domain::{
    Assignment, Incident, IncidentId, IncidentStatus, Plan, Resource, ResourceId, Snapshot,
};
use common::shared_state::{CommitOutcome, SharedState, StateUnavailable};
use futures_util::future::join_all;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use tokio::time::Instant;

/// One resource held by one request (see the module docs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    pub request_id: String,
    pub assignment: Assignment,
    /// Claimer's wall clock, Unix ms. Ignored once `confirmed`.
    pub expires_at_ms: u64,
    /// The request's record was written with this resource in its plan: held for good.
    pub confirmed: bool,
}

/// The stored answer for one client request. Its existence IS the commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestRecord {
    pub incident: Incident,
    pub plan: Plan,
}
/// Persistent record of a successful resolve operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionRecord {
    pub request_id: String,
    pub incident_id: IncidentId,
}
/// What a claim already in the bucket means for a request that wants the same resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimView {
    /// Same request_id: an earlier attempt of this request. Reuse it.
    Ours,
    /// Another request holds it and the lease is valid (or confirmed).
    Held,
    /// Another request's lease ran out. Free, unless that request committed with it
    /// (the caller must check the request record before taking it over).
    Expired,
}

/// Pure decision, so it can be tested without a gateway.
pub fn classify_claim(claim: &Claim, request_id: &str, now_ms: u64) -> ClaimView {
    if claim.request_id == request_id {
        ClaimView::Ours
    } else if claim.confirmed || now_ms < claim.expires_at_ms {
        ClaimView::Held
    } else {
        ClaimView::Expired
    }
}

/// Result of a conditional PUT.
enum Put {
    /// Written; the new ETag (needed to release or confirm only our own version later).
    Done(String),
    /// The condition failed (412): someone else got there first.
    Lost,
}

pub struct S3Store {
    http: Client,
    /// `http://host:port/bucket`, no trailing slash.
    base: String,
    lease: Duration,
    /// Resources are fixed definitions: read at connect and on `reload()`, never per request.
    resources: RwLock<Vec<Resource>>,
}

impl S3Store {
    /// Connects, creates the bucket if needed, writes the starting resources if nobody has
    /// yet (create-if-absent, so all three nodes can do it at once), and reads them back.
    /// Retries for about 10 s so a node may start a little before its gateway.
    pub async fn connect(
        cfg: &SharedStateConfig,
        starting: Vec<Resource>,
    ) -> Result<Self, StateUnavailable> {
        let http = Client::builder()
            .timeout(Duration::from_millis(cfg.request_timeout_ms))
            // The gateway is on the lab LAN. A system HTTP proxy (HTTP_PROXY) would send these
            // calls somewhere else, or nowhere, so proxies are always off here.
            .no_proxy()
            .build()
            .map_err(|e| unavailable(format!("http client: {e}")))?;
        let store = S3Store {
            http,
            base: format!("{}/{}", cfg.endpoint.trim_end_matches('/'), cfg.bucket),
            lease: Duration::from_millis(cfg.lease_ms),
            resources: RwLock::new(Vec::new()),
        };
        let mut last = None;
        for _ in 0..20 {
            match store.init(&starting).await {
                Ok(()) => return Ok(store),
                Err(e) => {
                    tracing::warn!(%e, "shared state not ready yet; retrying");
                    last = Some(e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        Err(last.unwrap_or_else(|| unavailable("gateway never answered")))
    }

    async fn init(&self, starting: &[Resource]) -> Result<(), StateUnavailable> {
        let resp = self.http.put(&self.base).send().await.map_err(transport)?;
        // 409 = BucketAlreadyOwnedByYou: another node (or an earlier start) made it.
        if !(resp.status().is_success() || resp.status() == StatusCode::CONFLICT) {
            return Err(unavailable(format!(
                "create bucket: HTTP {}",
                resp.status()
            )));
        }
        for r in starting {
            // Lost = already written by another node: fine, same constants everywhere.
            self.put(&resource_key(r.id), r, Cond::IfAbsent).await?;
        }
        self.load_resources(starting.iter().map(|r| r.id)).await
    }

    /// Re-reads the resource definitions into the cache.
    async fn load_resources(
        &self,
        ids: impl Iterator<Item = ResourceId>,
    ) -> Result<(), StateUnavailable> {
        let reads = ids.map(|id| async move { self.get::<Resource>(&resource_key(id)).await });
        let mut fresh = Vec::new();
        for r in join_all(reads).await {
            if let Some((r, _)) = r? {
                fresh.push(r);
            }
        }
        fresh.sort_by_key(|r| r.id);
        *self.resources.write().unwrap() = fresh;
        Ok(())
    }

    fn known_resources(&self) -> Vec<Resource> {
        self.resources.read().unwrap().clone()
    }

    async fn incident_is_resolved(
        &self,
        incident_id: IncidentId,
    ) -> Result<bool, StateUnavailable> {
        Ok(self
            .get::<ResolutionRecord>(&resolved_incident_key(incident_id))
            .await?
            .is_some())
    }

    // ---- HTTP helpers ---------------------------------------------------------------------

    fn url(&self, key: &str) -> String {
        format!("{}/{}", self.base, key)
    }

    /// GET a JSON object. `Ok(None)` = 404. Returns the ETag too.
    async fn get<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<(T, String)>, StateUnavailable> {
        let resp = self
            .http
            .get(self.url(key))
            .send()
            .await
            .map_err(transport)?;
        match resp.status() {
            StatusCode::NOT_FOUND => Ok(None),
            s if s.is_success() => {
                let etag = etag_of(&resp);
                let bytes = resp.bytes().await.map_err(transport)?;
                let value = serde_json::from_slice(&bytes)
                    .map_err(|e| unavailable(format!("{key}: unreadable JSON: {e}")))?;
                Ok(Some((value, etag)))
            }
            s => Err(unavailable(format!("GET {key}: HTTP {s}"))),
        }
    }

    async fn put<T: Serialize>(
        &self,
        key: &str,
        body: &T,
        cond: Cond<'_>,
    ) -> Result<Put, StateUnavailable> {
        let body =
            serde_json::to_vec(body).map_err(|e| unavailable(format!("encode {key}: {e}")))?;
        let req = self.http.put(self.url(key)).body(body);
        let req = match cond {
            Cond::IfAbsent => req.header("If-None-Match", "*"),
            Cond::IfMatch(etag) => req.header("If-Match", etag),
        };
        let resp = req.send().await.map_err(transport)?;
        match resp.status() {
            StatusCode::PRECONDITION_FAILED => Ok(Put::Lost),
            s if s.is_success() => Ok(Put::Done(etag_of(&resp))),
            s => Err(unavailable(format!("PUT {key}: HTTP {s}"))),
        }
    }

    /// DELETE only if the object is still the version we wrote. Best effort: a failure just
    /// leaves the claim to expire.
    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<(), StateUnavailable> {
        let resp = self
            .http
            .delete(self.url(key))
            .header("If-Match", etag)
            .send()
            .await
            .map_err(transport)?;

        match resp.status() {
            StatusCode::NOT_FOUND => Ok(()),
            s if s.is_success() => Ok(()),
            StatusCode::PRECONDITION_FAILED => Ok(()),
            s => Err(unavailable(format!("DELETE {key}: HTTP {s}"))),
        }
    }

    // ---- claims ---------------------------------------------------------------------------

    fn new_claim(&self, request_id: &str, a: &Assignment, confirmed: bool) -> Claim {
        Claim {
            request_id: request_id.to_string(),
            assignment: a.clone(),
            expires_at_ms: now_ms() + self.lease.as_millis() as u64,
            confirmed,
        }
    }

    /// Does `request_id`'s committed plan use `resource`? (Decides whether an expired claim
    /// is really free.)
    async fn committed_with(
        &self,
        request_id: &str,
        resource: ResourceId,
    ) -> Result<bool, StateUnavailable> {
        Ok(self
            .get::<RequestRecord>(&request_key(request_id))
            .await?
            .is_some_and(|(rec, _)| {
                rec.plan
                    .assignments
                    .iter()
                    .any(|a| a.resource_id == resource)
            }))
    }

    /// Claims one resource for `request_id`. `Ok(Some(etag))` = held by us now;
    /// `Ok(None)` = another request holds it.
    async fn claim(
        &self,
        request_id: &str,
        a: &Assignment,
    ) -> Result<Option<String>, StateUnavailable> {
        let key = claim_key(a.resource_id);
        // A few rounds: the claim can change between our GET and our swap.
        for _ in 0..4 {
            let mine = self.new_claim(request_id, a, false);
            if let Put::Done(etag) = self.put(&key, &mine, Cond::IfAbsent).await? {
                return Ok(Some(etag));
            }
            let Some((existing, etag)) = self.get::<Claim>(&key).await? else {
                continue; // released between our PUT and GET: try to create again
            };
            match classify_claim(&existing, request_id, now_ms()) {
                ClaimView::Held => return Ok(None),
                ClaimView::Expired
                    if self
                        .committed_with(&existing.request_id, a.resource_id)
                        .await? =>
                {
                    // Its request did commit with it; the confirm step was lost. Repair it.
                    let fixed = Claim {
                        confirmed: true,
                        ..existing
                    };
                    let _ = self.put(&key, &fixed, Cond::IfMatch(&etag)).await;
                    return Ok(None);
                }
                ClaimView::Ours | ClaimView::Expired => {
                    if let Put::Done(new) = self.put(&key, &mine, Cond::IfMatch(&etag)).await? {
                        return Ok(Some(new));
                    }
                    // Lost the swap: someone else changed it. Look again.
                }
            }
        }
        Ok(None)
    }

    async fn release(&self, held: &[(ResourceId, String)]) {
        join_all(held.iter().map(|(id, etag)| async move {
            if let Err(e) = self.delete_if_match(&claim_key(*id), etag).await {
                tracing::debug!(%e, "release failed; the lease will expire instead");
            }
        }))
        .await;
    }

    /// After a successful commit: mark claims confirmed so readers need not check expiry.
    /// Best effort: if it fails, `classify` + `committed_with` reach the same answer.
    async fn confirm(&self, request_id: &str, plan: &Plan, held: &[(ResourceId, String)]) {
        join_all(held.iter().map(|(id, etag)| async move {
            if let Some(a) = plan.assignments.iter().find(|a| a.resource_id == *id) {
                let done = self.new_claim(request_id, a, true);
                let _ = self.put(&claim_key(*id), &done, Cond::IfMatch(etag)).await;
            }
        }))
        .await;
    }
}

#[async_trait]
impl SharedState for S3Store {
    /// Resources from the cache, assignments from the live claims. `incidents` is left empty:
    /// the planner does not read it, and listing every request on every snapshot would make
    /// each request slower than the last. The consistency check reads `requests/` directly.
    async fn snapshot(&self) -> Result<Snapshot, StateUnavailable> {
        let resources = self.known_resources();
        let now = now_ms();
        let reads = resources.iter().map(|r| async move {
            let Some((claim, _)) = self.get::<Claim>(&claim_key(r.id)).await? else {
                return Ok(None);
            };
            let holds = match classify_claim(&claim, "", now) {
                ClaimView::Held | ClaimView::Ours => true,
                ClaimView::Expired => self.committed_with(&claim.request_id, r.id).await?,
            };
            Ok::<_, StateUnavailable>(holds.then_some(claim.assignment))
        });
        let mut assignments = Vec::new();
        for a in join_all(reads).await {
            if let Some(a) = a? {
                assignments.push(a);
            }
        }
        Ok(Snapshot {
            incidents: Vec::new(),
            resources,
            assignments,
        })
    }

    async fn commit(&self, request_id: &str, mut incident: Incident, plan: Plan) -> CommitOutcome {
        match self.lookup_request(request_id).await {
            Err(e) => return CommitOutcome::StateUnavailable(e),
            Ok(Some(stored)) => return CommitOutcome::AlreadyCommitted(stored),
            Ok(None) => {}
        }

        match self.incident_is_resolved(incident.id).await {
            Ok(true) => {
                return CommitOutcome::StateUnavailable(unavailable(
                    "cannot commit an already resolved incident",
                ));
            }
            Ok(false) => {}
            Err(e) => return CommitOutcome::StateUnavailable(e),
        }

        // Same rule as the in-memory store: unknown or repeated resource = taken.
        let known: HashSet<ResourceId> = self.known_resources().iter().map(|r| r.id).collect();
        let mut seen = HashSet::new();
        for a in &plan.assignments {
            if !known.contains(&a.resource_id) || !seen.insert(a.resource_id) {
                return CommitOutcome::ResourceTaken {
                    resource_id: a.resource_id,
                };
            }
        }

        // Step 2: claims, lowest resource id first, so two requests wanting the same
        // resources meet on the first one instead of each holding half.
        let started = Instant::now();
        let mut order: Vec<&Assignment> = plan.assignments.iter().collect();
        order.sort_by_key(|a| a.resource_id);
        let mut held: Vec<(ResourceId, String)> = Vec::new();
        for a in order {
            match self.claim(request_id, a).await {
                Ok(Some(etag)) => held.push((a.resource_id, etag)),
                Ok(None) => {
                    self.release(&held).await;
                    return CommitOutcome::ResourceTaken {
                        resource_id: a.resource_id,
                    };
                }
                Err(e) => {
                    self.release(&held).await;
                    return CommitOutcome::StateUnavailable(e);
                }
            }
        }
        if started.elapsed() > self.lease / 2 {
            // Our claims could expire before the record lands; do not risk a double booking.
            self.release(&held).await;
            return CommitOutcome::StateUnavailable(unavailable(
                "claims took over half the lease; retry",
            ));
        }

        // Check again because the incident may have been resolved
        // while we were acquiring resource claims.
        match self.incident_is_resolved(incident.id).await {
            Ok(true) => {
                self.release(&held).await;
                return CommitOutcome::StateUnavailable(unavailable(
                    "incident was resolved while committing",
                ));
            }
            Ok(false) => {}
            Err(e) => {
                self.release(&held).await;
                return CommitOutcome::StateUnavailable(e);
            }
        }

        // Step 3: the commit point.
        incident.status = if plan.assignments.is_empty() {
            IncidentStatus::Open
        } else {
            IncidentStatus::Assigned
        };
        let record = RequestRecord {
            incident,
            plan: plan.clone(),
        };
        match self
            .put(&request_key(request_id), &record, Cond::IfAbsent)
            .await
        {
            Ok(Put::Done(_)) => {
                self.confirm(request_id, &plan, &held).await;
                CommitOutcome::Committed
            }
            Ok(Put::Lost) => match self.lookup_request(request_id).await {
                Ok(Some(stored)) => {
                    let used: HashSet<ResourceId> =
                        stored.assignments.iter().map(|a| a.resource_id).collect();
                    let (kept, spare): (Vec<_>, Vec<_>) =
                        held.into_iter().partition(|(id, _)| used.contains(id));
                    self.release(&spare).await;
                    // Two nodes that both thought they won share the same claims ("Ours"), and
                    // our swap changed their ETag, so the winner's confirm failed. We hold the
                    // latest version: confirm the ones the stored plan uses (seen Oct 31 e2e).
                    self.confirm(request_id, &stored, &kept).await;
                    CommitOutcome::AlreadyCommitted(stored)
                }
                Ok(None) => CommitOutcome::StateUnavailable(unavailable("request record vanished")),
                Err(e) => CommitOutcome::StateUnavailable(e),
            },
            // Unknown whether it landed. Keep the claims: if it landed they are right, if not
            // they expire. The client retries with the same request_id either way.
            Err(e) => CommitOutcome::StateUnavailable(e),
        }
    }

    async fn lookup_request(&self, request_id: &str) -> Result<Option<Plan>, StateUnavailable> {
        Ok(self
            .get::<RequestRecord>(&request_key(request_id))
            .await?
            .map(|(rec, _)| rec.plan))
    }

    async fn resolve(
        &self,
        request_id: &str,
        incident_id: IncidentId,
    ) -> Result<(), StateUnavailable> {
        let record = ResolutionRecord {
            request_id: request_id.to_string(),
            incident_id,
        };

        // First reserve this resolve request ID.
        // Reusing it for another incident must fail.
        match self
            .put(&resolution_key(request_id), &record, Cond::IfAbsent)
            .await?
        {
            Put::Done(_) => {}
            Put::Lost => {
                let Some((existing, _)) = self
                    .get::<ResolutionRecord>(&resolution_key(request_id))
                    .await?
                else {
                    return Err(unavailable("resolve request record disappeared"));
                };

                if existing.incident_id != incident_id {
                    return Err(unavailable(
                        "resolve request_id was already used for another incident",
                    ));
                }
            }
        }

        // Persist the resolved marker before deleting claims.
        // This prevents new commits from assigning resources to this incident.
        match self
            .put(&resolved_incident_key(incident_id), &record, Cond::IfAbsent)
            .await?
        {
            Put::Done(_) => {}
            Put::Lost => {
                let Some((existing, _)) = self
                    .get::<ResolutionRecord>(&resolved_incident_key(incident_id))
                    .await?
                else {
                    return Err(unavailable("resolved incident marker disappeared"));
                };

                if existing.incident_id != incident_id {
                    return Err(unavailable("invalid resolved incident marker"));
                }
            }
        }

        // Release only claims belonging to this incident.
        for resource in self.known_resources() {
            let key = claim_key(resource.id);

            if let Some((claim, etag)) = self.get::<Claim>(&key).await? {
                if claim.assignment.incident_id == incident_id {
                    self.delete_if_match(&key, &etag).await?;
                }
            }
        }

        Ok(())
    }

    /// Rejoin: re-read the resource definitions from the bucket (not the cache), then
    /// return a fresh snapshot. `Ok` = this node can reach the shared state again.
    async fn reload(&self) -> Result<Snapshot, StateUnavailable> {
        let ids: Vec<ResourceId> = self.known_resources().iter().map(|r| r.id).collect();
        self.load_resources(ids.into_iter()).await?;
        self.snapshot().await
    }
}

enum Cond<'a> {
    IfAbsent,
    IfMatch(&'a str),
}

fn resource_key(id: ResourceId) -> String {
    format!("resources/{id}")
}

fn claim_key(id: ResourceId) -> String {
    format!("claims/{id}")
}

/// request_ids are client UUIDs; anything else is percent-encoded so it stays one path segment.
fn request_key(request_id: &str) -> String {
    let mut key = String::from("requests/");
    for b in request_id.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' {
            key.push(b as char);
        } else {
            key.push_str(&format!("%{b:02X}"));
        }
    }
    key
}
fn resolution_key(request_id: &str) -> String {
    let mut key = String::from("resolutions/");
    for b in request_id.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' {
            key.push(b as char);
        } else {
            key.push_str(&format!("%{b:02X}"));
        }
    }
    key
}

fn resolved_incident_key(incident_id: IncidentId) -> String {
    format!("resolved/{incident_id}")
}

fn etag_of(resp: &reqwest::Response) -> String {
    resp.headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn unavailable(reason: impl Into<String>) -> StateUnavailable {
    StateUnavailable {
        reason: reason.into(),
    }
}

fn transport(e: reqwest::Error) -> StateUnavailable {
    // The whole cause chain: reqwest's own message ("error sending request") hides the reason.
    let mut msg = format!("gateway: {e}");
    let mut cause = std::error::Error::source(&e);
    while let Some(c) = cause {
        msg.push_str(&format!(": {c}"));
        cause = c.source();
    }
    unavailable(msg)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::domain::*;

    use super::*;

    fn claim(request_id: &str, expires_at_ms: u64, confirmed: bool) -> Claim {
        Claim {
            request_id: request_id.into(),
            assignment: Assignment {
                incident_id: 1,
                resource_id: 1,
                score: 0.5,
            },
            expires_at_ms,
            confirmed,
        }
    }

    #[test]
    fn classify_claim_cases() {
        assert_eq!(
            classify_claim(&claim("r1", 100, false), "r1", 500),
            ClaimView::Ours
        );
        assert_eq!(
            classify_claim(&claim("r1", 1000, false), "r2", 500),
            ClaimView::Held
        );
        assert_eq!(
            classify_claim(&claim("r1", 100, true), "r2", 500),
            ClaimView::Held
        );
        assert_eq!(
            classify_claim(&claim("r1", 100, false), "r2", 500),
            ClaimView::Expired
        );
        assert_eq!(
            classify_claim(&claim("r1", 500, false), "r2", 500),
            ClaimView::Expired
        );
    }

    #[test]
    fn request_keys_stay_one_path_segment() {
        assert_eq!(request_key("3f2a-9c_x.1"), "requests/3f2a-9c_x.1");
        assert_eq!(request_key("a/b c"), "requests/a%2Fb%20c");
    }

    // ---- Against a real SeaweedFS S3 gateway -------------------------------------------
    // Ignored by default: they need a gateway. Run with
    //   RESCUEMESH_S3=http://127.0.0.1:8333 cargo test -p node s3_store -- --ignored --test-threads=1
    // Each test uses its own fresh bucket, so they never see each other's objects.

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

    fn plan(incident_id: u64, resources: &[u64]) -> Plan {
        Plan {
            incident_id,
            priority: 1.0,
            assignments: resources
                .iter()
                .map(|&resource_id| Assignment {
                    incident_id,
                    resource_id,
                    score: 0.5,
                })
                .collect(),
        }
    }

    fn resources(n: u64) -> Vec<Resource> {
        (1..=n)
            .map(|id| Resource {
                id,
                rtype: ResourceType::Ambulance,
                quantity: 1,
                available: true,
                location: Zone::new(0, 0),
                lease_expiry: 0,
            })
            .collect()
    }

    fn cfg(bucket: &str, lease_ms: u64) -> SharedStateConfig {
        SharedStateConfig {
            backend: common::config::StateBackend::S3,
            endpoint: std::env::var("RESCUEMESH_S3")
                .unwrap_or_else(|_| "http://127.0.0.1:8333".into()),
            bucket: bucket.into(),
            lease_ms,
            // A quarter of the lease (the config's minimum ratio), at most the 2.5 s default.
            request_timeout_ms: (lease_ms / 4).min(2_500),
        }
    }

    fn fresh_bucket(tag: &str) -> String {
        format!("t-{tag}-{}", now_ms())
    }

    /// Deletes the test's bucket (SeaweedFS allows deleting a non-empty one by default),
    /// so the gateway's volumes are not used up by old test buckets.
    async fn drop_bucket(s: &S3Store) {
        let _ = s.http.delete(&s.base).send().await;
    }

    /// Three stores on one bucket = three nodes sharing the DFS.
    async fn three(tag: &str, lease_ms: u64) -> Vec<Arc<S3Store>> {
        let c = cfg(&fresh_bucket(tag), lease_ms);
        let mut v = Vec::new();
        for _ in 0..3 {
            v.push(Arc::new(
                S3Store::connect(&c, resources(5))
                    .await
                    .expect("is the gateway running?"),
            ));
        }
        v
    }

    #[tokio::test]
    #[ignore]
    async fn same_request_committed_twice_leaves_one_record() {
        let n = three("dup", 10_000).await;
        assert_eq!(
            n[0].commit("r1", incident(1), plan(1, &[1])).await,
            CommitOutcome::Committed
        );
        // A retry on another node with a different plan gets the first plan back.
        assert_eq!(
            n[1].commit("r1", incident(1), plan(1, &[2])).await,
            CommitOutcome::AlreadyCommitted(plan(1, &[1]))
        );
        assert_eq!(
            n[2].lookup_request("r1").await.unwrap(),
            Some(plan(1, &[1]))
        );
        // Resource 2 was never held.
        let snap = n[2].snapshot().await.unwrap();
        assert_eq!(
            snap.assignments
                .iter()
                .map(|a| a.resource_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        drop_bucket(&n[0]).await;
    }

    #[tokio::test]
    #[ignore]
    async fn commit_race_exactly_one_committed_100_times() {
        let n = three("race", 10_000).await;
        for round in 0..100 {
            let id = format!("race-{round}");
            // Different plans per node: the losers must get the winner's plan, not their own.
            let tasks: Vec<_> = (0..3)
                .map(|i| {
                    let s = n[i].clone();
                    let id = id.clone();
                    tokio::spawn(
                        async move { s.commit(&id, incident(round), plan(round, &[])).await },
                    )
                })
                .collect();
            let outcomes: Vec<_> = join_all(tasks)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let won = outcomes
                .iter()
                .filter(|o| **o == CommitOutcome::Committed)
                .count();
            assert_eq!(won, 1, "round {round}: {outcomes:?}");
            assert!(outcomes.iter().all(|o| matches!(
                o,
                CommitOutcome::Committed | CommitOutcome::AlreadyCommitted(_)
            )));
        }
        drop_bucket(&n[0]).await;
    }

    #[tokio::test]
    #[ignore]
    async fn resource_race_exactly_one_gets_it() {
        let n = three("res", 10_000).await;
        for r in 1..=5u64 {
            let tasks: Vec<_> = (0..3)
                .map(|i| {
                    let s = n[i].clone();
                    tokio::spawn(async move {
                        s.commit(
                            &format!("res{r}-node{i}"),
                            incident(r * 10 + i as u64),
                            plan(r * 10 + i as u64, &[r]),
                        )
                        .await
                    })
                })
                .collect();
            let outcomes: Vec<_> = join_all(tasks)
                .await
                .into_iter()
                .map(|x| x.unwrap())
                .collect();
            let won = outcomes
                .iter()
                .filter(|o| **o == CommitOutcome::Committed)
                .count();
            let taken = outcomes
                .iter()
                .filter(|o| matches!(o, CommitOutcome::ResourceTaken { .. }))
                .count();
            assert_eq!((won, taken), (1, 2), "resource {r}: {outcomes:?}");
        }
        assert_eq!(n[0].snapshot().await.unwrap().assignments.len(), 5);
        drop_bucket(&n[0]).await;
    }

    #[tokio::test]
    #[ignore]
    async fn abandoned_claim_blocks_until_the_lease_ends_then_frees() {
        // Short lease so the test is quick: 4 x the 1 s timeout is the minimum the config allows.
        let n = three("lease", 4_000).await;
        // A "node that died between claim and commit": claim resource 1, never write the record.
        let a = Assignment {
            incident_id: 9,
            resource_id: 1,
            score: 0.5,
        };
        assert!(n[0].claim("dead-request", &a).await.unwrap().is_some());

        assert_eq!(
            n[1].commit("r-live", incident(1), plan(1, &[1])).await,
            CommitOutcome::ResourceTaken { resource_id: 1 }
        );
        assert_eq!(
            n[1].snapshot().await.unwrap().assignments.len(),
            1,
            "held while the lease runs"
        );

        tokio::time::sleep(Duration::from_millis(4_200)).await;
        assert!(
            n[1].snapshot().await.unwrap().assignments.is_empty(),
            "expired and never committed: free"
        );
        assert_eq!(
            n[1].commit("r-live", incident(1), plan(1, &[1])).await,
            CommitOutcome::Committed
        );
        drop_bucket(&n[0]).await;
    }

    #[tokio::test]
    #[ignore]
    async fn retry_of_the_same_request_reuses_its_own_claim_at_once() {
        let n = three("retry", 10_000).await;
        let a = Assignment {
            incident_id: 1,
            resource_id: 1,
            score: 0.5,
        };
        // Node 0 claimed for r1, then was muted before writing the record.
        assert!(n[0].claim("r1", &a).await.unwrap().is_some());
        // The client's retry lands on node 1, which plans the same resource: no waiting.
        assert_eq!(
            n[1].commit("r1", incident(1), plan(1, &[1])).await,
            CommitOutcome::Committed
        );
        drop_bucket(&n[0]).await;
    }

    #[tokio::test]
    #[ignore]
    async fn confirmed_claims_never_expire() {
        let n = three("confirm", 4_000).await;
        assert_eq!(
            n[0].commit("r1", incident(1), plan(1, &[1])).await,
            CommitOutcome::Committed
        );
        tokio::time::sleep(Duration::from_millis(4_200)).await;
        assert_eq!(
            n[1].commit("r2", incident(2), plan(2, &[1])).await,
            CommitOutcome::ResourceTaken { resource_id: 1 }
        );
        assert_eq!(n[2].snapshot().await.unwrap().assignments.len(), 1);
        drop_bucket(&n[0]).await;
    }

    #[tokio::test]
    #[ignore]
    async fn unreachable_gateway_is_state_unavailable_not_a_panic() {
        let n = three("down", 10_000).await;
        let broken = S3Store {
            http: Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .unwrap(),
            base: "http://127.0.0.1:1/nothing".into(),
            lease: Duration::from_secs(10),
            resources: RwLock::new(resources(5)),
        };
        assert!(broken.snapshot().await.is_err());
        assert!(broken.lookup_request("r1").await.is_err());
        assert!(matches!(
            broken.commit("r1", incident(1), plan(1, &[1])).await,
            CommitOutcome::StateUnavailable(_)
        ));
        // And it wrote nothing that the real cluster can see.
        assert!(n[0].lookup_request("r1").await.unwrap().is_none());
        drop_bucket(&n[0]).await;
    }
}
