//! TEMPORARY (P6, until P4's DFS is connected): shared state in one folder that every node uses.
//!
//! Why: each node's `InMemoryStore` is private, so 3 nodes meant 3 copies of the 10 starting
//! resources, and the same resource could be assigned up to 3 times. P1 and P4 confirmed the
//! system must have ONE set of 10 resources, also with multicast. This store implements P4's
//! `SharedState` interface on a folder all nodes can reach, so the cluster shares one set.
//! When the DFS-backed store exists, it replaces this file; nothing else has to change.
//!
//! Folder layout (`--shared-state-dir <dir>` on every node, the same dir for all of them):
//!   state.toml                 resources, assignments, and the incidents that got a resource
//!   requests/<request_id>.toml one per committed request: the create-if-absent record (incident + plan)
//!   state.lock                 a file lock: only one node changes the state at a time
//!
//! - One machine (local experiments): any folder, e.g. inside the run folder.
//! - The 3 lab machines: a folder shared over the network (e.g. a Windows shared folder),
//!   reachable from all 3. The lock works across machines on Windows shares.
//!
//! Same rules as P4's InMemoryStore: request_id is the dedup key, commit re-checks inside the
//! lock that every planned resource is still free, and an empty plan is stored too.
//! Differences: a snapshot lists only incidents that got a resource (the planner never reads
//! incidents, and storing every open incident here would grow the file with every request);
//! every open incident is still kept, in its request file.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use common::domain::*;
use common::shared_state::{CommitOutcome, SharedState, StateUnavailable};
use serde::{Deserialize, Serialize};

const STATE_FILE: &str = "state.toml";
const LOCK_FILE: &str = "state.lock";
const REQUESTS_DIR: &str = "requests";

/// The small shared part: what the planner reads. At most one assignment per resource,
/// so it never grows past the number of resources.
#[derive(Serialize, Deserialize, Default)]
struct StateFile {
    resources: Vec<Resource>,
    assignments: Vec<Assignment>,
    incidents: Vec<Incident>, // only incidents that got a resource
}

/// One committed request. Its existence is the "already committed" answer for retries.
#[derive(Serialize, Deserialize)]
struct RequestFile {
    incident: Incident,
    plan: Plan,
}

/// Shared state kept in a folder that every node uses. Implements [`SharedState`].
pub struct FileStore {
    dir: PathBuf,
}

impl FileStore {
    /// Uses the shared state in `dir`, creating it with `starting_resources` if this is the first
    /// node to open it. Later nodes (and a restarted node) keep what is already there.
    pub fn open(dir: impl AsRef<Path>, starting_resources: Vec<Resource>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(dir.join(REQUESTS_DIR))?;
        if dir.join(STATE_FILE).exists() {
            // Already created (state.toml only appears whole, by rename), so no lock is needed. A
            // node restarted under heavy load waited ~13 s for the lock here (Step C dry run).
            return Ok(FileStore { dir });
        }
        locked(&dir, |d| {
            if !d.join(STATE_FILE).exists() {
                save(d, &StateFile { resources: starting_resources, ..StateFile::default() })?;
            }
            Ok(())
        })?;
        Ok(FileStore { dir })
    }

    /// Runs file work off the async threads (it waits on the lock and the disk).
    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Path) -> io::Result<T> + Send + 'static,
    ) -> Result<T, StateUnavailable> {
        let dir = self.dir.clone();
        match tokio::task::spawn_blocking(move || work(&dir)).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(unavailable(e)), // the folder can't be read or written (e.g. share disconnected)
            Err(e) => Err(StateUnavailable { reason: format!("shared-state task failed: {e}") }),
        }
    }
}

#[async_trait]
impl SharedState for FileStore {
    async fn snapshot(&self) -> Result<Snapshot, StateUnavailable> {
        let state = self.run(|d| locked(d, load)).await?;
        Ok(Snapshot { incidents: state.incidents, resources: state.resources, assignments: state.assignments })
    }

    async fn commit(&self, request_id: &str, incident: Incident, plan: Plan) -> CommitOutcome {
        let request_id = request_id.to_string();
        match self.run(move |d| locked(d, |d| commit_locked(d, &request_id, incident, plan))).await {
            Ok(outcome) => outcome,
            Err(e) => CommitOutcome::StateUnavailable(e), // may or may not have landed: the caller retries with the same request_id
        }
    }

    async fn lookup_request(&self, request_id: &str) -> Result<Option<Plan>, StateUnavailable> {
        let request_id = request_id.to_string();
        self.run(move |d| {
            let path = request_path(d, &request_id);
            if !path.exists() {
                return Ok(None); // never committed (request files are only ever added, never removed)
            }
            let record: RequestFile = read_toml(&path)?;
            Ok(Some(record.plan))
        })
        .await
    }
}

/// The commit rules, run while holding the lock. Same order and checks as P4's InMemoryStore.
fn commit_locked(dir: &Path, request_id: &str, mut incident: Incident, plan: Plan) -> io::Result<CommitOutcome> {
    // Create-if-absent: this request may already be committed by any node (a client retry).
    let record_path = request_path(dir, request_id);
    if record_path.exists() {
        let existing: RequestFile = read_toml(&record_path)?;
        return Ok(CommitOutcome::AlreadyCommitted(existing.plan));
    }

    // Every planned resource must exist, be unassigned, and appear only once in the plan.
    let mut state = load(dir)?;
    let mut seen_in_this_plan = HashSet::new();
    for assignment in &plan.assignments {
        let resource_id = assignment.resource_id;
        let exists = state.resources.iter().any(|r| r.id == resource_id);
        let taken = state.assignments.iter().any(|a| a.resource_id == resource_id);
        if !exists || taken || !seen_in_this_plan.insert(resource_id) {
            return Ok(CommitOutcome::ResourceTaken { resource_id }); // nothing written: snapshot again and re-plan
        }
    }

    // An empty plan (nothing was free) is stored too, so a retry gets the same empty plan.
    incident.status = if plan.assignments.is_empty() { IncidentStatus::Open } else { IncidentStatus::Assigned };

    // Mark the resources first, then write the request record. If the node dies in between,
    // the resource is taken but the request is not recorded: a retry plans again and takes
    // another resource. Wasteful, but no resource is ever given to two requests.
    if !plan.assignments.is_empty() {
        state.assignments.extend(plan.assignments.iter().cloned());
        state.incidents.push(incident.clone());
        save(dir, &state)?;
    }
    write_atomic(&record_path, &to_toml(&RequestFile { incident, plan })?)?;
    Ok(CommitOutcome::Committed)
}

/// Holds the folder's lock while `work` runs: every other node waits, so each check-then-write is atomic.
fn locked<T>(dir: &Path, work: impl FnOnce(&Path) -> io::Result<T>) -> io::Result<T> {
    let lock = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join(LOCK_FILE))?;
    lock.lock()?; // blocks until no other node (process) holds it
    let result = work(dir);
    let _ = lock.unlock(); // also released when `lock` is dropped, e.g. if the node crashes
    result
}

fn load(dir: &Path) -> io::Result<StateFile> {
    read_toml(&dir.join(STATE_FILE))
}

fn save(dir: &Path, state: &StateFile) -> io::Result<()> {
    write_atomic(&dir.join(STATE_FILE), &to_toml(state)?)
}

/// Writes to a temporary file, then renames it over the target, so a reader (or a crash)
/// never sees a half-written file.
fn write_atomic(path: &Path, text: &str) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?; // on disk before the rename makes it visible
    }
    fs::rename(&tmp, path) // replaces the old file in one step
}

fn read_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<T> {
    let text = fs::read_to_string(path)?;
    toml::from_str(&text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {e}", path.display())))
}

fn to_toml<T: Serialize>(value: &T) -> io::Result<String> {
    toml::to_string(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// `requests/<request_id>.toml`. Client request IDs are Uuids (letters, digits, `-`); anything
/// else is hex-encoded so it is always a safe file name.
fn request_path(dir: &Path, request_id: &str) -> PathBuf {
    let safe = request_id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    let name = if safe && !request_id.is_empty() {
        request_id.to_string()
    } else {
        request_id.bytes().map(|b| format!("{b:02x}")).collect()
    };
    dir.join(REQUESTS_DIR).join(format!("{name}.toml"))
}

fn unavailable(e: io::Error) -> StateUnavailable {
    StateUnavailable { reason: format!("shared-state folder: {e}") }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// A fresh, empty folder per test.
    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rescuemesh-filestore-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn resource(id: u64) -> Resource {
        Resource { id, rtype: ResourceType::Ambulance, quantity: 1, available: true, location: Zone::new(0, 0), lease_expiry: 0 }
    }

    fn resources(count: u64) -> Vec<Resource> {
        (1..=count).map(resource).collect()
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

    fn plan(incident_id: u64, resource_ids: &[u64]) -> Plan {
        Plan {
            incident_id,
            priority: 1.0,
            assignments: resource_ids.iter().map(|&resource_id| Assignment { incident_id, resource_id, score: 0.5 }).collect(),
        }
    }

    /// The point of this store: what one node assigns, every other node sees as taken.
    #[tokio::test]
    async fn a_resource_taken_on_one_node_is_taken_on_the_others() {
        let dir = temp_dir("shared");
        let node1 = FileStore::open(&dir, resources(10)).unwrap();
        let node2 = FileStore::open(&dir, resources(10)).unwrap(); // second node: keeps the existing state

        assert_eq!(node1.commit("a", incident(1), plan(1, &[3])).await, CommitOutcome::Committed);
        let seen_by_2 = node2.snapshot().await.unwrap();
        assert_eq!(seen_by_2.resources.len(), 10, "one set of resources, not two");
        assert_eq!(seen_by_2.assignments.len(), 1);
        assert_eq!(node2.commit("b", incident(2), plan(2, &[3])).await, CommitOutcome::ResourceTaken { resource_id: 3 });
    }

    #[tokio::test]
    async fn a_request_committed_on_one_node_is_found_on_the_others() {
        let dir = temp_dir("dedup");
        let node1 = FileStore::open(&dir, resources(10)).unwrap();
        let node2 = FileStore::open(&dir, resources(10)).unwrap();

        assert_eq!(node1.commit("r", incident(1), plan(1, &[1])).await, CommitOutcome::Committed);
        assert_eq!(node2.commit("r", incident(1), plan(1, &[2])).await, CommitOutcome::AlreadyCommitted(plan(1, &[1])));
        assert_eq!(node2.lookup_request("r").await.unwrap(), Some(plan(1, &[1])));
        assert_eq!(node2.lookup_request("never").await.unwrap(), None);
        // The rejected second plan took nothing: only resource 1 is assigned.
        assert_eq!(node2.snapshot().await.unwrap().assignments.len(), 1);
    }

    /// 3 "nodes" race 30 commits for 10 resources (each resource wanted 3 times): exactly 10 win.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_nodes_never_assign_a_resource_twice() {
        let dir = temp_dir("race");
        let nodes: Vec<Arc<FileStore>> = (0..3).map(|_| Arc::new(FileStore::open(&dir, resources(10)).unwrap())).collect();
        let mut tasks = Vec::new();
        for i in 0..30u64 {
            let node = nodes[(i % 3) as usize].clone();
            let resource_id = i % 10 + 1;
            tasks.push(tokio::spawn(async move { node.commit(&format!("req{i}"), incident(i), plan(i, &[resource_id])).await }));
        }
        let mut committed = 0;
        for task in tasks {
            match task.await.unwrap() {
                CommitOutcome::Committed => committed += 1,
                CommitOutcome::ResourceTaken { .. } => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(committed, 10, "each of the 10 resources is given out exactly once");
        let snapshot = nodes[0].snapshot().await.unwrap();
        let distinct: HashSet<_> = snapshot.assignments.iter().map(|a| a.resource_id).collect();
        assert_eq!((snapshot.assignments.len(), distinct.len()), (10, 10));
    }

    #[tokio::test]
    async fn an_empty_plan_is_stored_and_takes_nothing() {
        let dir = temp_dir("empty");
        let store = FileStore::open(&dir, resources(2)).unwrap();
        assert_eq!(store.commit("e", incident(9), plan(9, &[])).await, CommitOutcome::Committed);
        assert_eq!(store.lookup_request("e").await.unwrap(), Some(plan(9, &[]))); // a retry gets the same empty plan
        assert!(store.snapshot().await.unwrap().assignments.is_empty());
    }

    #[tokio::test]
    async fn a_restarted_node_keeps_the_shared_state() {
        let dir = temp_dir("restart");
        let before = FileStore::open(&dir, resources(10)).unwrap();
        assert_eq!(before.commit("a", incident(1), plan(1, &[5])).await, CommitOutcome::Committed);
        drop(before);
        let after = FileStore::open(&dir, resources(10)).unwrap(); // must not reset the resources
        assert_eq!(after.snapshot().await.unwrap().assignments.len(), 1);
    }
}
