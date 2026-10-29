# P4 changes: shared state

Owner: P4. Decisions and alternatives are in `DESIGN_LOG.md`, section "P4: Shared state in a distributed file system". This file says what the code is and how to use it.

The earlier version of this file described a per-node store with a replicated log (`try_apply`, `apply_remote`, `entries_since`, `FetchLog`). That design was replaced on Oct 21. It is preserved on branch `p4/backup-step-a-store` (and in git history) if anyone needs it.

---

## Oct 23: the shared-state interface (branch `p4/state-interface`)

### What changed

| File | What changed | What you need to know |
|---|---|---|
| `crates/common/src/shared_state.rs` (new) | The `SharedState` trait, the `CommitOutcome` enum and the `StateUnavailable` error | This is the only thing other people's code should call. Every method has a doc comment saying why it is designed that way |
| `crates/node/src/store.rs` | The old `Store` is now `InMemoryStore`, which implements `SharedState`. The change log, the `applied` map, `entries_since`, `apply_remote` and `RemoteOutcome` are deleted | Each node still has its own private copy of the state. Nothing is shared between nodes until the DFS backend exists |
| `crates/node/src/state.rs` | `NodeState.store` is now `Arc<dyn SharedState>` | Code that used `state.store` keeps working through the trait methods |
| `crates/node/src/service.rs` | `handle_submit` uses `lookup_request`, `snapshot` and `commit`; its tests no longer read `applied()` | P1's file. Changed only as far as the new interface requires |
| `Cargo.toml`, crate `Cargo.toml`s | Added `async-trait = "=0.1.92"` | Already in `Cargo.lock` through tonic, so no new download |

### Why the interface is `async` and can fail

A DFS write is a network call, and network calls can be slow or fail. The in-memory store could be synchronous and infallible, but then P1, P3 and P6 would have to rewrite every call site when the DFS arrives. Making callers handle "unavailable" from day one costs a few `.await`s now.

### Why `#[async_trait]` and not native `async fn` in traits

Nodes hold the backend as `Arc<dyn SharedState>`, so the choice of backend is made once at start-up. Native `async fn` in traits cannot be used behind `dyn`. `async_trait` can, and costs one boxed future per call, which is negligible next to a network round trip.

### How to code against the trait

```rust
use common::shared_state::{CommitOutcome, SharedState};
// state.store is an Arc<dyn SharedState>

// 1. Already handled? A client retry can reach any node, so always ask first.
if let Some(plan) = state.store.lookup_request(&request_id).await? { /* reply with plan, duplicate = true */ }

// 2. Copy the state for the planner, then plan (CPU work, off the async threads).
let snapshot = state.store.snapshot().await?;

// 3. Store the plan. Exactly one committer wins per request_id.
match state.store.commit(&request_id, incident, plan.clone()).await {
    CommitOutcome::Committed => { /* we did the work: reply with plan, duplicate = false */ }
    CommitOutcome::AlreadyCommitted(stored) => { /* reply with `stored`, throw ours away, duplicate = true */ }
    CommitOutcome::ResourceTaken { .. } => { /* go back to snapshot and plan again, a few times at most */ }
    CommitOutcome::UnknownResource { .. } => { /* a bug: do NOT re-plan; return an internal error */ }
    CommitOutcome::StateUnavailable(why) => { /* tell the client to retry with the SAME request_id */ }
}
```

(`?` works because `snapshot` and `lookup_request` return `Result<_, StateUnavailable>`; `handle_submit` in `service.rs` maps it to a gRPC `Unavailable`.)

### What each outcome means

| Outcome | Meaning | What you do |
|---|---|---|
| `Committed` | This call stored the plan | Reply with your plan |
| `AlreadyCommitted(plan)` | That `request_id` already had a plan (earlier call, retry, or another node) | Reply with **that** plan, not yours. Otherwise a client could get two different plans for one request |
| `ResourceTaken { resource_id }` | A planned resource was not free at write time. Nothing was written | Take a new snapshot, plan again, commit again |
| `UnknownResource { resource_id }` | A planned resource does not exist in the shared state. Nothing was written | Do not retry: a fresh snapshot cannot fix it. Treat it as a bug and return an internal error (`handle_submit` does this) |
| `StateUnavailable(reason)` | Could not reach enough of the shared state to decide. The write may or may not have landed | Retry with the same `request_id`. This is safe: commit is create-if-absent, so the retry comes back `AlreadyCommitted` if the first attempt landed |

Other rules:

- `request_id` is the dedup key. Never invent a new one for a retry.
- An empty plan is a valid commit. It means "nothing was free"; the incident is stored as `Open`, and a retry gets the same empty plan.
- Do not use wall-clock time to decide which write came first. The backend decides.
- `InMemoryStore` never returns `StateUnavailable`. Write your handling anyway, and test it with a small fake of the trait that always fails.

### Tests

`cargo test -p node store`: 10 unit tests with descriptive names (same request twice, concurrent commits of one request, two requests for one resource, lookup, empty plan, and others). `cargo test -p node` also runs the `handle_submit` tests in `service.rs`. Total after this change: 66 passing in `node` (the old store had 18).

### Not built, on purpose

- Anything that talks to a DFS. The spike (below) only checks whether SeaweedFS can do what the interface needs.
- Resource leases, incident expiry (TTL), and recovery of a returning node. They depend on what the DFS backend offers.

### Open questions

| # | Question | What I did | Alternatives |
|---|---|---|---|
| 1 | The request said `commit(request_id, plan)`, but a `Plan` only holds an incident id and the stored state needs the incident itself (it must be recorded as `Open` when the plan is empty) | `commit(request_id, incident, plan)` | Keep `commit(request_id, plan)` and store incidents with a second call. That makes two writes that can half-fail on a DFS |
| 2 | RESOLVED Oct 29 (locked decision): a separate `UnknownResource` variant, so "retry on a fresh snapshot" (`ResourceTaken`) is not confused with "something is wrong" | `CommitOutcome` now has five variants | None |
| 3 | `StateUnavailable` carries a `reason` string | Yes, for logs only | A bare variant with no data |
| 4 | The earlier doc listed handling for two nodes planning the same free resource and "same request from two origins". With one shared create-if-absent record these should no longer happen, but that depends on the DFS giving the guarantee | Left to the spike (check 1 and 2) | None until the spike results are confirmed on the real machines |

---

## Oct 23: SeaweedFS spike (report only, nothing is built on it)

Question: can SeaweedFS give us the three things `SharedState` needs (create-if-absent, shared metadata without a central database, usable latency)? Scripts are in `spike/seaweedfs/`; the raw output of the run below is `spike/seaweedfs/results-windows11-seaweedfs-4.48.txt`.

### What was run

| Item | Value |
|---|---|
| SeaweedFS | 4.48 (commit `530be3e3`), official release asset `windows_amd64.zip`, md5 checked against the published `.md5`. 4.49 came out the same day and was not used |
| OS | Windows 11 Pro 10.0.26200, run from Git Bash; one machine, everything on `127.0.0.1` |
| Cluster | 3 masters (9333-9335, `-peers` set), 1 volume server (8080), 3 filers (8888-8890), each filer with its **own** `-defaultStoreDir` and `-filerGroup=spike`. For check 5 only: 2 S3 gateways (8333, 8334), each in front of a different filer |
| Tests | Python 3.11.8, standard library only. Plain HTTP, a fresh connection per request |

Rerun: `WEED_BIN=<path to weed> RUN_DIR=<empty dir> spike/seaweedfs/start_local_cluster.sh`, then `python spike/seaweedfs/run_checks.py spike/seaweedfs/cluster.local.toml`, then `stop_local_cluster.sh`.

### Results

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | Create-if-absent with `If-None-Match: *` on a filer PUT | **FAIL** | The filer ignores the header. First PUT: `201 Created`. Second PUT of the same path with `If-None-Match: *`: `201 Created` (expected 412), and a GET returns the second body, so the file was overwritten |
| 2 | 24 concurrent creates of one path, spread over 3 filers, 8 rounds | **FAIL** | All 24 succeeded in all 8 rounds. Worse, the filers ended up holding different winners after 3 s in 3 of 8 rounds of the saved run, and in 4 of 8 and 6 of 8 rounds of two earlier runs (for example `writer-4`, `writer-20`, `writer-4`). Reading from different nodes would give different answers |
| 3 | 3 filers, each with its own local store: write on A, read on B and C | **PASS** | Each filer has its own `filerldb2` (embedded LevelDB) directory and no external database was used. Writes appear on the other filers after min 3.1 ms, median 6.9 ms, max 27.9 ms (20 reads; localhost, so cross-machine will be slower). Mechanism (seen in the filer log): each filer subscribes to the other filers' metadata changes and replays them into its own store. That is asynchronous, which is why check 2 shows divergence |
| 4 | Latency and limits | informational | See below |
| 5 | **Extra, not on the task list:** same as checks 1 and 2, but through SeaweedFS's S3 gateway (`PUT` object with `If-None-Match: *`) | **PASS** | Single gateway: first create `200`, second `412`. 24 concurrent creates spread over 2 gateways (so 2 different filers), 8 rounds in the script plus 11 earlier manual rounds: every round had exactly one `200` and 23 `412`, and both gateways then returned the same winner. A further 4 rounds with one of the 3 filers killed: still exactly one winner |

### What this means (for Yasmina to decide)

- **Using the filer's own HTTP API for create-if-absent does not work in 4.48.** We would silently get last-writer-wins, and different filers can disagree about the winner. This breaks the "exactly one committer wins per `request_id`" rule.
- **The S3 gateway's conditional create did give the guarantee in every round we ran.** It is the same SeaweedFS and needs no central database, but it is a different front door (S3 API, an extra `weed s3` process per machine, bucket and object-key naming) from the one the plan assumed. This is a design decision, so I did not build on it.
- UNVERIFIED: why the S3 path is atomic across filers. The logs show a "LockRing" (a lock manager shared by the filers, kept in sync by the masters), and my guess is that the gateway takes a lock on the object path, but I did not read the source or confirm it. Whether the guarantee survives a filer crashing in the middle of a write, a network partition, or two filers disagreeing about the ring is untested.
- UNVERIFIED: the filer's gRPC `CreateEntry` has an exclusive-create option (`o_excl`) that I believe gives create-if-absent without the S3 layer. Not tested; a Rust client would need the SeaweedFS `.proto` files, which would be a new dependency.
- UNVERIFIED: other SeaweedFS-supported shared filer stores (Redis, MySQL, PostgreSQL, Cassandra, and others) exist, but would centralize the metadata, which the task rules out. Not tested.

### Check 4: latency and limits (localhost, one machine, 50 sequential operations per filer)

| Operation | Median | p95 |
|---|---|---|
| Write 512 B (filer HTTP PUT) | 6.5-7.0 ms | 18-28 ms |
| Read 512 B (filer HTTP GET) | 4.6-4.9 ms | 11-19 ms |

| Object size | PUT time |
|---|---|
| 1 KB | 5 ms |
| 1 MB | 52 ms |
| 16 MB | 269 ms |
| 64 MB | 643 ms |

A burst of 100 parallel small writes over the 3 filers finished in 0.15 s with no non-2xx answers. No size or rate limit was hit up to 64 MB and 100 parallel writes. Our objects (a plan and an incident) are well under 1 KB. These are loopback numbers: the real machines add network time and, for S3 conditional creates, lock-coordination time that was not measured separately.

### Running it on the real machines

1. Install SeaweedFS 4.48 on each machine (same version everywhere). Start the masters, volume servers and filers with the same flags as `start_local_cluster.sh`, with real IP addresses in `-ip` and `-peers`, and each filer with its own `-defaultStoreDir`. For check 5, also start `weed s3` on each machine with `-filer=<that machine's filer>`; give each `-port.iceberg=0 -port.lance=0` if more than one runs on a host.
2. Copy `cluster.local.toml` to `cluster.real.toml` and replace the URLs. Keep at least 2 filers, 3 for the full test. Remove the `[s3]` table to skip check 5.
3. From any machine that can reach them: `python run_checks.py cluster.real.toml --json results.json`. Needs Python 3.11 or newer and nothing else.

Foreseeable problems on other machines (all UNVERIFIED, except where noted):
- Windows Firewall will prompt for, or block, each port. SeaweedFS also opens a gRPC port at the HTTP port + 10000 (for example 19333 and 18888), which must be reachable between machines too.
- The `.sh` scripts need Git Bash or WSL on Windows. Verified: they run under Git Bash. In Git Bash the process ids recorded in `pids/` are Git Bash ids, so `taskkill //PID` with them fails; `kill` works. `stop_local_cluster.sh` uses `kill` first for that reason.
- Paths with spaces and non-ASCII characters: the repo path here has spaces and worked, but the SeaweedFS data directories should be short and plain anyway.
- Antivirus may scan or quarantine the 150 MB `weed.exe`.
- Mixed Linux and Windows nodes: nothing known against it, but not tested.
- The masters' Raft needs 2 of 3 up, and wall clocks that are roughly in sync is assumed by SeaweedFS's lock expiry (UNVERIFIED); check the machines' clocks before blaming SeaweedFS for odd behavior.
- Check 3's delay will be larger across a real network; the 30 s timeout in the config can stay.

### Open questions from the spike

| # | Question | Notes |
|---|---|---|
| 5 | Is the S3 gateway an acceptable way to use SeaweedFS? | The only path that passed create-if-absent. Alternatives: gRPC `o_excl` (untested), or give up on SeaweedFS for the atomic step (for example, one designated filer for all `commit` calls, which gives up the "no single point" goal). This needs Yasmina |
| 6 | `StateUnavailable`: when should it be returned? | With S3, a gateway or filer that cannot be reached returns a connection error or 5xx. The simplest rule: any transport error or 5xx on `commit` is `StateUnavailable`, and the caller retries with the same `request_id` (safe, because create-if-absent) |
| 7 | How to store `snapshot()`? | Not spiked. The simplest option is one object per incident, resource and assignment, and `snapshot()` lists and reads them. A read after a write on another filer can be stale by the delay in check 3 (a few ms to tens of ms locally), which is why `commit` must re-check resources |
