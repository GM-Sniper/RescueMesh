# RescueMesh Design Log

## P2: Domain Types and Planner v1

### 1. Resource Match Scores

- Exact match: 1.0
- Partial match: 0.5
- No match: 0.0

### 2. Incident Age

Incident age is measured in minutes and contributes to the incident priority score.

### 3. Tie-Breaking

When resources receive equal scores, the planner selects the resource with the lowest resource ID to ensure deterministic results.

### 4. Assignment Policy

Planner v1 assigns at most one resource to each incident.

### 5. Determinism

Generators use a seed to produce repeatable outputs, and planner tie-breaking is deterministic.

## P1: Protocol, Node, Transport, Fault Injection, Failure Detection

P1's entries are kept in their own file: [`docs/p1-design-log.md`](docs/p1-design-log.md).

## P3: Load Metric (LoadCollector)

Code: `crates/common/src/load.rs` (formula, config, `LoadReport`), `crates/node/src/load.rs` (sampling). Weights live in the `[load]` table of `config/nodes.toml`.

| Decision | Alternatives | Why |
|---|---|---|
| `load_score = 0.5 × queue + 0.3 × CPU + 0.2 × service time`, each normalized to 0-1 (queue / 10, CPU / 100 %, service / 100 ms, capped at 1) | Queue length only; CPU only | Queue length best predicts how long a new request waits. CPU catches load that the queue misses (the CPU-heavy planner, other programs on a lab machine). Service time catches a machine that is simply slower. Weights are in the config so stress tests can change them without rebuilding |
| Divide by the sum of the weights | Require weights to add up to 1 | The score stays in 0-1 even after someone edits one weight; with the default weights it changes nothing |
| Queue length is read **live** when a report is asked for; only CPU is sampled every 200 ms | Sample all three every 200 ms | With a 200 ms sample, requests arriving in the same 200 ms would all see the same scores and all go to the same node. The live counter is one atomic read, so it costs nothing |
| CPU sampled on its own OS thread | A tokio task | A busy async runtime (heavy planner) would delay a tokio task exactly when load is high. The thread keeps sampling on time |
| CPU sampled every 200 ms (never faster) | 50 ms; 1 s | sysinfo needs ≥ 200 ms between two readings to compute a correct percentage. 1 s would make the score lag a burst of requests |
| Whole-machine CPU | This process's CPU only | Another program using the machine also slows our planner down, so it should count against the node |
| Service time: EWMA with α = 0.2, starting at the first request's time | Plain average; last request only | A plain average never forgets old requests; the last request alone jumps around. α = 0.2 follows roughly the last 5 requests. Starting from the first value avoids a slow climb from 0 |
| Service time measured by P1's in-flight guard, from handler start to end | Timing only the planner call | The guard already wraps every Submit and runs even on early returns; a client feels the whole handler time, not only the planner's |
| NaN inputs count as 0 | Let NaN through | A NaN bid compares unequal to every other bid and would break the "lowest score wins" rule |
| The election winner is the node with the lowest load score, ties to the lowest node ID (modified Bully, survey Section 9) | Classic Bully (highest ID always wins); round robin; random | A fixed ID sends every request to the same node, and round robin and random ignore how busy a node is. The load score sends each request to the least busy live node. It also stops a node returning from a failure from taking everything just because of its ID |
| Round-trip time to the client is not part of the score | Include it (the survey lists it as a possible factor) | A node only learns a client's RTT once that client's request arrives, so for new clients it would be a guess. The weights are in the config, so it can be added in Phase 1 if the data shows it matters |
| The bid is measured when the request arrives (queue read live) and then fixed for that election | Use the last 1 s sample; recompute on every message | A burst of requests can make one node look idle to several of them at once; reading the queue live narrows that window. Fixing the bid once means every node compares the same number from each bidder. The experiments will show whether the load is shared fairly |

Checked: the plan's worked example (queue 4, CPU 50 %, service 20 ms) gives 0.39 in a unit test. Under artificial load (every core busy), sampled CPU rose from about 47 % to 99.7 % (`cargo test -p node -- --ignored --nocapture`). Live numbers: `RUST_LOG=info,node::load=debug cargo run -p node -- --id 1`.

**For P6 (node_metrics):** call `state.load_report()` once per second and log `queue_len`, `cpu_pct`, `service_ms`, `load_score`.


## P4: Store and Change Log

**SUPERSEDED on Oct 21** by "P4: Shared state in a distributed file system" at the end of this file. The log, `applied` map and replication path described here were removed from the code. Kept for history; the code is on branch `p4/backup-step-a-store`.

Code and usage: `crates/node/src/store.rs`, `docs/p4-changes.md`. This section holds the decisions only.

| Decision | Alternatives | Why |
|---|---|---|
| Two dedup layers: `(source_node, seq)` for log entries, `request_id` for client requests | Only one of them | They answer different questions (did I apply this entry? did I already answer this request?), so neither can replace the other |
| Order comes from `(source_node, seq)`; `timestamp_ms` is informational only | Wall-clock ordering | Machine clocks differ and nothing synchronizes them |
| `seq = applied[self] + 1`, assigned inside the write lock | Atomic counter outside the lock | Outside the lock, seq 6 could be written before 5 and leave a hole |
| `try_apply` re-checks `request_id` and that every planned resource exists and is free, inside the write lock; nothing is written on error | Hold the lock across snapshot + plan + apply | The planner runs between snapshot and apply. Holding the lock across it serializes every request, which defeats P3's load balancing. Re-checking costs only a rare retry |
| Gaps are rejected, not buffered (`Gap { expected }`) | Buffer out-of-order entries | A buffer needs a size limit, an expiry, and a rule for an entry that never comes. The caller can just `FetchLog` |
| A skipped entry (`Superseded`, `Invalid`) is still logged and still advances `applied[src]`, with a warning; state is untouched | Drop it | Otherwise the source looks like a gap forever and `entries_since` serves a log with a hole |
| No free resource: empty plan, incident logged as `Open`, plan kept in `by_request` | Reject the request; queue and re-plan | The request was processed and a retry gets the same answer. `TODO(P4)` in `try_apply` |
| Starting resources are constants in `store.rs` (seed 42, 10 resources) | A `[resources]` table in `nodes.toml` | Avoids touching `config.rs` for now. Every node needs the same set. `TODO(P4)`: remove once clients can offer resources as log operations; make configurable only if experiments need a different resource count |
| `by_request` stores `(origin node, seq, plan)` | Plan only | Lets a later rule compare origins. Nothing depends on it yet |
| Recovery by log replay (`FetchLog`), decided in Step C | Snapshot transfer | Replay reuses the idempotent `apply_remote`. A snapshot needs a point-in-time cut that matches `applied` and a new RPC. Revisit if the log gets long, and add log trimming then |

### Invalid in-order entries (provisional; revisit in Step B, when replication calls `apply_remote`)

| Alternative | What happens | Trade-off |
|---|---|---|
| **Log, advance, warn** (chosen for now) | Entry kept in the log, `applied[src]` advances, warning | Replication keeps moving and nothing is lost. This node's state may differ from the others' until someone looks |
| **Stop and alert** | Do not advance; halt replication from that source until a person looks | Safest for consistency, but it blocks every entry behind the bad one |
| **Quarantine** | As the first option, plus a list of `(source, seq, reason)` | The entry can be reported and re-applied later. Costs the list and a way to re-apply |

The reason starts with `unsupported op` or `bad data`. `TODO(P4)` in `Store::apply_remote` points here.

### Same request_id from two origins (provisional; revisit all three before building the undo in Step B)

P3's Q4: keep the plan from the lower origin `node_id`. Step A: **first applied wins** (`Superseded`).

| Alternative | How it works | Limits |
|---|---|---|
| **Undo** | Apply as now. On a late lower-origin entry, release the loser's assignments, reset its incident, keep its log entry | Works with the current state. Needs a reversible apply, and the winner's resources may have been taken meanwhile |
| **Replay** | Rebuild the state from the log with "lowest origin per request_id counts" | Needs a clock-free canonical order of entries. We do not have one |
| **Avoid** | Wait for Coordinator confirmation before `try_apply` | Adds delay to every request, and a late Coordinator still leaves the problem |


## P4: Shared state in a distributed file system (DRAFT, for Yasmina to edit)

Code and usage: `crates/common/src/shared_state.rs`, `docs/p4-changes.md`. Replaces the section "P4: Store and Change Log" above. Status as of Oct 29: **SeaweedFS is chosen, and the four decisions in "Locked decisions" below were recorded on Oct 29 from Yasmina's written instructions. The Oct 23 spike result (last table) is what led to decision 1.** This section is still a DRAFT for Yasmina to edit.

### What changed and what stayed

On Oct 21 the design changed: shared state lives in an existing distributed file system (DFS) instead of per-node stores kept equal by our own replicated log. Removed from the code: the log, the `applied` map, `entries_since`, `apply_remote`. Kept:

| Idea | Where it is now |
|---|---|
| `request_id` is the dedup key, because a client retry can land on a different node | `SharedState::commit` is create-if-absent on `request_id`; `lookup_request` reads it |
| Ordering comes from versions or counters, never from clocks | Left to the backend. The interface has no timestamps |
| The write path re-checks that every planned resource is still free, because `snapshot()` is a copy | `commit` returns `ResourceTaken` and writes nothing |
| A request with no free resource is recorded as `Open` with an empty plan | Same, and a retry gets the same empty plan |

### Decisions

| Decision | Alternatives | Why |
|---|---|---|
| The interface is an `async` trait whose methods can fail | A synchronous, infallible trait (simplest for the in-memory store) | A DFS write is a network call and can fail. A synchronous trait now would make P1, P3 and P6 rewrite every call site later |
| `#[async_trait]` (boxed futures) | Native `async fn` in traits | Nodes hold `Arc<dyn SharedState>` so the backend is chosen at start-up. Native async fn in traits cannot be used behind `dyn` |
| `commit` returns an enum with four cases: `Committed`, `AlreadyCommitted(plan)`, `ResourceTaken`, `StateUnavailable` | `Result<Plan, Error>`; separate calls for "create" and "check" | Each case needs different caller behavior, and an enum makes the compiler force every caller to handle all four. A separate check-then-create has a gap in which two nodes both pass the check |
| `commit(request_id, incident, plan)`: the incident is a parameter | `commit(request_id, plan)` as first specified | Locked decision 2 below |
| An unknown resource is its own outcome, `UnknownResource` | Folding it into `ResourceTaken` (what the first version did) | Locked decision 3 below |
| In-memory store stays behind the trait for now | Wait for the DFS backend before landing the interface | The interface unblocks P1, P2, P3 and P6 today. Each node's in-memory copy is NOT shared, so no cross-node guarantee exists until the DFS backend does |

### Locked decisions (Oct 29)

| # | Decision | Alternatives | Why |
|---|---|---|---|
| 1 | Create-if-absent goes through the SeaweedFS **S3 gateway** with `If-None-Match: *`. **Oct 29 spike 2 caveat: two writers both won in the first race after a filer rejoined; see the last table and open question 9 in `docs/p4-changes.md`. Decision left as locked; its safety is in question** | The filer's gRPC exclusive-create (`o_excl`): untested, needs the SeaweedFS `.proto` files wired into Rust, **kept as the fallback**. Plain filer HTTP PUT: failed (the header is ignored, last writer wins). Other DFSs: see the table below | It is the only route that passed our race tests (Oct 23 spike, see the table at the end) |
| 2 | `commit(request_id, incident, plan)` | Embed the incident in `Plan` | The stored state needs the incident, and passing it explicitly is clearer than a hidden lookup. Embedding it would change P2's `Plan` type and hide data inside it |
| 3 | `CommitOutcome` has a separate `UnknownResource { resource_id }` variant | Fold it into `ResourceTaken` (the first version did) | `ResourceTaken` means "take a fresh snapshot and retry", an ordinary race. An unknown resource cannot be fixed by retrying, so it must not look the same, or a caller would loop on a bug. Landed as its own commit (`Add UnknownResource to CommitOutcome`); `handle_submit` in `service.rs` was the only `match` that needed a new arm |
| 4 | **Rejoin readiness (placeholder, P4 may refine):** a returning node is ready when (a) its S3 gateway is reachable, (b) it has read the resource listing once, and (c) it has passed one heartbeat round | Replaying a log or copying a snapshot (the old design) | State lives in the DFS, so there is nothing to replay. The three conditions check that the node can reach the state, can read it, and is seen by its peers. Not implemented yet; see open question 8 in `docs/p4-changes.md` |

### Report note (ready to paste)

SeaweedFS is a service we use. The consistency rules are ours: how create-if-absent is used on `request_id`, why a retry is safe (idempotency), and how a retry is answered with the stored plan. SeaweedFS stores the bytes; our code decides what a correct write is.

### DFS decision: SeaweedFS (chosen pending the spike) and the alternatives

| Alternative | Why it was ruled out |
|---|---|
| **Our own thin replication layer** with a file API | It is the replication we already had (a log and sync between nodes), plus the work of defining a file protocol. It moves the same problem rather than removing it |
| **GlusterFS, Ceph, Lustre, BeeGFS** | Linux-only or too heavy to install and run on the team's mixed lab machines. Platform support: UNVERIFIED for each, not checked in detail |
| **Windows DFS Replication** | Windows Server only, multi-master, and no atomic create-if-absent |
| **JuiceFS** | Mainly setup cost: it needs three pieces (the file system client, a metadata database, and an object store) to install and run on every lab machine. Corrected Oct 29: the earlier reason, "metadata is centralized", is too strong, because the metadata database can itself be replicated (UNVERIFIED: JuiceFS's supported metadata engines and their replication were not checked) |
| **HDFS** | Central NameNode; high availability needs a standby NameNode plus JournalNodes plus ZooKeeper; files are write-once (no in-place update); a Linux and Java stack with poor Windows support; small records (ours are under 1 KB) are a poor fit. All of these points are UNVERIFIED: none was checked against HDFS documentation or tried |
| **XtreemFS** | Server runs only on Linux and Solaris. Maintenance status: UNVERIFIED |
| **etcd, ZooKeeper** | Strong atomic operations, but they are coordination stores, not file systems |
| **NFS** | One central server |
| **Syncthing** | Eventual consistency only: no atomic create |
| **Google Drive** | Central service; ruled out by Yasmina |
| **SeaweedFS** (chosen, pending the spike) | Runs as a single binary on Windows and Linux (Windows run verified on 4.48). Each filer can keep its own local metadata store (verified), so no central database is needed. Used over HTTP; nothing is mounted |

### Spike result that affects the decision (Oct 23, SeaweedFS 4.48, Windows 11, localhost)

| Finding | Evidence | Consequence |
|---|---|---|
| Plain filer HTTP `PUT` with `If-None-Match: *` does **not** create-if-absent | Second create returned 201 and overwrote the file | Cannot be used for `commit` |
| 24 concurrent creates over 3 filers all succeeded, and the filers sometimes ended up holding different winners | 0 of 8 rounds had exactly one winner; filers disagreed in 3 of 8 (4 of 8 and 6 of 8 in earlier runs) | The filers replicate metadata asynchronously, so writes to different filers are last-writer-wins |
| 3 filers with their own local stores do share metadata, with no central database | A write on one filer was readable on the others after 3 to 28 ms (median 7 ms, localhost) | Check passes, but the sharing is asynchronous |
| The **S3 gateway's** `If-None-Match: *` did give exactly one winner over two gateways on different filers | 8 of 8 scripted rounds, 11 of 11 earlier manual rounds, and 4 of 4 rounds with a filer killed | The only tested way to get create-if-absent from SeaweedFS. Why it works is UNVERIFIED (likely a shared lock among filers). **Needs Yasmina's decision**: use the S3 gateway, test the filer gRPC exclusive-create (UNVERIFIED), or reconsider |

### Spike 2 result (Oct 29, SeaweedFS 4.48, Windows 11, localhost, 3 masters / 3 volume servers on 3 racks with replication 010 / 3 filers / 3 S3 gateways)

| Finding | Evidence | Consequence |
|---|---|---|
| S3 `If-None-Match: *` gave exactly one winner and every filer and gateway agreed | 12 of 12 rounds with 24 writers; 10 of 10 with 3 writers; also 12 of 12 with a volume server killed and 12 of 12 with a filer and its gateway killed | Supports decision 1 in normal operation and during a crash |
| `If-Match` with a shared etag gives exactly one winner | 12 of 12 rounds | Usable later for updates (for example resource leases) |
| **Two writers both won in the first race after the killed filer and gateway were restarted; the three filers then held different values** | Writers 8 and 23, both through the same gateway; filers 8888 and 8890 held `writer-23`, filer 8889 held `writer-8`. Seen once, not reproduced, test stopped as instructed | Decision 1 does not hold during recovery. Cause is UNVERIFIED (hypothesis: the filers' lock ownership changes when the cluster membership changes). Open questions 8 to 10 |
| A client-side timeout does not mean the write failed | 10 of 10 timed-out writes landed (reply held back); 5 of 5 landed after a paused gateway resumed | A timed-out `commit` must be `StateUnavailable`; the retry with the same `request_id` returns `412`, which maps to `AlreadyCommitted` |
| Contended creates are slow for the losers | 24 writers: losers wait about 1 s each, slowest 13-18 s. 3 writers: slowest about 1 s | `commit` needs a timeout of several seconds. Only the elected executor should commit |
| Reads after a commit were never stale on localhost | 0 of 150 new-key reads and 0 of 150 updated-key reads | Not proof for a real network (UNVERIFIED) |
| The S3 gateway works with no credentials | Every run was anonymous | Minimal config is none; security is a deployment question |
| Not run | Check d (kill masters), rejoin of masters, gateway with credentials, replication `020` | Needs a rerun once the two-winner window is understood |
| Spike 3 (Oct 29): the double win did not recur | 0 two-winner rounds in 360 post-restart or post-mute races (30 runs; 5 s and 90 s outages, kill and suspend, offsets 0-60 s); writer-token read-back: 0 false alarms | Rate unknown; the spike-2 condition (kill during a running race) was not re-created. No hold time can be derived. Decisions unchanged. Details and open questions 13 and 14 in `docs/p4-changes.md` |
| Spike 3: masters | 1 of 3 down: creates hang 8-25 s during election, then work. 2 of 3 down: reads of existing and never-read data work, every create hangs. Recovery 17-35 s after restart | `commit` needs a timeout; a timeout is `StateUnavailable` |

### Assigned versus completed (Oct 29, from Yasmina's discussion with teammates)

Code: `crates/common/src/shared_state.rs` (trait, types), `crates/node/src/store.rs` (in-memory implementation and tests). Usage: `docs/p4-changes.md` and `docs/p4-team-note.md`. Status: implemented for the in-memory store only; nothing is wired into the failure detector or the election.

**The model.** Each request record has an owner node id and a status: `Assigned` (resources reserved, not yet confirmed done), `Completed`, or `Released` (the owner was declared failed, its resources were freed, the request waits for a new owner). `commit` stores the plan as `Assigned` with the executing node as owner. A commit over a `Released` record atomically moves it to `Assigned` with the new owner, and exactly one concurrent committer wins; `AlreadyCommitted` applies only to `Assigned` or `Completed`. A returning node forgets all in-flight work and re-reads state from shared state; it never resumes anything.

| # | Decision | Alternatives | Why |
|---|---|---|---|
| 1 | **No special "late owner" handling.** A node that returns and tries `mark_completed` is checked against the record: if the record is `Released` or owned by another node, the call is rejected (`NotCurrentOwner`) and nothing changes | Let the returned node finish and overwrite the record | Overwriting could clobber the new owner's work. One conditional check handles every case |
| 2 | **The node elected to handle a failed peer's cleanup releases that peer's requests**, using `release_unfinished_requests_of_failed_node`. The release stays conditional anyway | Every survivor releases independently | One cleaner avoids duplicate work. Independent release would need conflict handling for no benefit. The condition is a safety net: the election can briefly give two winners after a restart (spike 2 findings), and then a second release finds `AlreadyReleased` and does nothing |
| 3 | **A new election picks the new executor of a released request.** The cleaning node does not run it itself | Wait only for the client's retry; survivors re-run it directly | See open question A below |
| 4 | **This reverses the Oct 23 decision that cut server-side takeover.** P3 and P1 must review it. The professor's note that fault tolerance is client-side may conflict with it (UNVERIFIED: I did not see the note) | Keep takeover out and rely on client retries | Yasmina's decision after discussion with teammates; recorded here so the reversal is visible |
| 5 | **Leases as a backstop for late failure detection: undecided** | Not adding leases; adding a lease expiry to each `Assigned` record | See open question B below |
| 6 | `commit` takes the owner: `commit(request_id, owner_node_id, incident, plan)`. This changes the signature locked on Oct 29 (`commit(request_id, incident, plan)`) by one parameter | The store is built with its node id; the record's owner is looked up later | The owner must be stored with the record, and an explicit argument is clearer than hidden state in a store that, with a DFS backend, is shared by all nodes. Only `handle_submit` in `service.rs` needed the extra argument (`state.id`) |
| 7 | `lookup_request` returns `None` for a `Released` record | Return the old plan | A released request needs a fresh plan from its next owner; handing the old plan to a retry would reply with resources that were freed |
| 8 | `mark_completed` sets the incident `Resolved` and **keeps the resources reserved**; `release` sets the incident back to `Open` and frees the resources | Free the resources on completion | The task only defined freeing on release. Open question C |
| 9 | Every new outcome enum has its own variants (`NotCurrentOwner`, `AlreadyCompleted`, `NotFound`, `AlreadyReleased`, `StateUnavailable`) | Booleans or errors | Same reason as `CommitOutcome`: each case needs different caller behavior |

**Open questions.**
- **A. What triggers the new election for a released request** (the cleaning node after releasing, or the client's retry)? For P3 and P1 to settle. Until then a released request is picked up only when a client retries it with the same `request_id`, because `lookup_request` returns `None` and the retry is planned afresh.
- **B. Leases.** Detection takes at least the failure detector's 1.5 s (UNVERIFIED: figure taken from the task text, not re-checked in `failure_detector.rs`), so for that long a failed node's reservations look alive. A lease would let any node treat an `Assigned` record as expired after a fixed time. Not built.
- **C. Should completion free the reserved resources?** Today they stay assigned forever after `mark_completed` (as the old store did). If resources are reusable, `mark_completed` must free them. Needs P2/Yasmina.
- **D. Release is atomic here but not on S3** (see the mapping below): resources are separate objects from the record.

**How each transition maps to a conditional S3 write (documented, NOT built; all UNVERIFIED as a whole design).** Spike 1-3 showed `If-None-Match: *` and `If-Match: <etag>` on `PUT` through the S3 gateway giving exactly one winner (12 of 12 rounds each in spike 2), with the one unexplained double win after a filer restart (spike 2).

| Transition | S3 operations |
|---|---|
| commit (new request) | `PUT record` with `If-None-Match: *`. `412` means the record exists: read it, and if `Assigned` or `Completed` return `AlreadyCommitted(plan)` |
| commit over `Released` | `GET record` (note its etag), check `Released`, then `PUT record` with `If-Match: <etag>` and the new owner and `Assigned`. `412` means someone else won the move: `AlreadyCommitted` |
| `mark_completed` | `GET record`, check `Assigned` and owner, `PUT record` with `If-Match: <etag>` and `Completed`. `412` means the record changed: read again to produce `NotCurrentOwner` or `AlreadyCompleted` |
| `release_unfinished_request` | `GET record`, check `Assigned` and the expected owner, `PUT record` with `If-Match: <etag>` and `Released`; then delete or overwrite the reservation objects for the plan's resources. A second call finds `Released` and stops |
| `list_unfinished_requests_owned_by` | List the record objects and read each (or keep a per-owner index object). Cost not measured |

The weak point is the release: the record and the reservations are different objects, and S3 has no multi-object transaction. If the cleaner dies after writing `Released` and before freeing the resources, the resources stay reserved while the record says `Released`. A rerun of the cleaner would then need to free resources for an `AlreadyReleased` record too (the in-memory store does not need that). Open question D.
