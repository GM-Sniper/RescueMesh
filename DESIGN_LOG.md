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

### 6. Configurable Matching Workload

The planner performs deterministic incident-resource scoring for a configurable workload. The workload dimensions are controlled by `workload_incidents`, `workload_resources`, and `iterations`. The workload is generated using a fixed seed, so the same configuration produces the same workload inputs across nodes.

The extra workload is separate from the live snapshot. It adds CPU work but does not change the assignment returned for the incoming incident.

### 7. Planner Cost Benchmark

Measurements were collected using the release build with `cargo run --release -p common --example planner_benchmark`. Times are average milliseconds per planner call on the machine used for testing.

| Incidents | Resources | Iterations | Work units (N × M × I) | Average time (ms) |
|---:|---:|---:|---:|---:|
| 10 | 20 | 1 | 200 | 0.002942 |
| 50 | 50 | 1 | 2,500 | 0.025871 |
| 100 | 100 | 1 | 10,000 | 0.075175 |
| 200 | 200 | 1 | 40,000 | 0.197777 |
| 100 | 100 | 10 | 100,000 | 0.372429 |
| 300 | 300 | 1 | 90,000 | 0.533878 |
| 500 | 500 | 1 | 250,000 | 1.955595 |
| 500 | 500 | 10 | 2,500,000 | 17.585792 |

The measurements show increasing planning time with larger workloads. The configuration of 500 incidents, 500 resources, and 10 iterations measured approximately 17.59 ms per call, near the example target of about 20 ms. Actual timings depend on the machine and build environment.

**Benchmark limitation:** these timings include deterministic workload generation and scoring. The generated workload is used to add CPU cost; it is not a batch assignment of all generated incidents. The returned plan still concerns the original incoming incident.

### 8. Determinism and Validation

The common crate tests passed: 28 passed, 0 failed. The tests include deterministic planning and confirmation that adding the workload does not change the returned plan.

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


## P3: Election (Step C)

Code: `crates/node/src/election.rs`.

| Decision | Alternatives | Why |
|---|---|---|
| Tie rule confirmed by the team on Oct 30: lowest load score wins, ties go to the lowest node ID (see the load metric table above) | Ties to the highest ID; random | Every node reaches the same answer from the same bids with no extra message. Confirmed for Step C as the plan asked |
| A node still sends its bid to peers the detector suspects, but only waits for the peers it reports alive. With one node down that is 4 bid calls sent and 2 bids received per election (6 and 6 with all 3 up), not the 2 messages in the Step C plan's worked example | Send only to alive peers, so 2 calls per election | A peer suspected by mistake (a short network hiccup) still gets the bids and can still take part. The two extra calls cost no time, because nobody waits for them. Checked by `csv_row_with_a_node_down_has_two_bids_and_two_calls` |

Election on retry was already checked: `retry_after_the_winner_fails_elects_a_new_one_in_round_1` mutes the round-0 winner, and the client's retry elects a new winner in round 1 without it.


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

Code and usage: `crates/common/src/shared_state.rs`, `docs/p4-changes.md`. Replaces the section "P4: Store and Change Log" above. Status as of Oct 23: **SeaweedFS is the chosen candidate, pending the spike. The spike has run; its result changes how SeaweedFS can be used (see the last table).**

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
| `commit(request_id, incident, plan)`: the incident is a parameter | `commit(request_id, plan)` as first specified | A `Plan` holds only an incident id, and the stored state needs the incident (as `Open` when the plan is empty). Open question 1 in `docs/p4-changes.md` |
| An unknown resource is reported as `ResourceTaken` | A fifth variant for "unknown resource" | The spec had four variants and the caller's cure is the same (snapshot, plan again). Open question 2 |
| In-memory store stays behind the trait for now | Wait for the DFS backend before landing the interface | The interface unblocks P1, P2, P3 and P6 today. Each node's in-memory copy is NOT shared, so no cross-node guarantee exists until the DFS backend does |

### Report note (ready to paste)

SeaweedFS is a service we use. The consistency rules are ours: how create-if-absent is used on `request_id`, why a retry is safe (idempotency), and how a retry is answered with the stored plan. SeaweedFS stores the bytes; our code decides what a correct write is.

### DFS decision: SeaweedFS (chosen pending the spike) and the alternatives

| Alternative | Why it was ruled out |
|---|---|
| **Our own thin replication layer** with a file API | It is the replication we already had (a log and sync between nodes), plus the work of defining a file protocol. It moves the same problem rather than removing it |
| **GlusterFS, Ceph, Lustre, BeeGFS** | Linux-only or too heavy to install and run on the team's mixed lab machines. Platform support: UNVERIFIED for each, not checked in detail |
| **Windows DFS Replication** | Windows Server only, multi-master, and no atomic create-if-absent |
| **JuiceFS** | Keeps its metadata in a separate database (Redis, MySQL and similar), which centralizes the state |
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

## P5: Shared-state backend (Step C, Oct 31)

Code: `crates/node/src/s3_store.rs` (backend), `gated_store.rs` (mute cuts state access), `mute.rs`, `[shared_state]` in `crates/common/src/config.rs`. Change notes: `docs/p5-changes.md`, Oct 31. Answers P4's open question 5 above.

### Decision: SeaweedFS through its S3 gateway

| Decision | Alternatives | Why |
|---|---|---|
| Shared state lives in SeaweedFS, reached through its **S3 gateway** over plain HTTP | Filer HTTP API; filer gRPC `o_excl`; in-memory store per node (fallback) | The filer HTTP API has no create-if-absent (P4's spike). gRPC `o_excl` is untested and needs SeaweedFS's proto files. The in-memory fallback shares nothing between nodes: a client retry on another node would not find the first plan, and "every node reads the same state" could not pass. The S3 gateway gave exactly one winner in every round tested (P4: 8 of 8 over two gateways; P5: 10 of 10 rounds of 24 racing creates) |
| One gateway per machine; each node uses `http://127.0.0.1:8333` | All nodes use one gateway | One gateway would be a single point of failure. A node whose own gateway is down gets `StateUnavailable` and the client retries elsewhere, which matches a node failure |
| No auth on the gateway, plain `reqwest` HTTP, no AWS SDK | AWS SDK with signed requests | Lab network only. Saves a large dependency and a credentials setup on 3 machines. Note: if `AWS_ACCESS_KEY_ID` is set in the gateway's environment, SeaweedFS turns auth on and every call gets 403 |
| `incidents` is empty in an S3 `snapshot()` | List every request record per snapshot | The planner only reads resources and assignments. Listing every request would make each request slower than the last (O(n) per request). The consistency check reads `requests/` directly |
| Per-call deadline 2.5 s, lease 10 s, config check `lease >= 4 x deadline` | 1 s deadline | Measured Oct 31: when writers race for one key the gateway's lock backs off, and losers waited up to about 1 s. At 1 s some racing commits timed out |

### The commit path

Objects: `resources/<id>` (written once, create-if-absent), `claims/<resource_id>` (a lease), `requests/<request_id>` (the commit point).

1. `requests/<id>` exists: return its plan (`AlreadyCommitted`).
2. Claim each planned resource, lowest id first (create-if-absent). An existing claim is taken over with `If-Match` (compare-and-swap, 1 of 24 racing swaps won in testing) only if it has the same `request_id` (an earlier attempt of this request, so a client retry does not wait) or it has expired and its request never committed with that resource. Otherwise release ours and return `ResourceTaken`.
3. Create `requests/<id>`. Won: mark the claims `confirmed`. Lost to another node: release the claims its plan does not use, confirm the rest, return its plan.

| Decision | Alternatives | Why |
|---|---|---|
| Claims are leases with an expiry | Claims without expiry; rollback only | A node muted between steps 2 and 3 cannot roll back. Without expiry its resources would be held forever |
| The commit gives up if half the lease passed before step 3 | No check | Otherwise a claim could expire under a slow commit and another node could take the resource: two assignments |
| Expiry uses wall clocks (claimer's `expires_at_ms`, reader's clock) | No clocks | Expiry needs time. Safe while the clock offset between machines (P1 measures it, Step D) is far below 10 s. The only place a wall clock decides anything; stated as a limit in the report |
| `If-Match` on release | Plain DELETE | A plain DELETE could remove another node's newer claim |
| Muted node: every state call returns `StateUnavailable` at once (`MuteGatedStore`) | Wait until the mute ends | Waiting would commit a plan made from a snapshot up to 20 s old. Failing behaves like a crash |

### Evidence (Oct 31, localhost, SeaweedFS 4.48 on Linux, one gateway)

| Check | Result |
|---|---|
| Commit race: 3 stores commit the same request_id, 100 rounds | Exactly 1 `Committed` every round (run 3 times) |
| Resource race: 3 requests want the same resource, 5 resources | Exactly 1 winner per resource, 2 `ResourceTaken` |
| Abandoned claim (node died between claim and commit) | Blocks the resource until the lease ends, then it is free |
| Client retry of the same request on another node | Reuses its own claim at once |
| End to end: 3 nodes, 1,000 requests at 50/s, one node muted 8 s mid-run (twice: node 2, then node 1) | 1,000 ok, 0 timeout, 0 error. 1,000 records for 1,000 request_ids, no resource in two plans, all claims confirmed. Detection 1.32 s after the mute |

Not yet tested: two gateways on different machines under load (P4's spike tested two gateways, without load), a filer crash during a commit, and timing on the real network.

### Operations notes

- **Reset between runs = DELETE the bucket** (`curl -X DELETE http://127.0.0.1:8333/rescuemesh`), not a new bucket name. Each bucket gets its own volumes, and after a handful of buckets the volume server returned HTTP 500 "No writable volumes".
- Start `weed` with `-volume.max` raised (or `-master.volumeSizeLimitMB` lowered) on machines with small disks.

## P3: Rejoin readiness rule (Step C)

Code: `crates/node/src/rejoin.rs`, called by the mute hook in `service.rs` when a mute ends, and by `main.rs` when the node starts.

| Decision | Alternatives | Why |
|---|---|---|
| A node is not ready from the start of a mute until it has (1) dropped its election decisions from before the mute, (2) read the shared state once, (3) heard a heartbeat from at least one peer since waking. Only then do its bids carry `ready = true` | Ready as soon as the mute ends; a fixed wait (the plan's fallback: 3 s) | After a mute the node's load score is 0, so it would win every election at once with stale decisions and no proof it can reach the state or its peers. The three checks are the ones in Yasmina's placeholder (PR #30), so they test exactly what the node needs, and usually pass in well under a fixed 3 s |
| A not-ready node still answers bid calls, with `ready = false` | Stay silent until ready | It counts as answered, so peers do not wait the full bid timeout for it, but it cannot win (already in the election rule) |
| The reload is one successful `store.reload()` (P5, #33) | Read the state with `snapshot()` | `reload()` is the call P5 made for this rule, so a backend can do more there later. Today it is a `snapshot()`: nodes keep no copy of the state, so the read only proves the state is reachable. While muted, the gated store fails it at once. The rule retries every 50 ms while it fails |
| A freshly started node runs the same rule (fault log `READY` row with fault_id `startup`) | Start ready | A node restarted after a power-off has missed the same things as a muted one (Ramy's review, lab test B). Side effect: a node started alone stays not ready until a second node runs. Tests build nodes ready, so only the real binary does this |
| Dropped in-flight work = the election table is cleared | Keep old decisions | A request we had won may have been re-elected without us. A cleared table makes a client retry run a fresh election. Requests held at the mute gate are already dropped by the gate |
| The node writes a `READY` row to the fault log when the rule passes | Log only to the console | P6 measures recovery time as `READY` time minus `MUTE_END` time, on the same node, so no clock offset between machines |
| A node with no peers is ready once it can read the state | Wait for a heartbeat forever | Nobody could ever send it one. Only happens in single-node tests |

Expected recovery time: up to one heartbeat interval (500 ms) plus one state read. Measured on the real machines in Step D.
