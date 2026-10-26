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
