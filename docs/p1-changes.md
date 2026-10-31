# P1 progress: protocol, node, transport, mute hook, heartbeats, broadcast, Submit

Owner: P1 (reviewer: P5). Design reasons for everything below: [`docs/p1-design-log.md`](p1-design-log.md).

## Status
| Task | Due | Status |
|---|---|---|
| 1. Workspace compiles everywhere | Fri Oct 16 | Done, on `main` |
| 2. Proto v1 (`Submit`, domain types, plus P3/P4/P5 messages) | Sat Oct 17 | Done, on `main` (PR #6) |
| 3. Node skeleton: config loader, `0.0.0.0`, shared state, peer connections, dispatch point | Sun Oct 18 | Done, on `main` (PR #8) |
| 4. Mute hook | Sun Oct 18 | Done, PR #10 |
| 5. Heartbeat loop + failure detection | Mon Oct 19 | Done, PR #10 |
| 6. `broadcast()` + timeout/retry policy | Mon Oct 19 | Done, PR #10 |
| 7. Handler wiring: `Submit` → snapshot → plan → apply → reply, plus P5's fault log | Tue Oct 20 | Done, PR #10 |

Done-check from the plan: 3 localhost nodes log each other's heartbeats ✅, the mute hook silences one on command ✅, a request returns a plan end to end ✅.

## Run it
From the repo root, one terminal per node:
```
cargo run -p node -- --id 1
cargo run -p node -- --id 2
cargo run -p node -- --id 3
```
- Each prints `node listening id=N listen=0.0.0.0:<port> peers=[...] run_id=... fault_log=logs/faults-nodeN.csv`.
- Stop one (Ctrl+C): the other two log `WARN peer suspected down peer=N` after about 1.5 s. Start it again: `INFO peer alive again peer=N`.
- Send requests to node 1: `cargo run -p client -- --count 5` (plans, then a resend with `duplicate=true`), or `cargo run --release -p loadgen -- --requests 1000 --run-id r1` (CSV in `results/`).
- Mute a node: `cargo run -p injector -- --node 2 --duration 3s`. The fault rows land in `logs/faults-node<N>.csv`.
- Same run name in every file: start the nodes with `--run-id r1` too.
- Restart the nodes between load runs: the store keeps its 10 resources assigned (nothing is released in Step A), so later requests get empty plans.
- `RUST_LOG=debug` shows every heartbeat and every applied request (PowerShell: `$env:RUST_LOG="debug"` first).
- Real machines: `--config config/nodes.lab.toml` (fill in the IPs first).
- Tests: `cargo test --workspace`.

## Files
| File | What it is | What you need to know |
|---|---|---|
| `Cargo.toml` (root) | Workspace; every library pinned with `=x.y.z` | Only P1 edits this list. Ask P1 to add a library, then opt in with `name.workspace = true` |
| `Cargo.lock` | Exact version of every library | Committed. Never edit by hand |
| `proto/rescuemesh.proto` | The wire protocol (package `rescuemesh.v1`) | Changes go through P1. Never change or reuse a field number |
| `crates/common/build.rs` | Turns the proto into Rust on every build (protox + tonic-prost-build) | No protoc install needed. A proto mistake shows up as a build error here |
| `crates/common/src/lib.rs` | `common::pb` (generated code), `NodeId = u32`, and the modules below | |
| `crates/common/src/config.rs` | `ClusterConfig::load(path)`: nodes, `[timing]`, P2's `[weights]`, P3's `[load]`; checks the file at startup | `cfg.node(id)`, `cfg.peers(me)`, `cfg.timing.heartbeat_interval()` etc., `cfg.weights` |
| `config/nodes.toml` | 3 localhost nodes (ports 50051-50053), timing and weights | |
| `config/nodes.lab.toml` | The 3 lab machines | Replace each `REPLACE-ME` with the machine's IP; a node refuses to start until then |
| `crates/node/src/main.rs` | `node --id N [--config path] [--run-id name]`: load config, open the fault log, start heartbeats, the detector ticker and P3's sampler, serve on `0.0.0.0:<port>` | |
| `crates/node/src/state.rs` | `NodeState`, shared by everything in the node | P3: `state.load_report()`. P4: `state.store`. Outgoing calls: `state.peer_for_sending(peer)` (nothing while muted). Liveness: `state.alive_peers()` |
| `crates/node/src/peers.rs` | One lazy connection per peer, with a connect timeout | Wrap every call in `tokio::time::timeout(...)`; deadlines are ours |
| `crates/node/src/service.rs` | The `Node` service: `Heartbeat`, `Submit`, `InjectFault`; the mute gate | `handle_submit`: lookup → validate → snapshot → plan → `try_apply` (re-plans up to 3 times, then `aborted`) |
| `crates/node/src/heartbeat.rs` | One heartbeat worker per peer (500 ms), and the 100 ms detector ticker | Logs `peer suspected down` / `peer alive again` and writes `PEER_DOWN` / `PEER_UP` rows |
| `crates/node/src/broadcast.rs` | `broadcast(state, deadline, call)`: the same call to every peer at once, one result per peer (`Reply` / `Failed` / `TimedOut` / `NotSent`), at most one retry for `Unavailable` | For Step B's `Bid`, `Replicate` and broadcast `FaultCommand`; see "Using broadcast" below |
| `crates/node/src/failure_detector.rs`, `fault_log.rs` | P5's detector and fault CSV, used through `NodeState` | |
| `crates/node/src/store.rs`, `load.rs` | P4's store and P3's load collector, used through `NodeState` | |
| `docs/p1-design-log.md` | Every P1 design decision with alternatives and reasons | Material for the report |

## For each teammate
- **P2:** `[weights]` is read into `cfg.weights` (your `Weights`). `Submit` calls `incident_from_pb` (its `String` errors become `invalid_argument`), `planner::plan` with the node's Unix time in ms as `now`, and `plan_to_pb` for the reply. The planner runs in `spawn_blocking`, so a slow plan never delays heartbeats.
- **P3:** your collector is merged into `NodeState` (`state.load_report()`). `in_flight()` counts the `Submit` requests being processed right now, and the guard times each one. Time a request spends held by a mute is neither counted nor timed: the guard is created after the entry gate and dropped before the reply gate.
- **P4:** `Submit` uses `lookup_request`, `snapshot` and `try_apply`. On `ResourceTaken` it takes a new snapshot and plans again (3 attempts, then `aborted`); `UnknownResource` is `internal`. `apply_remote`, `entries_since` and `applied` wait for Step B. `FetchLog` replies over 4 MB are rejected by default, so recovery may need paging.
- **P5:** `InjectFault` works: target must be the node itself, `duration_ms` 1 to 20 000, refusals come back as `FaultAck { accepted: false, reason }`. Your fault log is wired: `MUTE_START` / `MUTE_END` from the mute hook, `PEER_DOWN` / `PEER_UP` from the ticker, into `logs/faults-node<N>.csv`, with the run name from `--run-id`.
- **P6:** `Submit` is real; the mock server is no longer needed against a node. Errors your client sees: `invalid_argument` (bad request), `aborted` (about 1 in 1000 under a burst; retry with the **same** `request_id`), `unavailable` (the node was muted). Loadgen does not retry `aborted` yet, so a run may show 1 `error`; a retry would turn it into `ok`. Start the nodes with the same `--run-id` as loadgen so the fault CSVs and your CSV join.

## Using broadcast (Step B: P3 election, P4 replication, P5 automatic injector)
```rust
let bid = Bid { request_id, node_id: state.id, load_score };
let results = broadcast(&state, deadline, move |mut client| {
    let bid = bid.clone();
    async move { client.bid(bid).await } // the RPC P1 adds to `service Node` in Step B
}).await;
for (peer, result) in results {
    match result {
        PeerResult::Reply(ack) => { /* peer answered */ }
        PeerResult::TimedOut | PeerResult::Failed(_) => { /* treat the peer as out */ }
        PeerResult::NotSent => { /* we are muted: take no part */ }
    }
}
```
- Every peer is called, even ones suspected down; the result tells you who answered.
- The whole call, retry included, takes at most `deadline`.
- A retried message can arrive twice: the receiving handler must ignore a `request_id` / `fault_id` it has already handled.

## Tests
`cargo test --workspace`: 26 in `common` (P2's planner, generators and conversions; config loader; P3's load formula), 3 in `injector` (P5), and 74 in `node`, including:
- mute behaviour on a fake clock (10 s scenarios in milliseconds) and over a real network;
- failure detection with 3 real nodes in one test: a muted node is dropped by the others and seen again afterwards;
- broadcast: a muted peer times out without holding up the other, calls run in parallel, and the retry rules on fake calls;
- Submit with the real store and planner: a plan comes back, a resend is a duplicate with no second log entry, bad inputs store nothing, and 20 concurrent requests for 10 resources never double-book;
- the mute hook writes `MUTE_START` / `MUTE_END` rows.

End to end (release build, 3 nodes, P6's loadgen, 1000 requests from 10 clients): 999 to 1000 `ok` per run, all `executor_node = 1`, about 10 000 requests per second.

## Next
- Before Wednesday: the real machines' IPs for `nodes.lab.toml`, allowing `node.exe` through each machine's firewall, and syncing the machines' clocks.
- Open question with P5: heartbeats received also count as "heard" (design log 7.3); P5's doc says replies only.
- Step B: the election (P3, PR #12) and replication use `broadcast()`.

## To handle later (found while building the demo)
**1. "Suspected down" is a suspicion, not a fact.** No timeout can tell a crashed node from a slow one or from one broken link, so a live node can be wrongly suspected (it is seen alive again at its next heartbeat).
- Step B: the election and replication must stay correct when a suspicion is wrong (a wrongly suspected node just misses a round; `request_id` / terms keep a late answer harmless). Agree this rule with P3 and P4.
- Count false alarms in the experiments: a `PEER_DOWN` with no matching `MUTE_START` or crash in the fault logs.
- If false alarms matter: before suspecting a peer, ask another node to reach it (indirect probe, as in SWIM). This catches a single broken link. Step C at the earliest.

**2. Retries.**
- Inside the node: after a lost race, `Submit` plans again immediately (up to 3 times). Two colliding requests then often pick the same next resource and collide again. Try a short random wait (0-2 ms) between attempts and measure the "gave up" count under overload before and after.
- Client side: nobody retries yet. An `aborted` (gave up) or `unavailable` (node muted) reply ends the request, and loadgen counts it as an error. P6: retry with the **same** `request_id`, after a short growing wait, a limited number of times.
- Empty plans: a request that finds no free resource is answered at once and its incident stays `Open`. Decide with P4 whether to queue it until a resource is released.

**3. Resource release (P4).** The demo branch frees each resource after `--mission-ms` with a temporary local `Store::release`. P4 replaces it with a "resource released" log entry, so the release replicates in Step B.

## Step C: client failure cases on real nodes (Oct 29)

**How it was tested.** 3 real nodes on one Windows laptop (release build), the real injector for mutes, and P6's loadgen with a temporary local change, never committed: `ClientMiddleware::connect_all` (multicast with the retry policy) instead of `connect`, plus an `attempts` column. Steady traffic, a fault part-way through, then the CSV, the nodes' logs and the election logs were checked. These are the Step C plan's tasks "multicast check" and "the three failure cases"; the lab run on Nov 1 repeats them.

| Run | What was done | Requests | Result |
|---|---|---|---|
| Multicast, silent node | Node 3 muted for 15 s, 10 requests/s | 300 | 300 committed, 0 retries. Node 3 suspected 1.07 s after the mute; in that time 10 requests took about 310 ms (the bid timeout), then about 1 ms again. The client never waited for the silent node |
| Case 1: network cut | Node 3's process stopped, restarted 15 s later | 300 | 298 committed, 2 duplicate, 0 retries, 0 failed. The other two answered every request; 11 took about 310 ms before node 3 was suspected (1.09 s) |
| Case 2: winner took the request, then died | Node 2 muted twice (8 s, 5 s); every plan made to take about 300 ms so a mute hits a winner mid-plan | 150 | 150 committed. The one request caught mid-plan got no plan, was retried at 2 s, both live nodes logged `winner reported down: re-electing`, node 3 planned it in round 1: **2.35 s** in total (2 s + pause + 0.3 s plan) |
| Case 3: slow request | Node 1's plans take about 3 s, longer than the 2 s deadline; 1 request every 5 s | 6, run twice | 5 committed, **1 failed** in each run: the request node 1 won never got a plan (see finding 1) |

Failure detection across all runs: a muted or stopped node was suspected 1.07 to 1.27 s after the fault (under 1.5 s, because its last heartbeat was already up to 0.5 s old).

### Findings

1. **Bug (P1): a slow request can never succeed.** When an attempt's deadline passes, the client drops that call, and tonic then cancels the node's Submit handler for it, planning included. Node 1 logged no `submit committed` at all for the request it won, in 6 s with 3 s plans. Each retry starts a fresh plan that is cancelled again at 2 s, so the request fails after 3 attempts (6.07 s) and the node wastes 9 s of CPU. The client tests did not catch it because the fake nodes keep working after the client gives up. **Fix:** run the Submit handler in its own task, so it finishes and commits even if the client stopped waiting; the retry then gets the saved plan (`duplicate`).
2. **The same request can be handled twice on one node (P1, with P3).** The node runs Submit for the client's call, and also when a peer's bid arrives before the client's copy (P3's rule, so a winner never misses a request). If both arrive close together, both run: two elections, two plans, and the store refuses the second save. No double assignment, but the planner runs twice and the client may see `duplicate` on a first attempt. Seen on 4 of 300 requests right after node 3 restarted, and 1 of 300 in another run. **Fix, together with 1:** a per-node table of requests in progress, so a second caller waits for the first one's result instead of planning again. The same table can feed the log of unfinished requests (P1's next Step C task).
3. **Two assignments across nodes (P5).** A muted winner keeps planning and saves into its own store; the re-elected node saves its own plan. With one store per node, the request then has two assignments. P5's shared store refuses the second save, and the mute must also cut the muted node's access to it (P5's Step C task).
4. **Rejoin (P3).** A node that comes back starts winning elections at once. The readiness rule (reload state, then bid) is P3's Step C task.
5. **Windows only.** A stopped process on localhost takes about 2 s to refuse a connection on Windows, so elections waited the 300 ms bid timeout until the node was suspected. On Linux the refusal is immediate; check in the lab.

**Next:** fix 1 and 2, rerun case 3 (and the others) locally, then repeat everything in the lab on Nov 1 once loadgen uses `connect_all` (P6).

## Step C: the failure cases again, on shared state, after the fixes (Oct 31)

**What changed since Oct 29.** The Submit fix (section 14 of `docs/p1-design-log.md`) fixes findings 1 and 2. P5's #33 puts the state in SeaweedFS for all 3 nodes and cuts a muted node off from it (finding 3). P1's new request log (`logs/requests-nodeN.csv`, see below) shows what each node did with each request.

**How it was tested.** Same as Oct 29, on one Windows laptop: 3 real nodes (release build) with `backend = "s3"` against a local SeaweedFS 4.48 (one `weed server -s3`), the real injector for mutes, and loadgen with multicast (the default since #24). The bucket was deleted before each run. After each run: the request CSV, the nodes' fault logs and request logs, and every object in the bucket were checked. Plan time per planner iteration is now about 0.32 us (P2's #27 halved it): 900 000 iterations take about 290 ms, 9 000 000 about 3 s.

| Run | What was done | Requests | Result |
|---|---|---|---|
| Multicast, silent node | Node 3 muted for 15 s, 10 requests/s | 300 | 300 committed, 0 retries. Longest wait 315 ms (the bid timeout, before node 3 was suspected 1.43 to 1.46 s after the mute) |
| Case 1: network cut | Node 3's process stopped at 8 s, started again 15 s later | 300 | 300 committed, 0 retries. Suspected 1.52 to 1.58 s after the stop. After the restart node 3 read the shared state and took 56 requests |
| Case 2: winner took the request, then died | Every plan about 300 ms; node 2 muted twice (8 s, 5 s) | 150 | 150 committed, 1 retry. The request node 2 was planning when muted: its save was refused (`error:unavailable` at 304 ms), the client retried at 2.05 s, both live nodes re-elected, node 3 saved it at **2.58 s** |
| Case 3: slow request | Node 1's plans about 3 s, over the 2 s deadline; 1 request every 5 s | 6, run twice | **6 committed in both runs** (Oct 29: 1 failed in each). Node 1 kept planning after the first attempt gave up; the retry at 2.03 s joined that work (no second START on node 1); plan at 2.9 to 3.2 s |

In every run the bucket held exactly one saved request per request_id (300, 300, 150, 6, 6), and no resource was in two plans.

**Request log, as P6 will read it.** Each node writes a START row when it starts working on a request and an END row when it stops, with how it ended (`committed`, `duplicate`, `lost` = another node won, or `error:<code>`) and how long it took. In case 2 the retried request reads: START on all 3 at 0 ms, nodes 1 and 3 `lost` at 1 ms, node 2 `error:unavailable` at 304 ms (muted mid-plan), START again on nodes 1 and 3 at 2046 ms (the retry), node 3 `committed` at 2584 ms. A switched-off node writes no END, so a START without an END is a request that was unfinished when its node died. No run had one: no process was stopped mid-request.

### Findings from Oct 29, now

1. **Slow request never succeeds:** fixed (case 3).
2. **Same request handled twice on one node:** fixed. The retry, and a peer's bid, join the work already running.
3. **Two assignments across nodes:** fixed by #33. One shared store, and a muted node's save is refused.
4. **Rejoin (P3):** in P3's #32, not merged yet.
5. **Windows only, slow connection refusal:** not seen to matter here; check on Linux in the lab.

**`duplicate` on a first attempt is not a bug.** With shared state, a node that gets the client's copy after the winner saved the plan answers from the store (19 node-side `duplicate` ENDs in case 1). The client keeps the first plan it gets, so all 300 were `committed` on the client side. P6's "committed twice" report is this: one saved plan, returned twice. The bucket checks above show no request was ever saved twice.

**Next:** the same runs on the 3 lab PCs (Nov 1, `docs/lab-setup.md`), with one SeaweedFS per PC.
