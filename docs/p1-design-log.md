# P1 Design Log: Protocol, Node, Transport, Fault Injection, Failure Detection, Submit

Owner: P1. One entry per decision: what we chose, the alternatives, and why.
Library boundary for the whole project: tonic carries bytes over HTTP/2 and prost encodes them. Everything below (message design, timeouts, heartbeats, failure detection, the mute hook) is our own code.

---

## 1. Build and dependencies

### 1.1 Compile the proto with protox, not protoc
**What.** `crates/common/build.rs` parses `proto/rescuemesh.proto` with `protox` (a protobuf compiler written in Rust) and generates Rust with `tonic-prost-build`.
**Alternatives.** Google's `protoc` binary, installed on every machine.
**Why.** Nothing to install on 6 laptops and 3 lab machines; Cargo downloads protox like any library. Build-time only: none of it ends up in the running node.

### 1.2 Exact version pins in one place
**What.** Every library is pinned with `=x.y.z` in the root `Cargo.toml` (`[workspace.dependencies]`); crates opt in with `name.workspace = true`. Only P1 edits the root list, so teammates' branches do not conflict on it.
**Alternatives.** Normal version ranges (`"1"`), or each crate choosing its own versions.
**Why.** Every machine builds the same code. tonic, tonic-prost and tonic-prost-build are released together and must match. `Cargo.lock` already records exact versions; the `=` pin is a second safety net that `cargo update` cannot move.

---

## 2. Wire protocol (`proto/rescuemesh.proto`)

### 2.1 Conventions
| Convention | Why |
|---|---|
| One package `rescuemesh.v1`; v1 only grows by adding messages and fields | Adding is backward compatible, so nobody's code breaks when the proto grows |
| A field number never changes and is never reused | On the wire fields are identified by number, not name; renumbering makes old and new nodes misread each other |
| Node IDs start at 1; 0 means a client | proto3 does not send zero values, so 0 cannot be told apart from "not set" |
| Every enum starts with `UNSPECIFIED = 0`, rejected by the conversions | A sender that forgot a field would otherwise silently mean the first real value |
| `request_id` is a `string` (client-generated UUID) in every state-changing message | Readable in logs and CSVs; consistent across `Submit`, `LogEntry`, `Bid` |
| Units in field names (`timestamp_ms`, `duration_ms`, `ttl_secs`) | No guessing between seconds and milliseconds |
| Domain `u8` fields (zone x/y, severity) are `uint32` on the wire; ranges are checked in P2's conversions | proto has no 8-bit type |

### 2.2 Heartbeat: unary, `{from_node, seq}` → `{node_id, seq}`
**What.** One request, one reply. The reply echoes `seq` and names who answered.
**Alternatives.** A long-lived stream per peer; adding a send timestamp.
**Why.** With unary calls, a missing reply before the deadline is exactly the failure signal; a broken stream is harder to reason about and to present as our own detection. `seq` matches late replies to their request and reveals gaps. `node_id` catches a wrong address in the config. No timestamp: the caller measures round-trip time on its own clock, because the machines' clocks differ.

### 2.3 Submit: `{request_id, oneof kind { Incident }}` → `{request_id, executor_node, plan, duplicate}`
**What.** The client entry point.
**Alternatives.** A plain `Incident` field; one RPC per request type; a status field in the reply.
**Why.**
- `oneof kind` lets resource offers and requests become new kinds later, all through one path (election, idempotency, log). Turning a plain field into a `oneof` later would change the generated Rust and break teammates' code.
- `request_id` is echoed because in Step B a client sends to all 3 nodes and may get several replies.
- `executor_node` is a CSV column (P6) and shows who won the election.
- `duplicate` marks a stored plan returned for a retry, so we can measure how often duplicates happen.
- Errors are normal gRPC status codes (`invalid_argument`, `unavailable`), not a status field.

---

## 3. Configuration (`config/nodes.toml`, `common::config`)

### 3.1 One shared file for every program
**What.** `[[nodes]]` lists only the 3 cloud nodes (id, addr). A node finds itself by `--id`; the others are its peers. Clients read the same file for the addresses. `nodes.toml` is localhost; `nodes.lab.toml` will hold the real machines' IPs.
**Alternatives.** Each node listing only its peers; clients configured separately; clients listed as nodes.
**Why.** One source of truth for addresses, so nodes and clients cannot disagree. Clients are not peers (no heartbeats), so they are not listed.

### 3.2 Checked at startup; timing in the file
**What.** Loading fails with a clear message on: no nodes, id 0, duplicate ids, an address that is not `ip:port`, or `suspect_after_ms` not longer than `heartbeat_interval_ms`. `[timing]` (heartbeat 500 ms, suspect 1500 ms, rpc deadline 1000 ms, connect timeout 500 ms) and P2's `[weights]` are optional; missing values use defaults.
**Alternatives.** Constants in the code; no validation.
**Why.** A typo should stop the node at startup, not cause strange behaviour later. Timing in the file lets the stress tests try other values without rebuilding.

---

## 4. Node structure (`crates/node`)

### 4.1 Listen on `0.0.0.0:<own port>`
**Why.** `127.0.0.1` only accepts connections from the same machine. The same binary works on localhost and on the 3 lab machines; only the config changes.

### 4.2 One shared state behind `Arc`
**What.** `NodeState` holds the id, config, peers, request counter, mute state and failure detector, shared by every request handler and background task.
**Why.** tonic runs many requests at once on several threads; `Arc` lets them all share one copy safely. Data owned by a single task (for example a heartbeat worker's `seq`) stays in that task.

### 4.3 Request counter for P3: an atomic with a drop guard
**What.** `in_flight` is an `AtomicUsize`, changed only through `start_request()`, which returns a guard that decrements when dropped.
**Alternatives.** A plain counter (not thread-safe); incrementing and decrementing by hand.
**Why.** The guard decrements however the handler ends (success, early error, cancelled request), so the "queue length" P3 reads can never drift upward.

**Interaction with the mute hook.** P3's collector also uses the guard to time each request (its service-time average). In `submit`, the guard is dropped right after the work is done, before the reply gate. Otherwise a request held by a mute would record up to 20 s as service time, and the node would look fully loaded, and lose elections, right after it unmutes. Requests held at the entry gate are never counted either, because the guard is created after that gate.

### 4.4 Thin handlers: the dispatch point
**What.** Each RPC method in `service.rs` only unwraps the message, counts it and applies the mute gate, then calls a `handle_*` function where the real work lives.
**Why.** Every request passes through one place, so the mute rule cannot be bypassed, and teammates' modules (planner, store, injector) plug in without rewriting the server.

---

## 5. Peer connections (`peers.rs`)

### 5.1 One lazy connection per peer, with a connect timeout
**What.** At startup each node prepares one tonic channel per peer with `connect_lazy()` and `connect_timeout` (500 ms). Every task clones the client, which shares the connection.
**Alternatives.** Connecting eagerly at startup; a new connection per call.
**Why.** Eager connecting fails when peers are not running yet, so the start order would matter. HTTP/2 carries many calls over one connection at once. The connect timeout makes an unreachable machine fail in 500 ms instead of the OS default (about 21 s on Windows).

### 5.2 Per-call deadlines live in our code
**What.** Every outgoing call is wrapped in `tokio::time::timeout(rpc_deadline, …)` by the caller (heartbeat workers, later broadcast), not configured inside tonic.
**Why.** A muted peer keeps its connection open and never answers; only a per-call deadline notices that. Writing it in our code keeps it visible as our mechanism.

### 5.3 Library features deliberately not used
tonic load balancing (the election does that), tonic retry layers (our retry policy, task 6), HTTP/2 keepalive pings and gRPC's health-check service (our heartbeats do that). The channel reconnects by itself after a peer returns; whether a peer is alive is decided only by our heartbeats and the failure detector, never by the connection state.

---

## 6. Mute hook (fault injection, task 4)

### 6.1 What a muted node does
**What.** `InjectFault { fault_id, target_node, duration_ms }` makes a node silent for `duration_ms` (1 to 20 000 ms; anything else is refused with `FaultAck { accepted: false, reason }`). While muted:
- every incoming request is held at a gate at the start of each handler until the mute ends, then dropped with `unavailable`; requests already in progress have their replies dropped the same way;
- the node sends nothing: every outgoing call gets its client from `peer_for_sending`, which returns nothing while muted;
- new fault orders are ignored. The node wakes up by itself; the `FaultAck` is sent before the silence starts.

**Alternatives considered.**
- *Reply with an error at once.* Peers would learn of the failure instantly, which no crashed machine does, and the measured detection and failover times would be meaningless.
- *Check in a tonic interceptor (the plan's wording).* Interceptors are synchronous: they can only pass or reject a request immediately, so they cannot hang without blocking a runtime thread. The check is an async `gate()` in each handler instead.
- *An `AtomicBool` plus a timer task that switches it off (the plan's wording).* With overlapping mutes, the first timer would end the second mute early. We store the end time (`muted_until`): nothing has to switch it off, and a later order can only extend it.
- *Close the TCP connection.* Not possible from a gRPC handler, and the caller would reconnect at once. Holding the request has the same effect for callers: silence until their own deadline.

**Consequences.**
- A dropped `Submit` reply may belong to work already stored. The client retries with the same `request_id` and gets the stored plan (`duplicate = true`).
- Held requests end when the caller's deadline cancels them, so they do not pile up; a caller without a deadline gets `unavailable` at most 20 s later.

### 6.2 What a mute models: a node cut off from the network
Peers cannot tell a muted node from a crashed one: both are silent until their deadlines expire. But a muted node keeps its memory (store, log), like a machine whose network cable was pulled, so on return it must catch up on what it missed (rejoin and state recovery, Step C). A real crash additionally loses memory; that case was demonstrated by killing a node process (see 7.6).

---

## 7. Heartbeats and failure detection (task 5)

### 7.1 Timing
| Value | Setting | Why |
|---|---|---|
| Heartbeat interval | 500 ms | Fast enough for detection around 1 to 1.5 s; 2 tiny messages per second per peer cost nothing |
| Per-call deadline | 1000 ms | Longer than one interval so a slow but alive peer is not counted as silent too early |
| Suspect after | 1500 ms (3 × interval) | Tolerates up to 2 lost heartbeats in a row. Shorter means faster failover but more false alarms on a busy machine or slow Wi-Fi; longer means fewer false alarms but slower failover |
| Detector tick | 100 ms | A change is noticed at most 0.1 s late, small next to 1.5 s |

Expected detection time: 1.0 to 1.6 s after a node goes silent, depending on how long before its death its last reply arrived.

### 7.2 One worker per peer; each heartbeat in its own task
**What.** `heartbeat.rs` runs one worker per peer. Every 500 ms it sends a heartbeat in its own small task with the 1 s deadline, so the timer never waits for replies.
**Alternatives.** One loop for all peers; a worker that waits for each reply before sending the next.
**Why.** A dead peer never delays heartbeats to a healthy one. Waiting for replies would drop heartbeats to a hung peer to one per second and notice its return later. At most 2 calls per peer are pending at once (1 s ÷ 500 ms), so they cannot pile up. Because replies can arrive late or out of order, each one is checked against its own `seq` and the expected `node_id`.

### 7.3 Both directions count as "heard"
**What.** A peer is recorded as heard when it answers our heartbeat, and when its own heartbeat reaches us.
**Alternatives.** Only replies count; or stop pinging a down peer and wait for it to announce its return.
**Why.** Two independent signals. A returning node's own heartbeats are its "I'm back" message (recovery was noticed 53 ms after a restarted node started listening). We keep pinging down peers anyway: a single announcement can be lost (making it reliable means repeating it, which is a heartbeat again), one rule is simpler than a separate "down" mode, and the cost is negligible. Pinging down peers less often would be a valid optimisation; it is not needed at our scale.

### 7.4 The detector lives in the shared state, used through three functions
**What.** P5's `FailureDetector` sits in `NodeState` behind a lock and is used only through `heard_from(peer)`, `check_peers()` and `alive_peers()`. The time passed to it is tokio's clock converted to the standard type (`now_std()`).
**Why.** Nobody can pass a time from a different clock by accident, and the lock is held only for an instant, never across a network wait. Using tokio's clock keeps the detector and the mute in sync, including in fake-clock tests.

### 7.5 Unmute starts a fresh detector
**What.** The ticker skips checking while the node is muted. On the first tick after the mute ends, the node replaces its detector with a fresh one (every peer alive, full 1.5 s each), as when it first starts.
**Alternatives.** Keep ticking during the mute.
**Why.** A muted node hears nothing, so after waking it would report false PeerDowns for silence it caused itself, followed by PeerUps half a second later. A truly disconnected machine would believe its peers died; our muted node cannot act on that belief (it sends nothing and takes part in nothing), so discarding it changes no behaviour, only removes false alarms from the logs. This is a deliberate simplification.

### 7.6 Evidence
- Unit tests on a fake clock: silence gives PeerDown at 1.5 s, hearing again gives PeerUp, unmute starts fresh (the test fails if the reset is removed).
- 3 real nodes in one test, real network: node 2 muted for 800 ms is dropped by nodes 1 and 3 within 450 ms (test timings: heartbeat 50 ms, suspect 150 ms) and seen again after the mute, by both and by node 2 itself.
- Manual run, 3 real processes, real timings: node 2's process killed; nodes 1 and 3 logged `peer suspected down` independently after 1.18 s; node 2 restarted and both logged `peer alive again` 94 ms later; no false alarms before or after.

---

## 8. Broadcast, timeouts and retries (task 6)

### 8.1 What `broadcast()` does
**What.** `broadcast(state, deadline, call)` makes the same call on every peer at the same time and returns one result per peer, sorted by ID: `Reply(answer)`, `Failed(status)`, `TimedOut`, or `NotSent` (this node is muted). It is generic over the call ("given a client, make this RPC"), so Step B's `Bid`, `Replicate` and broadcast `FaultCommand` all use it without changing it.
**Why.** Fan-out, deadlines and retries are written once, in one place, instead of in every protocol that needs them. The caller decides what missing answers mean (for example, the election treats a missing bid as "that node is out").

### 8.2 Every configured peer is called, not only the ones believed alive
**Alternatives.** Call only `alive_peers()`.
**Why.** The calls run in parallel, so a dead peer costs no extra time: the broadcast waits one deadline either way. A peer that has just come back but is not marked alive yet (up to one 100 ms tick) would otherwise be left out, and an election could pick a winner without its bid.

### 8.3 Parallel calls under one deadline
**What.** Each call runs in its own task inside a `JoinSet`, wrapped in our own `tokio::time::timeout`. Results are collected as they finish. If the caller stops waiting, every pending call is cancelled. A panic in one call is logged and does not lose the other peers' results.
**Alternatives.** Calling peers one after another; waiting only for a quorum of replies.
**Why.** One silent peer must not delay the others: in the election ("send the bid, wait T, decide"), a sequential broadcast would double the wait whenever a node is down, and that wait is part of the failover time we measure. Returning early at a quorum is not needed in Step A; it can be added for Step B if the election wants it.
**Evidence.** With a 300 ms deadline and one muted peer, the other peer's reply arrives and the broadcast ends between 300 and 450 ms. With both peers muted it also ends in under 450 ms; calling them one after another would take 600 ms.

### 8.4 Retry policy: at most one retry, only for "could not reach the peer"
**What.** A call that fails fast with `Unavailable` (connection refused, or a connection broken by a peer restart) is retried once, and only with the time left before the deadline. Everything else is final:
- a timeout (the deadline is used up; a silent peer will not answer a second time either);
- any other error, such as `invalid_argument` (the peer answered "no"; asking again gives the same answer);
- a second failure (a dead peer costs at most two quick attempts).

**Alternatives.** No retries at all; retrying until the deadline (with backoff); also retrying timeouts.
**Why.** One retry covers the common blip, the first call after a peer restarts hitting the old broken connection, so a peer that is actually back is not missing from an election. Retrying more would only add load on a peer that is really down; persistent failures are the failure detector's job, not the broadcast's. Retrying a timeout would break the deadline.
**Safety.** A retry may deliver a message twice (if the first attempt did arrive). Every state-changing message carries a `request_id` or `fault_id`, so receivers must ignore a duplicate; Step B's handlers for `Bid`, `Replicate` and `FaultCommand` have to keep that rule.
**Evidence.** Fake-call tests on a fake clock: `Unavailable` once then a reply gives a reply after 2 attempts; `Unavailable` every time gives `Failed` after 2 attempts (not 3); `invalid_argument` and timeouts are tried once; a first attempt failing at 250 ms of a 300 ms deadline leaves the retry only 50 ms, and the broadcast ends at exactly 300 ms.

### 8.5 All timeouts in one place
| Timeout | Value | Set in | Where it applies |
|---|---|---|---|
| Connect timeout | 500 ms | `[timing] connect_timeout_ms` | Opening a connection to a peer (instead of the OS's ~21 s) |
| Per-call deadline | 1000 ms | `[timing] rpc_deadline_ms` | Each heartbeat; the default `broadcast` deadline |
| Heartbeat interval | 500 ms | `[timing] heartbeat_interval_ms` | One heartbeat per peer |
| Suspect after | 1500 ms | `[timing] suspect_after_ms` | Silence before PeerDown (3 missed heartbeats) |
| Detector tick | 100 ms | code (`heartbeat.rs`) | How often PeerDown/PeerUp are checked |
| Broadcast deadline | caller's choice (default 1000 ms) | argument to `broadcast` | The whole fan-out, retry included; the election may choose a shorter T |
| Longest mute | 20 000 ms | code (`service.rs`) | Course limit on an injected fault |
| Client timeout (P6) | 5 s | `client::DEFAULT_TIMEOUT` | A client request; what ends a request held by a muted node |

---

## 9. Submit handler (task 7)

### 9.1 The path of one request
**What.** `handle_submit` in `service.rs`, in the order from P4's notes:
1. check `request_id` is not empty and the request carries an incident (`invalid_argument` otherwise);
2. `lookup_request`: an ID already handled gets its stored plan back with `duplicate = true`, and nothing is planned again;
3. P2's `incident_from_pb` checks the incident (category, severity 1 to 10, location); its error text becomes `invalid_argument`;
4. `snapshot` → P2's `planner::plan` → P4's `try_apply`, which re-checks under the store's lock that every planned resource is still free;
5. reply with the plan, `executor_node` = this node, and `duplicate` from `try_apply`.

Nothing is stored when a request is rejected.

### 9.2 Re-plan up to 3 times, then `aborted`
**What.** If `try_apply` says a planned resource was taken by another request between our snapshot and our write (`ResourceTaken`), the handler takes a new snapshot and plans again. After 3 conflicts in a row it replies `aborted` and logs a warning; the client may retry with the same `request_id`.
**Alternatives.** Hold the store's lock across snapshot, plan and apply; retry forever; give up on the first conflict.
**Why.** Holding the lock across the planner would make every request wait for every other one, which defeats P3's load balancing (P4's design log, same decision). Retrying forever has no upper bound on latency under heavy contention. Giving up at once fails far too often: with 20 simultaneous requests for 10 resources and a single attempt, 9 to 12 of the 20 failed; with 3 attempts the same test always ends with exactly 10 resources assigned, none twice. In a 1000-request run from 10 clients at once, about 1 request in 1000 is aborted, only in the opening burst while the last free resources are being taken.
**`UnknownResource`** (the plan names a resource the store does not have) means the planner and the store disagree, a bug, so it is `internal`, not retried.

### 9.3 The planner runs off the async threads
**What.** `planner::plan` runs inside `tokio::task::spawn_blocking`.
**Why.** The planner is pure CPU work. On the async threads, a slow plan would delay the heartbeat tasks sharing those threads, and peers could miss heartbeats and report false PeerDowns. Off them, heartbeats stay on time however heavy the planning is.

### 9.4 `now` is this node's wall clock
**What.** The planner's `now` (incident age) and the log entry's timestamp are this node's Unix time in milliseconds.
**Caveat.** Machines' clocks differ. That is harmless here: the age only weighs priorities inside one plan, and ordering between nodes uses `(source_node, seq)`, never timestamps (P4's design log). Keep lab clocks synced anyway, because P6 and P5 compare CSV timestamps across machines.

### 9.5 Not tested: a mute that starts in the middle of a request
A request that passed the entry gate and is then muted has its reply dropped at the second gate, and its in-flight guard was already dropped before that gate (4.3), so the held time is neither counted nor timed. There is no automated test for it: the planner finishes in under a millisecond, so a test cannot reliably start a mute during it, and a test that only sometimes hits the case would be flaky. The ordering is visible in the four lines of `submit()`, and the gate itself is tested on its own (task 4 tests).

### 9.6 Evidence
- Unit tests with the real store, planner and incidents: a valid request gets a plan; the same ID twice gives the same plan once (`duplicate = true`, one log entry); bad inputs store nothing; 20 concurrent requests never double-book a resource.
- 3 real nodes, P6's client and load generator against node 1 (release build): the client gets plans and `duplicate = true` on a resend; 3 runs of 1000 requests from 10 clients gave 1000, 999 and 999 `ok`, all with `executor_node = 1`, about 10 000 requests per second; each error matched one `submit aborted` warning in node 1's log.

---

## 10. Fault log wiring (task 7)

**What.** Each node appends P5's fault CSV to `logs/faults-node<N>.csv` (git-ignored): `MUTE_START` when a fault order is accepted, `MUTE_END` when the mute ends (the muted node writes both), and `PEER_DOWN` / `PEER_UP` from the detector ticker on every other node. `--run-id` names the run in every row; pass loadgen's `--run-id` so P6 can join the files. Default: `run-<unix seconds>`, as in loadgen.
**Alternatives.** One file per run; logging from inside the failure detector.
**Why.** Appending keeps a restarted node's rows in the same file (P5's `open` writes the header only once), and the run ID column separates runs. The ticker already turns detector changes into log lines, so the CSV rows are written at the same point; the detector stays a pure data structure. The log is optional in `NodeState`, so unit tests write no files.
**Evidence.** 3 real nodes, node 2 muted for 3 s by the injector: node 2 wrote `MUTE_START` and `MUTE_END` 3.01 s apart; node 3 wrote `PEER_DOWN` 1.14 s and node 1 1.53 s after the mute started; both wrote `PEER_UP` 0.15 to 0.2 s after it ended. A unit test checks the two mute rows on a fake clock.

---

## 11. Proto v3: replication messages removed (Step B)

**What.** `LogEntry`, `ApplyPlan`, `ReplicateRequest`, `ReplicateReply`, `FetchLogRequest` and `FetchLogReply` are deleted from `proto/rescuemesh.proto` (v3). They belonged to P4's Step A design, where each node kept its own store and pushed a change log to its peers.
**Alternatives.** Keep them unused in case Step C needs them; mark them deprecated with a comment.
**Why.** Shared state moved to a DFS (team decision Q8, P3 election design), and P4's PR #18 removed the log, `applied`, `apply_remote` and the replication code. No `rpc` ever used these messages and no code refers to them, so nothing on the wire changes. Unused messages in the proto would suggest that nodes still replicate to each other, which is no longer the design. The "never reuse a field number" rule is about fields of messages still in use; it does not apply to whole messages removed before any rpc carried them. If Step C needs a message for a returning node, it gets a new one designed for the DFS, not the old log format.
**Evidence.** A search of `crates/` and `proto/` finds no use of the six messages besides one stale comment in `broadcast.rs` (fixed). The workspace builds with no warnings and all tests pass as before.

---

## 12. Load knob for one node (Step B, P2's load knob)

**What.** `node --planner-iterations N` replaces `[planner] iterations` (P2's workload knob, PR #22) for that node only. The config file stays the same on all nodes; the flag makes one node a slower machine, so load-balancing tests have something to show. The value is printed in the "node listening" log line, so every run's log says which node was loaded. At the default sizes (10 x 20 work units per iteration, about 3.4 ns per unit) one iteration is about 0.7 us, so 30000 is about 20 ms per plan.
**Alternatives.** An admin RPC (reusing the `FaultCommand` path, as the Step B plan suggested), which can change the load while the node runs; a per-node value in the config file.
**Why.** The experiments compare whole runs ("baseline" vs "node 2 loaded"), so setting the load at startup is enough, and the flag is a few lines in `main.rs` with no proto change. The real load (queue, CPU, service time) still changes live during every run; only the artificial extra work is fixed. A per-node config value would make the shared config differ between machines. If the demo needs to overload a node mid-run, the RPC can be added on top of the same `PlannerConfig` (decided: not needed for now).
**Evidence.** Unit test: the flag replaces the config value, and 0 is refused. Live runs, 3 nodes on one laptop (release build), the client multicasting 600 requests at 40/s from 3 clients:

| Run | Node 1 wins | Node 2 wins | Node 3 wins | Result |
|---|---|---|---|---|
| Baseline (no flag) | 327 (54%) | 161 (27%) | 112 (19%) | 600/600 ok |
| Node 2 with `--planner-iterations 30000` | 290 (48%) | 23 (4%) | 287 (48%) | 600/600 ok |

Node 2's plans took 19.6 ms on average when it won (vs 0.8 ms in the baseline), confirming the 20 ms estimate. Its load score rose (service time and CPU), so it lost most elections and the other two took its share. It still wins now and then: losers count each request's short election time as service time, which pulls its average back down between wins. The baseline is uneven because idle nodes often tie, and ties go to the lowest node ID.

## 13. Final timeout and retry values (Step C)

**What.** The values every Step C and Step D run uses. Section 8.5 lists the Step A timeouts; this table replaces it for the client and adds the election.

| Value | Final | Set in | What it does |
|---|---|---|---|
| Heartbeat interval | 500 ms | `[timing] heartbeat_interval_ms` | One heartbeat per peer |
| Suspect after | 1500 ms | `[timing] suspect_after_ms` | 3 missed heartbeats, then the peer is down and the election skips it |
| Detector tick | 100 ms | code (`heartbeat.rs`) | So a peer is marked down 1.5 to 1.6 s after its last heartbeat |
| Peer connect timeout | 500 ms | `[timing] connect_timeout_ms` | Opening a connection to a peer |
| Peer call deadline | 1000 ms | `[timing] rpc_deadline_ms` | Each heartbeat, each fault-injector call |
| Bid timeout T | 300 ms | code (`election.rs`, P3) | Wait for the peers' bids, and for the winner's announcement; at most one retry inside it (section 8.4) |
| Client attempt deadline | 2000 ms | `RetryPolicy::attempt_timeout` | How long one multicast waits for a plan; also the client's connect timeout |
| Client attempts | 3 | `RetryPolicy::attempts` | The first try plus 2 retries, all with the same `request_id` |
| Pause before a retry | 0 to 50 ms, random | code (`client/src/lib.rs`) | Spreads out clients that fail at the same moment |
| Longest mute | 20 000 ms | code (`service.rs`) | Course limit on an injected fault |

**Retry rule.** An attempt that brings no plan always waits out its full 2 s deadline before the retry, even when every node already answered with an error or "no plan". Then a random 0 to 50 ms pause, then the same request goes to every node again. A request that gets no plan in 3 attempts is `failed` after about 6.1 s.

**Why 2 s per attempt.** If the winner dies after taking a request, the other nodes still call it alive until they suspect it, at most 1.6 s later (1.5 s plus one tick). A retry before that would only hear "the winner is node X" again. 2 s puts the retry after the suspicion, with 0.4 s to spare for the bid timeout and scheduling, so the retry's election runs without the dead node. A longer deadline would only add to every failover.

**Why 3 attempts.** One retry covers the three failure cases (a cut network, a winner that died, a slow request). The third attempt covers a second failure during the retry, such as the retry's winner being muted too. More would only make a lost request take longer to report.

**Alternatives for the pause.**
- **A fixed 0.5 s backoff, as in the Step C plan.** It isn't needed: the full 2 s wait already plays that role, and it is tied to the failure detector, not picked by hand.
- **Exponential backoff with jitter** (0.5 s, then 1 s), the usual pattern for clients of a shared service (gRPC and AWS use it). On top of the 2 s wait it would make every failed request slower (retries at about 2.5 and 5.5 s instead of 2 and 4 s) without fixing anything. Decided: keep the 0 to 50 ms pause.

**Worked example (failover, replaces the plan's 2.6 s).** The winner is muted just after taking a request at 0 s. The other two suspect it by 1.6 s. The client's deadline ends at 2.0 s, it pauses about 25 ms and multicasts again. The two live nodes elect each other in a few ms (no bid is missing, so no 300 ms wait) and plan in about 50 ms. Plan at about 2.1 s.
**Not a failure case.** A node muted *before* a request arrives costs only the bid timeout: the other two wait 300 ms for its bid, treat it as out and decide without it. No client retry is needed.
