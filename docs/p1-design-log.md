# P1 Design Log: Protocol, Node, Transport, Fault Injection, Failure Detection

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
