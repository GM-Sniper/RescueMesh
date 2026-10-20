# P1 progress: protocol, node, transport, mute hook, heartbeats, broadcast

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
| 7. Handler wiring: `Submit` → snapshot → plan → apply → reply | Tue Oct 20 | Not started; needs P4's store (a stub is used until it lands) |

Done-check from the plan: 3 localhost nodes log each other's heartbeats ✅, the mute hook silences one on command ✅, a request returns a plan end to end (task 7).

## Run it
From the repo root, one terminal per node:
```
cargo run -p node -- --id 1
cargo run -p node -- --id 2
cargo run -p node -- --id 3
```
- Each prints `node listening id=N listen=0.0.0.0:<port> peers=[...]`.
- Stop one (Ctrl+C): the other two log `WARN peer suspected down peer=N` after about 1.5 s. Start it again: `INFO peer alive again peer=N`.
- `RUST_LOG=debug` shows every heartbeat (PowerShell: `$env:RUST_LOG="debug"` first).
- Another config file: `--config config/nodes.lab.toml` (for the real machines, once their IPs are known).
- Tests: `cargo test --workspace`.

## Files
| File | What it is | What you need to know |
|---|---|---|
| `Cargo.toml` (root) | Workspace; every library pinned with `=x.y.z` | Only P1 edits this list. Ask P1 to add a library, then opt in with `name.workspace = true` |
| `Cargo.lock` | Exact version of every library | Committed. Never edit by hand |
| `proto/rescuemesh.proto` | The wire protocol (package `rescuemesh.v1`) | Changes go through P1. Never change or reuse a field number |
| `crates/common/build.rs` | Turns the proto into Rust on every build (protox + tonic-prost-build) | No protoc install needed. A proto mistake shows up as a build error here |
| `crates/common/src/lib.rs` | `common::pb` (generated code), `NodeId = u32`, and the modules below | |
| `crates/common/src/config.rs` | `ClusterConfig::load(path)`: nodes, `[timing]`, P2's `[weights]`; checks the file at startup | `cfg.node(id)`, `cfg.peers(me)`, `cfg.timing.heartbeat_interval()` etc., `cfg.weights` |
| `config/nodes.toml` | 3 localhost nodes (ports 50051-50053), timing and weights | Copy to `nodes.lab.toml` with the real IPs for Wednesday |
| `crates/node/src/main.rs` | `node --id N [--config path]`: load config, start heartbeats and the detector ticker, serve on `0.0.0.0:<port>` | |
| `crates/node/src/state.rs` | `NodeState`, shared by everything in the node | P3: `state.in_flight()`. Outgoing calls: `state.peer_for_sending(peer)` (nothing while muted). Liveness: `state.alive_peers()` |
| `crates/node/src/peers.rs` | One lazy connection per peer, with a connect timeout | Wrap every call in `tokio::time::timeout(...)`; deadlines are ours |
| `crates/node/src/service.rs` | The `Node` service: `Heartbeat`, `Submit` (stub until task 7), `InjectFault`; the mute gate | P4/P2 plug into `handle_submit` in task 7 |
| `crates/node/src/heartbeat.rs` | One heartbeat worker per peer (500 ms), and the 100 ms detector ticker | Logs `peer suspected down` / `peer alive again` with timestamps |
| `crates/node/src/broadcast.rs` | `broadcast(state, deadline, call)`: the same call to every peer at once, one result per peer (`Reply` / `Failed` / `TimedOut` / `NotSent`), at most one retry for `Unavailable` | For Step B's `Bid`, `Replicate` and broadcast `FaultCommand`; see "Using broadcast" below |
| `crates/node/src/failure_detector.rs` | P5's detector (unchanged), used through `NodeState` | |
| `docs/p1-design-log.md` | Every P1 design decision with alternatives and reasons | Material for the report |

## For each teammate
- **P2:** `[weights]` is read into `cfg.weights` (your `Weights`). Your conversions are used in task 7: their `String` errors become `Status::invalid_argument`.
- **P3:** your collector is merged into `NodeState` (`state.load_report()`). `in_flight()` counts the `Submit` requests being processed right now, and the guard times each one. Time a request spends held by a mute is neither counted nor timed: the guard is created after the entry gate and dropped before the reply gate.
- **P4:** task 7 calls `snapshot()`, `apply()` and `lookup_request()` from your store; until it lands, a stub with the same API is used. `FetchLog` replies over 4 MB are rejected by default, so recovery may need paging.
- **P5:** `InjectFault` works: target must be the node itself, `duration_ms` 1 to 20 000, refusals come back as `FaultAck { accepted: false, reason }`. The node logs `muted` and `mute ended` with timestamps for your CSV. Your detector is wired to real heartbeats.
- **P6:** `Submit` answers `unimplemented` until task 7; keep using your mock server until then. Muted nodes never answer quickly, so the client's own timeout is what ends those requests.

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
`cargo test --workspace`: 26 in `common` (P2's planner, generators and conversions; config loader; P3's load formula), 3 in `injector` (P5), and 51 in `node`, including:
- mute behaviour on a fake clock (10 s scenarios in milliseconds) and over a real network;
- failure detection with 3 real nodes in one test: a muted node is dropped by the others and seen again afterwards;
- broadcast: a muted peer times out without holding up the other, calls run in parallel, and the retry rules on fake calls.

## Next
- Task 7: wire `Submit` (with a stub store if P4's is not ready).
- Wire P5's fault log (`mute_start` / `mute_end`, `peer_down` / `peer_up`).
- Before Wednesday: the real machines' IPs for `nodes.lab.toml`, and allowing `node.exe` through each machine's firewall.
