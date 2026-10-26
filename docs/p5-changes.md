# P5 changes: fault injection and failure detection

Owner: P5. Covers Sat Oct 17 (FaultCommand proto), Sun Oct 18 (failure detector), Mon Oct 19 (injector CLI, fault CSV) and Tue Oct 20 (automatic injector design, finished Wed Oct 21).

Rule for this file: old decisions are never rewritten. When one changes, a new entry says so and the old one is marked "Replaced".

---

## Sat Oct 17: FaultCommand proto (merged, PR #1)

### What was created

| File                                        | What it is                                                                                              | What you need to know                                               |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `proto/rescuemesh.proto`: `FaultCommand`    | The "go silent" order sent to one node: `fault_id`, `target_node`, `duration_ms`, `issuer_node`, `term` | Sent only to the target node. Nobody else receives it               |
| `proto/rescuemesh.proto`: `FaultAck`        | The target's reply: `fault_id`, `accepted`, `reason`                                                    | Goes back to the injector only, never to other nodes                |
| `proto/rescuemesh.proto`: `rpc InjectFault` | `InjectFault(FaultCommand) returns (FaultAck)` on the `Node` service                                    | Called by the injector CLI (Step A) and by the coordinator (Step B) |

### Decisions

| Decision                                                                                                                 | Alternatives considered                  | Why                                                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The duration travels in the command; the node un-mutes itself on a local timer                                           | A separate "unmute" RPC                  | A muted node ignores all communication, so an unmute message could never reach it                                                                                                                                                             |
| No start timestamp in the command; the mute starts when the target receives it                                           | An absolute start time set by the sender | The machines' clocks differ, so a sender's timestamp means little on the target (same reasoning as "no timestamp in Heartbeat" in `p1-changes.md`)                                                                                            |
| `fault_id` is a unique ID (UUID)                                                                                         | No ID; a per-sender counter              | The spec requires every state-changing operation to have an unambiguous ID. A retried command with the same ID is recognized and not applied twice                                                                                            |
| `FaultAck` goes only to the injector                                                                                     | Announce the fault to all nodes          | Real failures send no warning. Other nodes must discover the failure through missing heartbeats, or the test is not realistic                                                                                                                 |
| `issuer_node` and `term` included now (`term` = 0 in Step A; `issuer_node` = 0 means the CLI, matching "0 = not a node") | Add them in Step B                       | Adding proto fields later is also safe, so this was a convenience choice. `issuer_node` lets logs show who caused each fault; `term` lets nodes reject orders from an outdated coordinator in Step B (prevents two nodes being muted at once) |
| The target sends `FaultAck` before going silent                                                                          | No reply; reply after the mute           | If the node muted first, the injector's own call would hang. **Confirmed (P1, PR #10):** the order passes the mute gate, the node mutes, then the ack still goes out                                                                          |

### Open questions

- **Does mute block sending as well as receiving?** Asked P1 on Sat. **Resolved Wed Oct 21: yes.** P1's mute (PR #10) holds every incoming call until the mute ends and then drops it, and `peer_for_sending()` returns nothing while muted, so a muted node sends no heartbeats, bids or replication. Original note: Since the heartbeat _reply_ is the failure signal (`p1-changes.md`), detection works even if only incoming calls are blocked. But a muted node must also stop sending bids and replication messages, or it still takes part in the system while "failed".
- **Step B rule** (superseded by `p5-auto-injector-design.md`, Section 7, where all three nodes compute the victim): the coordinator chooses the victim, so it knows who will fail. That knowledge stays inside the fault-injection code. Failover and election code must use only failure-detector events, so the coordinator detects the failure the same way the other node does. We will check this in the data: both survivors should report `PeerDown` about 1.5 s after the mute.

---

## Sun Oct 18: failure detector (merged, PR #4)

### What was created

| File                                  | What it is                                                 | What you need to know                                                              |
| ------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `crates/node/src/failure_detector.rs` | Decides which peers are alive and which are suspected down | Pure logic: no network, no clock. The caller passes the time in. 10 unit tests     |
| `crates/node/Cargo.toml`              | Adds `common = { path = "../common" }`                     | So the detector uses the shared `common::NodeId`                                   |
| `crates/node/src/main.rs`             | Adds `mod failure_detector;` with `#[allow(dead_code)]`    | Not called yet; wired to the heartbeat loop on Mon Oct 19. Remove the `allow` then |

### How to use it

| Function                                      | What it does                                                                   |
| --------------------------------------------- | ------------------------------------------------------------------------------ |
| `FailureDetector::new(peers, timeout, start)` | Creates the detector for the given peers (not including this node)             |
| `record(peer, now)`                           | "I heard from `peer` at `now`." Returns false for an unknown peer              |
| `tick(now)`                                   | Re-checks every peer; returns `PeerDown` / `PeerUp` only when a status changes |
| `is_alive(peer)` / `alive_peers()`            | What the last `tick` decided                                                   |

Run the tests with `cargo test -p node`.

### Decisions

| Decision                                                                                                                      | Alternatives considered                    | Why                                                                                                                                                                                          |
| ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Suspect a peer after **1.5 s** of silence (3 missed heartbeats at 500 ms)                                                     | 0.5 s (1 missed); 5 s (10 missed)          | 0.5 s raises false alarms on a single late heartbeat. 5 s makes clients wait 5 s on every real failure. 1.5 s tolerates one or two late heartbeats and is small next to faults of up to 20 s |
| `tick` runs about every 100 ms (set when wiring on Monday)                                                                    | Check on every heartbeat only              | Worst-case detection is then about 1.6 s after the last heartbeat                                                                                                                            |
| Time is passed in (`now`), never read inside                                                                                  | Call `Instant::now()` inside               | Tests can use fake time instead of sleeping, so they are instant and repeatable                                                                                                              |
| Use `Instant` (monotonic clock)                                                                                               | `SystemTime` (wall clock)                  | Wall clocks can jump (e.g. NTP corrections); a jump would cause false suspicions                                                                                                             |
| Every peer starts as Alive                                                                                                    | Start as Suspected until first heard       | Starting suspected would make every node look down at startup. A peer that never appears is still reported down after 1.5 s                                                                  |
| Status only changes inside `tick`                                                                                             | `record` marks a peer alive immediately    | One place makes all decisions, so `is_alive` and the event stream never disagree. Cost: a returning peer is reported up to 100 ms later                                                      |
| Events only on a status change                                                                                                | Report every peer's status on every tick   | Callers react once per failure, not 10 times a second                                                                                                                                        |
| A late, older message cannot move `last_heard` backwards                                                                      | Always overwrite with the newest call      | Messages can be processed out of order; going backwards would cause a false suspicion                                                                                                        |
| **Replaced Wed Oct 21 (see below).** Call `record` only when a peer **answers our heartbeat** (applies when wiring on Monday) | Also when a peer's heartbeat arrives at us | Matches P1's design (the reply is the failure signal). A muted node then cannot look alive just by continuing to send                                                                        |

### Tests (all passing)

Everyone starts alive · silent 1.5 s gives exactly one `PeerDown` · heard at 1.49 s stays alive · heard again gives exactly one `PeerUp` · one silent peer does not affect the other · both peers down and up together · late old message does not move time back · unknown peer ignored · status changes only on `tick` · full 10 s mute scenario (suspected, then back).

### Known limits and what we will measure

- **"Alive" only means "sending heartbeats", not "caught up".** A node back from a mute has stale state. Readiness is a separate question (Step C).
- **False alarms under load.** The planner is CPU-heavy on purpose. If it runs on the same async runtime as the heartbeats, it can delay them and healthy nodes get suspected. We will run load with zero injected faults and count false suspicions.
- **Faults shorter than about 1.5 s are not detected.** Expected; will show up in the measurements.
- **Wi-Fi jitter on the real machines (Wed Oct 21).** If false alarms are frequent, the timeout will be changed in a new entry here, not by editing this one.
- **Report terminology:** a timeout-based detector aiming for an eventually perfect detector (◇P, Chandra & Toueg 1996). It is complete (a silent node is always suspected eventually) but not guaranteed accurate (a slow node can be wrongly suspected).

---

## Mon Oct 19: injector CLI and fault CSV (merged, PR #9)

### What was created

| File                                  | What it is                                                                | What you need to know                                                                                            |
| ------------------------------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `crates/injector/` (program `inject`) | Manual fault injector: `cargo run -p injector -- --node 2 --duration 10s` | Reads the node's address from `config/nodes.toml`, sends one `FaultCommand`, prints the `FaultAck`. 3 unit tests |
| `crates/node/src/fault_log.rs`        | Writes fault events to a CSV, one file per node                           | Not called yet: P1's mute hook (task 4) and the detector ticker (task 5) call it. 4 unit tests                   |
| `Cargo.toml` (root)                   | Adds `crates/injector` to `members`                                       | One line                                                                                                         |
| `crates/node/src/main.rs`             | Adds `mod fault_log;` with `#[allow(dead_code)]`                          | Remove the `allow` once it is called                                                                             |
| `.gitignore`                          | Adds `/logs`                                                              | CSV output is per run, not source code                                                                           |

### What the CLI prints

| Situation                              | Output                                                         | Exit code |
| -------------------------------------- | -------------------------------------------------------------- | --------- |
| Node accepted                          | `node 2 accepted fault <id>: silent for 10000 ms`              | 0         |
| Node refused                           | `node 2 refused fault <id>: <reason>`                          | 1         |
| No reply within `--wait` (default 2 s) | `node 2 did not answer within 2000 ms (is it already silent?)` | 1         |
| Node not running                       | `could not connect to node 2 at 127.0.0.1:50052`               | 1         |
| Mute hook not built yet                | `node 2 does not handle InjectFault yet (P1 task 4)`           | 1         |
| Duration 0, over 20 s, or unknown node | Refused before sending                                         | 1         |
| Duration under `suspect_after_ms`      | Sent, with a note that the others will probably not notice     | 0         |

### CSV format (`run_id,node_id,event,fault_id,peer_id,duration_ms,wall_ts_us`)

| Event                    | Written by                                  | fault_id | peer_id |
| ------------------------ | ------------------------------------------- | -------- | ------- |
| `MUTE_START`, `MUTE_END` | The muted node                              | Set      | Empty   |
| `PEER_DOWN`, `PEER_UP`   | Each other node (from the failure detector) | Empty    | Set     |

Detection latency for a fault = `PEER_DOWN` time on a survivor minus `MUTE_START` time on the victim.

### Decisions

| Decision                                                          | Alternatives considered                 | Why                                                                                                                                                                 |
| ----------------------------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The **node** writes the CSV, not the CLI                          | CLI logs "sent" and "acked"             | Only the node knows when the silence really started and ended. In Step B there is no CLI (the coordinator injects), so CLI-side logging would disappear             |
| `PEER_DOWN` / `PEER_UP` rows carry no `fault_id`                  | Observers log which fault they detected | Observers must not know about the fault, only notice the silence. Rows are joined by time afterwards                                                                |
| One CSV file per node                                             | One shared file                         | The 3 nodes run on different machines; there is no shared disk                                                                                                      |
| Wall clock (`SystemTime`) for `wall_ts_us`                        | Monotonic clock (`Instant`)             | Rows from different machines are compared, and a monotonic clock means nothing across machines. Requires clock sync (chrony/NTP); the offset is a stated limitation |
| CLI waits at most 2 s for the `FaultAck`                          | Wait forever                            | A node that is already silent never answers; without a limit the CLI hangs                                                                                          |
| CLI refuses 0 and over 20 s before sending; the node checks again | Check only on the node                  | Fails fast with a clear message. The node must still check, because in Step B the coordinator sends orders, not the CLI                                             |
| Durations under 1.5 s are allowed, with a printed note            | Refuse them                             | Short faults are a real test case: the measurements should show how many go undetected                                                                              |
| `fault_id` built from `rand` as a version-4 UUID                  | Add the `uuid` crate now                | `rand` is already in the workspace; `uuid` arrives with P6's branch. Switch then, so the project uses one generator                                                 |
| Logging errors are reported as warnings and ignored               | Stop the node                           | A full disk must not crash a node in the middle of a fault test                                                                                                     |

### Not done yet (blocked)

- **The mute itself** (P1 task 4): `handle_inject_fault` still returns "unimplemented". Tested the CLI against P1's real node (gets the "not yet" message) and against a temporary accepting stub (prints "accepted").
- **Detector wiring** (P1 task 5, heartbeat loop): not in `main` yet. Until then the detector stays tested with fake times, as the plan's fallback says.

**Update Wed Oct 21:** both done by P1 in PR #10, including the calls into the fault log. See the Wed section.

---

## Tue Oct 20 (finished Wed Oct 21): automatic injector design

Full design: [`p5-auto-injector-design.md`](p5-auto-injector-design.md). Draft for team review; implemented in Step B.

**In one paragraph:** every node runs a fault-round loop. When its random gap timer fires, a node starts fault round r and the three nodes exchange bids with P3's election, each score drawn from a seeded generator. Only if all three bids arrive does every node compute the winner (lowest score, ties to lowest ID), and the winner silences itself for a seeded random 2 to 20 s. No announcement and no order goes over the network; the others notice through missing heartbeats.

### Decisions

| Decision                                                                                     | Alternatives considered                                              | Why                                                                                                                                            |
| -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| The winner silences itself                                                                   | The coordinator (or a survivor) sends a `FaultCommand` to the victim | All nodes compute the same winner, so no order is needed. Removes two failure cases: a lost order and a sender that dies mid-send              |
| A round needs bids from all three nodes, otherwise no fault                                  | Run with a majority (2 of 3)                                         | A muted node cannot bid, so no new fault can start while one is still running. With 3 nodes, two simultaneous failures would leave no majority |
| No `Coordinator` announcement in fault rounds                                                | Reuse P3's full request election, including the announcement         | Announcing "I am going silent" would warn the others, which a real failure never does                                                          |
| Seed in the election ID (`fault-s42-r7`)                                                     | Seed only in the config                                              | Nodes with different seeds then never complete a round, instead of picking different victims                                                   |
| Duration uniform in [2 s, 20 s]; gap uniform in [10 s, 30 s] after all peers are alive again | [0, 20 s]; fixed gap counted from the fault's start                  | Faults under 1.5 s are never detected; a gap counted from the start could overlap the returning node's catch-up                                |
| Fixed RNG algorithm (ChaCha8 or SplitMix64)                                                  | `rand::rngs::StdRng`                                                 | `StdRng` may change algorithm between `rand` versions; runs must repeat exactly                                                                |
| Off by default (`[faults] enabled = false`)                                                  | Always on                                                            | Baseline experiments need runs without faults; Step A behaviour stays unchanged                                                                |

---

## Wed Oct 21: review of P1's wiring (PR #10) and one changed decision

### What P1 built on top of P5's code

| Where          | What                                                                                                                     | Effect on P5's parts                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| `state.rs`     | The detector lives inside `NodeState`; `heard_from`, `check_peers`, `alive_peers`                                        | P5's `failure_detector.rs` is used unchanged                                        |
| `heartbeat.rs` | One sender per peer, each heartbeat in its own task; ticker every 100 ms writes `PEER_DOWN` / `PEER_UP` to the fault log | The tick period matches the 100 ms assumed in the timeout decision (Sun)            |
| `heartbeat.rs` | While muted, the ticker pauses; on unmute it starts a **fresh detector**                                                 | The returning node does not log false `PEER_DOWN` rows for silence it caused itself |
| `service.rs`   | `InjectFault` handler: checks target, 1 ms to 20 s, mutes, writes `MUTE_START`, schedules `MUTE_END`, acks               | The CLI's "accepted / refused" paths now work for real                              |
| `main.rs`      | `--run-id` flag written in every fault-log row; file `logs/faults-node<N>.csv`                                           | Fault rows can be joined with P6's latency CSV by run                               |

### Changed decision: incoming heartbeats also count as "heard"

|                                        |                                                                                                                                                                                                    |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Replaces**                           | Sun Oct 18: "Call `record` only when a peer answers our heartbeat"                                                                                                                                 |
| **New decision**                       | `record` is called both when a peer answers our heartbeat **and** when a peer's heartbeat arrives (P1, `service.rs`; P1 design log 7.3)                                                            |
| **Why the old reason no longer holds** | The old rule protected against a muted node that still _sends_. P1's mute blocks sending too (`peer_for_sending()` returns nothing while muted), so a muted node cannot keep itself alive this way |
| **What the new rule adds**             | A returning node is seen again as soon as its first heartbeat arrives, instead of waiting for our next heartbeat's reply: up to one heartbeat interval (500 ms) faster                             |
| **Cost**                               | Depends on mute blocking both directions. If that ever changes, this decision must be revisited                                                                                                    |

---

## Next

- **Wed Oct 21:** fault tests on the 3 real machines (detection latency, false suspicions with no faults, firewall).
- **Step B:** implement `fault_round.rs` from the design, after P3's election code lands; switch `fault_id` to the `uuid` crate (P6 added it to the workspace).
