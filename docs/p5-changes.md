# P5 changes: fault injection and failure detection

Owner: P5. Covers Sat Oct 17 (FaultCommand proto) and Sun Oct 18 (failure detector).

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
| The target sends `FaultAck` before going silent                                                                          | No reply; reply after the mute           | If the node muted first, the injector's own call would hang. **Requested from P1, to confirm in the skeleton**                                                                                                                                |

### Open questions

- **Does mute block sending as well as receiving?** Asked P1 on Sat. Since the heartbeat _reply_ is the failure signal (`p1-changes.md`), detection works even if only incoming calls are blocked. But a muted node must also stop sending bids and replication messages, or it still takes part in the system while "failed".
- **Step B rule:** the coordinator chooses the victim, so it knows who will fail. That knowledge stays inside the fault-injection code. Failover and election code must use only failure-detector events, so the coordinator detects the failure the same way the other node does. We will check this in the data: both survivors should report `PeerDown` about 1.5 s after the mute.

---

## Sun Oct 18: failure detector (branch `StepA/P5/failure-detector`)

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

| Decision                                                                                 | Alternatives considered                    | Why                                                                                                                                                                                          |
| ---------------------------------------------------------------------------------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Suspect a peer after **1.5 s** of silence (3 missed heartbeats at 500 ms)                | 0.5 s (1 missed); 5 s (10 missed)          | 0.5 s raises false alarms on a single late heartbeat. 5 s makes clients wait 5 s on every real failure. 1.5 s tolerates one or two late heartbeats and is small next to faults of up to 20 s |
| `tick` runs about every 100 ms (set when wiring on Monday)                               | Check on every heartbeat only              | Worst-case detection is then about 1.6 s after the last heartbeat                                                                                                                            |
| Time is passed in (`now`), never read inside                                             | Call `Instant::now()` inside               | Tests can use fake time instead of sleeping, so they are instant and repeatable                                                                                                              |
| Use `Instant` (monotonic clock)                                                          | `SystemTime` (wall clock)                  | Wall clocks can jump (e.g. NTP corrections); a jump would cause false suspicions                                                                                                             |
| Every peer starts as Alive                                                               | Start as Suspected until first heard       | Starting suspected would make every node look down at startup. A peer that never appears is still reported down after 1.5 s                                                                  |
| Status only changes inside `tick`                                                        | `record` marks a peer alive immediately    | One place makes all decisions, so `is_alive` and the event stream never disagree. Cost: a returning peer is reported up to 100 ms later                                                      |
| Events only on a status change                                                           | Report every peer's status on every tick   | Callers react once per failure, not 10 times a second                                                                                                                                        |
| A late, older message cannot move `last_heard` backwards                                 | Always overwrite with the newest call      | Messages can be processed out of order; going backwards would cause a false suspicion                                                                                                        |
| Call `record` only when a peer **answers our heartbeat** (applies when wiring on Monday) | Also when a peer's heartbeat arrives at us | Matches P1's design (the reply is the failure signal). A muted node then cannot look alive just by continuing to send                                                                        |

### Tests (all passing)

Everyone starts alive · silent 1.5 s gives exactly one `PeerDown` · heard at 1.49 s stays alive · heard again gives exactly one `PeerUp` · one silent peer does not affect the other · both peers down and up together · late old message does not move time back · unknown peer ignored · status changes only on `tick` · full 10 s mute scenario (suspected, then back).

### Known limits and what we will measure

- **"Alive" only means "sending heartbeats", not "caught up".** A node back from a mute has stale state. Readiness is a separate question (Step C).
- **False alarms under load.** The planner is CPU-heavy on purpose. If it runs on the same async runtime as the heartbeats, it can delay them and healthy nodes get suspected. We will run load with zero injected faults and count false suspicions.
- **Faults shorter than about 1.5 s are not detected.** Expected; will show up in the measurements.
- **Wi-Fi jitter on the real machines (Wed Oct 21).** If false alarms are frequent, the timeout will be changed in a new entry here, not by editing this one.
- **Report terminology:** a timeout-based detector aiming for an eventually perfect detector (◇P, Chandra & Toueg 1996). It is complete (a silent node is always suspected eventually) but not guaranteed accurate (a slow node can be wrongly suspected).

---

## Next

- **Mon Oct 19:** wire the detector to P1's heartbeat loop; injector CLI (`inject --node 2 --duration 10s`); fault start/end timestamps to CSV.
- **Tue Oct 20:** automatic injector design doc, after P3's election draft (12:00).
- **Wed Oct 21:** fault tests on the 3 real machines.
