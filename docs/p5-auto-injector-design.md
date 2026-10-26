# P5: Automatic fault injector design (draft for team review)

Owner: P5. Step A plan item "Design doc for the automatic injector" (due Tue Oct 20). Builds on P3's election draft (`docs/p3-election-design.md`, Section 7, branch `StepA/P3-election-doc`). Implemented in Step B.

**Who uses this:** P1 (bid RPC, background task), P3 (the fault round reuses the election), P6 (fault rows in the CSV, experiment setup). Everyone reviews: comment on the PR, especially on Section 8.

---

## 1. What the spec asks

> "The three cloud servers will periodically elect one machine to ignore communication [...] The selected node should remain nonresponsive for a random interval of up to 20 seconds and then rejoin the group."

So: **periodically**, **the servers elect** (no outside tool decides), **one machine**, **random duration up to 20 s**, then it **rejoins**. In Step A this was done by hand with the `inject` CLI. Step B automates it. The CLI stays for demos and targeted tests.

## 2. Summary

Every node runs a small **fault-round loop**. When it is time for a fault, a node starts **fault round r**: the three nodes exchange bids exactly like P3's request election, but each bid's score is a **random number from a seeded generator** instead of the load. If all three bids arrive, every node computes the same winner (lowest score, ties to lowest `node_id`), and the **winner silences itself** for a random duration drawn from the same seeded generator. Nobody announces the result and no node sends an order to the victim. The others find out only when its heartbeats stop.

```
N1                     N2                     N3
 | timer fires: start round 7                  |
 |==== Bid "fault-s42-r7", score 0.61 ========>|
 |<=== Bid score 0.18 ====|                    |
 |<=== Bid score 0.44 =========================|
 | all 3 bids: winner N2 (lowest), 9.4 s       |
 |                   N2: same bids, same result, mutes itself 9.4 s
 |                        |  (silent)          |
 |~1.5 s later: PEER_DOWN 2 (detector)  PEER_DOWN 2 (detector)
 |                        |  wakes up, fresh detector, heartbeats resume
 |                PEER_UP 2                PEER_UP 2
 | gap timer starts once all 3 are alive again |
```

## 3. Message sequence

1. **Timer fires** on some node (Section 5). It starts round `r = last_round + 1`.
2. **Bid exchange:** it sends `Bid{request_id = "fault-s<seed>-r<r>", node_id, load_score = score}` to both peers through P1's `broadcast()`, with the bid timeout T from P3's design (300 ms). No `SubmitRequest` is attached.
3. **Joining:** a node that receives a fault bid for a round it has not started joins it, but **only if it is not muted and its failure detector reports both peers alive**. It then replies with its own bid (P3: the reply carries the receiver's bid). Otherwise it does not bid, which makes the round fail.
4. **Score:** `score = rng(seed, r, node_id)`, a number in [0, 1). Fixed once per round. Every node compares the scores it received, never a recomputed copy (P3, Section 4, "Equal scores").
5. **Decide:** a node decides when it holds all **three** bids (its own included) or when T expires.
   - **All three bids:** winner = lowest `(score, node_id)`.
   - **Fewer than three:** the round is **skipped** on that node. No fault.
6. **Act:** only the winner acts. It builds `FaultCommand{fault_id = "fault-s<seed>-r<r>", target_node = itself, duration_ms = d, issuer_node = itself, term = r}` and passes it to the **same internal function** the `InjectFault` RPC uses (P1's `handle_inject_fault`). That gives the same checks, the same mute, and the same `MUTE_START` / `MUTE_END` rows as a CLI fault.
7. **No announcement.** Unlike a request election, the winner does **not** send `Coordinator`. Announcing "I am going silent" would tell the others about the failure in advance.

**Cost per round:** 6 bid calls (each pair, both directions; replies ride on the same calls). Compare P3's request election: 8 (6 bids + 2 Coordinator).

## 4. Why the round is safe

| Risk                                                                       | Why it cannot happen (or is harmless)                                                                                                                                                                                                                           |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Two victims at once**                                                    | Scores are deterministic and compared as received, so every node that holds all three bids computes the same winner. A node that holds fewer than three does nothing. Two nodes can never both conclude "I won"                                                 |
| **A second fault while a node is still down**                              | A round needs all three bids. A muted node sends nothing and answers nothing, so no round can complete while it is silent. A node also refuses to join while its detector reports a peer down (step 3)                                                          |
| **The winner misses a bid and does not mute, but the others think it did** | Harmless: no fault happens this round. The survivors see no `PEER_DOWN`, and the gap timer simply starts again                                                                                                                                                  |
| **A node misses a bid, but the winner mutes**                              | Harmless: the fault happens; that node detects it through heartbeats like anyone else                                                                                                                                                                           |
| **Nodes started with different seeds**                                     | The seed is part of the election ID (`fault-s42-r7`), so bids from a different seed never match, no round ever gets three bids, and no fault happens. Each node logs an error when it receives a fault bid with another seed, so the mistake is visible at once |
| **Two nodes start the same round at the same moment**                      | Bids are keyed by the election ID, so the two starts merge into one round with the same bids                                                                                                                                                                    |
| **A node's round counter is behind (e.g. after a restart)**                | A node that receives a fault bid for a higher round adopts that round number. Bids for a round older than the receiver's last completed round are ignored (P3: "a message from an older election is ignored")                                                   |
| **Lost FaultCommand, or the sender dies mid-send**                         | Cannot happen: there is no FaultCommand on the network in a fault round. The winner silences itself                                                                                                                                                             |

## 5. Randomness, duration and timing

**One seeded generator, one fixed algorithm.** Every random value comes from `rng(seed, round, purpose)`, where `purpose` is the node ID (for the score) or a fixed label (for the duration and the gap). Same seed + same round → same values on every machine, so a run can be repeated exactly. Use a fixed algorithm, either `rand_chacha::ChaCha8Rng` or a tiny SplitMix64 written by hand, not `rand::rngs::StdRng`, whose algorithm is allowed to change between `rand` versions.

| Decision                                                                                                                         | Alternatives considered                                | Why                                                                                                                                                                                                                                                                                                                            |
| -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Duration uniform in [2 s, 20 s]** by default, from `rng(seed, r, "duration")`                                                  | Uniform in [0, 20 s]; a fixed 10 s                     | The spec says "a random interval of up to 20 seconds". Faults shorter than the detector's 1.5 s are never detected, so most of them would test nothing. A 2 s minimum makes every fault visible. The minimum is in the config, and we keep a separate short-fault experiment that counts how many go undetected                |
| **Gap uniform in [10 s, 30 s]**, drawn from `rng(seed, r, "gap")` and counted from when this node sees **all peers alive again** | Fixed interval; count from the start of the last fault | Counting from the start could begin a new round before the last victim has rejoined. Starting the gap from "all alive" leaves time for the returning node to catch up (Step C) and for the system to settle, so each fault's effect can be measured on its own. Random gaps avoid faults lining up with periodic load patterns |
| **The victim can be any node, including the one doing most of the work**                                                         | Never pick the busiest node                            | The demo requires "force the elected server to become nonresponsive". Excluding busy nodes would hide the hardest case                                                                                                                                                                                                         |
| **Scores uniform in [0, 1) per node**, so each node is equally likely to be picked                                               | Weighted by load or uptime                             | The spec does not ask for targeting. Equal chances spread faults evenly, which keeps the per-node statistics comparable. Over 30 rounds we check each node was picked about 10 times                                                                                                                                           |

Each node measures the gap on its own clock, so the nodes' timers fire at slightly different times. That is fine: the first one to fire starts the round and the others join (step 3).

## 6. Configuration

New optional table in `config/nodes.toml` (off by default, so Step A behaviour and baseline runs are unchanged):

```toml
[faults]
enabled = false        # true = automatic fault rounds (Step B experiments)
seed = 42              # must be the same on all 3 nodes
min_duration_ms = 2000
max_duration_ms = 20000
min_gap_ms = 10000
max_gap_ms = 30000
```

The node checks at startup that `0 < min_duration ≤ max_duration ≤ 20000` and `min_gap ≤ max_gap`, and refuses to start otherwise, like the existing `[timing]` checks. The manual `inject` CLI keeps working whether or not this is enabled.

## 7. The knowledge rule (how we keep the test honest)

In this design **all three nodes know who the victim is**, because they all compute the winner. A real crash gives no such warning. The rule from `p5-changes.md` therefore applies to every node, not just a coordinator:

- **The fault round lives in its own module** (`crates/node/src/fault_round.rs`). The victim's identity, the round result and the schedule are private to it. It exposes only "start" and "handle a fault bid".
- **Failover, election and replication code learn about failures only from the failure detector** (`PeerDown` / `PeerUp`). If anyone ever needs "who is the current victim?" to make failover work, that is a design bug, not a missing function.
- **We check it in the data.** For every fault, both survivors' `PEER_DOWN` rows should come about 1.5 s (suspect_after + up to one 100 ms tick) after the victim's `MUTE_START`. A survivor reacting in about 0 ms means something is using knowledge it should not have.

## 8. Open questions for the review

| #   | Question                                                                                 | Proposal                                                                                                                                                                                                  | Who    |
| --- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| F1  | Bid proto                                                                                | Reuse P3's `ExchangeBid` and `Bid` unchanged (Q1 in P3's draft): fault rounds use `request_id = "fault-s<seed>-r<round>"` and no `SubmitRequest`. No new proto fields                                     | P1, P3 |
| F2  | How does the bid handler tell a fault bid from a request bid?                            | By the `fault-` prefix of `request_id`, routed to `fault_round.rs`. Client `request_id`s are UUIDs, so they never start with `fault-`                                                                     | P1, P3 |
| F3  | Should a fault round wait for the returning node to be "ready" (Step C), not just alive? | Yes, once Step C adds the ready flag: a node joins a round only if it is ready itself, and step 3's "all peers alive" becomes "all peers alive and ready". Until then, the 10 s minimum gap gives it time | P4, P5 |
| F4  | Is a 2 s minimum duration right?                                                         | Keep it for the main experiments; run one experiment with `min_duration_ms = 100` and report how many faults were never detected                                                                          | All    |
| F5  | Should the demo use automatic rounds or the CLI?                                         | CLI for "force the elected server to become nonresponsive" (we choose the target on stage). Automatic rounds for the stress experiments                                                                   | All    |

## 9. How we will test it (Step B)

- **Unit:** same `(seed, round)` gives the same scores, duration and gap on every node; different seeds give different values; a round with only two bids does nothing; ties go to the lowest `node_id`.
- **Local, 3 nodes:** run with `enabled = true` for 10 minutes. Check in the CSVs that faults never overlap (each `MUTE_END` comes before the next `MUTE_START`), every duration is within [2 s, 20 s], and each fault has a `PEER_DOWN` on both survivors.
- **Repeatability:** two runs with the same seed and no load produce the same sequence of victims and durations.
- **Fairness:** over 30 rounds, each node is picked roughly a third of the time.
- **Lab machines:** the same checks with load running, plus detection latency per fault from the CSVs.
