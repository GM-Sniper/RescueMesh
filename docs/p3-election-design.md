# P3: Election design (draft for team review)

Owner: P3. Step A plan item "Election design doc" (draft Tue Oct 20). It turns the algorithm we chose in the Phase 0 survey (Section 9, modified Bully) into a concrete design for Step B.

**Who uses this:** P1 (bid RPC and wiring), P5 (automatic injector reuses the election), P4 (duplicate rule, Section 5), P6 (election metrics, Section 8). Everyone reviews: comment on the PR, especially on the open questions in Section 9.

**Scope (professor's feedback):** between the cloud nodes we do the election and fault injection. Duplicate filtering and failure detection on the client side belong to the client middleware (Section 2). P2P between clients is out of scope until Phase 2.

**Shared state (team decision, Q8):** shared state lives in a distributed file system (likely SeaweedFS) that all three nodes read and write. Nodes do not replicate to each other. Heartbeats between nodes stay, so the failure detector is still used below.

---

## 1. Summary

For every client request, each live node sends its `load_score` to the other two, tagged with the `request_id`. After a short wait, every node applies the same rule to the bids it has: **lowest `load_score` wins, ties go to the lowest `node_id`**. The winner tells the others it is handling the request, runs the planner and replies. A silent node simply sends no bid, so the other two elect between themselves. The fault injector runs the same election with a random score in place of the load.

This is the survey's modified Bully (Section 9.2): the load score replaces the fixed ID as the priority, and the `request_id` plays the role of Garcia-Molina's field h and Raft's term (it tells elections apart, and a message from an older election is ignored). We add a small `round` counter per request, so a retry after the winner fails can run a fresh election (Section 4).

## 2. Message sequence (normal case, 3 nodes alive)

1. **Client → all 3 nodes:** `SubmitRequest{request_id, incident}`, sent to every node at once (survey 1.2: the client multicasts). The client keeps the same `request_id` on every retry.
2. **Duplicate check:** each request has a record in the shared state (DFS) with its executor, round and a **status flag**: `in_progress` when the winner starts on it, `completed` once its plan is written. A node checks this record (P4 `lookup_request`) when the request arrives and still takes part in the election as normal:
   - `completed`: if it wins, it returns the stored plan (`duplicate = true`) instead of running the planner again.
   - `in_progress` and the executor is down: the executor failed before finishing, so the request is re-elected (Section 4, "Winner fails").
   - `in_progress` and the executor is alive: the request is still being handled; no new election.

   This way a retry still has exactly one executor, and a task left unfinished by a failed node is visible to everyone.
3. **Each node fixes its bid:** `Bid{request_id, round, node_id, load_score}`, with `load_score` from `state.load_report()` (P3 load collector). **The bid is computed once per (request, round), the first time the node sees the request**, from the client or from a peer's bid call, and stored in the node's election table. Every later message for this request uses that same value, so all nodes compare the same numbers. `round` starts at 0 (retries, Section 4).
4. **Bid exchange:** each node sends its bid to the other two through P1's `broadcast()`. **The call also carries the client's request**, so a node whose copy from the client is late or lost still has the incident and can run the planner if it wins. The reply to each call carries the receiver's own bid. Each node therefore gets every peer's bid twice (once as a call, once as a reply), so a single lost message does not hide a bid.
5. **Decide:** a node decides as soon as it has a bid from every peer the failure detector reports alive, or when the bid timeout **T** expires, whichever comes first. Winner = lowest `(load_score, node_id)` among the bids it holds, its own included.
6. **Announce:** the winner sends `Coordinator{request_id, node_id}` to the other two (the Bully announcement). Then it runs `plan()` → `store.try_apply()`, which writes the plan to the DFS (P4) → reply to the client with `executor_node` = itself.
7. **Non-winners** reply to the client with `executor_node` = the winner and no plan (open question Q2). They record "request X → winner W" for the logs and the split check (Section 5).

```
Client          N1 (0.20)          N2 (0.45)          N3 (0.31)
  |--Submit------>|                  |                  |
  |--Submit------------------------->|                  |
  |--Submit-------------------------------------------->|
  |               |<====== Bid exchange (all pairs) ====>|
  |               | all decide: N1 (lowest score)        |
  |               |--Coordinator---->|                  |
  |               |--Coordinator----------------------->|
  |               | plan, write to DFS                   |
  |<--plan (executor 1)--------------|                  |
  |<--no plan (executor 1)-----------|                  |
  |<--no plan (executor 1)------------------------------|
```

**Cost per request:** 6 bid calls + 2 Coordinator calls = **8 messages** (survey 9.3), or 6 while one node is silent (4 bid calls between the two live nodes, plus 2 Coordinator calls). Replies ride on the same gRPC calls.

**Client middleware (professor's feedback):**
- **Filters duplicate responses:** the client gets up to three replies per request (one per node), and after a split decision two of them can carry a plan. The middleware keeps the first reply with a plan for each `request_id` and drops the rest (a reply with the `plan` field present counts, even if the plan is empty; see Q2), counting the dropped plans for the split metrics (Section 8).
- **Fault tolerance:** the middleware's request timeout is the client's failure detection. If no reply with a plan arrives in time, it retries with the same `request_id` (Section 4, "Winner fails").

## 3. Bid timeout T

| Decision | Alternatives | Why |
|---|---|---|
| **T = 300 ms**, in config as `[election] bid_timeout_ms` | Reuse `rpc_deadline_ms` (1000 ms); 100 ms | Survey 3.2.6: T must be well above the real delay between the servers and far below the 20 s outage. LAN round trips are a few ms; 300 ms leaves room for Wi-Fi jitter and a busy planner. 1000 ms would add 1 s to every election during an outage. P1 already tested a 300 ms broadcast with one muted peer: it ends in 300 to 450 ms |
| **Stop waiting early** once every peer the detector reports alive has bid | Always wait the full T | In the normal case an election then takes one round trip, not 300 ms. Needs P1's `broadcast()` to return early (P1 design log 8.3 says this can be added for Step B) |
| **A missing bid means that node is out of this election** | Retry; wait longer | The broadcast already retries a fast `Unavailable` once (P1 8.4). A muted node never answers, and waiting longer cannot change that |
| **A node always has its own bid**, so every election has a winner | Require a majority | With 3 nodes and one silent, the other two must keep serving (R4). If both peers are silent, the node serves alone |

**During an outage:** for the first ~1.5 s after a node goes silent, the detector still reports it alive, so elections wait the full T (the survey's "election duration rises during each outage", 9.6). After `PeerDown`, the early stop no longer waits for it, and elections are fast again. We will measure both phases.

## 4. Edge cases

| Case | What happens |
|---|---|
| A peer's bid call arrives **before** the client's copy, or the client's copy never arrives | The call carries the request, so the node computes and stores its bid right away, replies with it, and joins the election normally. When the client's copy arrives, it is matched by `request_id` and does not create a second bid |
| **Late bid** (after the node already decided this round) | Logged and ignored. The decision for a round is never changed afterwards |
| **Muted node** | With P1's mute hook (PR #10), a muted node neither answers nor sends calls, so it sends no bids. The others elect without it |
| **Winner fails** before replying | The client middleware times out (client-side failure detection) and retries with the same `request_id`. A node that receives the retry while the request's record is still `in_progress` (step 2) and its executor is reported down by the detector starts **round + 1**: new bids, without the failed node. Same-round messages are still ignored, so a retry while the winner is alive does not start a second election. If the plan was already written to the DFS, the new winner finds it there and returns it (`duplicate = true`) |
| **Returning node** | Survey 9.2: it must bring its state up to date before it bids again. With shared state in the DFS there is nothing to fetch from a peer: the node reads the current state from the DFS when it plans, as every node does. What it must not do is win while it still cannot reach the DFS, or it would plan from a stale or missing snapshot. Until it can, it answers bid calls with `ready = false` (Q1), so the others count it as answered and do not wait the full T for it, but it cannot win that round |
| **Equal scores** | `load_score` is a `double` sent as-is, and every node compares the received values, never a recomputed copy, so equality is exact and the lowest `node_id` wins everywhere |

## 5. Split decision (two nodes saw different bids)

**How it can happen:** survey 9.6 and Garcia-Molina's Theorem 2: no algorithm can guarantee a single winner once messages can be lost or a node can pause. Example: N2's bid reaches N3 but not N1. N1 decides without N2 and picks itself, while N3 has all bids and picks N2. Both N1 and N2 believe they won.

**How we make it rare:**
- Every bid travels twice (call and reply, step 4), so one lost message is not enough.
- The bid is fixed once per (request, round), so nodes never compare different values from the same node.
- The bid call carries the request, so a node that missed the client's copy still bids instead of dropping out.
- Every node waits for all *alive* peers, not just the first reply.

**How we detect it:** a node that believes it won and receives `Coordinator` from another node for the same `request_id` and round has found a split. It logs `split_decision` (counted in the metrics).

**How we resolve it (deterministic, every node converges):** **the first plan written to the DFS wins.**
- If a claimant receives `Coordinator` from a lower `node_id` before it has started planning, it drops the request, so the planner usually runs only once.
- If both already ran the planner, both try to write a plan for the same `request_id`. **Requirement for P4 (Q4, Q9):** the write must be first-writer-wins (an atomic "create if absent", or a lock on the `request_id`), so exactly one plan is stored. The node whose write fails discards its own plan and returns the stored one (`duplicate = true`). Every node reads the same stored plan, so there is nothing to undo. This is the same rule as the Step A store (the first plan applied is kept).
- A node replies to the client only after its write, so the client never receives the losing plan. At most it gets the same stored plan twice, which the client middleware filters (Section 2). We measure how often splits happen rather than add a confirmation round to every election, which would cost extra messages and another wait on every request to fix a rare case.

## 6. Why this metric

`load_score = 0.5 × queue + 0.3 × CPU + 0.2 × service time`, each normalized to 0-1. Implemented and merged (PR #11); full reasoning in `DESIGN_LOG.md`, "P3: Load Metric".
- **Survey 9.2:** Bully only needs priorities that are unique and totally ordered, not fixed. A load score with ties broken by ID is exactly that, so the least-loaded live node wins. It also removes classic Bully's main drawback for us: a node returning from a failure no longer takes over just because its ID is high.
- **Queue length** predicts how long a new request waits. **CPU** catches work the queue misses (the CPU-heavy planner, other programs on the machine). **Service time** catches a machine that is simply slower.
- **Not included yet:** round-trip time to the client, which the survey lists as a possible factor. A node only learns a client's RTT once that client's request arrives, so the value would be a guess for new clients. The weights are in the config, and the survey says they are set and justified by experiment in Phase 1, so RTT can be added then if the data shows it matters.
- **Known weakness (survey 9.6):** scores are measured slightly before the election, so a burst of requests can make one node look less loaded than it is. Our collector reads the queue length live at bid time, which narrows this window. The Phase 1 experiments will show whether the load is spread fairly.

**How load is distributed:** there is no separate load balancer. Every request is multicast to all three nodes, and the election hands it to the node with the lowest `load_score` at that moment. A node that takes a request sees its queue length, and so its score, go up, so the next request tends to go elsewhere. A silent node sends no bid, so its share moves to the two live nodes. We check that the spread is fair with requests executed per node and each node's `load_score` over a run (Section 8).

## 7. Reuse for the automatic fault injector (P5)

Survey 9.2: the same election selects the node to silence.
- **Election ID:** a round number in place of the `request_id` (e.g. `"fault-17"`), also used as the `term` in `FaultCommand`.
- **Score:** a random number in place of the load score, from a seeded RNG keyed on `(seed, round, node_id)`, so a run can be repeated exactly and failures are spread across the three nodes.
- **Only when all three bid:** a round with a missing bid is skipped, so a second node is never silenced while another is still down.
- **Winner = the node to silence** (survey). P5's design decides who sends the `FaultCommand` and how. Rule from `p5-changes.md`: the victim's identity stays inside the injector code; failover code learns about the failure only from the failure detector.

## 8. What to measure (for P6)

One row per election, logged by every node to `logs/elections-node<N>.csv` (same style as the fault log and `node_metrics`):

```
run_id,node_id,request_id,round,bids,winner,duration_us,waited_full_t,late_bids,split_decision,bid_calls_sent,wall_ts_us
```

- `run_id` comes from `--run-id`, so rows join with loadgen's CSV.
- `bids` lists the bids received, own included, as `node:score` pairs: `1:0.20;2:0.45;3:0.31`.
- `duration_us`: request received → decision.
- `bid_calls_sent`: bid calls this node actually sent, including `broadcast()` retries.

From these: election frequency, election duration (normal vs. during an outage), message overhead (8 vs. 6, plus retries), and split-decision rate, as the brief requires. Request latency is measured by the client up to the reply **with a plan**, not the first reply (Q2).

**Success of the system (professor's feedback):** measured by the client, under which conditions how many requests were met. Success rate = requests that got a reply with a plan ÷ requests sent, reported per condition: no fault, during an outage, after a node returns, and at each load level. Alongside it: retries per request, duplicate plans dropped by the middleware, and requests executed per node (load distribution, Section 6).

## 9. Open questions for the review

| # | Question | Proposal | Who |
|---|---|---|---|
| Q1 | Proto for the bid exchange | Add `uint32 round = 4;` and `bool ready = 5;` to `Bid` (adding fields is safe; `ready = false` means a returning node that has not caught up, Section 4). `rpc ExchangeBid(BidRequest) returns (Bid)` with `BidRequest { Bid bid = 1; SubmitRequest request = 2; }`. `rpc AnnounceWinner(Coordinator) returns (CoordinatorAck)` with `Coordinator { string request_id = 1; uint32 round = 2; uint32 node_id = 3; }`. A node that answers `ready = false` counts as answered but cannot win that round. **Agreed (P1)** | P1 |
| Q2 | What do non-winners reply to the client? | `SubmitReply` with `executor_node` = winner and no plan; the client keeps the reply that has a plan and measures latency up to that reply, so fast "no plan" replies don't make latency look lower. **No plan** (the `plan` field is missing: "I'm not the executor") is different from an **empty plan** (zero assignments: "I'm the executor and nothing was free"); the client keeps the reply where `plan` is present, even if it is empty. Alternative: non-winners wait for the replicated plan and return it too. **Agreed (P1, P6)** | P1, P6 |
| Q3 | Early return in `broadcast()` | A new variant takes the set of peers to wait for (the detector-alive ones) and returns once all of them have answered or at the deadline, whichever comes first; calls still pending are cancelled. The existing `broadcast()` stays as it is. **Agreed (P1)** | P1 |
| Q4 | Same `request_id` written twice by different nodes | The first plan written to the DFS wins; the other node discards its plan and returns the stored one (Section 5). Same rule as the Step A store. Needs an atomic write (Q9) | P4 |
| Q5 | Step A sends each request to one fixed node; Step B multicasts to all 3 | Client sends to all 3 in Step B (survey 1.2) | P6 |
| Q6 | Is T = 300 ms right on the lab network? | P1 measures node-to-node round trips once we have the lab IPs; pick T well above their maximum | All |
| Q7 | Who owns the `Replicate` and `FetchLog` RPCs? | **No longer needed:** nodes do not replicate to each other; shared state is in the DFS (Q8) | P1, P4 |
| Q8 | The professor said that between cloud nodes we only do the election and fault injection. Does that rule out node-to-node heartbeats and replication? | **Decided (team):** heartbeats and the failure detector stay. The client times out and resends the request. Shared state moves to a DFS (likely SeaweedFS) instead of nodes replicating to each other | All |
| Q9 | How do nodes write to the DFS safely at the same time? | Two needs: (1) first-writer-wins for a plan per `request_id` (Section 5); (2) two nodes planning at once must not assign the same resource, e.g. a lock or a compare-and-set write on the resource state; (3) a round + 1 winner takes over an `in_progress` record left by a failed executor, so that update must check the round too (step 2). To check which of these SeaweedFS offers, and who sets up the DFS and connects the store to it | P4, All |
