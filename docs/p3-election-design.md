# P3: Election design (draft for team review)

Owner: P3. Step A plan item "Election design doc" (draft Tue Oct 20). It turns the algorithm we chose in the Phase 0 survey (Section 9, modified Bully) into a concrete design for Step B.

**Who uses this:** P1 (bid RPC and wiring), P5 (automatic injector reuses the election), P4 (duplicate rule, Section 5), P6 (election metrics, Section 8). Everyone reviews: comment on the PR, especially on the open questions in Section 9.

---

## 1. Summary

For every client request, each live node sends its `load_score` to the other two, tagged with the `request_id`. After a short wait, every node applies the same rule to the bids it has: **lowest `load_score` wins, ties go to the lowest `node_id`**. The winner tells the others it is handling the request, runs the planner and replies. A silent node simply sends no bid, so the other two elect between themselves. The fault injector runs the same election with a random score in place of the load.

This is the survey's modified Bully (Section 9.2): the load score replaces the fixed ID as the priority, and the `request_id` plays the role of Garcia-Molina's field h and Raft's term (it tells elections apart, and a message from an older election is ignored). We add a small `round` counter per request, so a retry after the winner fails can run a fresh election (Section 4).

## 2. Message sequence (normal case, 3 nodes alive)

1. **Client → all 3 nodes:** `SubmitRequest{request_id, incident}`, sent to every node at once (survey 1.2: the client multicasts). The client keeps the same `request_id` on every retry.
2. **Duplicate check:** a node whose store already has this `request_id` (P4 `lookup_request`) still takes part in the election as normal. If it wins, it returns the stored plan (`duplicate = true`) instead of running the planner again. This way a retry that reaches nodes with different stores still has exactly one executor.
3. **Each node fixes its bid:** `Bid{request_id, round, node_id, load_score}`, with `load_score` from `state.load_report()` (P3 load collector). **The bid is computed once per (request, round), the first time the node sees the request**, from the client or from a peer's bid call, and stored in the node's election table. Every later message for this request uses that same value, so all nodes compare the same numbers. `round` starts at 0 (retries, Section 4).
4. **Bid exchange:** each node sends its bid to the other two through P1's `broadcast()`. **The call also carries the client's request**, so a node whose copy from the client is late or lost still has the incident and can run the planner if it wins. The reply to each call carries the receiver's own bid. Each node therefore gets every peer's bid twice (once as a call, once as a reply), so a single lost message does not hide a bid.
5. **Decide:** a node decides as soon as it has a bid from every peer the failure detector reports alive, or when the bid timeout **T** expires, whichever comes first. Winner = lowest `(load_score, node_id)` among the bids it holds, its own included.
6. **Announce:** the winner sends `Coordinator{request_id, node_id}` to the other two (the Bully announcement). Then it runs `plan()` → `store.apply()` → replication (P4) → reply to the client with `executor_node` = itself.
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
  |               | plan, apply, replicate               |
  |<--plan (executor 1)--------------|                  |
  |<--no plan (executor 1)-----------|                  |
  |<--no plan (executor 1)------------------------------|
```

**Cost per request:** 6 bid calls + 2 Coordinator calls = **8 messages** (survey 9.3), or 6 while one node is silent (4 bid calls between the two live nodes, plus 2 Coordinator calls). Replies ride on the same gRPC calls.

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
| **Muted node** | With P1's mute hook (PR #10, not merged yet), a muted node neither answers nor sends calls, so it sends no bids. The others elect without it |
| **Winner fails** before replying | The client times out and retries with the same `request_id`. A node that receives the retry while its recorded winner for that request is reported down by the detector starts **round + 1**: new bids, without the failed node. Same-round messages are still ignored, so a retry while the winner is alive does not start a second election. If the plan was already stored and replicated, the new winner returns it (`duplicate = true`) |
| **Returning node** | Survey 9.2: it must bring its state up to date before it bids again. In Step B (no recovery yet) it bids at once; Step C adds a "ready" flag that it must have before bidding |
| **Equal scores** | `load_score` is a `double` sent as-is, and every node compares the received values, never a recomputed copy, so equality is exact and the lowest `node_id` wins everywhere |

## 5. Split decision (two nodes saw different bids)

**How it can happen:** survey 9.6 and Garcia-Molina's Theorem 2: no algorithm can guarantee a single winner once messages can be lost or a node can pause. Example: N2's bid reaches N3 but not N1. N1 decides without N2 and picks itself, while N3 has all bids and picks N2. Both N1 and N2 believe they won.

**How we make it rare:**
- Every bid travels twice (call and reply, step 4), so one lost message is not enough.
- The bid is fixed once per (request, round), so nodes never compare different values from the same node.
- The bid call carries the request, so a node that missed the client's copy still bids instead of dropping out.
- Every node waits for all *alive* peers, not just the first reply.

**How we detect it:** a node that believes it won and receives `Coordinator` from another node for the same `request_id` and round has found a split. It logs `split_decision` (counted in the metrics).

**How we resolve it (deterministic, every node converges):** **the claim from the lower `node_id` wins.**
- If the higher-ID claimant has not started planning, it drops the request.
- If both already ran the planner, each node keeps the plan from the lower origin `node_id` when the second plan for the same `request_id` arrives through replication. **Requirement for P4 (Q4):** the store must apply this rule, so all three nodes keep the same plan.
- The client may have received the losing plan first. This is the duplicate-execution case the survey accepts, as long as idempotent request IDs keep it harmless. We measure how often it happens rather than add a confirmation round to every election, which would cost extra messages and another wait on every request to fix a rare case.

## 6. Why this metric

`load_score = 0.5 × queue + 0.3 × CPU + 0.2 × service time`, each normalized to 0-1. Implemented and merged (PR #11); full reasoning in `DESIGN_LOG.md`, "P3: Load Metric".
- **Survey 9.2:** Bully only needs priorities that are unique and totally ordered, not fixed. A load score with ties broken by ID is exactly that, so the least-loaded live node wins. It also removes classic Bully's main drawback for us: a node returning from a failure no longer takes over just because its ID is high.
- **Queue length** predicts how long a new request waits. **CPU** catches work the queue misses (the CPU-heavy planner, other programs on the machine). **Service time** catches a machine that is simply slower.
- **Not included yet:** round-trip time to the client, which the survey lists as a possible factor. A node only learns a client's RTT once that client's request arrives, so the value would be a guess for new clients. The weights are in the config, and the survey says they are set and justified by experiment in Phase 1, so RTT can be added then if the data shows it matters.
- **Known weakness (survey 9.6):** scores are measured slightly before the election, so a burst of requests can make one node look less loaded than it is. Our collector reads the queue length live at bid time, which narrows this window. The Phase 1 experiments will show whether the load is spread fairly.

## 7. Reuse for the automatic fault injector (P5)

Survey 9.2: the same election selects the node to silence.
- **Election ID:** a round number in place of the `request_id` (e.g. `"fault-17"`), also used as the `term` in `FaultCommand`.
- **Score:** a random number in place of the load score, from a seeded RNG keyed on `(seed, round, node_id)`, so a run can be repeated exactly and failures are spread across the three nodes.
- **Only when all three bid:** a round with a missing bid is skipped, so a second node is never silenced while another is still down.
- **Winner = the node to silence** (survey). P5's design decides who sends the `FaultCommand` and how. Rule from `p5-changes.md`: the victim's identity stays inside the injector code; failover code learns about the failure only from the failure detector.

## 8. What to measure (for P6)

Per election, logged by every node: `request_id`, bids received (node and score), winner, election duration (request received → decision), whether it waited the full T, late bids, `split_decision`. From these: election frequency, election duration (normal vs. during an outage), message overhead (8 vs. 6), and split-decision rate, as the brief requires.

## 9. Open questions for the review

| # | Question | Proposal | Who |
|---|---|---|---|
| Q1 | Proto for the bid exchange | Add `uint32 round = 4;` to `Bid` (adding a field is safe). `rpc ExchangeBid(BidRequest) returns (Bid)` with `BidRequest { Bid bid = 1; SubmitRequest request = 2; }`. `rpc AnnounceWinner(Coordinator) returns (CoordinatorAck)` with `Coordinator { string request_id = 1; uint32 round = 2; uint32 node_id = 3; }` | P1 |
| Q2 | What do non-winners reply to the client? | `SubmitReply` with `executor_node` = winner and no plan; the client keeps the reply that has a plan. Alternative: non-winners wait for the replicated plan and return it too | P1, P6 |
| Q3 | Early return in `broadcast()` | Return when every detector-alive peer has answered, instead of waiting for the slowest call | P1 |
| Q4 | Same `request_id` applied twice from different origins | Keep the plan from the lower origin `node_id` (Section 5) | P4 |
| Q5 | Step A sends each request to one fixed node; Step B multicasts to all 3 | Client sends to all 3 in Step B (survey 1.2) | P6 |
| Q6 | Is T = 300 ms right on the lab network? | Check on Wed Oct 21: measure node-to-node round trips and pick T well above their maximum | All |
