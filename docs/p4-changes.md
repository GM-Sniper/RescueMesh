# P4 changes: store and change log

Owner: P4. Decisions and alternatives are in `DESIGN_LOG.md`, section "P4: Store and Change Log".

---

## What the store is

- `crates/node/src/store.rs` holds the node's state (incidents, resources, assignments) and a log of every change made to it.
- Every log entry is numbered `(source_node, seq)`: the node that made it and a counter that node increases by 1. Applying the same entry twice gives the same state, and a repeated `request_id` gets the saved plan back.
- `NodeState.store` is created in `NodeState::new` (its signature did not change). 18 unit tests: `cargo test -p node store`.

## Files

| File | What it is | What you need to know |
|---|---|---|
| `crates/node/src/store.rs` | The store and its tests | No handler calls it yet, so `mod store;` in `main.rs` has `#[allow(dead_code)]`. Remove that when the handler is wired |
| `crates/node/src/state.rs` | One new field, `pub store: Store`, and one initializer | Small change; expect a small merge conflict with `StepA/P1` |
| `DESIGN_LOG.md` | P4 section | Decisions and alternatives only |

## API

| Function | What it does |
|---|---|
| `Store::with_default_resources(node_id)` | Creates the store for this node with 10 starting resources (seed 42). Every node gets the same ones |
| `snapshot()` | Returns a copy of the current state for the planner to read, with each list sorted by id |
| `lookup_request(request_id)` | Says whether this request was already handled, and returns the saved plan if so |
| `try_apply(request_id, incident, plan, now_ms)` | Saves a new incident and its plan, and adds the log entry for it. It is the only call that changes the store on this node. If something is wrong it returns an error and saves nothing |
| `apply_remote(entry)` | Takes a log entry made by another node and applies it here. It never applies the same entry twice |
| `entries_since(last_seq_by_source)` | Returns every log entry newer than the numbers the caller says it already has. This is the answer to a `FetchLog` request |
| `applied()` | Returns, for each node, the highest entry number this store has from it |

## How the Submit handler uses the store

The handler is the function that runs when a client sends a request. Call the store in this order:

1. `lookup_request(request_id)`: has this request already been handled? If yes, it returns the saved plan: reply with it and `duplicate = true`, and stop. If no, it returns nothing: continue.
2. `incident_from_pb`: convert the incident from the network format.
3. `snapshot()`: get a copy of the current state.
4. `plan(...)`: the planner picks resources using that copy.
5. `try_apply(...)`: save the result. This is the only step that changes the store. It returns one of:
   - `Applied`: saved. Reply with the plan. (Step B: also send the new log entry to the other nodes.) If `duplicate = true`, another thread saved the same request a moment earlier, so reply with the stored plan, as in step 1.
   - `ResourceTaken`: another request took a resource you planned on, between steps 3 and 5. Go back to step 3 and plan again (a few times at most, then return an error).
   - `UnknownResource`: the plan names a resource the store doesn't have. That is a bug today: return an error, don't retry.
6. Reply to the client.

In Step B the order changes. With the election, a node that already has the request still takes part and replies with the stored plan only if it wins (see `docs/p3-election-design.md`, Section 2, step 2). In Step A there is no election, so the steps above are right as they are.

In Step B, a node that does not win the election replies with no plan (the plan field is absent). The winner always sets the plan, even when it is empty. So an absent plan means "this node was not the executor", and an empty plan means "this node was the executor and nothing was free".

## What to do with entries from other nodes (Step B)

Pass each entry to `apply_remote(entry)`, reply to the sender with `acked_seq = applied()[source]` ("I have everything from that node up to this number"), then act on the result:

| Result | What it means | What to do |
|---|---|---|
| Applied | New entry, saved | Reply |
| Duplicate | Already had it | Reply |
| Superseded | Another node already handled the same request; the entry is kept in the log, state unchanged | Reply, nothing else |
| Invalid | Entry unreadable; kept in the log, state unchanged, warning logged | Reply, nothing else |
| Gap | Earlier entries are missing; nothing changed | Ask that node for everything newer than `applied()[source]` with `FetchLog` |

## Handle later

| What is open | Owner and step |
|---|---|
| The `Replicate` and `FetchLog` calls are missing from the gRPC service, so nobody can send or fetch log entries between nodes yet. Someone has to write those handlers. | P1 and P4, Step B |
| When two nodes handle the same request, the store keeps whichever plan it applied first, but P3's Q4 says the plan from the lower node id should win. Undoing the first plan is not built; the options are in `DESIGN_LOG.md`. Having exactly one executor per request only holds once replication has caught up. If a retry reaches a node that has not yet received the first entry, that node can win and execute the request too. The lower-origin rule (undo, Step B) is what makes all nodes end up with the same plan. | P4 and P3, Step B |
| The client sends each request to all three nodes, so two different requests can be planned on different nodes before replication reaches the other node, and both can pick the same free resource. When the second entry arrives, the store keeps the first assignment it applied and logs a warning, so nodes can end up with different assignments. This is likelier if a returning node bids before it has caught up (see `docs/p3-election-design.md`, Section 4), because it can plan from a stale snapshot. P4 detects and records the collision in the store. The rule for resolving it and a conflict status value are still undecided (see the detect-and-reconcile row below). | P4 for detecting and recording; P3 and P2 for the rule and status value, Step B |
| Nobody owns detecting and fixing conflicts yet. Proposal: P4 detects and records them in the store, P3 owns the rule, P2 adds the status value, P1 writes the handler. The spec wants a visible reconciliation state, so the incident and assignment status enum needs a conflict value. | P2 for the enum, whole team for owners, Steps B and C |
| `UnknownResource` is a bug today. Once resources can expire or be offered by clients, a missing resource becomes normal and the handler should re-plan, as it does for `ResourceTaken`. | P4 and P1, Step B |
| An unreadable entry is logged, counted as applied and skipped with a warning. That is a provisional choice; the alternatives are in `DESIGN_LOG.md`. | P4, Step B |
| When no resource is free, the plan is empty and the incident stays `Open`; a retry with the same `request_id` gets the same empty plan. Later we may queue the incident and plan again when resources free up. | P2 and the team, Step B |
| The starting resources are constants in `store.rs`. Remove them once clients can offer resources as log operations; make them configurable only if experiments need a different resource count. | P4 and P1, Step B |
| Resource leases, incident expiry (TTL), saving the store to disk, and recovery of a returning node (replaying the log or copying a snapshot) are not built. | P4, Step C |
