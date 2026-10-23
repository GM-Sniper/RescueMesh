# P4 changes: shared state

Owner: P4. Decisions and alternatives are in `DESIGN_LOG.md`, section "P4: Shared state in a distributed file system". This file says what the code is and how to use it.

The earlier version of this file described a per-node store with a replicated log (`try_apply`, `apply_remote`, `entries_since`, `FetchLog`). That design was replaced on Oct 21. It is preserved on branch `p4/backup-step-a-store` (and in git history) if anyone needs it.

---

## Oct 23: the shared-state interface (branch `p4/state-interface`)

### What changed

| File | What changed | What you need to know |
|---|---|---|
| `crates/common/src/shared_state.rs` (new) | The `SharedState` trait, the `CommitOutcome` enum and the `StateUnavailable` error | This is the only thing other people's code should call. Every method has a doc comment saying why it is designed that way |
| `crates/node/src/store.rs` | The old `Store` is now `InMemoryStore`, which implements `SharedState`. The change log, the `applied` map, `entries_since`, `apply_remote` and `RemoteOutcome` are deleted | Each node still has its own private copy of the state. Nothing is shared between nodes until the DFS backend exists |
| `crates/node/src/state.rs` | `NodeState.store` is now `Arc<dyn SharedState>` | Code that used `state.store` keeps working through the trait methods |
| `crates/node/src/service.rs` | `handle_submit` uses `lookup_request`, `snapshot` and `commit`; its tests no longer read `applied()` | P1's file. Changed only as far as the new interface requires |
| `Cargo.toml`, crate `Cargo.toml`s | Added `async-trait = "=0.1.92"` | Already in `Cargo.lock` through tonic, so no new download |

### Why the interface is `async` and can fail

A DFS write is a network call, and network calls can be slow or fail. The in-memory store could be synchronous and infallible, but then P1, P3 and P6 would have to rewrite every call site when the DFS arrives. Making callers handle "unavailable" from day one costs a few `.await`s now.

### Why `#[async_trait]` and not native `async fn` in traits

Nodes hold the backend as `Arc<dyn SharedState>`, so the choice of backend is made once at start-up. Native `async fn` in traits cannot be used behind `dyn`. `async_trait` can, and costs one boxed future per call, which is negligible next to a network round trip.

### How to code against the trait

```rust
use common::shared_state::{CommitOutcome, SharedState};
// state.store is an Arc<dyn SharedState>

// 1. Already handled? A client retry can reach any node, so always ask first.
if let Some(plan) = state.store.lookup_request(&request_id).await? { /* reply with plan, duplicate = true */ }

// 2. Copy the state for the planner, then plan (CPU work, off the async threads).
let snapshot = state.store.snapshot().await?;

// 3. Store the plan. Exactly one committer wins per request_id.
match state.store.commit(&request_id, incident, plan.clone()).await {
    CommitOutcome::Committed => { /* we did the work: reply with plan, duplicate = false */ }
    CommitOutcome::AlreadyCommitted(stored) => { /* reply with `stored`, throw ours away, duplicate = true */ }
    CommitOutcome::ResourceTaken { .. } => { /* go back to snapshot and plan again, a few times at most */ }
    CommitOutcome::StateUnavailable(why) => { /* tell the client to retry with the SAME request_id */ }
}
```

(`?` works because `snapshot` and `lookup_request` return `Result<_, StateUnavailable>`; `handle_submit` in `service.rs` maps it to a gRPC `Unavailable`.)

### What each outcome means

| Outcome | Meaning | What you do |
|---|---|---|
| `Committed` | This call stored the plan | Reply with your plan |
| `AlreadyCommitted(plan)` | That `request_id` already had a plan (earlier call, retry, or another node) | Reply with **that** plan, not yours. Otherwise a client could get two different plans for one request |
| `ResourceTaken { resource_id }` | A planned resource was not free at write time. Nothing was written | Take a new snapshot, plan again, commit again |
| `StateUnavailable(reason)` | Could not reach enough of the shared state to decide. The write may or may not have landed | Retry with the same `request_id`. This is safe: commit is create-if-absent, so the retry comes back `AlreadyCommitted` if the first attempt landed |

Other rules:

- `request_id` is the dedup key. Never invent a new one for a retry.
- An empty plan is a valid commit. It means "nothing was free"; the incident is stored as `Open`, and a retry gets the same empty plan.
- Do not use wall-clock time to decide which write came first. The backend decides.
- `InMemoryStore` never returns `StateUnavailable`. Write your handling anyway, and test it with a small fake of the trait that always fails.

### Tests

`cargo test -p node store`: 10 unit tests with descriptive names (same request twice, concurrent commits of one request, two requests for one resource, lookup, empty plan, and others). `cargo test -p node` also runs the `handle_submit` tests in `service.rs`. Total after this change: 66 passing in `node` (the old store had 18).

### Not built, on purpose

- Anything that talks to a DFS. The spike (below) only checks whether SeaweedFS can do what the interface needs.
- Resource leases, incident expiry (TTL), and recovery of a returning node. They depend on what the DFS backend offers.

### Open questions

| # | Question | What I did | Alternatives |
|---|---|---|---|
| 1 | The request said `commit(request_id, plan)`, but a `Plan` only holds an incident id and the stored state needs the incident itself (it must be recorded as `Open` when the plan is empty) | `commit(request_id, incident, plan)` | Keep `commit(request_id, plan)` and store incidents with a second call. That makes two writes that can half-fail on a DFS |
| 2 | The old store returned a separate `UnknownResource` error (a bug at the time). The new enum has four variants only | An unknown resource is reported as `ResourceTaken`: the caller's cure is the same (snapshot and plan again) | Add a fifth variant if the team wants to tell "bug" from "race" |
| 3 | `StateUnavailable` carries a `reason` string | Yes, for logs only | A bare variant with no data |
| 4 | The earlier doc listed handling for two nodes planning the same free resource and "same request from two origins". With one shared create-if-absent record these should no longer happen, but that depends on the DFS giving the guarantee | Left to the spike (check 1 and 2) | None until the spike results are confirmed on the real machines |

### Spike results

See the section "SeaweedFS spike" below (added in Step 2).
