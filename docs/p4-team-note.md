# P4 team note: the shared-state interface has landed

Ready to send to P1, P2, P3 and P6. Written Oct 23 for Yasmina to review before sending.

---

**To P1, P2, P3, P6: shared-state interface is on branch `p4/state-interface`**

The state interface is ready to code against. No PR yet; P3 reviews it first.

**What it is.** A trait `SharedState` in `common::shared_state`, held in the node as `state.store: Arc<dyn SharedState>`. Three methods, all `async` and fallible, because the real backend (a distributed file system) will be a network call:

- `snapshot().await?`: a copy of the state for the planner.
- `lookup_request(request_id).await?`: the stored plan for a retry, or `None`.
- `commit(request_id, incident, plan).await`: store the plan if this `request_id` has none yet. Exactly one committer wins per `request_id`.

**What `commit` returns.** (Oct 29: a fifth case, `UnknownResource`, was added. If you `match` on `CommitOutcome`, add an arm for it.)

| Outcome | What it means | What you do |
|---|---|---|
| `Committed` | You stored the plan | Reply with your plan |
| `AlreadyCommitted(plan)` | Someone already stored a plan for this `request_id` | Reply with **that** plan, not yours |
| `ResourceTaken { resource_id }` | A resource in your plan was no longer free; nothing was written | Take a new `snapshot()`, plan again, commit again |
| `UnknownResource { resource_id }` | A resource in your plan does not exist at all; nothing was written | Do not retry. It is a bug: return an internal error |
| `StateUnavailable(why)` | The shared state could not be reached; the write may or may not have landed | Retry with the **same** `request_id` (safe) |

`snapshot()` and `lookup_request()` return `Err(StateUnavailable)` in the same situation.

**Rules.** `request_id` is the dedup key: never make a new one for a retry. An empty plan is a normal commit ("nothing was free"). Don't use clocks to order writes.

**For now** the in-memory store (`InMemoryStore`, `crates/node/src/store.rs`) sits behind the trait, one private copy per node, so nothing is shared between nodes yet and it never returns `StateUnavailable`. Please write your `StateUnavailable` handling anyway; a small fake that always fails is enough to test it.

**What changed in your code.** P1: `handle_submit` in `service.rs` now calls the trait (lookup, snapshot, plan, commit) and maps `StateUnavailable` to a gRPC `Unavailable`. The old log, `applied()`, `entries_since` and `apply_remote` are gone, so don't call them. `Replicate` and `FetchLog` messages in the proto are no longer used by anything; P1 owns the proto, so P1 decides whether to delete them.

Details, examples and open questions: `docs/p4-changes.md`.
