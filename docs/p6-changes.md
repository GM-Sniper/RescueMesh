# P6 changes: client, load generator, CSV, stats and node metrics

Owner: P6. Branch: `StepA/p6`. This file is a timeline of everything P6 added in Step A, oldest first, with the code that matters and how to run it. Every line in the code examples has a comment saying what it does.

---

## At a glance

| When | Commit | What | Plan task |
|---|---|---|---|
| Sun Oct 18 | `c3f1161` | Merged `StepA/P1` to get the `Submit` RPC before it reached `main` | Handoff 6 (proto v1) |
| Sun Oct 18 | `02fc796` | `ClientMiddleware`, mock server, client demo | Client against a mock server (Sun Oct 18) |
| Tue Oct 20 | `4843fda` | Merged `main` (P2's domain types, planner, conversions) | |
| Tue Oct 20 | `b41acd2` | Demo and mock use P2's real `Incident` / `Plan` | Fallout of handoff 13 |
| Tue Oct 20 | `ba86c15` | Load generator, CSV writer, run metadata file | Load generator + CSV writer (Mon Oct 19) |
| Tue Oct 20 | `a8d19de` | PR #5 merged into `main` | |
| Wed Oct 21 | not committed yet | `assigned` column, stats script, `node_metrics` log | Summary script + node_metrics (Tue Oct 20) |

```text
crates/client/src/lib.rs              # ClientMiddleware: send one request, time it        (Oct 18)
crates/client/src/bin/mock_server.rs  # fake node for testing                              (Oct 18, Oct 20)
crates/client/src/main.rs             # demo: 5 requests + a duplicate check               (Oct 18, Oct 20)
crates/loadgen/src/main.rs            # many clients at once -> CSV + metadata             (Oct 20, Oct 21)
crates/loadgen/src/bin/stats.rs       # CSV -> mean, p50, p95, p99, throughput             (Oct 21)
crates/node/src/node_metrics.rs       # node writes queue / CPU / load once per second     (Oct 21)
```

---

## 1. Sun Oct 18: get the `Submit` RPC early (`c3f1161`)

**Problem:** `Submit` (client sends an incident, gets a plan) was on P1's branch `StepA/P1`, not on `main`. Without it there were no generated client stubs to code against.

**What I did:** merged `origin/StepA/P1` into `StepA/p6` (a test merge showed no conflicts first). That brought in:

```proto
rpc Submit(SubmitRequest) returns (SubmitReply);  // client sends a SubmitRequest, the node answers with a SubmitReply

message SubmitRequest {                           // what the client sends
  string request_id = 1;                          // client-generated Uuid; same ID on every retry
  oneof kind { Incident incident = 2; }           // the payload; Step A only sends incidents
}                                                 // end of SubmitRequest
message SubmitReply {                             // what the node answers
  string request_id = 1;                          // echoes the request's ID, so replies can be matched
  uint32 executor_node = 2;                       // node that ran the planner
  Plan plan = 3;                                  // the response plan
  bool duplicate = 4;                             // true if this request_id was already processed
}                                                 // end of SubmitReply
```

`cargo build` turns this into `NodeClient::submit()` (used by the client) and the `Node` trait (implemented by the mock server).

---

## 2. Sun Oct 18: client, mock server and demo (`02fc796`)

### Files

| File | Change |
|---|---|
| `Cargo.toml` (root) | Added `tokio` (async runtime) and `uuid` (request IDs), pinned with `=` |
| `crates/client/Cargo.toml` | Depends on `common`, `tonic`, `tokio`, `uuid`; `default-run = "client"` |
| `crates/client/src/lib.rs` | **New.** `ClientMiddleware` |
| `crates/client/src/bin/mock_server.rs` | **New.** Fake node |
| `crates/client/src/main.rs` | "Hello, world!" replaced by the demo |

### `ClientMiddleware` (`crates/client/src/lib.rs`)

Sends one incident to one fixed node (Step A) and measures how long the reply takes. Both the demo and the load generator use it, so every request in every experiment is sent and timed the same way.

```rust
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5); // give up on a reply after 5 s

pub enum SubmitStatus { Ok, Timeout, Error }   // how a request ended; CSV `status` column: "ok" / "timeout" / "error"

pub struct SubmitOutcome {                     // everything learned from one request = one CSV row
    pub request_id: String,                    // the Uuid generated for this request
    pub latency_us: u64,                       // send-to-reply time in microseconds
    pub status: SubmitStatus,                  // ok / timeout / error
    pub executor_node: Option<NodeId>,         // which node ran the planner; None if it failed
    pub plan: Option<pb::Plan>,                // the plan returned; None if it failed
    pub duplicate: bool,                       // true = the node had already seen this request_id
    pub error: Option<String>,                 // why it failed; None if ok
}                                              // end of SubmitOutcome

impl ClientMiddleware {                                                                              // the client's functions
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Self, tonic::transport::Error>;     // open the connection to one node, once
    pub async fn submit(&self, incident: pb::Incident) -> SubmitOutcome;                             // send with a NEW request_id
    pub async fn submit_with_id(&self, request_id: String, incident: pb::Incident) -> SubmitOutcome; // resend with the SAME request_id (retry)
}                                                                                                    // end of impl
```

What `submit_with_id` does, step by step:

```rust
let message = pb::SubmitRequest {                                             // build the envelope
    request_id: request_id.clone(),                                           // the tracking number (copy kept to check the reply)
    kind: Some(submit_request::Kind::Incident(incident)),                     // the incident inside
};                                                                            // envelope done
let start = Instant::now();                                                   // start the stopwatch
let result = tokio::time::timeout(self.timeout, node.submit(message)).await;  // send, wait at most 5 s
let latency_us = start.elapsed().as_micros() as u64;                          // stop the stopwatch

match result {                                                                // three possible endings:
    Err(_elapsed)        => /* status = Timeout */,                           // no reply within 5 s
    Ok(Err(status))      => /* status = Error, with the gRPC code and message */, // the node or network returned an error
    Ok(Ok(response))     => /* status = Ok, unless reply.request_id != request_id (then Error) */, // got a reply; check it's for this request
}                                                                             // the result is returned as a SubmitOutcome
```

### Mock server (`crates/client/src/bin/mock_server.rs`)

A fake node so the client could be tested before P1's node handles `Submit`. It implements the generated `Node` trait:

- `submit`: rejects a missing `request_id` or missing incident, optionally sleeps `--delay-ms`, remembers every `request_id` in a set and replies with `duplicate = true` for a repeat, echoes the `request_id`, `executor_node = --id`.
- `heartbeat`, `inject_fault`: return `unimplemented` (P1's and P5's jobs).
- Flags: `--port` (default 50051), `--id` (default 1), `--delay-ms` (default 0). Listens on `127.0.0.1` only.

### Demo (`crates/client/src/main.rs`)

Connects, sends `--count` incidents (default 5), prints one line each, then resends the first `request_id` to check that the node answers `duplicate=true`.

### Decisions

| Decision | Why |
|---|---|
| `ClientMiddleware` in `lib.rs`, not `main.rs` | The load generator reuses it |
| Client generates `request_id` (Uuid v4) | Retries keep the same ID, so the node can return the stored plan instead of planning twice |
| Latency measured with `Instant`, in microseconds | `Instant` never jumps when the wall clock is adjusted; local requests are under 1 ms, so ms would round to 0 |
| 5 s timeout with `tokio::time::timeout` | P1's mute hook makes a node hang, not error; without a limit the client would wait forever |
| Failures are returned as data, never `Err` or a panic | A 1000-request run counts its failures instead of stopping at the first one |

---

## 3. Tue Oct 20: merge `main` (`4843fda`)

`main` had 17 new commits (P2's domain model, planner, conversions; P5's failure detector; P1's node setup). Only `Cargo.toml` and `Cargo.lock` conflicted. Resolved by taking `main`'s versions and adding the `uuid` line back; `Cargo.lock` was regenerated by `cargo build`.

---

## 4. Tue Oct 20: use P2's real `Incident` and `Plan` (`b41acd2`)

**Problem:** P2 gave `Incident` and `Plan` real fields, so `pb::Incident {}` and `pb::Plan {}` no longer compiled.

**Demo (`main.rs`)**: sends realistic, repeatable incidents. New flag `--seed` (request `i` uses `seed + i`).

```rust
let incident = incident_to_pb(&generate_incident(seed + i)); // P2's generator makes incident i, P2's converter turns it into the proto type
let outcome = client.submit(incident.clone()).await;         // send it; clone so the first one can be resent for the duplicate test
```

**Mock server**: validates incidents the same way the real node will, and returns a plan with the right shape.

```rust
let incident = incident_from_pb(incident).map_err(Status::invalid_argument)?; // P2's check: rejects missing location, bad severity, unknown category

plan: Some(pb::Plan {         // the plan sent back
    incident_id: incident.id, // which incident this plan is for
    priority: 0.0,            // the mock does not run P2's planner, so no real priority
    assignments: vec![],      // so nothing is ever assigned
}),                           // end of the plan
```

`lib.rs` did not change: it passes on whatever incident it is given.

---

## 5. Tue Oct 20: load generator, CSV and run metadata (`ba86c15`)

### Files

| File | Change |
|---|---|
| `crates/loadgen/Cargo.toml` | Depends on `client`, `common`, `tokio`, `clap`, `anyhow` (all already in the workspace) |
| `crates/loadgen/src/main.rs` | "Hello, world!" replaced by the load generator |
| `.gitignore` | Added `/results` |

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--node` | `127.0.0.1:50051` | The one node to send to (Step A) |
| `--clients` | `10` | Fake clients sending at the same time |
| `--requests` | `1000` | Total requests, shared by all clients |
| `--seed` | `1` | Request `n` sends `generate_incident(seed + n)` |
| `--rate` | `0` | Target requests per second; 0 = as fast as possible |
| `--out` | `results` | Output folder |
| `--run-id` | `run-<unix secs>` | Name of the run |

### How it works

```rust
// 1. Connect every client BEFORE the clock starts (setup is not request latency).
for _ in 0..args.clients {                                               // once per fake client
    clients.push(ClientMiddleware::connect(&args.node, DEFAULT_TIMEOUT).await?); // open its own connection
}                                                                        // all clients connected

// 2. CSV writer on its own thread; clients drop rows into a channel.
let (tx, rx) = mpsc::channel::<Row>();                                   // the mailbox: tx = put rows in, rx = take rows out
let writer = std::thread::spawn(move || write_csv(&csv_path, &run_id, rx)); // the writer thread empties the mailbox into the file

// 3. One tokio task per client (this loop runs inside each task).
let n = next.fetch_add(1, Ordering::Relaxed);                            // take the next request number from the shared counter
if n >= total { break; }                                                 // no numbers left: this client is done
if let Some(gap) = interval {                                            // only when --rate is set:
    tokio::time::sleep_until(start + gap.mul_f64(n as f64)).await;       // wait until request n is due (n / rate seconds after start)
}                                                                        // now it's this request's turn
let incident = incident_to_pb(&generate_incident(seed + n));             // P2's incident n (same seed = same incidents every run)
let outcome = client.submit(incident).await;                             // send and time it (ClientMiddleware)
tx.send(row);                                                            // drop the result in the mailbox for the CSV writer

// 4. Wait for every client and the writer, print a summary, write <run_id>.meta.txt.
```

### Output (`results/`, ignored by git)

`<run_id>.csv`, one row per request:

```text
run_id,timestamp,client_id,request_id,latency_us,executor_node,status,assigned   # header: column names
test-rate,1792451351879,1,d4e6b22b-7d01-471b-9576-d603e51d2870,27624,1,ok,false  # run "test-rate", sent at that unix ms, by client 1, took 27.6 ms, node 1 answered, ok, nothing assigned
```

(`assigned` was added on Oct 21, see section 7.)

`<run_id>.meta.txt`: `run_id`, `started_at_ms`, the exact command, `node`, `clients`, `requests`, `seed`, `rate`, `git_commit` (`git describe --always --dirty`), `duration_s`, `ok`, `unassigned`, `timeout`, `error`, `throughput_ok_per_s`.

### Tested against the mock

| Test | Result |
|---|---|
| 200 requests, `--rate 100` | 2.01 s, 200 ok, 99.5 req/s |
| 500 requests, no rate limit | 0.37 s, 500 ok, about 1360 req/s |
| Mock with `--delay-ms 6000` (longer than the 5 s timeout) | All requests recorded as `timeout`, no crash |
| No server running | Clear `ConnectionRefused` error before anything is sent |

### Decisions

| Decision | Why |
|---|---|
| One connection per client | Behaves like separate users |
| Connect before the clock starts | Connection setup is not request latency |
| CSV written on a separate thread | Disk writes never delay a request, so latency stays accurate |
| `--rate` schedules request `n` at `n / rate` s | Spreads requests evenly; if the node is slower, the achieved rate drops and the summary shows it |
| Metadata file with seed and git commit | Any run can be repeated exactly |
| `results/` in `.gitignore` | Test runs are not committed by accident |

---

## 6. Tue Oct 20: PR #5 merged into `main` (`a8d19de`)

Client, mock server and load generator are on `main`. Anything after this goes in a new PR.

---

## 7. Wed Oct 21: `assigned` column (not committed yet)

**Why:** P4's note: when no resource is free, the node replies with an **empty plan**, and a retry must use a **new** `request_id`. Without a column, "got an ambulance" and "nothing was free" both looked like `ok`.

`crates/client/src/lib.rs`:

```rust
/// Some(true) = a resource was assigned, Some(false) = empty plan (nothing free), None = no plan.
pub fn assigned(&self) -> Option<bool> {                         // new helper on SubmitOutcome
    self.plan.as_ref().map(|plan| !plan.assignments.is_empty())  // no plan -> None; plan -> does it assign anything?
}                                                                // end of assigned()
```

`crates/loadgen/src/main.rs`: new CSV column `assigned` (`true`, `false`, or empty when there is no plan), an `unassigned` count in the summary line and the metadata file.

The load generator does **not** resubmit empty plans automatically: that would change how many requests are sent and skew the numbers. `submit()` already uses a new `request_id`, which is what P4 asks for; `submit_with_id()` is only for network retries.

With the mock, every row shows `false`, because the mock never assigns anything. With the real node, `false` only appears when every resource is taken.

---

## 8. Wed Oct 21: stats script (not committed yet)

`crates/loadgen/src/bin/stats.rs` reads one or more loadgen CSVs and prints:

```text
== results/test-rate.csv                                   # which file is being summarized
requests:    200  (200 ok, 0 timeout, 0 error)             # how every request ended
latency (ok requests only):                                # failed requests are left out of the timing
  mean       3.13 ms                                       # average
  p50        3.03 ms                                       # typical request: half were faster
  p95        4.94 ms                                       # slow tail: only 5% were slower
  p99        5.66 ms                                       # worst cases: only 1% were slower
  min / max  1.19 ms / 11.06 ms                            # fastest and slowest
throughput:  50.2 ok requests/s over 3.98 s                # successful requests per second
by node:     node 1: 200                                   # which node answered how many
```

(An `assigned:` line is printed too when the CSV has that column.)

```rust
/// Nearest-rank percentile, the method in the Step A plan: the value at position ceil(p% × n).
fn percentile(sorted: &[u64], p: f64) -> u64 {                      // sorted latencies in, one latency out
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;  // e.g. p95 of 10 values -> position 10
    sorted[rank.clamp(1, sorted.len()) - 1]                        // -1 because Rust counts from 0
}                                                                   // end of percentile()
```

| Decision | Why |
|---|---|
| Latency stats use `ok` requests only | A 5 s timeout would distort the mean and percentiles; failures are counted separately |
| Throughput = ok requests ÷ (first send → last reply) | Uses only the CSV, so it works for any old run |
| Columns found by name from the header | Adding a column later does not break the script |
| `default-run = "loadgen"` in `crates/loadgen/Cargo.toml` | The crate now has two programs; `cargo run -p loadgen` still starts the load generator |

Tests (`cargo test -p loadgen`): the plan's worked example (latencies 12, 13, 13, 14, 15, 15, 16, 18, 25, 90 ms give mean 23.1, p50 15, p95 90), and failures being counted but not timed.

---

## 9. Wed Oct 21: `node_metrics` log (not committed yet)

**What:** once per second, each node writes how busy it is, so latency spikes in the loadgen CSV can be lined up with what the node was going through. The numbers come from P3's `NodeState::load_report()`; this code only writes them.

`logs/node_metrics-node<ID>.csv` (`logs/` is already in `.gitignore`):

```text
node_id,wall_ts_us,queue_len,cpu_pct,service_ms,load_score   # header: column names
3,1792535856128148,0,12.9,0.00,0.039                         # node 3, at that time (µs since 1970): 0 requests in progress, CPU 12.9%, no service time yet, load 0.039
3,1792535857128092,0,8.3,0.00,0.025                          # one second later: CPU 8.3%, load 0.025
```

| File | Change |
|---|---|
| `crates/node/src/node_metrics.rs` | **New.** The logger and its tests |
| `crates/node/src/main.rs` (P1's file) | Two lines, marked with a note in capitals for P1 |

```rust
// main.rs
mod node_metrics;                                                                    // P6: include the new file in the node program
// ...                                                                              // (rest of main.rs unchanged)
load::spawn_sampler(state.clone());                                                 // P3: starts CPU sampling (already there)
node_metrics::spawn(state.clone(), format!("logs/node_metrics-node{}.csv", args.id)) // P6: start writing one row per second for this node
    .context("cannot open the node_metrics log")?;                                  // if the file can't be opened, stop the node with a clear message
```

| Decision | Why |
|---|---|
| A plain thread, not a tokio task | Same reason as P3's sampler: a busy async runtime would write late exactly when the node is most loaded |
| Fixed 1 s schedule, restarted if far behind | Rows stay 1 s apart; after a long pause it does not write a burst of catch-up rows |
| `wall_ts_us` (microseconds since 1970) | Same as P5's fault log, so all files can be joined by time |
| Append, header only on an empty file, flush per row | Same as P5's fault log: restarts add to the same file, a crash loses at most one row |
| A write error is logged, never stops the node | Logging must not take the node down |

Tested: 2 unit tests (`cargo test -p node node_metrics`), and a real node run for 4.5 s wrote exactly one row per second. `queue_len` stays 0 until `Submit` is wired.

---

## How to run everything

```bash
# Fake node (until P1's Submit is wired). Uses port 50051, the same as node 1: don't run both.
cargo run -p client --bin mock_server

# Quick demo: 5 requests + a duplicate check
cargo run -p client

# Load test -> results/<run_id>.csv and .meta.txt
cargo run -p loadgen -- --clients 10 --requests 1000 --seed 1 --rate 100

# Stats from one or more CSVs
cargo run -p loadgen --bin stats -- results/<run_id>.csv

# Real node (writes logs/node_metrics-node1.csv)
cargo run -p node -- --id 1 --config config/nodes.toml

# Tests: the stats script (includes the plan's worked example)
cargo test -p loadgen

# Tests: the node_metrics log
cargo test -p node node_metrics
```

On the lab machines, use `--release` for real measurements, and `--node <machine IP>:50051` (see `config/nodes.lab.toml`).

## What P6 needs from others

| From | What |
|---|---|
| P1 | `Submit` wired (task 7). `SubmitReply.request_id` must echo the request's ID (otherwise the client counts an error), `executor_node` set, `duplicate = true` for a repeated `request_id` |
| P1 | Keep the two `node_metrics` lines in `crates/node/src/main.rs` when merging `main` into `StepA/P1` |
| P4 | Empty plan = nothing free; absent plan = not the executor (Step B). The `assigned` column relies on this |

## Still to do

- Switch from the mock to P1's real node once `Submit` is wired (only `--node` changes).
- Wed Oct 21: small run first, then the 1000-request run on the 3 real machines, then the stats script on its CSV.
- Step B: line up loadgen CSVs with `node_metrics` and P5's fault logs for the plots.
