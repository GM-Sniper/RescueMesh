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

## 10. Thu Oct 22: switch from the mock to the real node (not committed yet)

P1's `Submit` is on `main` (PR #10). Merged `main` into `StepA/p6`; the only conflict was the two `node_metrics` lines in `crates/node/src/main.rs`, re-added after P1's new step 6.

The client and the load generator now read the node's address from **the same config file the nodes use**, instead of a hard-coded address:

```rust
pub fn target_addr(config_path: &str, target: NodeId, override_addr: Option<&str>) -> Result<String, ...> { // in client/lib.rs
    if let Some(addr) = override_addr { return Ok(addr.to_string()); }              // --node given: use it as is (e.g. the mock)
    let config = common::config::ClusterConfig::load(config_path)?;                 // same loader and checks as the nodes
    let node = config.node(target).ok_or_else(|| format!("node {target} is not listed in {config_path}"))?; // find node --target
    Ok(node.addr.clone())                                                           // e.g. "10.0.0.12:50051"
}                                                                                   // end of target_addr
```

| Flag (client demo and loadgen) | Default | Meaning |
|---|---|---|
| `--config` | `config/nodes.toml` | Cluster config; `config/nodes.lab.toml` on the real machines |
| `--target` | `1` | Which node in that file to send to |
| `--node` | none | Any address instead (e.g. the mock server); wins over `--config` |

A lab config that still says `REPLACE-ME` is refused with a clear error, so the run can't go to a wrong address by mistake.

### Rehearsal on one laptop (release build, real node 2)

```text
1000 requests  (1000 ok, 0 timeout, 0 error)      # every request answered
assigned:  8 got a resource, 992 got nothing      # the store starts with 10 resources; the demo used 2 first
mean 7.51 ms | p50 7.65 ms | p95 15.42 ms | p99 26.82 ms | max 42.29 ms   # latency of ok requests
throughput: 100.0 ok requests/s over 10.00 s       # exactly the --rate 100 target
```

Almost every plan is empty because P4's store starts with only 10 resources and Step A never frees them (on P4's handle-later list). Latency is still meaningful: the planner runs for every request.

---

## 11. Step B, Mon Oct 26: ramping load generator (branch `StepB/p6`)

Plan task: "stages (clients 1 to N), duration per stage, stage column in the CSV".

| Flag | Default | Meaning |
|---|---|---|
| `--stages` | none | Clients per stage, e.g. `1,2,4,8`. Turns on ramp mode; can't be combined with `--clients`, `--requests` or `--rate` |
| `--stage-secs` | `30` | How long each stage lasts |

```rust
for (s, &stage_clients) in stages.iter().enumerate() {          // stages run one after another
    let stage = s as u32 + 1;                                    // numbered from 1
    let stage_end = ramp.then(|| Instant::now() + stage_len);    // ramp mode: when this stage stops sending
    for (i, client) in clients.iter().take(stage_clients as usize).enumerate() { // the first k connections
        tasks.push(tokio::spawn(ClientJob { /* client, stage, stage_end, ... */ }.run())); // one task per client
    }
    for task in tasks { task.await?; }                           // next stage starts when this one is done
}
```

- Ramp mode is **closed loop**: each client sends its next request as soon as the previous one returns, so more clients = more load.
- A request still in flight when its stage ends finishes and is recorded in that stage, so a stage can run slightly over `--stage-secs`.
- All connections (as many as the biggest stage) open before the clock starts.
- Without `--stages`, the run is one stage and behaves exactly as in Step A.
- CSV: new last column `stage`. `.meta.txt`: new `mode`, `stages`, `stage_secs`.
- Stats: when a CSV has more than one stage, a per-stage table (clients, requests, ok, mean, p50, p95, p99, ok/s). Older CSVs still work. New test: `ramp_runs_are_split_by_stage`.

Example run (laptop, release, real node 2, `--stages 1,2,4,8 --stage-secs 5`):

```text
  stage  clients  requests      ok       mean        p50        p95        p99      ok/s   # header
      1        1       989     989    5.05 ms    4.23 ms    8.13 ms   12.30 ms     197.7   # 1 client
      2        2      1525    1525    6.55 ms    6.33 ms    9.44 ms   13.09 ms     304.9   # 2 clients
      3        4     18988   18988    1.04 ms    0.48 ms    7.23 ms   10.58 ms    3793.6   # 4 clients
      4        8     13452   13452    2.96 ms    1.51 ms    9.14 ms    9.93 ms    2687.0   # 8 clients
```

Laptop numbers jump around (one machine runs the node and all the clients); the real ramp needs the lab machines and planner v2. Once the 10 starting resources are taken, plans are empty and cheap, which also flattens latency.

---

## 12. Step B, Mon Oct 26: experiment runner (branch `StepB/p6`)

Plan task: "one command runs a config (clients, duration, seed) and gathers CSVs from all 3 nodes".

```bash
cargo build --release                                                    # builds node, loadgen, stats and experiment
cargo run --release -p loadgen --bin experiment -- experiments/ramp-local.toml
```

An experiment is a small TOML file in `experiments/`:

```toml
name = "ramp-local"              # the run is called <name>-<unix seconds>
cluster = "config/nodes.toml"    # which nodes (config/nodes.lab.toml on the real machines)
start_nodes = true               # true: start every node on this machine; false: they already run (lab)
target = 1                       # node the load goes to (until P1's multicast)
seed = 1                         # same seed = same incidents
stages = [1, 2, 4, 8]            # ramp; or clients / requests / rate for a fixed run
stage_secs = 10                  # seconds per stage
```

What one command does, in local mode:

1. Checks the node ports are free (so it never measures an old node or the mock by mistake), starts every node with the run's `--run-id`, inside the run folder, and waits until each accepts connections.
2. Runs `loadgen` with the file's settings, writing straight into the run folder.
3. Stops the nodes. They are also stopped if any step fails, so none are left running.
4. Runs `stats`, prints it and saves it as `summary.txt`.

Everything lands in `results/<run_id>/`: `experiment.toml` (copy of the config), the request CSV and `.meta.txt`, `summary.txt`, `node<N>.log`, and `logs/faults-node<N>.csv` + `logs/node_metrics-node<N>.csv` for each node. Because each node runs inside the run folder, its logs belong to that run only, not appended to older runs.

Lab mode (`start_nodes = false`, `experiments/ramp-lab.toml`): prints the exact command to start each node with the right `--run-id`, waits for Enter, runs the load and stats, then lists which node files to copy into the run folder. Copying them automatically needs a shared folder or ssh between the machines, which is not set up yet.

Example configs: `experiments/ramp-local.toml` (40 s ramp on one laptop), `experiments/ramp-lab.toml` (2.5 min ramp on the lab machines), `experiments/fixed-local.toml` (Step A style: 10 clients, 1000 requests at 100/s).

Tested on one laptop: 3 nodes started, 4-stage ramp, 13 files gathered, no node left running afterwards.

**Open observation for P1/P4:** on the first run after a fresh `cargo build`, a fresh node answered every request with an empty plan (0 of 12723 assigned). Every later run on the same binary assigned the 10 starting resources as expected. Seen 3 times. `assigned` comes straight from the node's reply, so the cause is on the node side; not investigated further.

---

## 13. Step B, Tue Oct 27: election metrics and win share (branch `StepB/p6`)

Plan tasks: "elections.csv from P3's events" and "load distribution: wins per node per time window, share per node". P3's `logs/elections-node<N>.csv` landed on `main` (PR #19) in the format agreed in the PR #12 review, so both can be built now. **Nothing calls the election yet** (P1 still has to wire it into the Submit handler), so real runs give header-only files until then.

New program `crates/loadgen/src/bin/elections.rs`:

```bash
cargo run --release -p loadgen --bin elections -- results/<run_id>/logs/elections-node1.csv results/<run_id>/logs/elections-node2.csv results/<run_id>/logs/elections-node3.csv
cargo run --release -p loadgen --bin elections -- --window-secs 5 <files...>   # narrower time windows
```

Every node logs its own view of every election, so one election is 3 rows. The program groups rows by **(request_id, round)** to count each election once:

```rust
by_key.entry((r.request_id.clone(), r.round)).or_default().push(r);   // group the nodes' rows per election
let winner = /* the winner most nodes reported; ties to the lowest node ID */;
disagreed: votes.len() > 1,                                           // nodes named different winners = split
bid_calls_sent: views.iter().map(|v| v.bid_calls_sent).sum(),         // all nodes' messages for this election
```

What it prints (sample from made-up rows):

```text
elections:   60 (0 re-elections after a winner went down), seen by nodes {1, 2, 3}          # count, re-elections
disagreement: 0 elections where nodes named different winners, 0 split announcements, ...    # split-decision rate
duration (per node view, 180 views):                                                          # each node times its own election
  mean 3.51 ms  p50 3.48 ms  p95 4.80 ms  p99 4.95 ms  max 5.00 ms
  waited the full bid timeout: 0 of 180 (0.0%)                                                # a silent peer not yet marked down
messages: 6.00 bid calls per election on average (min 6, max 6)                               # message overhead
win share (60 decided elections, even split = 33.3%):                                         # the plan's worked example format
  node 1: 27 wins = 45.0% (+11.7 points vs even)
win share per 10 s window:                                                                    # load distribution over time
         window  elections    node 1    node 2    node 3
         0-10 s         20    100.0%      0.0%      0.0%
```

The experiment runner now also runs it on the nodes' election logs and saves `elections-summary.txt`; in lab mode it lists `logs/elections-node<N>.csv` among the files to copy.

5 tests (`cargo test -p loadgen`): P3's example row parses correctly; one election seen by 3 nodes counts once with 6 messages; a split is flagged and the majority winner kept; re-elections (round 1) count separately; the plan's worked example (40 / 35 / 25 wins out of 100).

**Note for P3:** `cargo test -p node csv_row_counts_retries` fails every time on this laptop: `waited_full_t` is `true` where the test expects `false` (election.rs line 1180). Looks timing-sensitive.

---

## 14. Step B, Fri Oct 30: load generator switched to multicast (branch `StepB/p6`)

P1's multicast (PR #23, `ClientMiddleware::connect_all`) is on `main`, and the nodes now run P3's election in Submit. A client that talks to one node no longer works: that node is usually not the least loaded, loses the election, and replies with no plan (tested: 60 of 60 requests failed). So the load generator, the demo and the experiment runner now **multicast by default**.

| Flag (loadgen) | Default | Meaning |
|---|---|---|
| *(none)* | | Multicast: every request to every node in `--config`; first reply with a plan wins; retries keep the request_id |
| `--attempts` | `3` | Tries per request (P1's default) |
| `--attempt-timeout-ms` | `2000` | How long one attempt waits for a plan (P1's default) |
| `--target N` / `--node ADDR` | none | Step A style: one node only, one try (e.g. the mock) |

```rust
clients.push(if single {
    ClientMiddleware::connect(&destination, DEFAULT_TIMEOUT).await?          // Step A: one node, connects now
} else {
    ClientMiddleware::connect_all(&args.config, policy).map_err(|e| anyhow::anyhow!(e))?  // Step B: all nodes, lazy
});
```

- CSV: two new last columns, `attempts` (1 = no retry) and `no_plan_replies` (losers naming the winner). `executor_node` is now the **election winner** (plan task "request CSV gets the winning node from the reply").
- Summary line and `.meta.txt`: `send` (multicast/single), `attempts`, `attempt_timeout_ms`, `retried` (requests with attempts > 1) and `late_plans` (a second plan for a request that already had one; read after waiting one attempt timeout, since P1's client counts them in the background).
- With `connect_all` each client connects on its first request, so that request includes connection setup (about a millisecond locally).
- Demo (`cargo run -p client`): multicast by default, prints attempts and no-plan replies; `--target` / `--node` for one node.
- Experiment files: `target` is now optional (missing = multicast); new optional `attempts`, `attempt_timeout_ms`, and a `[planner_iterations]` table that starts chosen nodes with P2's `--planner-iterations` knob (local mode only). The example files no longer set `target`. New `experiments/loaded-node-local.toml`.

Results on one laptop (3 local nodes, release, 4-stage ramp of 5 s):

```text
969 requests, 969 ok, 0 retried, 0 errors                      # multicast works end to end
elections: 969, 0 splits                                       # one election per request
win share: node 1 32.8%, node 2 31.2%, node 3 36.0%            # equal nodes share the work
```

Loaded-node check (`experiments/loaded-node-local.toml`, node 2 with `--planner-iterations 30000`):

```text
node 1: 35.4%   node 2: 20.0%   node 3: 44.6%                  # node 2 drops from ~33% to 20%
0-10 s: node 2 16.6%   10-20 s: 15.2%   20-30 s: 29.6%           # strongest early; rises again later
```

**Observations for the team:**
- P1: node ports 50051-50053 are inside Windows' range for temporary outgoing ports (49152-65535). Once, node 1 could not bind 50051 because the port was briefly in use. Ports below 49152 would avoid this.
- P1/P3: some nodes log more election rows than requests (e.g. 1056 rows for 969 requests on node 1), i.e. the same (request_id, round) twice on one node, probably once from the client's Submit and once from a peer's bid. The elections program counts each election once, but the per-node duration numbers include the extra rows.
- P3/P2: the loaded node's share drops but does not reach zero, and rises again after ~20 s (possibly because plans become empty and cheap once the starting resources are taken).
- Runs must not overlap: a first attempt picked up 1002 requests from another client sending to the same ports at the same time.

---

## 15. Step B, Fri Oct 30: safeguards in the experiment runner

Two checks run at the end of every experiment. Each problem found is printed as `WARNING: ...` and saved in `WARNINGS.txt` in the run folder, so a later reader of the folder sees it too. No file = no problem found.

| Check | Catches | How |
|---|---|---|
| **A. Foreign traffic** | Another client sending to the same nodes during the run (results mixed) | Every request_id in the nodes' election logs must be in this run's CSV |
| **B. Nothing assigned** | Requests got plans but not one assigned a resource, although fresh nodes start with free ones | Count `assigned = true` among `ok` rows; warn if 0 |

```rust
for id in column_values(log, "request_id") {   // every request a node elected for
    if !sent.contains(&id) {                    // not sent by this run
        foreign.insert(id);                     // counted once, even if all 3 nodes logged it
    }
}
```

Check A needs the election logs, so in lab mode (logs on other machines) only check B runs.

Tested: 4 unit tests (clean run, foreign requests counted once, nothing assigned, a run with only failures is not called "unassigned"), plus two real runs on 3 local nodes: a clean run gave no warnings, and a run where a second load generator sent 40 requests mid-run was reported as "the nodes handled 40 requests that are not in this run's CSV".

---

## 16. Step B, Fri Oct 30: one set of resources for the whole system (TEMPORARY, until P4's DFS)

**Problem:** each node used its own private `InMemoryStore` ([state.rs](../crates/node/src/state.rs), `NodeState::new`), so 3 nodes meant 3 copies of the 10 starting resources. With multicast all 3 nodes win elections, so runs showed **30** resources assigned, and the same resource could go to up to 3 incidents. P1 and P4 confirmed the system must have **10 in total**, also with multicast.

**Fix:** a temporary shared store, `crates/node/src/file_store.rs`, that implements P4's `SharedState` interface on **one folder all nodes use**. When the DFS-backed store exists it replaces this file; the handler, election and load generator don't change.

```text
<shared-state-dir>/
  state.toml                  resources, assignments, incidents that got a resource   (small: at most 10 each)
  requests/<request_id>.toml  one per committed request: the create-if-absent record
  state.lock                  file lock: one node at a time changes the state
```

- `commit` follows P4's InMemoryStore rules inside the lock: request already committed → `AlreadyCommitted`; a planned resource not free → `ResourceTaken`; otherwise mark the resources, then write the request record.
- Files are written to a temporary file and renamed, so nobody reads a half-written file.
- Reading or writing problems (e.g. a network share dropping) come back as `StateUnavailable`, as the interface expects.

| Change | File | Owner |
|---|---|---|
| New shared-folder store + 5 tests | `crates/node/src/file_store.rs` | P6 (new) |
| `--shared-state-dir <dir>` flag; uses the store when given | `crates/node/src/main.rs` | P1 (marked in capitals) |
| `with_store` usable outside tests | `crates/node/src/state.rs` | P1/P4 (marked) |
| `default_resources()` moved out, same constants | `crates/node/src/store.rs` | P4 (marked) |
| `serde`, `toml` dependencies | `crates/node/Cargo.toml` | (already in the workspace) |
| Runner: shared state on by default; fresh folder per run (local), `shared_state_dir` (lab) | `crates/loadgen/src/bin/experiment.rs` | P6 |

Without `--shared-state-dir`, a node behaves exactly as before (its own private copy).

**Tests:** two nodes see each other's assignments; a request committed on one node is found on another; **3 nodes racing 30 commits for 10 resources give out exactly 10, none twice**; an empty plan is stored and takes nothing; a restarted node keeps the shared state.

**Result** (3 local nodes, same short ramp, release):

```text
shared state ON:   960 requests, 10 got a resource, 10 different resources, 0 errors   # one set of 10 ✅
shared state OFF: 1159 requests, 30 got a resource                                    # 3 private copies ❌
```

All 3 nodes still win elections with the shared store (325 / 325 / 310). Throughput was a bit lower in this run (79 vs 96 ok/s), as expected from the file lock.

**Running nodes by hand:** give all 3 nodes the same empty folder, e.g. `cargo run --release -p node -- --id 1 --shared-state-dir shared-state` (and the same for 2 and 3). Empty it before each run, or the resources stay taken from the previous run.

---

## 17. Step B, Fri Oct 30: reliability per scenario

Plan task: "success percentage, failed count and lost-request count per scenario (no failure, one node down, network cut), from the request CSV status column and the fault CSV". Unblocked by P1's client failure handling (PR #23) and P5's fault log.

**Definitions** (the plan asked P6 to confirm what counts as lost):

| Term | Meaning | From the CSV |
|---|---|---|
| success % | requests that got a plan, out of all requests | `status = ok` |
| failed | at least one attempt got no plan | `attempts > 1` or `status != ok` |
| retried | got a plan, but only after a retry (failover worked) | `status = ok` and `attempts > 1` |
| **lost** | **no plan after all attempts** | `status = timeout` or `error` |

**Scenarios.** Each request gets the scenario that was going on while it ran (from sending until its reply), taken from the fault logs:
- **one node down** = overlaps a `MUTE_START`..`MUTE_END` window in P5's `logs/faults-node<N>.csv` (the node hangs and answers nothing);
- **network cut** = overlaps a `CUT_START`..`CUT_END` window in `logs/faults-runner.csv` (the node's program is stopped, so calls fail at once, then restarted with the same shared state);
- **no failure** = neither. `PEER_DOWN`/`PEER_UP` rows are ignored: they say when others *noticed*, not when the fault was.

**New program** `crates/loadgen/src/bin/reliability.rs`:

```bash
cargo run --release -p loadgen --bin reliability -- results/<run>/<run>.csv --faults results/<run>/logs/faults-node1.csv results/<run>/logs/faults-runner.csv
```

**Experiment runner:** a new `[[faults]]` list causes failures on a timer while the load runs, and every run now saves `reliability.txt`.

```toml
[[faults]]
kind = "mute"        # one node down: P5's injector (inject --node 2 --duration 10000ms --wait 5s)
node = 2
at_secs = 10         # seconds after the load starts
duration_secs = 10   # a mute lasts at most 20 s

[[faults]]
kind = "cut"         # network cut: the runner stops node 3's program, then starts it again (local mode only)
node = 3
at_secs = 30
duration_secs = 8
```

Also in the runner: a 1 s wait before stopping the nodes, so the last election rows are written (Farida's review of PR #24).

**Result** (`experiments/reliability-local.toml`, 3 local nodes, 4 clients for 50 s, shared state):

```text
scenario        requests        ok   success  failed  retried   lost        p50        p95
no failure          2362      2362   100.00%       0        0      0   50.05 ms   80.34 ms
one node down        819       819   100.00%       2        2      0   41.63 ms   63.89 ms
network cut          650       650   100.00%       3        3      0   40.96 ms   71.98 ms
all                 3831      3831   100.00%       5        5      0   46.42 ms   77.60 ms
```

**No request was lost** in any scenario: when a node was muted or cut off, the other two took over, and the few requests caught mid-failure (5) got their plan on a retry. Win share per 10 s window shows the failover: node 2 fell to 0.1% while muted (10-20 s) and node 3 to 7.7% while cut off (30-40 s).

Tests (`cargo test -p loadgen`): requests split by the fault they overlap (a request sent just before a fault but still running counts as affected); a fault with no end lasts its planned duration; `PEER_DOWN` rows are not faults; a cut wins over a mute when both overlap; older CSVs without `attempts` still work.

**Observation for P1/P3:** around the faults, the elections log shows a few split decisions (7 elections where nodes named different winners in one run) and loadgen counts ~170 late plans (a second plan for a request that already had one). With the shared store only one plan can be committed per request, so a second plan is the stored one returned again; worth checking with the election design.

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

On the lab machines, use `--release` for real measurements and `--config config/nodes.lab.toml --target <N>` (fill in the IPs in that file first).

## Real-machine run checklist (1000 requests)

1. **Everyone pulls `main`** and runs `cargo build --release` on their machine.
2. **Fill in `config/nodes.lab.toml`**: replace each `REPLACE-ME` with that machine's IPv4 (`ipconfig`). Everyone uses the same file.
3. **Firewall:** allow `node.exe` on private networks on each node machine. Avoid Wi-Fi with client isolation (campus guest Wi-Fi often has it).
4. **Clear old files:** delete the `logs` and `results` folders (both git-ignored; they are recreated).
5. **Start the 3 nodes**, one per machine, with the same run name:
   `cargo run --release -p node -- --id <N> --config config/nodes.lab.toml --run-id lab-1000`
6. **Smoke test** from the client laptop: `cargo run --release -p client -- --config config/nodes.lab.toml --target 1`
7. **Small run:** `cargo run --release -p loadgen -- --config config/nodes.lab.toml --target 1 --requests 50 --run-id lab-50`
8. **The 1000-request run:** `cargo run --release -p loadgen -- --config config/nodes.lab.toml --target 1 --clients 10 --requests 1000 --seed 1 --rate 100 --run-id lab-1000`
9. **Stats:** `cargo run --release -p loadgen --bin stats -- results/lab-1000.csv`
10. **Collect:** `results/lab-1000.csv` + `.meta.txt` from the client laptop, and `logs/node_metrics-node<N>.csv` + `logs/faults-node<N>.csv` from each node machine.

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

## 18. Step C, Sat Oct 31: metric definitions, failover and recovery time, latency around a failure

Plan tasks: "Metric definitions in the design log", "Script for failover time and recovery time from the node fault CSVs and the request CSV", "Latency in three windows (before, during, after a failure) per run".

**Definitions:** new section "P6: Failure metrics (Step C)" at the end of `DESIGN_LOG.md` (failover, detection, recovery, reliability %, lost requests, which failure case a lost request was, clocks). Waiting for review by P5, P3 and P1.

**New program** `crates/loadgen/src/bin/failover.rs`. For every mute or cut in the fault logs it prints:

| Line | Meaning | From |
|---|---|---|
| detected | first time a live node suspected the failed one | `PEER_DOWN` in the live nodes' fault logs |
| answered (failover) | first plan from a live node that could only have come without the failed node: sent after the failure started, or caught by it and saved by a retry | request CSV (`timestamp + latency_us`, `executor_node`, `attempts`) |
| ready (recovery) | **STAND-IN** until P3 logs a readiness event: the first election the returning node wins after it is back | `MUTE_END`/`CUT_END`, then P3's `elections-node<N>.csv` |
| latency before / during / after | p50, p95, max and lost requests; before and after are as long as the failure, and stop at the neighbouring failures | request CSV |

```bash
cargo run --release -p loadgen --bin failover -- results/<run>/<run>.csv --faults results/<run>/logs/faults-node1.csv results/<run>/logs/faults-node2.csv results/<run>/logs/faults-node3.csv results/<run>/logs/faults-runner.csv --elections results/<run>/logs/elections-node1.csv results/<run>/logs/elections-node2.csv results/<run>/logs/elections-node3.csv
```

**Experiment runner:** every run with fault logs now also saves `failover.txt`.

**Result** on the Step B reliability run (`results/reliability-local-1793394717`):

```text
failure 1: node 2 muted, 10.03 s -> 20.03 s (10.00 s)
  detected     11.30 s   detection   1.27 s
  answered     10.35 s   failover    0.32 s
  ready        20.04 s   recovery    0.00 s   STAND-IN
  latency   requests   lost        p50        p95        max
  before        2520      0   16.20 ms   18.81 ms   38.83 ms
  during        2181      0   16.00 ms   18.19 ms 2034.73 ms
  after         2432      0   16.25 ms   18.59 ms   31.34 ms
failure 2: node 3 cut, 29.99 s -> 38.57 s (8.58 s)
  detected     31.49 s   detection   1.49 s
  answered     30.31 s   failover    0.32 s
  ready        38.57 s   recovery    0.00 s   STAND-IN
```

- **Failover 0.32 s, not the plan's 2.6 s:** with multicast every live node gets every request, so the client does not have to wait for its 2 s deadline. The two live nodes wait one bid timeout (300 ms) for the silent node's bid, then elect without it. The 2.6 s case still happens to a few requests (the 2034 ms max "during"): those hit the deadline and were saved by the retry.
- **Recovery 0.00 s** because no readiness rule exists yet: a returning node bids at once. When P3's rule (reload state, then set ready) is in, this number becomes real.
- **Latency:** p50 and p95 are the same before, during and after: a failure only hurts the few requests caught in it (the max), and the system is back to normal right after.

Tests (`cargo test -p loadgen --bin failover`): the plan's worked examples give 1.5 s detection, 2.6 s failover and 1.7 s recovery (with a request elected just before the mute and one answered by the muted node, which must not count); no answer while the node is down is "not measured", never 0; a node that never came back has no recovery; a cut is measured like a mute; latency is split into the three windows; the side windows stop at the neighbouring failures.

## 19. Step C, Sat Oct 31: lost requests explained, and the fail-recover dry run

### Task 4: reliability % and lost requests (`crates/loadgen/src/bin/reliability.rs`)

- Reads **P1's `outcome` column** (`committed` / `duplicate` / `failed`) when the CSV has it; otherwise derives it from `status` and says so. `ok` = committed + duplicate.
- Prints how many of the ok requests were **duplicates** (a retry found the stored plan) and how many got **no resource** (answered, all resources taken: not lost).
- The column is now called **reliability** (was "success").
- **Every lost request is listed** with its request_id, send time, attempts, last status and **failure case**: overlaps a cut → network cut; overlaps a mute → winner died after taking the request; no failure and timed out → slow request; anything else → unexplained (check the node logs). Counts per case first; the first 100 are listed.
- New tests: the plan's worked example as a hand-made CSV (10,000 requests, 38 timeouts) gives **9,962 ok, 99.62%, 38 lost**; the outcome column counts duplicates as ok and failed as lost; each lost request gets the case of the failure it ran in.

### Task 6: dry run (`experiments/fail-recover-local.toml`)

Ramp 1 → 2 → 4 → 8 clients, 15 s each. Node 2 muted at 17 s for 8.4 s (the plan's example length); node 3 cut at 47 s for 8 s, under the heaviest load. One command prints everything (`reliability.txt` and `failover.txt` in the run folder):

```bash
cargo build --release
cargo run --release -p loadgen --bin experiment -- experiments/fail-recover-local.toml
```

Result (`results/fail-recover-local-1793408269`, clean run, no warnings):

```text
reliability 99.99% (7,062 of 7,063), lost 1: network cut (sent 46.96 s, just before the cut, 3 attempts, timeout)
failure 1: node 2 muted 17.08 -> 25.48 s   detection 1.40 s   failover 0.34 s   recovery 21.81 s (STAND-IN)
failure 2: node 3 cut   46.99 -> 55.57 s   detection 1.19 s   failover 0.62 s   recovery 0.00 s (STAND-IN)
latency p50 / p95:  mute  before 17.9 / 34.8 ms, during 15.7 / 23.8 ms, after 17.5 / 36.3 ms
                    cut   before 18.5 / 50.3 ms, during 33.8 / 212.7 ms, after 129.6 / 310.3 ms
```

**Three things the dry run found:**

1. **A restarted node waited ~13 s to start** (an earlier run's cut lasted 21 s instead of 8 s). `FileStore::open` took the shared lock just to check that `state.toml` exists, and under 8 clients the two live nodes held that lock almost all the time. Fixed in `crates/node/src/file_store.rs`: when `state.toml` exists, no lock is taken (it only ever appears whole, by rename). After the fix the cut lasts 8.58 s.
2. **For P3: node 2 won no election for 21.8 s after its mute ended.** The election rows from 25.4 s on hold only the bids of nodes 1 and 3 (node 2's bid is missing), node 1's score stays at 0.89, and node 3 wins 100% of 20-40 s. The fault logs also show `PEER_DOWN`/`PEER_UP` flapping at ~33 s under the heavier stages. Likely the laptop is overloaded (3 nodes and 8 clients on one machine) and heartbeats and bids arrive late. The recovery stand-in reports it honestly as "not taking work", which is why P3's readiness event is the better measure.
3. **Foreign traffic** in the first attempt: 2,262 requests (steady ~85/s) reached the nodes in the 26 s before our load started and took all 10 resources. Nothing else was running afterwards, and the nodes listen on `0.0.0.0`, so a client on the same network can reach them. The safeguards flagged it and the run was discarded.

**Not the plan's 2.6 s failover:** with multicast every live node already has the request, so failover is about one bid timeout (0.3-0.6 s). The 2.6 s case still shows as the ~2 s max latency during a failure (requests that hit the client deadline and were saved by a retry).

## 20. Step C, Sat Oct 31: checks after merging PR #26, and the "foreign traffic" explained

**After merging `main` (PR #26 outcome column, PR #28 P3's test fix) into `StepC/p6`:** everything builds, and every test in the workspace passes (node 134, loadgen 29 across the binaries, client 9). The node test that used to fail is fixed by PR #28. The dry run's CSV now ends with P1's `outcome` column, and `reliability.txt` splits ok into committed and duplicate (for example 2,946 committed + 316 duplicate, 0 lost).

**Observation for P1/P3: duplicates on the first attempt.** Almost all duplicates have `attempts = 1`, so no retry caused them: the same request was committed twice (two nodes, or twice on one node) and the client's first plan was the second, "already stored" reply. This matches P1's own finding in `docs/p1-changes.md` (the same request handled twice). The shared store keeps it to one assignment, so it is not a correctness problem, but it is extra work.

**The "foreign traffic" was our own load generator, copied by the antivirus.** Three runs (one on Oct 31 at 02:51, two today) had 668 to 2,262 requests in the node logs that were not in the CSV, all in the ~26 s before our first request, and they took all 10 resources. What showed the cause:

| Check | Result |
|---|---|
| Incidents of the foreign requests | the same as ours (seed 1: Supply 5, Fire 5, Fire 8, ...): sent by a load generator with our settings |
| Processes connected to the node ports, each second | **two `loadgen.exe`**: one from 15:38:16 to 15:38:38, then the runner's own from 15:38:41 |
| The runner's `loadgen` | started at 15:38:15 but only sent from 15:38:41: held for 26 s |
| Antivirus | **AVG Antivirus** active (Windows Defender off) |
| When it happens | only on the first run after a `cargo build` (a new program) |

AVG holds a newly built program and first runs a sandboxed **copy** of it, with the same arguments, for ~25 s. The copy really reaches `127.0.0.1`, so it sent requests to the nodes. Then the real one ran with the same run ID and overwrote the copy's CSV. (The nodes' 26-43 s slow starts after a build are the same check.)

**Fix in `crates/loadgen/src/bin/experiment.rs`:** before the real run, the runner starts `node`, `loadgen` and `inject` once with `--help`, so AVG checks a harmless run. After a fresh build the next run was clean (no warnings). The first run after a build takes ~50 s longer while AVG checks the `--help` runs. Another option is an AVG exception for the repo's `target` folder, a security setting each person changes themselves. The safeguards (section 15) are what caught this: without them the polluted runs would have looked like "0 resources assigned".
