# RescueMesh Design Log

## P2: Domain Types and Planner v1

### 1. Resource Match Scores

- Exact match: 1.0
- Partial match: 0.5
- No match: 0.0

### 2. Incident Age

Incident age is measured in minutes and contributes to the incident priority score.

### 3. Tie-Breaking

When resources receive equal scores, the planner selects the resource with the lowest resource ID to ensure deterministic results.

### 4. Assignment Policy

Planner v1 assigns at most one resource to each incident.

### 5. Determinism

Generators use a seed to produce repeatable outputs, and planner tie-breaking is deterministic.

## P3: Load Metric (LoadCollector)

Code: `crates/common/src/load.rs` (formula, config, `LoadReport`), `crates/node/src/load.rs` (sampling). Weights live in the `[load]` table of `config/nodes.toml`.

| Decision | Alternatives | Why |
|---|---|---|
| `load_score = 0.5 × queue + 0.3 × CPU + 0.2 × service time`, each normalized to 0-1 (queue / 10, CPU / 100 %, service / 100 ms, capped at 1) | Queue length only; CPU only | Queue length best predicts how long a new request waits. CPU catches load that the queue misses (the CPU-heavy planner, other programs on a lab machine). Service time catches a machine that is simply slower. Weights are in the config so stress tests can change them without rebuilding |
| Divide by the sum of the weights | Require weights to add up to 1 | The score stays in 0-1 even after someone edits one weight; with the default weights it changes nothing |
| Queue length is read **live** when a report is asked for; only CPU is sampled every 200 ms | Sample all three every 200 ms | With a 200 ms sample, requests arriving in the same 200 ms would all see the same scores and all go to the same node. The live counter is one atomic read, so it costs nothing |
| CPU sampled on its own OS thread | A tokio task | A busy async runtime (heavy planner) would delay a tokio task exactly when load is high. The thread keeps sampling on time |
| CPU sampled every 200 ms (never faster) | 50 ms; 1 s | sysinfo needs ≥ 200 ms between two readings to compute a correct percentage. 1 s would make the score lag a burst of requests |
| Whole-machine CPU | This process's CPU only | Another program using the machine also slows our planner down, so it should count against the node |
| Service time: EWMA with α = 0.2, starting at the first request's time | Plain average; last request only | A plain average never forgets old requests; the last request alone jumps around. α = 0.2 follows roughly the last 5 requests. Starting from the first value avoids a slow climb from 0 |
| Service time measured by P1's in-flight guard, from handler start to end | Timing only the planner call | The guard already wraps every Submit and runs even on early returns; a client feels the whole handler time, not only the planner's |
| NaN inputs count as 0 | Let NaN through | A NaN bid compares unequal to every other bid and would break the "lowest score wins" rule |

Checked: the plan's worked example (queue 4, CPU 50 %, service 20 ms) gives 0.39 in a unit test. Under artificial load (every core busy), sampled CPU rose from about 47 % to 99.7 % (`cargo test -p node -- --ignored --nocapture`). Live numbers: `RUST_LOG=info,node::load=debug cargo run -p node -- --id 1`.

**For P6 (node_metrics):** call `state.load_report()` once per second and log `queue_len`, `cpu_pct`, `service_ms`, `load_score`.
