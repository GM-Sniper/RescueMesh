# RescueMesh demo: cluster, real requests, load, fault injection, crash, fault log

About 5 minutes. Shows the 3-node cloud handling real emergency requests end to end, staying up while a node fails, detecting a mute and a real crash, and the measurements the experiments will use.

## Run it
From the repo root, in PowerShell:
```
powershell -ExecutionPolicy Bypass -File demo\run-demo.ps1
```
Three node windows open (titled "RescueMesh node 1/2/3"). Put them side by side, keep this console visible, and press Enter between steps. The script stops all nodes at the end, even if interrupted.

To check that it works before showing it (no windows, no pauses, prints the logs at the end):
```
powershell -ExecutionPolicy Bypass -File demo\run-demo.ps1 -Auto
```
The first run builds in release mode and takes a minute or two; run `-Auto` once beforehand so the real demo starts at once.

## What to say at each step
**Step 1: three nodes start.**
"These are three identical cloud nodes, no master. Each one reads the same config file, finds itself by its ID, and treats the other two as peers. Every node sends its own heartbeat to each peer twice a second. When nothing is printed, everyone is healthy."

**Step 2: a real request.**
"A client sends five emergencies to node 1. For each one, node 1 checks the request, takes a snapshot of the free resources, runs our planner to pick the best one, stores the plan in its log, and replies, in about a millisecond." Point at node 1's window: one `submit applied assigned=1` line per request. "Now the client sends the first request again with the same ID, as it would after a timeout. It gets the stored plan back, marked `duplicate=true`: nothing is planned or stored twice."

**Step 3: load.**
"Ten clients now send a thousand requests as fast as they can." Read out the summary: all `ok`, all executed by node 1, p50 and p99 latency. "Only ten resources exist and none are released in Step A, so after the first ten, requests get an empty plan: 'nothing free'. That is correct behaviour, not an error." If node 1's window shows `resource taken meanwhile; planning again`: "Two requests planned the same resource at the same moment. The store re-checks under its lock, the loser plans again on fresh data, so a resource is never given twice."

**Step 4: a fault while serving.**
"The fault injector tells node 2 to go silent for 6 seconds, and at the same time node 1 serves 500 more requests. Nobody tells nodes 1 and 3." Point at windows 1 and 3: "After about 1.5 seconds, three missed heartbeats, both mark node 2 as suspected down, each on its own." Then the summary: "And node 1 answered every request: a failed peer does not stop the service. When the 6 seconds are over, node 2 comes back by itself."
If asked why it does not just return an error: "A crashed machine never says it is down. If it answered with an error, the others would know instantly, and our detection and failover measurements would be meaningless."

**Step 5: a real crash.**
"Now the process of node 3 is killed, a real crash. Same result: detected after about 1.5 seconds." After the restart: "Its own heartbeats announce it, so the others see it again within a fraction of a second."

**Step 6: the fault log.**
"Every node writes its own fault log: when it was muted, and when it noticed a peer go down or come back. The script lines them up on one timeline and computes the detection time: about 1.3 to 1.4 seconds, for the mute and for the crash. These files, together with the load generator's CSV, are what our experiments and graphs are built from."

**Step 7: tests.**
"Everything shown is also covered by about a hundred automated tests: the request path with the real store and planner, including 20 simultaneous requests for 10 resources that never double-book; the mute behaviour on a simulated clock; a test with three real nodes where one is muted and comes back; and the broadcast helper the election will use in Step B."

## Likely questions
| Question | Answer |
|---|---|
| Is there an election? | Not yet. In Step A the client always talks to node 1, so node 1 executes every request. In Step B every node receives the request, they bid with their current load, and the least-loaded node executes it (Bully-style election). The broadcast with deadlines and retries it needs is already built and tested. |
| What if node 1 itself fails? | In Step A, its clients time out, which is exactly the problem Step B solves: the other nodes elect a new executor, and replication means they already have the data. |
| What does the library do, and what is yours? | gRPC (tonic) only carries the bytes. The message formats, request handling, heartbeats, failure detection, deadlines, mute hook, broadcast and retry policy are our own code. We deliberately do not use gRPC's health checks, retries or load balancing. |
| Why do most requests get an empty plan? | Only 10 resources exist, and Step A never releases them. Restart the nodes to get them back. |
| What if two requests want the same resource? | Both plan on their own snapshot; the store re-checks under its lock when saving, so the second one sees the resource is gone and plans again (up to 3 times, then the client is told to retry). Holding the lock during planning would make every request wait for every other one. |
| What if the same request arrives twice? | Every request carries a client-generated ID. A known ID gets the stored plan back (`duplicate=true`); nothing is planned twice. |
| Why 1.5 seconds? | 3 missed heartbeats at 500 ms. Shorter means faster failover but false alarms on a slow network; longer means fewer false alarms but slower failover. It is in the config, so the experiments can try other values. |
| What if a node is slow, not dead? | It can be wrongly suspected, then seen alive again at its next heartbeat. The detector is complete (a dead node is always detected) but not perfectly accurate; we measure the false alarms. |
| What happens to requests during a mute? | The muted node holds them and answers nothing; the client's own timeout ends them, and a retry with the same request ID returns the same stored plan. |
| Can heavy planning delay heartbeats? | No: the planner runs on separate threads, so heartbeats stay on time however busy the node is. |

## If the script does not work
Run each step by hand, each command in its own terminal, from the repo root:
```
cargo run --release -p node -- --id 1 --run-id demo
cargo run --release -p node -- --id 2 --run-id demo
cargo run --release -p node -- --id 3 --run-id demo
cargo run --release -p client -- --count 5
cargo run --release -p loadgen -- --requests 1000 --run-id demo-load
cargo run --release -p injector -- --node 2 --duration 6s
```
Stop a node with Ctrl+C to show a crash; start it again to show the recovery. The fault rows are in `logs\faults-node<N>.csv`, the load results in `results\`. Restart the nodes to free the 10 resources again.
