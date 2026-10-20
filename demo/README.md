# P1 demo: cluster, fault injection, failure detection

About 3 minutes. Shows the 3-node cloud, a node told to go silent, a real crash, and the automated tests.

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

## What to say at each step
**Step 1: three nodes start.**
"These are three identical cloud nodes, no master. Each one reads the same config file, finds itself by its ID, and treats the other two as peers. Every node sends its own heartbeat to each peer twice a second. When nothing is printed, everyone is healthy."

**Step 2: fault injection.**
"This is the fault injector. It tells node 2 to go silent for 8 seconds. Node 2 confirms, then stops answering and stops sending, like a machine cut off from the network. Nobody tells nodes 1 and 3." Point at windows 1 and 3: "After about 1.5 seconds of silence, three missed heartbeats, both mark node 2 as suspected down, each on its own. When the 8 seconds are over, node 2 comes back by itself and both see it alive again."
If asked why it does not just return an error: "A crashed machine never says it is down. If it answered with an error, the others would know instantly, and our detection and failover measurements would be meaningless."

**Step 3: a real crash.**
"Now the process of node 3 is killed, a real crash. Same result: detected after about 1.5 seconds." After the restart: "Its own heartbeats announce it, so the others see it again within a fraction of a second."

**Step 4: tests.**
"Everything shown is also covered by automated tests: the mute behaviour on a simulated clock, a test with three real nodes where one is muted and comes back, and the broadcast helper the election will use in Step B: one silent peer does not slow down the others, and failed calls get at most one quick retry."

## Likely questions
| Question | Answer |
|---|---|
| What does the library do, and what is yours? | gRPC (tonic) only carries the bytes. The message formats, heartbeats, failure detection, deadlines, mute hook and broadcast are our own code. We deliberately do not use gRPC's health checks, retries or load balancing. |
| Why 1.5 seconds? | 3 missed heartbeats at 500 ms. Shorter means faster failover but false alarms on a slow network; longer means fewer false alarms but slower failover. It is in the config, so the experiments can try other values. |
| What if a node is slow, not dead? | It can be wrongly suspected, then seen alive again at its next heartbeat. The detector is complete (a dead node is always detected) but not perfectly accurate; we measure the false alarms. |
| What happens to requests during a mute? | They get no answer; the client's own timeout ends them, and a retry with the same request ID returns the same stored plan instead of planning twice. |
| What is not done yet? | Connecting requests to the planner and the store (task 7), so a client request returns a real plan; then the election and replication in Step B. |

## If the script does not work
Run each step by hand, each node in its own terminal:
```
cargo run -p node -- --id 1
cargo run -p node -- --id 2
cargo run -p node -- --id 3
cargo run -p injector -- --node 2 --duration 8s
```
Stop a node with Ctrl+C to show a crash; start it again to show the recovery.
