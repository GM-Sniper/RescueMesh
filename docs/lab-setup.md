# Lab setup checklist (P1, for Sun Nov 1)

Goal: the 3 nodes run on 3 real machines and can reach each other, so the Step C runs
(failure cases, injector, P6's scripts) can start right away. Commands are for Windows
PowerShell; Linux lines are given where they differ. Tick each box on the day.

Unknown until we are in the lab: the machines' OS, whether we have admin rights, and
whether the lab network blocks ports or internet time servers. Each step says what to do
if it does.

## 1. Before the lab (Sat Oct 31)

- [ ] **Build once, on one Windows laptop:** `cargo build --release`. Building on each lab
  machine needs Rust and the C++ build tools there, and takes minutes.
- [ ] **Copy to a USB stick** (one folder, same layout on every machine):
  ```
  rescuemesh-lab/
    node.exe  loadgen.exe  inject.exe  stats.exe  experiment.exe   (from target/release/; plus elections.exe once PR #24 is merged)
    config/nodes.lab.toml
  ```
  The programs read `config/...` relative to the folder you run them from, so always run
  them from inside `rescuemesh-lab`. Linux machines need a Linux build instead.
- [ ] Bring the repo at the commit we will run, and note that commit in the run notes.

## 2. On each of the 3 machines

- [ ] **Copy the folder** to the machine and open PowerShell inside it.
- [ ] **Find its IP:** `ipconfig`, the `IPv4 Address` of the adapter on the lab network
  (not `127.0.0.1`, not a `169.254.x.x` or virtual adapter). Linux: `ip -4 addr`.
- [ ] **Open port 50051** (needs admin, once per machine):
  ```
  New-NetFirewallRule -DisplayName "RescueMesh node" -Direction Inbound -Protocol TCP -LocalPort 50051 -Action Allow -Profile Any
  ```
  Linux: `sudo ufw allow 50051/tcp`. A port rule keeps working if `node.exe` is rebuilt or
  moved. No admin rights: the first `node.exe` start shows a Windows prompt; ask the lab
  staff to allow it. If that is impossible, the machine cannot run a node.
- [ ] **Check the clock** (no admin needed):
  ```
  w32tm /stripchart /computer:time.windows.com /samples:3 /dataonly
  ```
  It prints this machine's offset from internet time, e.g. `+04.38s` (a team laptop on
  Oct 29). Write the offset down. Under 0.1 s on all 3 is fine. If larger, resync (admin):
  `w32tm /resync /force`, then check again. Linux: `chronyc tracking` or `timedatectl`.
  If the lab blocks internet time: write down the offsets from step 4 instead and state
  them as a limitation in the report.
  **Why:** the nodes never use clocks to decide anything, but P5's and P6's scripts
  compare timestamps from different machines (fault CSV, election CSV, request CSV). A 4 s
  error would make failover time meaningless.

## 3. Fill in the config (once, then copy to all machines)

- [ ] In `config/nodes.lab.toml`, replace each `REPLACE-ME` with that node's IP; keep `:50051`.
- [ ] Copy the **same file** to all 3 machines and to the client machine. Only `--id` differs.
- [ ] Commit the filled-in file to the repo, so every run says which IPs it used.

## 4. Smoke test (about 10 minutes)

Use one run ID for everything, so P6 can join the files, e.g. `lab-nov1-smoke`.

- [ ] **Start the nodes**, one per machine:
  ```
  .\node.exe --id N --config config/nodes.lab.toml --run-id lab-nov1-smoke
  ```
  Each prints `node listening` with its peers. A node that says `addr ... is not ip:port`
  still has a `REPLACE-ME` in its config.
- [ ] **Reach each node from the client machine:**
  `Test-NetConnection <node IP> -Port 50051` should print `TcpTestSucceeded : True` for all
  3. `False` means firewall or wrong IP.
- [ ] **Failure detector across machines:** mute node 2 for 5 s:
  ```
  .\inject.exe --node 2 --duration 5s --config config/nodes.lab.toml
  ```
  Nodes 1 and 3 should log `peer suspected down` within about 1.6 s and `peer alive again` about 0.2 s after
  the 5 s end. This is the only check that proves heartbeats flow **both ways** between
  every pair. Repeat with nodes 1 and 3. (P3's Friday check, handoff 6.)
- [ ] **One request end to end:** from the client machine,
  `.\loadgen.exe --config config/nodes.lab.toml --requests 30 --clients 3 --rate 10 --run-id lab-nov1-smoke`.
  All 30 should be `committed`, once loadgen uses `connect_all` (P6).
- [ ] **Copy the logs** (`logs/*.csv` from each node machine, `results/` from the client)
  into one folder per run, as the experiment runner prints in lab mode.

## 5. Then the Step C runs

- [ ] P1: the three client failure cases with a real mute (Step C plan, P1's table).
- [ ] P6: the automatic injector, `[faults] enabled = true` with the same seed on all 3.
- [ ] P3: round-trip times between the machines, to confirm the 300 ms bid timeout (P3's Q6).

## If something fails

| Symptom | Likely cause | Fix |
|---|---|---|
| `TcpTestSucceeded : False` | Firewall, or the wrong IP | Re-run the firewall rule; check `ipconfig` on that machine |
| A node never sees a peer, but the peer sees it | Firewall open on one side only | Open port 50051 on the silent side |
| Every node sees every peer as down | Different config files, or the lab isolates machines from each other (client isolation on Wi-Fi) | Same file on all; use wired or a phone hotspot as a last resort |
| Failover times are negative or seconds off | Clocks not synced | Step 2 clock check |
