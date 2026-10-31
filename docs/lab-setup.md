# Lab setup checklist (P1, for Sun Nov 1)

Goal: the 3 nodes run on 3 Linux lab PCs and can reach each other, so the Step C runs
(failure cases, injector, P6's scripts) can start right away. Tick each box on the day.

Unknown until we are in the lab: the distro, whether we have `sudo`, whether home folders
are shared between the PCs, and whether the lab network blocks ports or internet time
servers. Each step says what to do if it does.

## 1. Build on a lab PC (first thing, about 10 minutes)

Our laptops make Windows `.exe` files, which do not run on Linux. A Linux build from WSL
can fail on the lab PCs with `GLIBC_x.yy not found` if their Linux is older. So build on
a lab PC itself. No `sudo` is needed: Rust installs into your home folder.

- [ ] Install Rust (skip if `cargo --version` already works):
  ```
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  source "$HOME/.cargo/env"
  ```
  Rust needs a C linker: `cc --version` should print something. If it does not, ask the
  lab staff for `gcc` (`build-essential`). No protoc is needed (we use protox).
- [ ] Get the code at the commit we will run, and build:
  ```
  git clone https://github.com/GM-Sniper/RescueMesh && cd RescueMesh
  git checkout <commit>            # note it in the run notes
  cargo build --release
  ```
- [ ] **Other two PCs:** if home folders are shared (the repo is already there when you log
  in on another PC), there is nothing to copy; still run each node on its own PC. If not,
  copy the folder over the network or USB:
  ```
  scp -r RescueMesh/target/release/{node,loadgen,inject,stats,experiment} RescueMesh/config  <user>@<ip>:rescuemesh-lab/
  ```
  After a USB copy, make them runnable again: `chmod +x node loadgen inject stats experiment`.
  Add `elections` once PR #24 is merged.

Run every program from the folder that holds `config/`, so `config/nodes.lab.toml` is found.

## 2. On each of the 3 PCs

- [ ] **Find its IP:** `hostname -I` (first address) or `ip -4 addr`. Use the lab network's
  address, not `127.0.0.1` or a `172.17.x.x` Docker one.
- [ ] **Firewall.** Many lab PCs have none. Check (needs `sudo`):
  `sudo ufw status` (Ubuntu) or `sudo firewall-cmd --state` (Fedora/RHEL). If one is on:
  ```
  sudo ufw allow 50051/tcp
  sudo firewall-cmd --add-port=50051/tcp          # firewalld; lasts until reboot, enough for the day
  ```
  No `sudo`: skip it and let step 4's reachability test decide. If that test fails, ask the
  lab staff to open TCP 50051 between the 3 PCs.
- [ ] **Check the clock** (no `sudo` needed):
  ```
  timedatectl                 # want: "System clock synchronized: yes"
  chronyc tracking            # if chrony is installed: "System time" = offset from the time server
  ```
  Write the offset down for each PC. Under 0.1 s on all 3 is fine. Not synchronized and no
  `sudo`: write down `date +%s.%N` on all 3 at the same moment as a rough check, and state
  the clock offset as a limitation in the report.
  **Why:** the nodes never use clocks to decide anything, but P5's and P6's scripts compare
  timestamps from different machines (fault CSV, election CSV, request CSV). One team laptop
  was 4.38 s off on Oct 29, which would make failover times meaningless.

## 3. Fill in the config (once, then the same file everywhere)

- [ ] In `config/nodes.lab.toml`, replace each `REPLACE-ME` with that node's IP; keep `:50051`.
- [ ] The **same file** on all 3 PCs and on the client machine. Only `--id` differs.
- [ ] Commit the filled-in file to the repo, so every run says which IPs it used.

**Where the client runs:** on a lab PC (a 4th one if there is one, otherwise next to a node).
A laptop on the campus Wi-Fi is often on a different network from the lab PCs and cannot
reach them; use it only if step 4's reachability test passes from it.

## 4. Smoke test (about 10 minutes)

Use one run ID for everything, so P6 can join the files, e.g. `lab-nov1-smoke`.

- [ ] **Start the nodes**, one per PC:
  ```
  ./node --id N --config config/nodes.lab.toml --run-id lab-nov1-smoke
  ```
  Each prints `node listening` with its peers. A node that says `addr ... is not ip:port`
  still has a `REPLACE-ME` in its config.
- [ ] **Reach each node from the client machine** (works without extra tools):
  ```
  for ip in <ip1> <ip2> <ip3>; do timeout 2 bash -c "</dev/tcp/$ip/50051" && echo "$ip open" || echo "$ip BLOCKED"; done
  ```
  `BLOCKED` means firewall or wrong IP.
- [ ] **Failure detector across machines:** mute node 2 for 5 s:
  ```
  ./inject --node 2 --duration 5s --config config/nodes.lab.toml
  ```
  Nodes 1 and 3 should log `peer suspected down` within about 1.6 s and `peer alive again`
  about 0.2 s after the 5 s end. This is the only check that proves heartbeats flow **both
  ways** between every pair. Repeat with nodes 1 and 3. (P3's check, handoff 6.)
- [ ] **Requests end to end:**
  `./loadgen --config config/nodes.lab.toml --requests 30 --clients 3 --rate 10 --run-id lab-nov1-smoke`.
  All 30 should be `committed`, once loadgen uses `connect_all` (P6).
- [ ] **Collect the logs** (`logs/*.csv` from each node PC, `results/` from the client) into
  one folder per run, as the experiment runner prints in lab mode. `scp` works if SSH is on.

## 5. Then the Step C runs

- [ ] P1: the three client failure cases with a real mute (Step C plan, P1's table).
- [ ] P6: the automatic injector, `[faults] enabled = true` with the same seed on all 3.
- [ ] P3: round-trip times between the PCs, to confirm the 300 ms bid timeout (P3's Q6).
  `ping -c 20 <ip>` from each PC to the others; the `avg` and `max` on the last line.

Stop a node with Ctrl+C. Never kill programs on a lab PC that are not ours.

## 6. Real failures: network cable out, PC switched off

The supervisor may test these by hand, so we test them first. Both run under load
(loadgen at about 10 requests/s from the client machine), each on a different node.
For each one, write down the times (fault, suspected, back) and the loadgen result.

**A. Network cable out for about 10 s, then back in.** The PC keeps running but nobody can
reach it. This is what our mute imitates.
- [ ] The other two log `peer suspected down` within about 1.5 s and keep serving.
- [ ] No request `failed`; requests the cut-off node was handling come back after one retry (about 2 to 2.5 s).
- [ ] After the cable is back, the others log `peer alive again` within a few seconds.
  **If it takes much longer** (tens of seconds or minutes), a dead connection is lingering:
  tell P1 (fix: a keep-alive setting on the connections).
- [ ] The node does not win elections until P3's rejoin rule passes.

**B. PC switched off (power cable out, or `sudo poweroff -f`), then booted again.** The node
loses everything in memory.
- [ ] The other two log `peer suspected down` within about 1.5 s and keep serving.
- [ ] No request `failed` (same as A).
- [ ] After the boot, start the node with the same command and run ID; the others log
  `peer alive again`.
- [ ] The node reloads the shared state before taking work (P3's rejoin rule with P5's
  reload call): it must not assign a resource that is already assigned.
- [ ] P6: failover time comes from the other nodes' `PEER_DOWN` rows and the client CSV,
  because the switched-off node cannot log its own failure.

**C. Shared state survives one PC switched off (P5, before Nov 1 if possible).** A DFS only
survives a machine failure if it is set up for it:
- [ ] Replication keeps **2 copies** of every file on different machines (SeaweedFS keeps one
  by default, to be confirmed; e.g. replication `001` or `010`).
- [ ] The masters run on all 3 PCs (they need 2 of 3 up).
- [ ] The S3 gateway (the only create-if-absent path that passed P4's spike) is reachable
  with any one PC off, not only from one machine.
- [ ] With one PC off: every saved plan can still be read, and P4's create-if-absent check
  (`spike/seaweedfs/run_checks.py`) still gives exactly one winner.

Until the DFS is in, each node has its own in-memory store: test B then **loses that node's
data**, and the report must say so.

## If something fails

| Symptom | Likely cause | Fix |
|---|---|---|
| `BLOCKED` in the reach test | Firewall, or the wrong IP | Step 2 firewall; check `hostname -I` on that PC |
| A node never sees a peer, but the peer sees it | Firewall open on one side only | Open TCP 50051 on the silent side |
| Every node sees every peer as down | Different config files, or the PCs are isolated from each other | Same file on all; ask the lab staff |
| `Permission denied` starting a program | Lost the run permission in the copy | `chmod +x` (step 1) |
| `GLIBC_... not found` | Built on a newer Linux | Build on a lab PC (step 1) |
| Failover times are negative or seconds off | Clocks not synced | Step 2 clock check |
