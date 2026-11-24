# Lab: the 3 client PCs

The lab run uses 6 Linux PCs: **3 node PCs** (PC1, PC2, PC3: the cloud nodes and SeaweedFS)
and **3 client PCs** (A, B, C: they send requests, like the mobile clients in the design).
This file is for the client PCs. The node PCs follow `docs/lab-bringup.md` (P5) and
`docs/lab-setup.md` (P1) first; start here once the node PCs pass their smoke test.

**What a client PC does:** runs `loadgen` (requests to all 3 nodes, with retries) and, on
client A only, `inject` (faults). It runs no node and no SeaweedFS, and it only opens
connections *out*, so it needs no firewall changes. It only needs to reach port 50051 on the
3 node PCs.

Every command is run from the `RescueMesh` folder.

---

## Part 1: set up each client PC (about 10 minutes)

**1. Get the code at the same commit as the node PCs**, and build only the two programs a
client needs (faster than a full build):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y   # skip if cargo works
source "$HOME/.cargo/env"
git clone https://github.com/GM-Sniper/RescueMesh && cd RescueMesh
git checkout <commit the node PCs use>        # PC1's `git log -1 --oneline`
cargo build --release -p loadgen -p injector
```
If home folders are shared with the node PCs, the build is already there: skip this step.

**2. Copy the filled-in config from PC1** (the one with the 3 real IPs):
```bash
scp <user>@<PC1-ip>:RescueMesh/config/nodes.lab.toml config/
grep -n "addr" config/nodes.lab.toml          # 3 real IPs with :50051, no REPLACE-ME
```
It must be the **same file** as on the node PCs. A USB stick works too.

**3. Set the 3 node IPs in this terminal** (the same ones as in the config), then check that
this client PC reaches every node:
```bash
IP1=<PC1-ip>; IP2=<PC2-ip>; IP3=<PC3-ip>
for ip in $IP1 $IP2 $IP3; do timeout 2 bash -c "</dev/tcp/$ip/50051" && echo "$ip open" || echo "$ip BLOCKED"; done
```
All three must say `open`. `BLOCKED` = that node is not running, its firewall blocks 50051,
or this client PC is on another network (ask the lab staff).

**4. Check the clock** and write the offset down next to the node PCs' offsets:
```bash
timedatectl | grep synchronized
chronyc tracking 2>/dev/null | grep "System time"
```
Failover times compare this PC's CSV with the node PCs' logs, so the offset matters here too.
Under 0.1 s is fine.

**5. Agree who is A, B and C.** Each letter gets its own seed (so the 3 PCs send different
incidents) and its own results folder. `scripts/lab-client.sh` does this; never run two
client PCs with the same letter.

---

## Part 2: smoke test (each client PC, one at a time)

With the 3 nodes running (any run ID), on client A, then B, then C:
```bash
./scripts/lab-client.sh A smoke 30 10         # B and C: change the letter
```
The last line must say `30 ok`, `0 timeout`, `0 error`. Then reset the shared state (from any
node PC: `curl -X DELETE http://127.0.0.1:8333/rescuemesh`) and restart the nodes before the
first real run.

---

## Part 3: one run, all 6 PCs

**1. Pick a run ID**, for example `lab-c1`. Every PC uses the same one.

**2. Node PCs:** reset the shared state, then start each node with that run ID
(`docs/lab-bringup.md`, "Between runs" and step 16). Wait until all 3 logged `rejoin: ready`.

**3. Client PCs:** one person says "go", and all three press Enter together:
```bash
./scripts/lab-client.sh A lab-c1 600 10       # client A
./scripts/lab-client.sh B lab-c1 600 10       # client B
./scripts/lab-client.sh C lab-c1 600 10       # client C
```
600 requests at 10/s each = 60 s, 30 requests/s in total over the 3 PCs. The rate is per
client PC; the totals add up.

**4. The fault, from client A only**, in a second terminal, about 15 s after "go":

| Test | What to do | What should happen |
|---|---|---|
| Node down (mute) | `./target/release/inject --node 2 --duration 8s --config config/nodes.lab.toml` | The other two log `peer suspected down` within about 1.6 s. No request `failed`; one caught mid-plan comes back after a retry, about 2 to 2.6 s |
| Network cable out | Pull PC2's network cable for about 10 s, then plug it back | Same as the mute (`lab-setup.md` 6A) |
| PC switched off | Switch PC2 off, then on; restart its node with the same run ID | Same, but see "Known problems" below (`lab-setup.md` 6B) |

Write down the time of each fault (`date +%T` when you do it).

**5. When all 3 clients have finished**, check the shared state **before the next reset**. Copy
the 3 client CSVs to one PC, for example client A:
```bash
scp <user>@<clientB-ip>:RescueMesh/results/clientB/lab-c1.csv results/clientB/
scp <user>@<clientC-ip>:RescueMesh/results/clientC/lab-c1.csv results/clientC/
python3 scripts/check-bucket.py --endpoint http://$IP1:8333 results/client*/lab-c1.csv
```
`--endpoint` needs port 8333 on PC1 open to client A. If it isn't, copy the 3 CSVs to PC1 and
run the same command there without `--endpoint`. You want `ALL CHECKS PASS`:
- every request a client got a plan for is saved exactly once;
- nothing else is in the bucket (it was reset before the run);
- no resource is in two plans.

**6. Collect everything** into one folder named after the run ID:
- each client PC: `results/client<X>/lab-c1.csv` and `lab-c1.meta.txt`;
- each node PC: `logs/faults-nodeN.csv`, `logs/elections-nodeN.csv`,
  `logs/requests-nodeN.csv`, `logs/node_metrics-nodeN.csv`;
- the fault times you wrote down and the clock offsets.

---

## Reading a client's result

The last line of each client, for example
`done in 60.0 s: 600 ok (590 with nothing assigned, 2 retried), 0 timeout, 0 error, 0 late plans`:

| Field | Meaning |
|---|---|
| `ok` | Got a plan: committed or duplicate. Reliability = ok / requests |
| `with nothing assigned` | Got a plan with no resource, because all resources are taken (issue #36: nothing frees them yet). Answered, so not lost |
| `retried` | Needed more than one attempt (a fault caught it). Their latency is the failover time |
| `timeout`, `error` | **Lost**: no plan after 3 attempts. Each one needs an explanation for the report |
| `late plans` | Plans that arrived after the first one; ignored, as designed |

In the CSV, `outcome` is `committed`, `duplicate` or `failed`. A `duplicate` on a first attempt
is fine: a slower node answered from the shared state.

---

## Known problems (expected, not a client fault)

| What you may see | Why | Owner |
|---|---|---|
| Almost every plan has nothing assigned after the first 10 | Resources are never freed | P2, issue #36 |
| With one PC switched off, some requests time out | P5 found that while one PC's SeaweedFS is stopped, about 30% of saved objects cannot be read from the others | P5 |
| A client sees `BLOCKED` for one node only | That node PC's firewall, or its node is not running | Node PC |
| Every request `timeout` on one client PC only | Its config differs from the node PCs' (an old or unfilled copy) | Copy PC1's `config/nodes.lab.toml` again (Part 1, step 2) |
| `check-bucket.py`: "no saved request outside this run" FAILS | The bucket was not reset before the run, or a client used another run's settings | Reset, restart the nodes, run again |

Never stop programs on a lab PC that are not ours.
