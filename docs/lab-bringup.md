# Lab bring-up: 3 PCs from zero to a working cluster

Follow this top to bottom on the lab PCs. Every command is copy and paste.
Background and reasons for each step: [`lab-setup.md`](lab-setup.md) (P1's checklist).
Shared-state design: `DESIGN_LOG.md`, "P5: Shared-state backend".

**Words used here**
- **Each PC**: do it on all 3 node PCs.
- **One PC**: do it once, on any of them.
- **PC1, PC2, PC3**: the PCs that run node 1, node 2 and node 3.
- **Client machine**: where `loadgen` and `inject` run. A 4th lab PC if there is one,
  otherwise any of the 3.

Write each PC's IP on paper as you go. You need all three in Part 3.

**Time:** about 45 minutes the first time, with 2 to 3 people.

---

## Part 1: build the code (each PC, about 10 min)

**1. Install Rust** (skip if `cargo --version` already works):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
cc --version          # must print something; if not, ask lab staff for gcc
```

**2. Get the code and build:**
```bash
git clone https://github.com/GM-Sniper/RescueMesh && cd RescueMesh
git log -1 --oneline          # write this commit down for the run notes
cargo build --release
```
If home folders are shared between the PCs (the repo is already there when you log in on
another PC), build once. Still run each node on its own PC.

---

## Part 2: network basics (each PC, about 5 min)

**3. Find this PC's IP** and write it down as PC1, PC2 or PC3:
```bash
hostname -I           # first address; NOT 127.x or 172.17.x
```

**4. Open the firewall.** Only needed if the first command says `active`:
```bash
sudo ufw status
for p in 50051 9333 19333 8080 18080 8888 18888 8333; do sudo ufw allow $p/tcp; done
```
No `sudo`? Skip it; step 9 and step 16 show whether the ports are open. If they are blocked,
ask the lab staff to open those TCP ports between the 3 PCs.

**5. Check the clock.** You want `System clock synchronized: yes`. Write the offset down:
```bash
timedatectl
chronyc tracking 2>/dev/null | grep "System time"
```
Under 0.1 s on all 3 is fine. Resource claims expire after 10 s by the wall clock, and the
failover-time measurements compare clocks across PCs, so a large offset breaks both.

---

## Part 3: fill in the config (one PC, then copy to the others)

**6. Set the three IPs in this terminal** (replace the example addresses with the real ones):
```bash
IP1=10.0.0.11; IP2=10.0.0.12; IP3=10.0.0.13
```
Every later step that uses `$IP1`, `$IP2` or `$IP3` needs this line run first in that
terminal. Run it again on each PC and in each new terminal.

**7. Put the IPs into the node config:**
```bash
awk -v a="$IP1" -v b="$IP2" -v c="$IP3" '/REPLACE-ME/{n++; sub(/REPLACE-ME/, n==1?a:n==2?b:c)} 1' \
  config/nodes.lab.toml > /tmp/lab.toml && mv /tmp/lab.toml config/nodes.lab.toml
grep -n -B1 "addr" config/nodes.lab.toml
```
Check the output: `id = 1` must have PC1's IP, `id = 2` PC2's, `id = 3` PC3's, each with
`:50051`.

**8. Put the same IPs into the SeaweedFS check config:**
```bash
sed -i "s/IP1/$IP1/g; s/IP2/$IP2/g; s/IP3/$IP3/g" spike/seaweedfs/cluster.real.toml
grep -n "http" spike/seaweedfs/cluster.real.toml
```

**9. Get the same two files onto the other 2 PCs.** If home folders are shared, there is
nothing to do. Otherwise, from this PC:
```bash
for ip in $IP2 $IP3; do
  scp config/nodes.lab.toml <user>@$ip:RescueMesh/config/
  scp spike/seaweedfs/cluster.real.toml <user>@$ip:RescueMesh/spike/seaweedfs/
done
```
Replace `<user>` with the lab login. A USB stick works too. All 3 PCs (and the client
machine) must have **the same** `config/nodes.lab.toml`.

---

## Part 4: check the PCs can reach each other (one PC)

**10. Ping the other two PCs** (from PC1):
```bash
for ip in $IP2 $IP3; do ping -c 3 $ip | tail -1; done
```
No reply means the PCs cannot see each other. Stop and ask the lab staff.

---

## Part 5: SeaweedFS, the shared state (each PC, then one check)

**11. Download SeaweedFS 4.48** (each PC, inside the `RescueMesh` folder):
```bash
curl -sSLO https://github.com/seaweedfs/seaweedfs/releases/download/4.48/linux_amd64.tar.gz
curl -sSLO https://github.com/seaweedfs/seaweedfs/releases/download/4.48/linux_amd64.tar.gz.md5
md5sum linux_amd64.tar.gz; cat linux_amd64.tar.gz.md5      # the two hashes must match
tar xzf linux_amd64.tar.gz && ./weed version                # must say 4.48
env | grep AWS_                                             # must print NOTHING
```
If `env | grep AWS_` prints anything, run `unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN`.
With those set, SeaweedFS turns on S3 auth and every node call gets HTTP 403.

**12. Start SeaweedFS on all 3 PCs within about 8 seconds of each other.** One person says
"go" and all three press Enter. Replace `<THIS-PC-IP>` with this PC's own IP; the list after
it is the same on every PC:
```bash
chmod +x scripts/lab-seaweedfs.sh
WEED_BIN=./weed ./scripts/lab-seaweedfs.sh start <THIS-PC-IP> $IP1,$IP2,$IP3
```
The script starts 4 parts on this PC: a master, a volume server, a filer and the S3 gateway.
Every file is kept on 2 PCs. Data and logs go to `~/rescuemesh-seaweed`.

**13. Check it on each PC.** You want all 4 parts `running`, then `200`:
```bash
./scripts/lab-seaweedfs.sh status
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8333/
```

| You see | Fix |
|---|---|
| `403` | AWS variables were set. `./scripts/lab-seaweedfs.sh stop`, `unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY`, start again |
| A part `NOT running` | Read its log: `tail -20 ~/rescuemesh-seaweed/logs/master.log` (or `volume`, `filer`, `s3`) |
| The master log keeps saying it has no leader | The PCs were not started close enough together. `stop` on all 3, then start them together again |
| `000` or connection refused | The S3 gateway is not up yet; wait 10 s and retry, then read `logs/s3.log` |

**14. Check SeaweedFS across the network** (one PC):
```bash
python3 spike/seaweedfs/run_checks.py spike/seaweedfs/cluster.real.toml --json results-lab.json
```
Only **check 5 (S3 gateway)** matters: it must show exactly one winner in every round. The
checks on the plain filer API are *expected* to show more than one winner (P4's spike
finding, and the reason the nodes use the S3 gateway). Keep `results-lab.json` for the report.

Then the shared-state tests against a real gateway (should end with `7 passed`):
```bash
RESCUEMESH_S3=http://$IP1:8333 cargo test --release -p node s3_store -- --ignored --test-threads=1
```

---

## Part 6: start the nodes (each PC)

**15. Switch the nodes to SeaweedFS** (each PC's copy of the config, and the client's):
```bash
sed -i 's/^backend = "memory"/backend = "s3"/' config/nodes.lab.toml
grep -n '^backend' config/nodes.lab.toml          # must show: backend = "s3"
```

**16. Start the node.** `--id 1` on PC1, `--id 2` on PC2, `--id 3` on PC3. Use the **same
run ID** on every PC (pick one name per run, for example `lab-nov1-smoke`):
```bash
./target/release/node --id 1 --config config/nodes.lab.toml --run-id lab-nov1-smoke
```
Within a few seconds the log should show `shared state backend=S3`, then `node listening`,
then `rejoin: ready`.

| You see | Fix |
|---|---|
| `addr ... is not ip:port` | A `REPLACE-ME` is still in the config: redo step 7 |
| `cannot reach the shared state` | SeaweedFS on this PC is not up: step 13 |
| No `rejoin: ready` | The node cannot hear its peers: firewall on port 50051 (step 4) |

---

## Part 7: smoke test (client machine)

The client machine needs the same `config/nodes.lab.toml`, and `IP1`, `IP2`, `IP3` set (step 6).

**17. All 3 nodes reachable:**
```bash
for ip in $IP1 $IP2 $IP3; do timeout 2 bash -c "</dev/tcp/$ip/50051" && echo "$ip open" || echo "$ip BLOCKED"; done
```
`BLOCKED`: firewall on that PC (step 4), or the wrong IP in the config.

**18. Mute test.** Nodes 1 and 3 should log `peer suspected down` within about 1.6 s; after
the 5 s, node 2 should log `rejoin: ready`:
```bash
./target/release/inject --node 2 --duration 5s --config config/nodes.lab.toml
```
Repeat with `--node 1` and `--node 3`: this is the only check that heartbeats flow both
ways between every pair of PCs.

**19. 30 requests end to end.** All 30 should come back `ok`:
```bash
./target/release/loadgen --config config/nodes.lab.toml --requests 30 --clients 3 --rate 10 --run-id lab-nov1-smoke
```

**If steps 17 to 19 pass, the cluster is up.** Post in the group chat so the Step C runs can start.

---

## Between runs

**Reset the shared state**, from any one PC, then restart the 3 nodes with the new run ID:
```bash
curl -X DELETE http://127.0.0.1:8333/rescuemesh
```
Never use a new bucket name per run instead: SeaweedFS gives every bucket its own storage
volumes and runs out after a handful (HTTP 500, "No writable volumes").

**Collect the logs after each run** into one folder named after the run ID:
- from each node PC: `logs/faults-nodeN.csv`, `logs/elections-nodeN.csv`,
  `logs/requests-nodeN.csv`, `logs/node_metrics-nodeN.csv`
- from the client machine: `results/<run-id>.csv` and `results/<run-id>.meta.txt`

## Fallback: SeaweedFS will not work

Skip Part 5, leave `backend = "memory"` in step 15, and continue from step 16. Everything
runs, but each node keeps its own private copy of the state: a client retry on another node
does not see the first plan. Tell the team, so the report says which runs used `memory`.

## Do not demo: a PC switched fully off

While one PC's SeaweedFS is stopped, about 30% of saved objects cannot be read from the
other two PCs until it returns (P5's test, Nov 1; nothing is lost). Our injected faults
(the mute) stop only the node program and leave SeaweedFS running, so they are not
affected. A real power-off is: run it for the report (lab-setup.md, 6C), not in the demo.

## End of the day (each PC)

Stop the node with Ctrl+C, then:
```bash
./scripts/lab-seaweedfs.sh stop
```
To delete all SeaweedFS data on that PC as well: `./scripts/lab-seaweedfs.sh wipe`.
Never stop programs on a lab PC that are not ours.
