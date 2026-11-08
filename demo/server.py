#!/usr/bin/env python3
"""RescueMesh demo control panel (Phase 1 demo). Run it on a PC that can SSH to all 6 lab PCs:

    python3 demo/server.py            then open http://localhost:8090
    python3 demo/server.py --port 9000 --bind 0.0.0.0

Not 8080: SeaweedFS's volume server uses it, and the panel may run on a node PC (e.g. N1).

What it does:
- Settings page: the 6 PCs' IPs and SSH login, the repo folder, run options (demo/settings.json).
- Start/stop: writes the node list into config/demo.toml on all 6 PCs, starts SeaweedFS and the
  3 nodes, starts and stops loadgen on the client PCs, mutes a node (P5's injector), puts CPU
  stress on a node PC, runs the duplicate test and a state check across the 3 gateways.
- Live view: demo/agent.py runs on each PC over SSH and streams what the programs already log
  (elections, requests, faults, node metrics, loadgen CSV); this server joins it into one
  picture and sends it to the browser once a second (Server-Sent Events).

Nothing here is part of RescueMesh itself: it only runs the same commands as the runbook.
Standard library only (Python 3.8+). SSH uses the system `ssh`: set up key login first
(ssh-keygen, then ssh-copy-id user@pc for each PC), or set a password if `sshpass` is installed.
"""
import argparse
import base64
import copy
import json
import math
import os
import re
import shlex
import shutil
import struct
import subprocess
import sys
import threading
import time
import traceback
import uuid
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
STATIC = os.path.join(HERE, "static")
SETTINGS_PATH = os.path.join(HERE, "settings.json")
COLLECT_DIR = os.path.join(HERE, "collected")
with open(os.path.join(HERE, "agent.py"), encoding="utf-8") as _f:
    AGENT_SRC = _f.read()

NODE_IDS = (1, 2, 3)
CLIENT_NAMES = ("A", "B", "C")

DEFAULT_SETTINGS = {
    "machines": [
        {"key": "n1", "role": "node", "id": 1, "name": "", "label": "Node 1", "ip": "", "ssh_host": "", "user": "", "port": 22},
        {"key": "n2", "role": "node", "id": 2, "name": "", "label": "Node 2", "ip": "", "ssh_host": "", "user": "", "port": 22},
        {"key": "n3", "role": "node", "id": 3, "name": "", "label": "Node 3", "ip": "", "ssh_host": "", "user": "", "port": 22},
        {"key": "ca", "role": "client", "id": 0, "name": "A", "label": "Client A", "ip": "", "ssh_host": "", "user": "", "port": 22},
        {"key": "cb", "role": "client", "id": 0, "name": "B", "label": "Client B", "ip": "", "ssh_host": "", "user": "", "port": 22},
        {"key": "cc", "role": "client", "id": 0, "name": "C", "label": "Client C", "ip": "", "ssh_host": "", "user": "", "port": 22},
    ],
    "ssh": {"user": "", "key": "", "password": ""},
    "paths": {"repo": "~/Group3/RescueMesh", "weed": "./weed", "seaweed_dir": "", "python": "python3",
              "config": "config/demo.toml"},
    "cluster": {"resource_count": 100, "node_port": 50051, "run_id": "demo", "faults_enabled": False, "faults_seed": 42,
                "faults_min_ms": 2000, "faults_max_ms": 20000, "faults_gap_min_ms": 10000,
                "faults_gap_max_ms": 30000, "lease_ms": 10000, "request_timeout_ms": 2500},
    "load": {"mode": "fixed", "rate": 35, "clients": 8, "requests": 500000, "resolve_after_ms": 1000,
             "stages": "2,4,8,16,32", "stage_secs": 30},
    "demo": {"mute_secs": 12, "hog_secs": 300, "slow_iterations": 900000},
}

# Fallback if config/nodes.lab.toml is not next to this folder (same content as the repo's file).
CONFIG_TEMPLATE = """# Written by demo/server.py from the Settings page. Same file on all 6 PCs.
[[nodes]]
id = 1
addr = "REPLACE-ME:50051"

[[nodes]]
id = 2
addr = "REPLACE-ME:50051"

[[nodes]]
id = 3
addr = "REPLACE-ME:50051"

[timing]
heartbeat_interval_ms = 500
suspect_after_ms = 1500
rpc_deadline_ms = 1000
connect_timeout_ms = 500

[weights]
severity = 0.6
age = 0.4
capability = 0.7
distance = 0.3

[load]
sample_interval_ms = 200
queue_weight = 0.5
cpu_weight = 0.3
service_weight = 0.2
queue_max = 10
service_cap_ms = 100.0
service_ewma_alpha = 0.2

[faults]
enabled = false
seed = 42
min_duration_ms = 2000
max_duration_ms = 20000
min_gap_ms = 10000
max_gap_ms = 30000

[shared_state]
backend = "memory"
endpoint = "http://127.0.0.1:8333"
bucket = "rescuemesh"
lease_ms = 10000
request_timeout_ms = 2500
"""


def now_ms():
    return int(time.time() * 1000)


def deep_merge(base, extra):
    out = copy.deepcopy(base)
    for k, v in (extra or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_settings():
    if os.path.exists(SETTINGS_PATH):
        try:
            with open(SETTINGS_PATH, encoding="utf-8") as f:
                saved = json.load(f)
            s = deep_merge(DEFAULT_SETTINGS, saved)
            if saved.get("machines"):
                s["machines"] = [deep_merge(d, m) for d, m in zip(DEFAULT_SETTINGS["machines"], saved["machines"])]
            return s
        except Exception:  # noqa: BLE001 - a broken file falls back to the defaults
            traceback.print_exc()
    return copy.deepcopy(DEFAULT_SETTINGS)


def save_settings(s):
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2)


# ======================================================================== config file

def build_config(s):
    path = os.path.join(REPO, "config", "nodes.lab.toml")
    tpl = CONFIG_TEMPLATE
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            tpl = f.read()
    tpl = tpl.replace("\r\n", "\n")
    c = s["cluster"]
    ips = {int(m["id"]): m["ip"].strip() for m in s["machines"] if m["role"] == "node"}

    def addr(mo):
        nid = int(mo.group(2))
        return '%s"%s:%d"' % (mo.group(1), ips.get(nid) or "REPLACE-ME", int(c["node_port"]))

    tpl = re.sub(r'(?m)(^id = (\d+)\s*\naddr = )"[^"]*"', addr, tpl)
    reps = [
        (r"(?m)^enabled = \w+", "enabled = %s" % ("true" if c["faults_enabled"] else "false")),
        (r"(?m)^seed = \d+", "seed = %d" % int(c["faults_seed"])),
        (r"(?m)^min_duration_ms = \d+", "min_duration_ms = %d" % int(c["faults_min_ms"])),
        (r"(?m)^max_duration_ms = \d+", "max_duration_ms = %d" % int(c["faults_max_ms"])),
        (r"(?m)^min_gap_ms = \d+", "min_gap_ms = %d" % int(c["faults_gap_min_ms"])),
        (r"(?m)^max_gap_ms = \d+", "max_gap_ms = %d" % int(c["faults_gap_max_ms"])),
        (r'(?m)^backend = "[^"]*"', 'backend = "s3"'),
        (r"(?m)^lease_ms = \d+", "lease_ms = %d" % int(c["lease_ms"])),
        (r"(?m)^request_timeout_ms = \d+", "request_timeout_ms = %d" % int(c["request_timeout_ms"])),
    ]
    for pat, rep in reps:
        tpl = re.sub(pat, rep, tpl)
    return "# Written by the RescueMesh demo panel (demo/server.py). Same file on all 6 PCs.\n" + tpl


# ======================================================================== SSH

def ssh_base(m, s):
    host = (m.get("ssh_host") or m.get("ip") or "").strip()
    if not host:
        raise RuntimeError("no IP set for %s (Setup page)" % m["label"])
    user = (m.get("user") or s["ssh"].get("user") or "").strip()
    target = "%s@%s" % (user, host) if user else host
    cmd = []
    pw = s["ssh"].get("password") or ""
    if pw:
        sp = shutil.which("sshpass")
        if not sp:
            raise RuntimeError("an SSH password is set but `sshpass` is not installed: use an SSH key (ssh-copy-id) instead")
        cmd = [sp, "-p", pw]
    ssh = shutil.which("ssh") or "ssh"
    cmd += [ssh, "-o", "ConnectTimeout=6", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=3", "-p", str(m.get("port") or 22)]
    if not pw:
        cmd += ["-o", "BatchMode=yes"]
    key = (s["ssh"].get("key") or "").strip()
    if key:
        cmd += ["-i", os.path.expanduser(key)]
    cmd.append(target)
    return cmd


# ======================================================================== TERMINALS
# One interactive ssh session (on a pty) per lab PC, shown in the "Terminals" tab with xterm.js.

class TermSession:
    MAX_CHUNKS = 4000

    def __init__(self, key, cmd):
        import pty
        self.key = key
        self.cv = threading.Condition()
        self.chunks = []
        self.base = 0            # index of chunks[0]
        self.alive = True
        self.master, slave = pty.openpty()
        self.proc = subprocess.Popen(cmd, stdin=slave, stdout=slave, stderr=slave,
                                     start_new_session=True, close_fds=True)
        os.close(slave)
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        while True:
            try:
                data = os.read(self.master, 65536)
            except OSError:
                data = b""
            if not data:
                break
            with self.cv:
                self.chunks.append(data)
                if len(self.chunks) > self.MAX_CHUNKS:
                    drop = len(self.chunks) - self.MAX_CHUNKS
                    del self.chunks[:drop]
                    self.base += drop
                self.cv.notify_all()
        with self.cv:
            self.alive = False
            self.cv.notify_all()

    def write(self, data):
        if self.alive:
            os.write(self.master, data)

    def resize(self, cols, rows):
        import fcntl, termios
        try:
            fcntl.ioctl(self.master, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
        except OSError:
            pass

    def close(self):
        try:
            self.proc.terminate()
        except OSError:
            pass


TERMS = {}
TERMS_LOCK = threading.Lock()


def term_open(key, cols, rows):
    with TERMS_LOCK:
        t = TERMS.get(key)
        if t and t.alive and t.proc.poll() is None:
            t.resize(cols, rows)
            return t
        s = HUB.settings
        m = next((x for x in s["machines"] if x["key"] == key), None)
        if not m:
            raise RuntimeError("unknown PC %r" % key)
        cmd = ssh_base(m, s)
        target = cmd.pop()
        cmd += ["-tt", target, 'cd %s 2>/dev/null; exec bash -l' % remote_path(s["paths"]["repo"])]
        t = TermSession(key, cmd)
        t.resize(cols, rows)
        TERMS[key] = t
        return t



def remote_path(p):
    """~/x -> "$HOME/x" so it survives quoting; anything else is shell-quoted."""
    p = (p or "").strip()
    if p == "~":
        return '"$HOME"'
    if p.startswith("~/"):
        return '"$HOME/%s"' % p[2:].replace('"', '\\"')
    return shlex.quote(p)


def fill(script, **kw):
    """Replaces @@name@@ in a shell script (no clash with shell's own braces)."""
    for k, v in kw.items():
        script = script.replace("@@%s@@" % k, str(v))
    return script


def run_remote(m, s, script, timeout=90, binary=False):
    prelude = fill('set -u\nexport LC_ALL=C\ncd @@repo@@ 2>/dev/null || { echo "ERROR: folder @@raw@@ not found on this PC"; exit 3; }\n',
                   repo=remote_path(s["paths"]["repo"]), raw=s["paths"]["repo"])
    cmd = ssh_base(m, s) + ["bash -s"]
    try:
        r = subprocess.run(cmd, input=(prelude + script).encode("utf-8"), stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT if not binary else subprocess.PIPE, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, b"" if binary else "ERROR: timed out after %d s" % timeout
    if binary:
        return r.returncode, r.stdout
    out = r.stdout.decode("utf-8", "replace")
    out = re.sub(r"\x1b\[[0-9;]*m", "", out)
    if r.returncode == 255 and not out.strip():
        out = "ERROR: SSH failed (exit 255)"
    if r.returncode == 255:
        out += "\nHint: can this PC log in without a password?  ssh-copy-id <user>@<ip>"
    return r.returncode, out.strip()


# ======================================================================== live data hub

def quantile_from_hist(hist, q):
    total = sum(hist.values())
    if not total:
        return None
    target = q * total
    acc = 0
    for b in sorted(hist):
        acc += hist[b]
        if acc >= target:
            return round(1.04 ** (b + 0.5) / 1000.0, 2)  # ms
    return None


def pct(sorted_vals, q):
    if not sorted_vals:
        return None
    i = min(len(sorted_vals) - 1, max(0, int(math.ceil(q * len(sorted_vals))) - 1))
    return sorted_vals[i]


def hist_bucket(lat_us):
    return int(math.log(max(lat_us, 1)) / math.log(1.04))


class Hub:
    WINDOW = 900  # seconds of time series kept

    def __init__(self):
        self.lock = threading.RLock()
        self.settings = load_settings()
        self.machines = {}
        self.offsets = {}
        self.timeline = deque(maxlen=300)
        self.actions = deque(maxlen=40)
        self.slowed = {}  # node id -> planner iterations it was restarted with
        self.agents = {}
        self.demo = None  # the Demo Run tab's progress (DemoRun below); None until a run starts
        self.demo_cancel = threading.Event()
        self.reset_stats(log=False)
        self.reset_machine_state()

    # ---------------- state
    def reset_machine_state(self):
        with self.lock:
            for m in self.settings["machines"]:
                old = self.machines.get(m["key"], {})
                self.machines[m["key"]] = {
                    "key": m["key"], "role": m["role"], "id": m["id"], "name": m["name"], "label": m["label"],
                    "ip": m["ip"], "connected": False, "last": 0, "error": old.get("error"), "host": old.get("host"),
                    "sys": None, "proc": None, "metrics": None, "log": old.get("log", deque(maxlen=400)),
                    "peers_down": set(), "muted": None, "run_file": None, "req": None, "fatal": None,
                }

    def reset_stats(self, log=True):
        with self.lock:
            self.cs = {}  # client series: sec -> bucket
            self.ns = {}  # node series: sec -> bucket
            self.ms = {}  # metrics series: sec -> {node: {...}}
            self.tot = {"n": 0, "ok": 0, "lost": 0, "retried": 0, "dup": 0, "unasg": 0, "hist": Counter(),
                        "ex": [0, 0, 0, 0], "wins": [0, 0, 0, 0], "elections": 0, "waited": 0, "split": 0,
                        "reelect": 0, "el_hist": Counter(), "since": now_ms(), "per_client": Counter(),
                        "snap_n": 0, "snap_sum": 0, "commit_n": 0, "commit_sum": 0, "taken": 0, "aborted": 0}
            self.failures = []
            self.fevents = []
            self.ftimeline = []
            self.last_election = None
            if log:
                self.event("info", "Counters reset")

    def event(self, kind, text, node=None, t=None):
        self.timeline.append({"t": t or now_ms(), "kind": kind, "text": text, "node": node})

    def window_stats(self, t0, t1):
        """What the clients and nodes saw between t0 and t1 (server ms), from the per-second bins."""
        with self.lock:
            a, z = int(t0 // 1000), int(t1 // 1000)
            n = ok = lost = ret = 0
            lats, wins, loads = [], {i: 0 for i in NODE_IDS}, {i: [] for i in NODE_IDS}
            for s in range(a, z + 1):
                b = self.cs.get(s)
                if b:
                    n += b["n"]
                    ok += b["ok"]
                    lost += b["lost"]
                    ret += b["retried"]
                    lats.extend(b["lat"])
                nb = self.ns.get(s)
                if nb:
                    for i in NODE_IDS:
                        wins[i] += nb["w"][i]
                for i, v in (self.ms.get(s) or {}).items():
                    if v and v.get("load") is not None:
                        loads[i].append(v["load"])
            lats.sort()
            tw = sum(wins.values())
            to_ms = lambda v: round(v / 1000.0, 1) if v is not None else None  # noqa: E731
            return {"n": n, "ok": ok, "lost": lost, "retried": ret, "okps": ok / max(1.0, (t1 - t0) / 1000.0),
                    "p50": to_ms(pct(lats, 0.5)), "p95": to_ms(pct(lats, 0.95)), "max": to_ms(lats[-1] if lats else None),
                    "elections": tw, "share": {i: (100.0 * wins[i] / tw if tw else None) for i in NODE_IDS},
                    "load": {i: (sum(v) / len(v) if v else None) for i, v in loads.items()}}

    def machine_cfg(self, key):
        for m in self.settings["machines"]:
            if m["key"] == key:
                return m
        return None

    def nodes(self):
        return [m for m in self.settings["machines"] if m["role"] == "node"]

    def clients(self):
        return [m for m in self.settings["machines"] if m["role"] == "client"]

    def node_cfg(self, nid):
        for m in self.nodes():
            if int(m["id"]) == int(nid):
                return m
        raise RuntimeError("no node %s in settings" % nid)

    def client_cfg(self, name):
        for m in self.clients():
            if m["name"] == name:
                return m
        raise RuntimeError("no client %s in settings" % name)

    # ---------------- agents
    def start_agents(self):
        self.stop_agents()
        for m in self.settings["machines"]:
            if not (m.get("ip") or m.get("ssh_host")):
                continue
            a = Agent(self, m["key"])
            self.agents[m["key"]] = a
            a.start()

    def stop_agents(self):
        for a in list(self.agents.values()):
            a.stop()
        self.agents = {}

    def to_srv(self, key, ts_ms):
        return ts_ms + self.offsets.get(key, (0, deque()))[0]

    def ingest(self, key, msg):
        with self.lock:
            st = self.machines.get(key)
            if st is None:
                return
            recv = now_ms()
            off, samples = self.offsets.get(key, (0, deque(maxlen=30)))
            samples.append(recv - int(msg.get("t", recv)))
            self.offsets[key] = (min(samples), samples)
            st["connected"] = True
            st["last"] = recv
            st["error"] = msg.get("err")
            st["fatal"] = msg.get("fatal")
            st["host"] = msg.get("host", st["host"])
            if msg.get("sys"):
                st["sys"] = msg["sys"]
            if msg.get("proc"):
                st["proc"] = msg["proc"]
            if st["role"] == "client":
                self._ingest_client(key, st, msg)
            else:
                self._ingest_node(key, st, msg)

    def _cbucket(self, sec):
        b = self.cs.get(sec)
        if b is None:
            b = {"n": 0, "ok": 0, "lost": 0, "retried": 0, "dup": 0, "unasg": 0, "ex": [0, 0, 0, 0], "lat": [], "cl": {}}
            self.cs[sec] = b
        return b

    def _nbucket(self, sec):
        b = self.ns.get(sec)
        if b is None:
            b = {"w": [0, 0, 0, 0], "seen": [0, 0, 0, 0], "dur": [], "waited": 0, "split": 0, "re": 0}
            self.ns[sec] = b
        return b

    def _ingest_client(self, key, st, msg):
        st["run_file"] = msg.get("run_file")
        name = st["name"]
        t = self.tot
        for line in msg.get("out") or []:
            st["log"].append(line)
        for ts, lat, ex, status, asg, att, oc, _stage in msg.get("rows") or []:
            sec = int(self.to_srv(key, ts) // 1000)
            b = self._cbucket(sec)
            b["n"] += 1
            b["cl"][name] = b["cl"].get(name, 0) + 1
            t["n"] += 1
            t["per_client"][name] += 1
            ok = status == 0 and oc in (0, 1, 3)
            if ok:
                b["ok"] += 1
                t["ok"] += 1
                if len(b["lat"]) < 4000:
                    b["lat"].append(lat)
                t["hist"][hist_bucket(lat)] += 1
                if 0 < ex <= 3:
                    b["ex"][ex] += 1
                    t["ex"][ex] += 1
            else:
                b["lost"] += 1
                t["lost"] += 1
            if att > 1 and ok:
                b["retried"] += 1
                t["retried"] += 1
            if oc == 1:
                b["dup"] += 1
                t["dup"] += 1
            if asg == 0:
                b["unasg"] += 1
                t["unasg"] += 1

    def _ingest_node(self, key, st, msg):
        nid = int(st["id"])
        running = bool((msg.get("proc") or {}).get("node"))
        if running and not st.get("up_since"):
            st["up_since"] = now_ms()
        elif not running:
            st["up_since"] = None
        if msg.get("metrics"):
            st["metrics"] = msg["metrics"]
            sec = int(self.to_srv(key, msg["metrics"]["wall"]) // 1000)
            self.ms.setdefault(sec, {})[nid] = {"cpu": msg["metrics"]["cpu"], "load": msg["metrics"]["load"],
                                                "queue": msg["metrics"]["queue"]}
        req = msg.get("req")
        if req:
            st["req"] = req
            t = self.tot
            t["snap_n"] += req["snap_n"]
            t["snap_sum"] += req["snap_sum"]
            t["commit_n"] += req["commit_n"]
            t["commit_sum"] += req["commit_sum"]
            t["taken"] += req["taken"]
            t["aborted"] += req["end"].get("error:aborted", 0)
        for line in msg.get("log") or []:
            st["log"].append(line)
        for wall, winner, dur, waited, split, rnd in msg.get("el") or []:
            sec = int(self.to_srv(key, wall) // 1000)
            b = self._nbucket(sec)
            b["seen"][nid] += 1
            if winner == nid:
                b["w"][nid] += 1
                self.tot["wins"][nid] += 1
                self.tot["elections"] += 1
                if rnd > 0:
                    b["re"] += 1
                    self.tot["reelect"] += 1
            if len(b["dur"]) < 3000:
                b["dur"].append(dur)
            self.tot["el_hist"][hist_bucket(max(dur, 1))] += 1
            if waited:
                b["waited"] += 1
                self.tot["waited"] += 1
            if split:
                b["split"] += 1
                self.tot["split"] += 1
        last = msg.get("el_last")
        if last and last.get("winner"):
            last = dict(last)
            last["seen_by"] = nid
            last["srv"] = self.to_srv(key, last["wall"])
            if self.last_election is None or last["srv"] >= self.last_election["srv"]:
                self.last_election = last
        for wall, ev, fault_id, peer, dur in msg.get("faults") or []:
            self._fault(key, nid, self.to_srv(key, wall), ev, fault_id, peer, dur)

    def _fault(self, key, nid, t, ev, fault_id, peer, dur):
        """Keeps the live state (muted, who hears whom) and stores the raw event. The failure cards
        are rebuilt from all events sorted by time (rebuild_failures), because the 6 agents report
        in any order: node 1's PEER_DOWN can arrive before node 2's own MUTE_START."""
        st = self.machines[key]
        starting = False
        if ev == "MUTE_START":
            st["muted"] = {"fault_id": fault_id, "start": t, "dur": dur}
        elif ev == "MUTE_END":
            st["muted"] = None
        elif ev == "PEER_DOWN" and peer:
            self.machines_by_id(nid)["peers_down"].add(peer)
            # Nodes starting one after another suspect each other for a moment: not a failure.
            starting = any(now_ms() - (self.machines_by_id(x).get("up_since") or 0) < 20000 for x in (nid, peer))
        elif ev == "PEER_UP" and peer:
            self.machines_by_id(nid)["peers_down"].discard(peer)
        self.fevents.append((t, nid, ev, fault_id, peer, dur, starting))
        if len(self.fevents) > 6000:
            del self.fevents[:1000]

    def rebuild_failures(self):
        fails, tl = [], []

        def open_failure(node):
            for f in reversed(fails):
                if f["node"] == node and f["ready"] is None:
                    return f
            return None

        def ev_(kind, text, node, t):
            tl.append({"t": t, "kind": kind, "text": text, "node": node})

        for t, nid, ev, fault_id, peer, dur, starting in sorted(self.fevents, key=lambda e: e[0]):
            if ev == "MUTE_START":
                kind = "auto" if str(fault_id).startswith("auto") else "manual"
                fails.append({"id": "%s-%d" % (fault_id, nid), "node": nid, "fault_id": fault_id, "kind": kind, "start": t,
                              "planned": dur, "detect": None, "detected_by": None, "end": None, "ready": None,
                              "recovery": None})
                ev_("mute", "N%d silent for %.1f s (%s)" % (nid, dur / 1000.0, kind), nid, t)
            elif ev == "MUTE_END":
                f = open_failure(nid)
                if f and f["end"] is None:
                    f["end"] = t
                ev_("unmute", "N%d talking again" % nid, nid, t)
            elif ev == "READY":
                f = None
                for x in reversed(fails):
                    if x["node"] == nid and x["ready"] is None and (x["fault_id"] == fault_id or (fault_id == "startup" and x["kind"] == "down")):
                        f = x
                        break
                if f:
                    f["ready"] = t
                    if f["end"] is None:
                        f["end"] = t
                    f["recovery"] = max(0, t - f["end"])
                    ev_("ready", "N%d ready (+%.2f s)" % (nid, f["recovery"] / 1000.0), nid, t)
                else:
                    ev_("ready", "N%d ready" % nid, nid, t)
            elif ev == "PEER_DOWN" and peer:
                f = open_failure(peer)
                if f is not None and f["kind"] != "down" and f["detect"] is None:
                    f["detect"] = max(0, t - f["start"])
                    f["detected_by"] = nid
                    ev_("down", "N%d sees N%d down (%.2f s)" % (nid, peer, f["detect"] / 1000.0), peer, t)
                elif f is not None:
                    ev_("down", "N%d sees N%d down" % (nid, peer), peer, t)
                elif starting:
                    ev_("info", "N%d waits for N%d (start-up)" % (nid, peer), peer, t)
                else:
                    fails.append({"id": "down-%d-%d" % (peer, t), "node": peer, "fault_id": "", "kind": "down", "start": t,
                                  "planned": 0, "detect": None, "detected_by": nid, "end": None, "ready": None,
                                  "recovery": None})
                    ev_("down", "N%d sees N%d down" % (nid, peer), peer, t)
            elif ev == "PEER_UP" and peer:
                f = open_failure(peer)
                if f and f["kind"] == "down" and f["end"] is None:
                    f["end"] = t
                ev_("up", "N%d hears N%d" % (nid, peer), peer, t)
        self.failures = fails
        self.ftimeline = tl

    def machines_by_id(self, nid):
        for st in self.machines.values():
            if st["role"] == "node" and int(st["id"]) == int(nid):
                return st
        return {"peers_down": set()}

    def set_disconnected(self, key, err):
        with self.lock:
            st = self.machines.get(key)
            if st:
                st["connected"] = False
                if err:
                    st["error"] = err

    # ---------------- snapshot for the browser
    def node_state(self, st):
        if not st["connected"]:
            return "offline"
        p = st["proc"] or {}
        if not p.get("node"):
            return "stopped"
        mu = st["muted"]
        if mu and mu.get("dur") and now_ms() - mu["start"] > mu["dur"] + 6000:
            st["muted"] = None  # MUTE_END never arrived (node restarted or logs moved): the mute is over
            mu = None
        if mu:
            return "muted"
        if p.get("hogs"):
            return "stressed"
        if self.slowed.get(int(st["id"])):
            return "slowed"
        return "online"

    def snapshot(self):
        """The whole picture for the browser; built at most twice a second, however many tabs are open."""
        with self.lock:
            cached = getattr(self, "_snap", None)
            if cached and now_ms() - cached[0] < 500:
                return cached[1]
            snap = self._build_snapshot()
            self._snap = (now_ms(), snap)
            return snap

    def invalidate(self):
        self._snap = None

    def _build_snapshot(self):
        with self.lock:
            self.rebuild_failures()
            now = now_ms()
            edge = int(now // 1000) - 3  # the last 2-3 s are still arriving (loadgen buffers its CSV)
            for d in (self.cs, self.ns, self.ms):
                for k in [k for k in d if k < edge - self.WINDOW]:
                    del d[k]
            span = 180
            secs = list(range(edge - span + 1, edge + 1))
            ser = {"t": [s * 1000 for s in secs], "ok": [], "lost": [], "retried": [], "p50": [], "p95": [], "p99": [],
                   "ex": {1: [], 2: [], 3: []}, "wins": {1: [], 2: [], 3: []}, "el": [], "el_p50": [], "waited": [],
                   "cpu": {1: [], 2: [], 3: []}, "load": {1: [], 2: [], 3: []}, "queue": {1: [], 2: [], 3: []},
                   "cl": {n: [] for n in CLIENT_NAMES}}
            for s in secs:
                b = self.cs.get(s)
                ser["ok"].append(b["ok"] if b else 0)
                ser["lost"].append(b["lost"] if b else 0)
                ser["retried"].append(b["retried"] if b else 0)
                for n in CLIENT_NAMES:
                    ser["cl"][n].append(b["cl"].get(n, 0) if b else 0)
                for i in NODE_IDS:
                    ser["ex"][i].append(b["ex"][i] if b else 0)
                # rolling 5 s latency percentiles, so one quiet second does not make a gap
                lats = []
                for k in range(s - 4, s + 1):
                    bb = self.cs.get(k)
                    if bb:
                        lats.extend(bb["lat"])
                lats.sort()
                for q, name in ((0.5, "p50"), (0.95, "p95"), (0.99, "p99")):
                    v = pct(lats, q)
                    ser[name].append(round(v / 1000.0, 2) if v is not None else None)
                nb = self.ns.get(s)
                total = 0
                for i in NODE_IDS:
                    w = nb["w"][i] if nb else 0
                    ser["wins"][i].append(w)
                    total += w
                ser["el"].append(total)
                d = sorted(nb["dur"]) if nb else []
                ser["el_p50"].append(round(pct(d, 0.5) / 1000.0, 3) if d else None)
                ser["waited"].append(nb["waited"] if nb else 0)
                mb = self.ms.get(s, {})
                for i in NODE_IDS:
                    v = mb.get(i)
                    ser["cpu"][i].append(v["cpu"] if v else None)
                    ser["load"][i].append(v["load"] if v else None)
                    ser["queue"][i].append(v["queue"] if v else None)

            def window_sum(arr, n):
                return sum(x for x in arr[-n:] if x)

            okps = window_sum(ser["ok"], 5) / 5.0
            lats = []
            for s in secs[-10:]:
                b = self.cs.get(s)
                if b:
                    lats.extend(b["lat"])
            lats.sort()
            share30 = {i: window_sum(ser["wins"][i], 30) for i in NODE_IDS}
            s30 = sum(share30.values())
            t = self.tot
            wins_total = sum(t["wins"][1:])
            kpi = {
                "requests": t["n"], "ok": t["ok"], "lost": t["lost"], "retried": t["retried"], "dup": t["dup"],
                "unassigned": t["unasg"], "okps": round(okps, 1),
                "p50": round(pct(lats, 0.5) / 1000.0, 1) if lats else None,
                "p95": round(pct(lats, 0.95) / 1000.0, 1) if lats else None,
                "p99": round(pct(lats, 0.99) / 1000.0, 1) if lats else None,
                "p50_all": quantile_from_hist(t["hist"], 0.5), "p95_all": quantile_from_hist(t["hist"], 0.95),
                "p99_all": quantile_from_hist(t["hist"], 0.99),
                "reliability": round(100.0 * t["ok"] / t["n"], 3) if t["n"] else None,
                "share30": {i: (round(100.0 * share30[i] / s30, 1) if s30 else None) for i in NODE_IDS},
                "share_all": {i: (round(100.0 * t["wins"][i] / wins_total, 1) if wins_total else None) for i in NODE_IDS},
                "wins": {i: t["wins"][i] for i in NODE_IDS},
                "elections": t["elections"], "reelect": t["reelect"], "split": t["split"],
                "waited_pct": round(100.0 * t["waited"] / max(1, sum(t["el_hist"].values())), 2),
                "el_p50": quantile_from_hist(t["el_hist"], 0.5), "el_p99": quantile_from_hist(t["el_hist"], 0.99),
                "elps": round(window_sum(ser["el"], 5) / 5.0, 1),
                "snap_ms": round(t["snap_sum"] / t["snap_n"] / 1000.0, 2) if t["snap_n"] else None,
                "commit_ms": round(t["commit_sum"] / t["commit_n"] / 1000.0, 2) if t["commit_n"] else None,
                "taken_pct": round(100.0 * t["taken"] / t["commit_n"], 1) if t["commit_n"] else None,
                "aborted": t["aborted"], "failures": len(self.failures), "since": t["since"],
                "per_client": dict(t["per_client"]),
            }
            machines = {}
            for k, st in self.machines.items():
                d = {x: st[x] for x in ("key", "role", "id", "name", "label", "ip", "connected", "error", "host",
                                         "sys", "proc", "metrics", "run_file", "fatal", "req")}
                d["age"] = (now - st["last"]) / 1000.0 if st["last"] else None
                d["log"] = list(st["log"])[-80:]
                if st["role"] == "node":
                    d["state"] = self.node_state(st)
                    d["peers_down"] = sorted(st["peers_down"])
                    # SeaweedFS parts not running on this PC while its node runs (a crash; the
                    # supervisor restarts it). Logged once per change in the timeline.
                    w = (st["proc"] or {}).get("weed") or {}
                    down = [r for r in ("master", "volume", "filer", "s3") if w and not w.get(r)] if d["state"] not in ("offline", "stopped") else []
                    d["weed_down"] = down
                    if down != st.get("weed_down_seen", []):
                        if down:
                            self.event("warn", "SeaweedFS %s down on node %d's PC (restarting automatically)" % ("+".join(down), st["id"]), node=st["id"])
                        elif st.get("weed_down_seen"):
                            self.event("ready", "SeaweedFS on node %d's PC is complete again" % st["id"], node=st["id"])
                        st["weed_down_seen"] = down
                    if st["muted"]:
                        mu = st["muted"]
                        d["muted"] = {"fault_id": mu["fault_id"], "dur": mu["dur"],
                                      "left": max(0, mu["start"] + mu["dur"] - now)}
                    else:
                        d["muted"] = None
                    d["slowed"] = self.slowed.get(int(st["id"]))
                else:
                    p = st["proc"] or {}
                    d["state"] = "offline" if not st["connected"] else ("running" if p.get("loadgen") else "idle")
                machines[k] = d
            fails = []
            for f in self.failures[-24:]:
                end = f["end"] or now
                a, z = int(f["start"] // 1000), int(end // 1000)
                okc = lost = ret = 0
                mx = 0
                for s in range(a, z + 1):
                    b = self.cs.get(s)
                    if b:
                        okc += b["ok"]
                        lost += b["lost"]
                        ret += b["retried"]
                        if b["lat"]:
                            mx = max(mx, max(b["lat"]))
                ff = dict(f)
                ff.update({"during_ok": okc, "during_lost": lost, "during_retried": ret, "during_max_ms": round(mx / 1000.0, 1),
                           "open": f["ready"] is None})
                fails.append(ff)
            return {
                "now": now, "edge": edge * 1000, "series": ser, "kpi": kpi, "machines": machines,
                "last_election": self.last_election, "failures": fails, "timeline": sorted(list(self.timeline) + self.ftimeline[-200:], key=lambda e: e["t"])[-120:],
                "actions": [self._action_view(a) for a in list(self.actions)[-25:]],
                "faults_enabled": self.settings["cluster"]["faults_enabled"],
                "run_id": self.settings["cluster"]["run_id"],
                "demo": copy.deepcopy(self.demo),
                "demo_tests": DEMO_TESTS,
            }

    def _action_view(self, a):
        return {k: a[k] for k in ("id", "name", "title", "status", "started", "ended", "steps", "summary")}


class Agent(threading.Thread):
    def __init__(self, hub, key):
        super().__init__(daemon=True)
        self.hub = hub
        self.key = key
        self.stop_ev = threading.Event()
        self.proc = None

    def stop(self):
        self.stop_ev.set()
        p = self.proc
        if p and p.poll() is None:
            try:
                p.kill()
            except OSError:
                pass

    def run(self):
        while not self.stop_ev.is_set():
            s = self.hub.settings
            m = self.hub.machine_cfg(self.key)
            err = None
            try:
                cfg = {"role": m["role"], "id": m["id"], "name": m["name"], "repo": s["paths"]["repo"],
                       "seaweed_dir": s["paths"]["seaweed_dir"]}
                arg = base64.b64encode(json.dumps(cfg).encode()).decode()
                cmd = ssh_base(m, s) + ["%s -u - %s" % (s["paths"]["python"] or "python3", arg)]
                flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
                self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE, creationflags=flags)
                errbuf = []

                def drain(pipe):
                    for line in pipe:
                        errbuf.append(line.decode("utf-8", "replace").strip())
                        del errbuf[:-5]

                threading.Thread(target=drain, args=(self.proc.stderr,), daemon=True).start()
                self.proc.stdin.write(AGENT_SRC.encode("utf-8"))
                self.proc.stdin.close()
                for line in self.proc.stdout:
                    if self.stop_ev.is_set():
                        break
                    try:
                        self.hub.ingest(self.key, json.loads(line.decode("utf-8")))
                    except ValueError:
                        continue
                self.proc.wait(timeout=5)
                err = " | ".join(x for x in errbuf if x) or "agent connection closed"
            except Exception as e:  # noqa: BLE001
                err = str(e)
            self.hub.set_disconnected(self.key, err)
            self.stop_ev.wait(4)


# ======================================================================== actions

SCRIPTS = {
    "test": r'''
BAD=0
ok()   { echo "OK    $1"; }
miss() { echo "MISS  $1"; BAD=1; }
echo "host $(hostname)   ip $(hostname -I 2>/dev/null | awk '{print $1}')   folder $(pwd)"
for b in @@bins@@; do
  if [ -x target/release/$b ]; then ok "target/release/$b"; else miss "target/release/$b  (run: cargo build --release)"; fi
done
if command -v @@py@@ >/dev/null 2>&1; then ok "$(@@py@@ --version 2>&1)"; else miss "@@py@@ (the live view needs Python 3)"; fi
if [ "@@role@@" = "node" ]; then
  if [ -x @@weed@@ ]; then ok "weed binary @@weed@@"; else miss "weed binary at @@weed@@ (Setup: weed path, or chmod +x)"; fi
  [ -f scripts/lab-seaweedfs.sh ] && ok "scripts/lab-seaweedfs.sh" || miss "scripts/lab-seaweedfs.sh"
  command -v curl >/dev/null 2>&1 && ok "curl" || miss "curl"
  grep -h "const DEFAULT_RESOURCE_COUNT" crates/node/src/store.rs 2>/dev/null | sed 's/^ */INFO  /'
fi
[ -f @@cfg@@ ] && ok "@@cfg@@ is there" || echo "INFO  @@cfg@@ not pushed yet"
exit $BAD
''',
    "push_config": r'''
mkdir -p "$(dirname @@cfg@@)"
echo '@@b64@@' | base64 -d > @@cfg@@ && echo "wrote @@cfg@@ ($(wc -l < @@cfg@@) lines)" && grep '^addr' @@cfg@@
''',
    "weed_start": r'''
@@env@@ bash ./scripts/lab-seaweedfs.sh start @@ip@@ @@ips@@ </dev/null
sleep 1
code=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' http://127.0.0.1:8333/ || true)
echo "S3 gateway answered HTTP $code"
[ "$code" = "200" ]
''',
    "weed_status": r'''
@@env@@ bash ./scripts/lab-seaweedfs.sh status
code=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' http://127.0.0.1:8333/ || true)
echo "S3 gateway answered HTTP $code"
[ "$code" = "200" ]
''',
    "weed_stop": r'''
@@env@@ bash ./scripts/lab-seaweedfs.sh stop
''',
    "weed_wipe": r'''
@@env@@ bash ./scripts/lab-seaweedfs.sh wipe
''',
    "node_start": r'''
[ -f @@cfg@@ ] || { echo "ERROR: @@cfg@@ is missing: press 'Push config' first"; exit 4; }
[ -x target/release/node ] || { echo "ERROR: target/release/node is missing: run cargo build --release"; exit 4; }
P=$(pgrep -f "target/release/node --id @@id@@ " | head -1)
if [ -n "$P" ]; then echo "node @@id@@ is already running (pid $P)"; exit 0; fi
mkdir -p logs
if [ "@@clear@@" = "1" ] && [ -n "$(ls -A logs 2>/dev/null)" ]; then
  TS=$(date +%Y%m%d-%H%M%S); mkdir -p old-logs/$TS && mv logs/* old-logs/$TS/ && echo "previous logs moved to old-logs/$TS"
fi
L=$(cat logs/node@@id@@.log 2>/dev/null | wc -l)
# Open-file limit: 1024 by default on the lab PCs (hard limit 1048576). The node caps its own
# calls to the gateway (128 at once), plus gRPC to peers and clients: 65536 leaves wide margin.
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
nohup ./target/release/node --id @@id@@ --config @@cfg@@ --run-id @@run@@ @@extra@@ </dev/null >>logs/node@@id@@.log 2>&1 &
NP=$!
echo $NP > logs/node.pid
# "node listening" is logged only after the shared state (SeaweedFS S3) answered, so wait for
# it instead of trusting that the process is merely alive: a node that cannot write to the
# shared state keeps retrying for ~10 s and then exits.
for i in $(seq 60); do
  sleep 0.5
  if ! kill -0 $NP 2>/dev/null; then
    echo "ERROR: node @@id@@ exited while starting:"; tail -n +$((L+1)) logs/node@@id@@.log | sed 's/\x1b\[[0-9;]*m//g' | tail -n 12; exit 1
  fi
  if tail -n +$((L+1)) logs/node@@id@@.log | grep -q "node listening"; then
    echo "node @@id@@ started (pid $NP) @@extra@@, shared state OK after $((i/2)) s"
    tail -n +$((L+1)) logs/node@@id@@.log | sed 's/\x1b\[[0-9;]*m//g' | grep -v "not ready yet" | tail -n 3
    exit 0
  fi
done
echo "ERROR: node @@id@@ is running but did not reach the shared state in 30 s:"
tail -n 6 logs/node@@id@@.log | sed 's/\x1b\[[0-9;]*m//g'
exit 1
''',
    "node_stop": r'''
P=$(pgrep -f "target/release/node --id @@id@@ ")
if [ -z "$P" ]; then echo "node @@id@@ was not running"; exit 0; fi
kill $P
for i in 1 2 3 4 5 6 7 8; do sleep 0.5; pgrep -f "target/release/node --id @@id@@ " >/dev/null || { echo "node @@id@@ stopped"; exit 0; }; done
kill -9 $P 2>/dev/null; echo "node @@id@@ killed (did not stop in 4 s)"
''',
    "mute": r'''
[ -x target/release/inject ] || { echo "ERROR: target/release/inject is missing: run cargo build --release"; exit 4; }
./target/release/inject --node @@id@@ --duration @@secs@@s --config @@cfg@@ 2>&1 | sed 's/\x1b\[[0-9;]*m//g'
exit ${PIPESTATUS[0]}
''',
    "power_cut": r'''
# A power cut: everything on this PC that belongs to the system dies at once, no clean shutdown.
pkill -9 -f 'rescuemesh-hog' 2>/dev/null
pkill -9 -f "target/release/node --id @@id@@ " 2>/dev/null && echo "node @@id@@ killed (kill -9)" || echo "node @@id@@ was not running"
# The supervisors first (they would restart SeaweedFS 2 s later), then SeaweedFS itself.
pkill -9 -f 'rescuemesh-weed-supervisor' 2>/dev/null
pkill -9 -x weed 2>/dev/null && echo "SeaweedFS (master, volume, filer, S3) killed (kill -9)" || echo "SeaweedFS was not running"
rm -f "${SEAWEED_DIR:-$HOME/rescuemesh-seaweed}"/pids/*.pid logs/node.pid 2>/dev/null
sleep 0.5
echo "left running on $(hostname): node=$(pgrep -fc 'target/release/node --id') weed=$(pgrep -xc weed)"
exit 0
''',
    "hog_on": r'''
N=@@cores@@
[ "$N" -ge 1 ] 2>/dev/null || N=$(nproc)
[ "$N" -le "$(nproc)" ] || N=$(nproc)
for i in $(seq $N); do nohup timeout @@secs@@ sh -c ': rescuemesh-hog; while :; do :; done' </dev/null >/dev/null 2>&1 & done
echo "CPU stress started on $N of $(nproc) cores of $(hostname) for @@secs@@ s"
''',
    "hog_off": r'''
if pkill -f rescuemesh-hog; then echo "CPU stress stopped on $(hostname)"; else echo "no CPU stress was running"; fi
exit 0
''',
    "load_start": r'''
[ -f @@cfg@@ ] || { echo "ERROR: @@cfg@@ is missing: press 'Push config' first"; exit 4; }
[ -x target/release/loadgen ] || { echo "ERROR: target/release/loadgen is missing: run cargo build --release"; exit 4; }
P=$(pgrep -f "target/release/loadgen " | head -1)
if [ -n "$P" ]; then echo "ERROR: loadgen is already running here (pid $P): stop it first"; exit 5; fi
mkdir -p results/demo-@@name@@
nohup ./target/release/loadgen --config @@cfg@@ --run-id @@rid@@ --out results/demo-@@name@@ --seed @@seed@@ @@args@@ --resolve-after-ms @@resolve@@ </dev/null >results/demo-@@name@@.out 2>&1 &
sleep 1
if kill -0 $! 2>/dev/null; then echo "loadgen started (pid $!): @@args@@, seed @@seed@@, run @@rid@@"; else echo "ERROR: loadgen stopped right away:"; tail -n 20 results/demo-@@name@@.out; exit 1; fi
''',
    "load_stop": r'''
P=$(pgrep -f "target/release/loadgen ")
if [ -z "$P" ]; then echo "loadgen was not running"; exit 0; fi
# SIGTERM = a clean stop: loadgen stops sending, resolves every incident still waiting for its
# mission delay (otherwise their resources stay claimed for good), writes its totals and exits.
kill $P
for i in $(seq 50); do
  sleep 0.5
  pgrep -f "target/release/loadgen " >/dev/null || { echo "loadgen stopped cleanly after $((i/2)) s"; tail -n 2 results/demo-*.out 2>/dev/null | grep -E "done in|Failed" | sed 's/^/  /'; exit 0; }
done
kill -9 $P 2>/dev/null
echo "loadgen did not finish resolving in 25 s: killed (some resources may stay claimed until the next Full reset)"
''',
    "archive_results": r'''
if ls -d results/demo-* >/dev/null 2>&1; then
  TS=$(date +%Y%m%d-%H%M%S); mkdir -p results/old-demo/$TS && mv results/demo-* results/old-demo/$TS/ && echo "previous demo results moved to results/old-demo/$TS"
else echo "no previous demo results"; fi
''',
    "dup_test": r'''
[ -x target/release/client ] || { echo "ERROR: target/release/client is missing: run cargo build --release"; exit 4; }
./target/release/client --config @@cfg@@ --count @@count@@ --seed @@seed@@ 2>&1 | sed 's/\x1b\[[0-9;]*m//g'
''',
    "state": r'''
@@py@@ - <<'PYEOF'
import hashlib, json, re, urllib.error, urllib.parse, urllib.request
O = urllib.request.build_opener(urllib.request.ProxyHandler({}))
base = "http://127.0.0.1:8333/rescuemesh"
def keys(prefix, cap=400):
    ks, tok, pages = [], None, 0
    while pages < cap:
        url = base + "?list-type=2&max-keys=1000&prefix=" + urllib.parse.quote(prefix)
        if tok:
            url += "&continuation-token=" + urllib.parse.quote(tok)
        t = O.open(url, timeout=15).read().decode()
        ks += re.findall(r"<Key>([^<]+)</Key>", t)
        pages += 1
        m = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", t)
        if not m:
            break
        tok = m.group(1)
    return ks
try:
    res, cl, rq, rs = keys("resources/"), keys("claims/"), keys("requests/"), keys("resolved/")
    h = hashlib.sha256()
    broken = []  # listed by the gateway but not readable: the filers disagree about this key
    for k in sorted(cl):
        try:
            body = O.open(base + "/" + k, timeout=15).read()
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            broken.append(k)
            continue
        h.update(k.encode()); h.update(body)
    hr = hashlib.sha256("\n".join(sorted(rq)).encode()).hexdigest()[:12]
    if broken:
        print("WARNING: listed but not readable (filers out of sync): " + ", ".join(broken[:20]))
    print("STATE " + json.dumps({"resources": len(res), "claims": len(cl) - len(broken), "requests": len(rq), "resolved": len(rs),
                                 "claims_hash": h.hexdigest()[:12], "requests_hash": hr, "unreadable": len(broken)}))
except Exception as e:
    print("ERROR reading the local gateway: %s" % e)
PYEOF
''',
    "collect_node": r'''if [ -d logs ]; then tar czf - logs 2>/dev/null; fi; exit 0''',
    "collect_client": r'''if ls -d results/demo-* >/dev/null 2>&1; then tar czf - results/demo-* 2>/dev/null; fi; exit 0''',
}


class Actions:
    def __init__(self, hub):
        self.hub = hub

    # ---------------- plumbing
    def start(self, name, params):
        fn = getattr(self, "do_" + name, None)
        if fn is None:
            raise RuntimeError("unknown action " + name)
        a = {"id": uuid.uuid4().hex[:8], "name": name, "title": TITLES.get(name, name), "status": "running",
             "started": now_ms(), "ended": None, "steps": [], "summary": ""}
        with self.hub.lock:
            self.hub.actions.append(a)

        def run():
            try:
                ok = fn(a, params or {})
                a["status"] = "ok" if ok is not False else "fail"
            except Exception as e:  # noqa: BLE001
                a["status"] = "fail"
                a["summary"] = "%s" % e
                traceback.print_exc()
            a["ended"] = now_ms()
            with self.hub.lock:
                ok = a["status"] == "ok"
                self.hub.event("action" if ok else "warn", "%s %s" % (a["title"], "done" if ok else "FAILED")
                               + ("" if ok or not a["summary"] else ": " + a["summary"]))

        threading.Thread(target=run, daemon=True).start()
        return a["id"]

    def step(self, a, title):
        st = {"title": title, "status": "running", "hosts": {}}
        a["steps"].append(st)
        return st

    def par(self, a, title, machines, script_fn, timeout=90):
        """Runs script_fn(m) on every machine at the same time. True if all succeeded."""
        st = self.step(a, title)
        s = self.hub.settings
        results = {}

        def one(m):
            st["hosts"][m["label"]] = {"status": "running", "out": ""}
            try:
                script = script_fn(m)
                rc, out = run_remote(m, s, script, timeout=timeout)
            except Exception as e:  # noqa: BLE001
                rc, out = 1, "ERROR: %s" % e
            results[m["key"]] = (rc, out)
            st["hosts"][m["label"]] = {"status": "ok" if rc == 0 else "fail", "out": out[-6000:]}

        threads = [threading.Thread(target=one, args=(m,)) for m in machines]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        ok = all(rc == 0 for rc, _ in results.values()) and len(results) == len(machines)
        st["status"] = "ok" if ok else "fail"
        st["results"] = {k: out for k, (rc, out) in results.items()}
        return ok

    def common(self):
        s = self.hub.settings
        return {"cfg": shlex.quote(s["paths"]["config"]), "py": shlex.quote(s["paths"]["python"] or "python3"),
                "weed": shlex.quote(s["paths"]["weed"]), "run": shlex.quote(s["cluster"]["run_id"])}

    def res_arg(self):
        n = int(self.hub.settings["cluster"].get("resource_count") or 0)
        return "--resource-count %d" % n if n > 0 else ""

    def weed_env(self):
        s = self.hub.settings
        env = "WEED_BIN=%s" % shlex.quote(s["paths"]["weed"])
        if s["paths"]["seaweed_dir"]:
            env += " SEAWEED_DIR=%s" % remote_path(s["paths"]["seaweed_dir"])
        return env

    def pick_nodes(self, p):
        ids = p.get("nodes") or list(NODE_IDS)
        return [self.hub.node_cfg(i) for i in ids]

    def pick_clients(self, p):
        names = p.get("pcs") or list(CLIENT_NAMES)  # which client PCs ("clients" is clients per PC)
        return [self.hub.client_cfg(n) for n in names]

    def check_ips(self, machines):
        missing = [m["label"] for m in machines if not (m.get("ip") or "").strip()]
        if missing:
            raise RuntimeError("set the IP of %s on the Setup page" % ", ".join(missing))

    # ---------------- setup
    def do_test(self, a, p):
        ms = self.hub.settings["machines"]
        self.check_ips(ms)
        c = self.common()

        def script(m):
            bins = "node inject" if m["role"] == "node" else "loadgen client"
            return fill(SCRIPTS["test"], bins=bins, role=m["role"], **c)

        ok = self.par(a, "Check SSH, binaries and folders on all 6 PCs", ms, script, timeout=30)
        a["summary"] = "all 6 PCs ready" if ok else "some PCs need attention (see output)"
        return ok

    def do_push_config(self, a, p):
        ms = self.hub.settings["machines"]
        self.check_ips(self.hub.nodes())
        text = build_config(self.hub.settings)
        b64 = base64.b64encode(text.encode()).decode()
        c = self.common()
        ok = self.par(a, "Write %s on all 6 PCs" % self.hub.settings["paths"]["config"], ms,
                      lambda m: fill(SCRIPTS["push_config"], b64=b64, **c), timeout=30)
        a["summary"] = "same config on all 6 PCs" if ok else "could not write the config everywhere"
        return ok

    def _weed(self, a, kind, title, timeout=60):
        nodes = self.hub.nodes()
        self.check_ips(nodes)
        ips = ",".join(m["ip"].strip() for m in sorted(nodes, key=lambda m: m["id"]))
        env = self.weed_env()
        return self.par(a, title, nodes, lambda m: fill(SCRIPTS[kind], env=env, ip=m["ip"].strip(), ips=ips),
                        timeout=timeout)

    def do_weed_start(self, a, p):
        ok = self._weed(a, "weed_start", "Start SeaweedFS on the 3 node PCs (together, ~25 s)", timeout=120)
        a["summary"] = "shared storage up on all 3" if ok else "a gateway did not answer 200"
        return ok

    def do_weed_status(self, a, p):
        return self._weed(a, "weed_status", "SeaweedFS status", timeout=30)

    def do_weed_stop(self, a, p):
        return self._weed(a, "weed_stop", "Stop SeaweedFS", timeout=30)

    def do_weed_wipe(self, a, p):
        self.do_nodes_stop(a, {})
        ok = self._weed(a, "weed_wipe", "Wipe SeaweedFS data (empty bucket)", timeout=60)
        ok = self.par(a, "Move old demo results away on the client PCs", self.hub.clients(),
                      lambda m: SCRIPTS["archive_results"], timeout=30) and ok
        return ok

    def do_nodes_start(self, a, p):
        nodes = self.pick_nodes(p)
        self.check_ips(nodes)
        c = self.common()
        clear = "1" if p.get("clear_logs", True) else "0"
        for m in nodes:
            self.hub.slowed.pop(int(m["id"]), None)
        ok = self.par(a, "Start node(s) %s" % ", ".join(str(m["id"]) for m in nodes), nodes,
                      lambda m: fill(SCRIPTS["node_start"], id=m["id"], clear=clear, extra=self.res_arg(), **c), timeout=55)
        a["summary"] = "nodes running" if ok else "a node did not start (see its log)"
        return ok

    def do_nodes_stop(self, a, p):
        nodes = self.pick_nodes(p)
        self.check_ips(nodes)
        ok = self.par(a, "Stop node(s) %s" % ", ".join(str(m["id"]) for m in nodes), nodes,
                      lambda m: fill(SCRIPTS["node_stop"], id=m["id"]), timeout=30)
        return ok

    def do_node_restart(self, a, p):
        nid = int(p["node"])
        it = int(p.get("iterations") or 0)
        m = self.hub.node_cfg(nid)
        c = self.common()
        self.par(a, "Stop node %d" % nid, [m], lambda m: fill(SCRIPTS["node_stop"], id=nid), timeout=30)
        extra = ("--planner-iterations %d " % it if it > 0 else "") + self.res_arg()
        ok = self.par(a, "Start node %d %s" % (nid, extra), [m],
                      lambda m: fill(SCRIPTS["node_start"], id=nid, clear="0", extra=extra, **c), timeout=55)
        if it > 0:
            self.hub.slowed[nid] = it
        else:
            self.hub.slowed.pop(nid, None)
        a["summary"] = ("node %d restarted with %d planner iterations (a slower machine)" % (nid, it)) if it else "node %d restarted normally" % nid
        return ok

    def do_bringup(self, a, p):
        if not self.do_test(a, p):
            a["summary"] = "stopped: the PC check failed"
            return False
        if not self.do_push_config(a, p):
            return False
        if p.get("wipe"):
            self.do_load_stop(a, {})
            self.do_weed_wipe(a, p)
        if not self.do_weed_start(a, p):
            a["summary"] = "stopped: SeaweedFS did not come up"
            return False
        if not self.do_nodes_start(a, {"clear_logs": True}):
            return False
        st = self.step(a, "Wait until all 3 nodes report in")
        for _ in range(40):
            snap = self.hub.snapshot()["machines"]
            up = [m for m in snap.values() if m["role"] == "node" and m["state"] not in ("offline", "stopped")]
            st["hosts"] = {"panel": {"status": "running", "out": "%d of 3 nodes running" % len(up)}}
            if len(up) == 3:
                break
            time.sleep(1)
        st["status"] = "ok" if len(up) == 3 else "fail"
        self.hub.reset_stats()
        a["summary"] = "cluster is up: 3 nodes + shared storage" if len(up) == 3 else "nodes started but not all report in"
        return len(up) == 3

    def do_full_reset(self, a, p):
        self.do_load_stop(a, {})
        self.do_hog_off(a, {})
        self.do_weed_wipe(a, p)
        if not self.do_push_config(a, p):
            return False
        if not self.do_weed_start(a, p):
            return False
        ok = self.do_nodes_start(a, {"clear_logs": True})
        self.hub.reset_stats()
        a["summary"] = "fresh, empty cluster" if ok else "reset finished with errors"
        return ok

    def do_stop_all(self, a, p):
        self.do_load_stop(a, {})
        self.do_hog_off(a, {})
        ok = self.do_nodes_stop(a, {})
        if p.get("weed"):
            ok = self.do_weed_stop(a, p) and ok
        a["summary"] = "everything stopped"
        return ok

    def do_apply_faults(self, a, p):
        self.hub.settings["cluster"]["faults_enabled"] = bool(p.get("enabled"))
        save_settings(self.hub.settings)
        if not self.do_push_config(a, p):
            return False
        nodes = self.hub.nodes()
        self.par(a, "Stop the 3 nodes", nodes, lambda m: fill(SCRIPTS["node_stop"], id=m["id"]), timeout=30)
        c = self.common()
        ok = self.par(a, "Start the 3 nodes with automatic faults %s" % ("ON" if p.get("enabled") else "OFF"), nodes,
                      lambda m: fill(SCRIPTS["node_start"], id=m["id"], clear="0", extra=self.res_arg(), **c), timeout=55)
        self.hub.slowed.clear()
        a["summary"] = "automatic fault injection is %s" % ("ON" if p.get("enabled") else "OFF")
        return ok

    # ---------------- load
    def do_load_start(self, a, p):
        s = self.hub.settings
        L = deep_merge(s["load"], {k: v for k, v in p.items() if k in s["load"]})
        clients = self.pick_clients(p)
        self.check_ips(clients)
        c = self.common()
        rid = "%s-%s" % (s["cluster"]["run_id"], time.strftime("%H%M%S"))
        base = int(time.time()) * 10000000  # fresh incidents every start: no wipe needed between demos
        if L["mode"] == "ramp":
            stages = re.sub(r"[^0-9,]", "", str(L["stages"]))
            args = "--stages %s --stage-secs %d" % (stages, int(L["stage_secs"]))
        else:
            args = "--requests %d --rate %s --clients %d" % (min(int(L["requests"]), 1999999), float(L["rate"]),
                                                              int(L["clients"]))
        idx = {n: i for i, n in enumerate(CLIENT_NAMES)}
        ok = self.par(a, "Start loadgen on client(s) %s" % ", ".join(m["name"] for m in clients), clients,
                      lambda m: fill(SCRIPTS["load_start"], name=m["name"], rid=rid, seed=base + idx[m["name"]] * 2000000,
                                     args=args, resolve=int(L["resolve_after_ms"]), **c), timeout=30)
        a["summary"] = ("load running: %s per PC" % args) if ok else "loadgen did not start everywhere"
        return ok

    def do_load_stop(self, a, p):
        clients = [m for m in self.pick_clients(p) if (m.get("ip") or "").strip()]
        if not clients:
            return True
        return self.par(a, "Stop loadgen", clients, lambda m: SCRIPTS["load_stop"], timeout=45)

    # ---------------- power cut
    def do_power_cut(self, a, p):
        nid = int(p["node"])
        m = self.hub.node_cfg(nid)
        env = self.weed_env()
        ok = self.par(a, "POWER CUT on node %d's PC: kill -9 the node and SeaweedFS" % nid, [m],
                      lambda m: env + "\nexport SEAWEED_DIR=${SEAWEED_DIR:-}\n" + fill(SCRIPTS["power_cut"], id=nid), timeout=30)
        self.hub.slowed.pop(nid, None)
        a["summary"] = ("node %d's PC lost power: node and storage are gone, the other two keep serving" % nid) if ok else "power cut failed"
        return ok

    def do_power_on(self, a, p):
        nid = int(p["node"])
        m = self.hub.node_cfg(nid)
        c = self.common()
        ips = ",".join((x.get("ip") or "").strip() for x in self.hub.nodes())
        env = self.weed_env()
        if not self.par(a, "Power on node %d's PC: start SeaweedFS (keeps its data)" % nid, [m],
                        lambda m: "%s bash ./scripts/lab-seaweedfs.sh start %s %s </dev/null 2>&1 | tail -n 9" % (env, m["ip"].strip(), ips) + "\nexit ${PIPESTATUS[0]}", timeout=150):
            a["summary"] = "SeaweedFS did not come back on node %d's PC (see output); the node was not started" % nid
            return False
        ok = self.par(a, "Start node %d (it rejoins and recovers its state)" % nid, [m],
                      lambda m: fill(SCRIPTS["node_start"], id=nid, clear="0", extra=self.res_arg(), **c), timeout=55)
        a["summary"] = "node %d's PC is back" % nid if ok else "node %d did not come back (see its log)" % nid
        return ok

    # ---------------- faults and imbalance
    def do_mute(self, a, p):
        nid = int(p["node"])
        secs = max(2, min(20, int(p.get("secs") or self.hub.settings["demo"]["mute_secs"])))
        m = self.hub.node_cfg(nid)
        ok = self.par(a, "Mute node %d for %d s (it ignores all communication)" % (nid, secs), [m],
                      lambda m: fill(SCRIPTS["mute"], id=nid, secs=secs, **self.common()), timeout=30)
        a["summary"] = "node %d is silent for %d s" % (nid, secs) if ok else "the injector failed"
        return ok

    def do_mute_leader(self, a, p):
        snap = self.hub.snapshot()
        share = snap["kpi"]["share30"]
        alive = [int(m["id"]) for m in snap["machines"].values() if m["role"] == "node" and m["state"] in ("online", "stressed", "slowed")]
        if not alive:
            raise RuntimeError("no running, unmuted node to mute")
        nid = max(alive, key=lambda i: share.get(i) or 0)
        p = dict(p)
        p["node"] = nid
        a["title"] = "Mute the busiest node (node %d)" % nid
        return self.do_mute(a, p)

    def do_hog_on(self, a, p):
        nid = int(p["node"])
        secs = int(p.get("secs") or self.hub.settings["demo"]["hog_secs"])
        ok = self.par(a, "CPU stress on node %d's PC" % nid, [self.hub.node_cfg(nid)],
                      lambda m: fill(SCRIPTS["hog_on"], secs=secs, cores=int(p.get("cores") or 0)), timeout=30)
        a["summary"] = "node %d's PC is now busy: its load score rises, so it should win fewer elections" % nid
        return ok

    def do_hog_off(self, a, p):
        nodes = [self.hub.node_cfg(int(p["node"]))] if p.get("node") is not None else self.hub.nodes()
        nodes = [m for m in nodes if (m.get("ip") or "").strip()]
        if not nodes:
            return True
        return self.par(a, "Stop CPU stress", nodes, lambda m: SCRIPTS["hog_off"], timeout=30)

    # ---------------- correctness
    def do_dup_test(self, a, p):
        name = p.get("client") or "A"
        m = self.hub.client_cfg(name)
        seed = int(time.time()) * 10000000 + 7000000
        ok = self.par(a, "Send 5 incidents from client %s, then resend the first one" % name, [m],
                      lambda m: fill(SCRIPTS["dup_test"], count=int(p.get("count") or 5), seed=seed, **self.common()),
                      timeout=60)
        out = a["steps"][-1]["results"].get(m["key"], "")
        last = [l for l in out.splitlines() if "duplicate=" in l]
        passed = bool(last) and "duplicate=true" in last[-1]
        a["summary"] = ("PASS: the resent request got the stored plan back (duplicate=true), nothing ran twice"
                        if passed else "the resend did not report duplicate=true (see output)")
        return ok and passed

    # ---------------- storage repair (anti-entropy between the 3 SeaweedFS filers)
    def do_repair_storage(self, a, p):
        """Makes the 3 filers' copies of the bucket agree again after a crash.

        Each node PC runs its own filer and the filers copy every change to each other in the
        background. In SeaweedFS 4.48 that copying is not crash-safe: after a power cut a few
        entries written around the cut can be missing on one filer (Nov 7 lab). This pass
        talks to the 3 filers directly (port 8888, from this PC):
        - requests/, resolved/, resolutions/, resources/ never change once written, so an
          entry one filer lacks is copied to it from a filer that has it (same bytes);
        - claims/ of an incident that is already resolved are deleted (the nodes' sweeper does
          the same through S3, but only sees the filer its own gateway uses).
        Entries younger than 10 s are left alone: they may still be on their way."""
        import urllib.error
        import urllib.parse
        import urllib.request
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        bucket = "/buckets/rescuemesh"
        nodes = [m for m in self.hub.nodes() if (m.get("ip") or "").strip()]
        st = self.step(a, "Compare and repair the 3 filers' copies of the shared state")
        out, live = [], []
        for m in nodes:
            try:
                op.open("http://%s:8888/" % m["ip"].strip(), timeout=4).read()
                live.append(m)
            except Exception as e:  # noqa: BLE001
                out.append("%s: filer not reachable (%s): skipped" % (m["label"], e))
        if len(live) < 2:
            st["status"] = "fail"
            st["hosts"] = {"panel": {"status": "fail", "out": "\n".join(out + ["need at least 2 filers up"])}}
            a["summary"] = "need at least 2 filers up"
            return False

        def listing(m, prefix):
            names, last = {}, ""
            while True:
                url = "http://%s:8888%s/%s/?limit=1000&lastFileName=%s" % (m["ip"].strip(), bucket, prefix, urllib.parse.quote(last))
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                try:
                    d = json.load(op.open(req, timeout=20))
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        return names
                    raise
                for e in d.get("Entries") or []:
                    names[e["FullPath"].rsplit("/", 1)[1]] = (e.get("Md5"), e.get("Mtime") or "")
                if not d.get("ShouldDisplayLoadMore"):
                    return names
                last = d["LastFileName"]

        def fresh(mtime):  # written in the last 10 s (filer Mtime is local time with offset)
            try:
                from datetime import datetime
                return time.time() - datetime.fromisoformat(mtime).timestamp() < 10
            except Exception:  # noqa: BLE001
                return False

        copied = deleted = conflicts = 0
        views = {}
        for prefix in ("resources", "requests", "resolved", "resolutions"):
            views[prefix] = {m["key"]: listing(m, prefix) for m in live}
            union = set().union(*(v.keys() for v in views[prefix].values()))
            for name in sorted(union):
                have = [m for m in live if name in views[prefix][m["key"]]]
                if len(have) == len(live):
                    continue
                md5s = {views[prefix][m["key"]][name][0] for m in have}
                if len(md5s) > 1:
                    conflicts += 1
                    continue
                if any(fresh(views[prefix][m["key"]][name][1]) for m in have):
                    continue
                src = have[0]
                body = op.open("http://%s:8888%s/%s/%s" % (src["ip"].strip(), bucket, prefix, name), timeout=10).read()
                for m in live:
                    if name not in views[prefix][m["key"]]:
                        req = urllib.request.Request("http://%s:8888%s/%s/%s" % (m["ip"].strip(), bucket, prefix, name),
                                                     data=body, method="PUT", headers={"Content-Type": "application/octet-stream"})
                        op.open(req, timeout=10).read()
                        copied += 1
            out.append("%-12s %s" % (prefix + "/", "  ".join("%s %d" % (m["label"], len(views[prefix][m["key"]])) for m in live)))
        resolved = set().union(*(v.keys() for v in views["resolved"].values()))
        claims = {m["key"]: listing(m, "claims") for m in live}
        for name in sorted(set().union(*(v.keys() for v in claims.values()))):
            have = [m for m in live if name in claims[m["key"]]]
            if any(fresh(claims[m["key"]][name][1]) for m in have):
                continue
            try:
                c = json.loads(op.open("http://%s:8888%s/claims/%s" % (have[0]["ip"].strip(), bucket, name), timeout=10).read())
                incident = str(c["assignment"]["incident_id"])
            except Exception:  # noqa: BLE001  (listed but unreadable: a broken entry)
                incident = None
            if incident is None or incident in resolved:
                for m in have:
                    try:
                        op.open(urllib.request.Request("http://%s:8888%s/claims/%s" % (m["ip"].strip(), bucket, name), method="DELETE"), timeout=10).read()
                    except urllib.error.HTTPError as e:
                        if e.code != 404:
                            raise
                deleted += 1
        out.append("claims/      " + "  ".join("%s %d" % (m["label"], len(claims[m["key"]])) for m in live))
        summary = ("copied %d missing entr%s, deleted %d leftover claim%s%s"
                   % (copied, "y" if copied == 1 else "ies", deleted, "" if deleted == 1 else "s",
                      (", %d entries differ in content (left alone)" % conflicts) if conflicts else ""))
        out.append(summary)
        st["hosts"] = {"panel": {"status": "ok", "out": "\n".join(out)}}
        st["status"] = "ok"
        a["summary"] = summary + ("; run the state check again" if copied or deleted else "; the filers already agreed")
        return True

    def do_state_check(self, a, p):
        nodes = self.hub.nodes()
        self.check_ips(nodes)
        self.par(a, "Read the shared state through each node PC's own gateway", nodes,
                 lambda m: fill(SCRIPTS["state"], **self.common()), timeout=120)
        res = a["steps"][-1].get("results", {})
        views = {}
        for m in nodes:
            out = res.get(m["key"], "")
            mo = re.search(r"STATE (\{.*\})", out)
            if mo:
                views[m["id"]] = json.loads(mo.group(1))
        st = self.step(a, "Compare the 3 views")
        lines = []
        for nid in sorted(views):
            v = views[nid]
            lines.append("node %d PC: %d resources, %d active claims, %d saved requests, %d resolved  (claims %s, requests %s)"
                         % (nid, v["resources"], v["claims"], v["requests"], v["resolved"], v["claims_hash"], v["requests_hash"]))
        same = len(views) == 3 and len({json.dumps(v, sort_keys=True) for v in views.values()}) == 1
        unreadable = {nid: v.get("unreadable", 0) for nid, v in views.items() if v.get("unreadable")}
        if unreadable:
            same = False
            lines.append("SHARED STORAGE IS OUT OF SYNC: %s claim(s) are listed but cannot be read (%s). "
                         "The SeaweedFS filers disagree, usually after a power cut: press Repair storage, then check again."
                         % (max(unreadable.values()), ", ".join("via node %d" % n for n in sorted(unreadable))))
        elif same:
            v = list(views.values())[0]
            lines.append("ALL 3 GATEWAYS AGREE: same %d resources, same %d claims (byte for byte), same %d requests"
                         % (v["resources"], v["claims"], v["requests"]))
        elif len(views) == 3:
            lines.append("The views differ: if load is running the state changes while we read it, so stop the "
                         "load and check again. If they still differ (after a power cut), press Repair storage.")
        else:
            lines.append("Could not read all 3 gateways.")
        st["hosts"] = {"panel": {"status": "ok" if same else "fail", "out": "\n".join(lines)}}
        st["status"] = "ok" if same else "fail"
        a["summary"] = lines[-1]
        return same

    def do_reset_stats(self, a, p):
        self.hub.reset_stats()
        a["summary"] = "live counters reset"
        return True

    # ---------------- demo run (the Demo Run tab)
    def do_demo_run(self, a, p):
        with self.hub.lock:
            if self.hub.demo and self.hub.demo["status"] == "running":
                raise RuntimeError("a demo run is already running: stop it first")
            self.hub.demo_cancel.clear()
        return DemoRun(self, a, p).run()

    def do_demo_stop(self, a, p):
        self.hub.demo_cancel.set()
        a["summary"] = "the demo run stops after the current step and cleans up"
        return True

    def do_collect(self, a, p):
        s = self.hub.settings
        folder = os.path.join(COLLECT_DIR, time.strftime("%Y%m%d-%H%M%S"))
        os.makedirs(folder, exist_ok=True)
        st = self.step(a, "Download logs and results into %s" % folder)
        ok = True
        for m in s["machines"]:
            if not (m.get("ip") or "").strip():
                continue
            script = SCRIPTS["collect_node"] if m["role"] == "node" else SCRIPTS["collect_client"]
            rc, data = run_remote(m, s, script, timeout=300, binary=True)
            if rc != 0:
                ok = False
                st["hosts"][m["label"]] = {"status": "fail", "out": "SSH failed (exit %d)" % rc}
                continue
            if not data:
                st["hosts"][m["label"]] = {"status": "ok", "out": "nothing to collect yet"}
                continue
            path = os.path.join(folder, "%s.tar.gz" % m["label"].replace(" ", "-"))
            with open(path, "wb") as f:
                f.write(data)
            st["hosts"][m["label"]] = {"status": "ok", "out": "%s (%d KB)" % (path, len(data) // 1024)}
        st["status"] = "ok" if ok else "fail"
        a["summary"] = "saved in " + folder
        return ok


TITLES = {
    "test": "Check all PCs", "push_config": "Push config", "weed_start": "Start SeaweedFS", "weed_status": "SeaweedFS status",
    "weed_stop": "Stop SeaweedFS", "weed_wipe": "Wipe shared storage", "nodes_start": "Start nodes", "nodes_stop": "Stop nodes",
    "node_restart": "Restart a node", "bringup": "One-click bring-up", "full_reset": "Full reset", "stop_all": "Stop everything",
    "apply_faults": "Automatic fault injection", "load_start": "Start load", "load_stop": "Stop load", "mute": "Mute a node",
    "mute_leader": "Mute the busiest node", "power_cut": "Power cut", "power_on": "Power on", "hog_on": "CPU stress on", "hog_off": "CPU stress off",
    "dup_test": "Duplicate request test", "state_check": "Replicated state check", "repair_storage": "Repair storage", "reset_stats": "Reset counters",
    "collect": "Collect logs", "demo_run": "Demo run", "demo_stop": "Stop the demo run",
}


# ======================================================================== demo run
# The lab's test scenarios, run one after another by the Demo Run tab. Each one uses the same
# actions as the Mission Control buttons, so the dashboard shows it live, and checks its result
# from the same live data. "secs" is a rough length, shown in the tab.
DEMO_TESTS = [
    {"id": "health", "title": "Cluster health", "secs": 30,
     "goal": "PCs, peers and storage up; starts the steady load."},
    {"id": "normal", "title": "Normal operation and elections", "secs": 45,
     "goal": "Every peer wins work, fast answers, nothing lost."},
    {"id": "imbalance", "title": "Load imbalance", "secs": 85,
     "goal": "CPU-stress the busiest node: work moves away, then back."},
    {"id": "failure", "title": "Server failure and recovery", "secs": 60,
     "goal": "Mute the busiest node: detected, 0 lost, rejoins."},
    {"id": "powercut", "title": "Power cut and power on", "secs": 130,
     "goal": "Kill a node PC and its storage, then power it back on."},
    {"id": "auto", "title": "Automatic fault injection", "secs": 150,
     "goal": "The servers silence one of themselves at random."},
    {"id": "scale", "title": "Scale ramp", "secs": 100,
     "goal": "Ramp 1, 2, 4, 8 clients per PC."},
    {"id": "correct", "title": "Correctness", "secs": 60,
     "goal": "Same state on all 3 gateways, resources returned, duplicate detected."},
]
DEMO_ELIGIBLE = ("online", "stressed", "slowed")
RAMP_STAGES = (1, 2, 4, 8)
RAMP_STAGE_SECS = 20


class DemoCancelled(Exception):
    pass


class DemoRun:
    """Runs the chosen DEMO_TESTS in order inside one panel action (its steps show in Consoles).
    Progress and results live in hub.demo, which every snapshot carries to the browser."""

    def __init__(self, acts, a, p):
        self.acts, self.hub, self.a = acts, acts.hub, a
        self.rate = max(1, int(p.get("rate") or 20))
        self.clients = max(1, int(p.get("clients") or 4))
        chosen = set(p.get("tests") or [t["id"] for t in DEMO_TESTS])
        self.load_on = False
        self.stressed = None  # node we put under CPU stress
        self.cut = None       # node whose PC we powered off
        self.faults_on = False
        with self.hub.lock:
            self.hub.demo = {"id": a["id"], "status": "running", "started": now_ms(), "ended": None, "current": 0,
                             "phase": "starting", "until": None, "rate": self.rate, "clients": self.clients, "summary": "",
                             "tests": [dict(t, status="pending", checks=[], started=None, ended=None, note="")
                                       for t in DEMO_TESTS if t["id"] in chosen]}
        if not self.hub.demo["tests"]:
            raise RuntimeError("no test selected")

    # ---------------- helpers
    def phase(self, text, secs=None):
        with self.hub.lock:
            self.hub.demo["phase"] = text
            self.hub.demo["until"] = now_ms() + int(secs * 1000) if secs else None
        self.hub.invalidate()

    def sleep(self, secs):
        end = time.time() + secs
        while True:
            if self.hub.demo_cancel.is_set():
                raise DemoCancelled()
            left = end - time.time()
            if left <= 0:
                return
            time.sleep(min(0.25, left))

    def wait(self, text, secs):
        self.phase(text, secs)
        self.sleep(secs)

    def wait_until(self, text, cond, timeout):
        self.phase(text, timeout)
        end = time.time() + timeout
        while time.time() < end:
            v = cond()
            if v:
                return v
            self.sleep(1)
        return cond()

    def check(self, t, label, ok, value):
        """ok=None: a measurement, shown but not scored."""
        with self.hub.lock:
            t["checks"].append({"label": label, "pass": None if ok is None else bool(ok), "value": value})
        self.hub.invalidate()

    def snap(self):
        self.hub.invalidate()
        return self.hub.snapshot()

    def node_states(self):
        return {int(m["id"]): m["state"] for m in self.snap()["machines"].values() if m["role"] == "node"}

    def all_up(self):
        st = self.node_states()
        return len(st) == 3 and all(s in DEMO_ELIGIBLE for s in st.values())

    def wait_all_up(self, timeout=60):
        if not self.wait_until("Waiting for all 3 cloud peers to be up", self.all_up, timeout):
            raise RuntimeError("not all 3 cloud peers are up: %s" % self.node_states())
        self.hub.invalidate()

    def stats(self, t0, t1):
        return self.hub.window_stats(t0, t1)

    def settle(self, t1, secs=8):
        """Client rows arrive a few seconds late (loadgen buffers its CSV; a lost request is only
        known after its retries), so wait before reading a window that ended at t1."""
        left = (t1 + secs * 1000 - now_ms()) / 1000.0
        if left > 0:
            self.wait("Collecting the last results", left)

    def start_load(self):
        self.phase("Starting the steady client load: %d req/s and %d clients per PC" % (self.rate, self.clients))
        if not self.acts.do_load_start(self.a, {"mode": "fixed", "rate": self.rate, "clients": self.clients, "requests": 1999999}):
            raise RuntimeError("the client load did not start on every client PC")
        self.load_on = True
        self.wait("Letting the load settle", 8)

    def ensure_load(self):
        if self.load_on:
            return
        running = sum(1 for m in self.snap()["machines"].values() if m["role"] == "client" and m["state"] == "running")
        if running == len(CLIENT_NAMES):
            self.load_on = True  # the user's load is already running: use it
            return
        self.start_load()

    def stop_load(self):
        self.phase("Stopping the client load")
        self.acts.do_load_stop(self.a, {})
        self.load_on = False

    def busiest(self):
        """The node that did most of the work in the last 20 s (the elected server), among the live ones."""
        now = now_ms()
        w = self.stats(now - 23000, now - 3000)
        st = self.node_states()
        live = [i for i in NODE_IDS if st.get(i) in DEMO_ELIGIBLE] or list(NODE_IDS)
        nid = max(live, key=lambda i: w["share"][i] or 0)
        return nid, w["share"][nid]

    def storage_leader_is(self, nid):
        """True if node nid's PC runs the SeaweedFS master leader (asked over HTTP, port 9333)."""
        import urllib.request
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        for m in self.hub.nodes():
            try:
                st = json.loads(op.open("http://%s:9333/cluster/status" % m["ip"].strip(), timeout=3).read())
                return st.get("Leader", "").split(":")[0] == self.hub.node_cfg(nid)["ip"].strip()
            except Exception:  # noqa: BLE001
                continue
        return None

    def failure_of(self, nid, t0, kinds, ready=False):
        for f in reversed(self.snap()["failures"]):
            if f["node"] == nid and f["kind"] in kinds and f["start"] >= t0 - 3000 and (f["ready"] is not None or not ready):
                return f
        return None

    def share_after(self, nid, secs=20):
        self.wait("Watching node %d get work again" % nid, secs)
        now = now_ms()
        v = self.stats(now - (secs - 5) * 1000, now - 3000)["share"][nid]
        return v

    @staticmethod
    def pct_s(v):
        return "-" if v is None else "%.0f%%" % v

    @staticmethod
    def shares(w):
        return " / ".join("N%d %s" % (i, DemoRun.pct_s(w["share"][i])) for i in NODE_IDS)

    @staticmethod
    def lost_s(w):
        return "%s answered, %d lost, %d saved by a retry" % ("{:,}".format(w["ok"]), w["lost"], w["retried"])

    # ---------------- the tests
    def t_health(self, t):
        sn = self.snap()
        ms = list(sn["machines"].values())
        conn = sum(1 for m in ms if m["connected"])
        self.check(t, "all 6 lab PCs reachable over SSH", conn == 6, "%d/6" % conn)
        nodes = [m for m in ms if m["role"] == "node"]
        up = sum(1 for m in nodes if m["state"] in DEMO_ELIGIBLE)
        self.check(t, "3 cloud peers running", up == 3, "%d/3" % up)
        weed = sum(1 for m in nodes if (m["proc"] or {}).get("weed") and not m.get("weed_down"))
        self.check(t, "SeaweedFS replica complete on the 3 node PCs", weed == 3, "%d/3" % weed)
        if up < 3 or weed < 3:
            return
        self.phase("Clearing old load and CPU stress")
        self.acts.do_hog_off(self.a, {})
        self.stop_load()
        self.hub.reset_stats()
        self.hub.event("info", "Demo run: counters reset")
        self.start_load()
        self.wait("Checking that incidents flow", 4)
        sn = self.snap()
        running = sum(1 for m in sn["machines"].values() if m["role"] == "client" and m["state"] == "running")
        self.check(t, "the 3 client PCs send incidents and get plans back", running == 3 and sn["kpi"]["okps"] > 0,
                   "%d/3 PCs · %.0f ok/s" % (running, sn["kpi"]["okps"]))

    def t_normal(self, t):
        self.ensure_load()
        t0 = now_ms()
        self.wait("Normal operation: watching the elections for 30 s", 30)
        t1 = now_ms()
        self.settle(t1)
        w = self.stats(t0, t1)
        offered = self.rate * len(CLIENT_NAMES)
        self.check(t, "every peer wins elections (each at least 5%)", all((w["share"][i] or 0) >= 5 for i in NODE_IDS), self.shares(w))
        self.check(t, "requests answered fast (p50 under 50 ms)", w["p50"] is not None and w["p50"] < 50,
                   "p50 %s ms · p95 %s ms" % (w["p50"], w["p95"]))
        self.check(t, "throughput keeps up with the offered load", w["okps"] >= 0.85 * offered,
                   "%.0f of %d req/s" % (w["okps"], offered))
        self.check(t, "0 requests lost", w["lost"] == 0 and w["ok"] > 0, self.lost_s(w))

    def t_imbalance(self, t):
        self.ensure_load()
        self.wait_all_up()
        nid, before = self.busiest()
        self.phase("Putting node %d's PC under CPU stress" % nid)
        if not self.acts.do_hog_on(self.a, {"node": nid, "secs": 180, "cores": 0}):
            self.check(t, "CPU stress started on node %d's PC" % nid, False, "failed: see Consoles")
            return
        self.stressed = nid
        self.wait("Node %d is under CPU stress: its load score rises" % nid, 10)
        t0 = now_ms()
        self.wait("Measuring the elections while node %d is stressed" % nid, 25)
        t1 = now_ms()
        self.phase("Removing the CPU stress")
        self.acts.do_hog_off(self.a, {"node": nid})
        self.stressed = None
        after = self.share_after(nid, 25)
        w = self.stats(t0, t1)
        ld = w["load"]
        top = max(NODE_IDS, key=lambda i: ld[i] or 0)
        self.check(t, "node %d's load score became the highest" % nid, top == nid,
                   " / ".join("N%d %s" % (i, "-" if ld[i] is None else "%.3f" % ld[i]) for i in NODE_IDS))
        self.check(t, "its share of the work fell below 15%", w["share"][nid] is not None and w["share"][nid] < 15,
                   "%s (before: %s)" % (self.pct_s(w["share"][nid]), self.pct_s(before)))
        self.check(t, "0 requests lost while it was stressed", w["lost"] == 0 and w["ok"] > 0, self.lost_s(w))
        self.check(t, "after the stress it does work again (share above 5%)", (after or 0) > 5, self.pct_s(after))

    def t_failure(self, t):
        self.ensure_load()
        self.wait_all_up()
        nid, _ = self.busiest()
        secs = max(2, min(20, int(self.hub.settings["demo"].get("mute_secs") or 12)))
        t0 = now_ms()
        if not self.acts.do_mute(self.a, {"node": nid, "secs": secs}):
            self.check(t, "node %d silenced" % nid, False, "the injector failed: see Consoles")
            return
        self.wait("Node %d is silent for %d s: the other two take over" % (nid, secs), secs)
        f = self.wait_until("Node %d talks again: it catches up with the shared state" % nid,
                            lambda: self.failure_of(nid, t0, ("manual",), ready=True), 30)
        after = self.share_after(nid, 20)
        f = f or self.failure_of(nid, t0, ("manual",))
        if not f:
            self.check(t, "node %d's silence was recorded" % nid, False, "no MUTE_START seen in its fault log")
            return
        w = self.stats(f["start"], (f["end"] or now_ms()) + 3000)
        self.check(t, "its peers detected the silence in under 2 s", f["detect"] is not None and f["detect"] < 2000,
                   "-" if f["detect"] is None else "%.2f s (by node %s)" % (f["detect"] / 1000.0, f["detected_by"]))
        self.check(t, "service continued: 0 requests lost", w["lost"] == 0 and w["ok"] > 0, self.lost_s(w))
        self.check(t, "it caught up and rejoined (READY)", f["recovery"] is not None,
                   "-" if f["recovery"] is None else "+%.2f s after the silence" % (f["recovery"] / 1000.0))
        self.check(t, "it does work again (share above 5%)", (after or 0) > 5, self.pct_s(after))

    def t_powercut(self, t):
        self.ensure_load()
        self.wait_all_up()
        nid, _ = self.busiest()
        leader = self.storage_leader_is(nid)
        t0 = now_ms()
        if not self.acts.do_power_cut(self.a, {"node": nid}):
            self.check(t, "power cut of node %d's PC" % nid, False, "failed: see Consoles")
            return
        self.cut = nid
        # Cutting the PC of the SeaweedFS master leader is the hard case: writes wait for a new
        # leader and for the volume servers to re-register with it.
        self.check(t, "node %d's PC lost power (node and storage replica killed)" % nid, True,
                   "kill -9 · %s" % {True: "it was the storage (Raft) leader", False: "not the storage leader",
                                     None: "storage leader unknown"}[leader])
        self.wait("Node %d's PC has no power: the other two keep serving" % nid, 25)
        t1 = now_ms()
        self.phase("Powering node %d's PC back on: SeaweedFS first, then the node" % nid)
        on = self.acts.do_power_on(self.a, {"node": nid})
        if on:
            self.cut = None
        f = None
        if on:
            f = self.wait_until("Node %d is catching up with the shared state" % nid,
                                lambda: self.failure_of(nid, t0, ("down",), ready=True), 45)
        after = self.share_after(nid, 20) if on else None
        f = f or self.failure_of(nid, t0, ("down",))
        w = self.stats(t0, t1)
        self.check(t, "its peers noticed the missing heartbeats", f is not None,
                   "-" if not f else "after %.2f s" % (max(0, f["start"] - t0) / 1000.0))
        self.check(t, "the other two kept answering during the outage", w["ok"] > 0, "%.0f ok/s" % w["okps"])
        self.check(t, "0 requests lost during the outage", w["lost"] == 0 and w["ok"] > 0, self.lost_s(w))
        self.check(t, "after power on it recovered its state and rejoined", bool(on and f and f["ready"]),
                   "-" if not (on and f and f["ready"]) else "ready %.1f s after power on began" % (max(0, f["ready"] - t1) / 1000.0))
        self.check(t, "it does work again (share above 5%)", (after or 0) > 5, self.pct_s(after))
        # Known SeaweedFS 4.48 limit: entries written around the cut can be missing on one filer.
        self.phase("Repairing storage entries a filer may have missed during the cut")
        self.acts.do_repair_storage(self.a, {})
        self.check(t, "storage replicas compared and repaired", True, self.a.get("summary") or "done")

    def t_auto(self, t):
        self.stop_load()  # changing the injector setting restarts the 3 nodes
        self.phase("Restarting the 3 nodes with automatic fault injection ON")
        if not self.acts.do_apply_faults(self.a, {"enabled": True}):
            self.check(t, "automatic injector turned on", False, "the nodes did not restart: see Consoles")
            return
        self.faults_on = True
        self.check(t, "automatic injector turned on (nodes restarted with it)", True, "on")
        self.wait_all_up()
        self.wait("Letting the nodes rejoin", 8)
        self.start_load()
        t0 = now_ms()

        def closed():
            fs = [f for f in self.snap()["failures"] if f["kind"] == "auto" and f["start"] >= t0 - 1000]
            done = [f for f in fs if f["ready"] is not None]
            return fs if len(done) >= 2 and len(done) == len(fs) else None
        self.wait_until("The servers pick a victim at random and silence it (every 10-30 s, for 2-20 s)", closed, 100)
        t1 = now_ms()
        self.settle(t1)
        fs = [f for f in self.snap()["failures"] if f["kind"] == "auto" and t0 - 1000 <= f["start"] <= t1]
        w = self.stats(t0, t1)
        self.check(t, "the servers silenced a node by themselves", len(fs) > 0,
                   ", ".join("N%d %.1f s" % (f["node"], (f["planned"] or 0) / 1000.0) for f in fs) or "none in 100 s")
        # A fault that started just before the end may still be running: judge only finished ones.
        judged = [f for f in fs if f["ready"] is not None or f["start"] < t1 - 30000]
        ok_rec = [f for f in judged if f["ready"] is not None]
        self.check(t, "every silenced node was detected and rejoined", bool(judged) and len(ok_rec) == len(judged),
                   "%d of %d" % (len(ok_rec), len(judged)))
        self.check(t, "service continued: 0 requests lost", w["lost"] == 0 and w["ok"] > 0, self.lost_s(w))
        self.stop_load()
        self.phase("Restarting the 3 nodes with automatic fault injection OFF")
        if self.acts.do_apply_faults(self.a, {"enabled": False}):
            self.faults_on = False
        self.wait_all_up()
        self.wait("Letting the nodes rejoin", 8)

    def t_scale(self, t):
        self.stop_load()
        stages = ",".join(str(x) for x in RAMP_STAGES)
        self.phase("Starting a client ramp: %s clients per PC, %d s each" % (stages, RAMP_STAGE_SECS))
        if not self.acts.do_load_start(self.a, {"mode": "ramp", "stages": stages, "stage_secs": RAMP_STAGE_SECS}):
            self.check(t, "ramp started", False, "loadgen did not start: see Consoles")
            return
        self.load_on = True
        t0 = now_ms() + 1000  # loadgen start-up
        for i, c in enumerate(RAMP_STAGES):
            self.wait("Ramp stage %d of %d: %d clients per PC (%d in total)" % (i + 1, len(RAMP_STAGES), c, c * len(CLIENT_NAMES)),
                      max(1, (t0 + (i + 1) * RAMP_STAGE_SECS * 1000 - now_ms()) / 1000.0))
        t1 = now_ms()
        self.settle(t1)
        self.stop_load()
        ws = [self.stats(t0 + i * RAMP_STAGE_SECS * 1000 + 4000, t0 + (i + 1) * RAMP_STAGE_SECS * 1000 - 1000)
              for i in range(len(RAMP_STAGES))]
        lost = sum(w["lost"] for w in ws)
        peak = max(range(len(ws)), key=lambda i: ws[i]["okps"])
        # Measurements (pass=None: shown, not scored). Past saturation latency grows with the
        # number of clients by Little's law (latency = clients / throughput): that is expected.
        self.check(t, "throughput per stage (answered/s)", None,
                   " → ".join("%.0f" % w["okps"] for w in ws) + "  (peak at %d clients)" % (RAMP_STAGES[peak] * len(CLIENT_NAMES)))
        self.check(t, "latency p50 per stage", None, " / ".join("%s ms" % w["p50"] for w in ws))
        self.check(t, "fast at light load (p50 under 50 ms with %d client per PC)" % RAMP_STAGES[0],
                   ws[0]["p50"] is not None and ws[0]["p50"] < 50, "%s ms" % ws[0]["p50"])
        self.check(t, "no collapse: the top stage still answers at least half the peak",
                   ws[-1]["okps"] >= 0.5 * ws[peak]["okps"] > 0, "%.0f of %.0f/s" % (ws[-1]["okps"], ws[peak]["okps"]))
        self.check(t, "0 requests lost as the load grows", lost == 0 and sum(w["ok"] for w in ws) > 0,
                   "%s answered, %d lost" % ("{:,}".format(sum(w["ok"] for w in ws)), lost))

    def t_correct(self, t):
        self.stop_load()
        self.wait("Load stopped: letting every incident be resolved", 12)
        self.phase("Reading the shared state through each node PC's own gateway")
        same = self.acts.do_state_check(self.a, {})
        summ = self.a.get("summary") or ""
        if not same:
            self.phase("The views differ: repairing storage, then checking again")
            self.acts.do_repair_storage(self.a, {})
            self.sleep(3)
            same = self.acts.do_state_check(self.a, {})
            summ = (self.a.get("summary") or "") + " (after Repair storage)"
        self.check(t, "all 3 gateways see the same state", same, summ)
        m = re.search(r"same (\d+) claims", summ)
        self.check(t, "every resource was given back (0 claims left)", bool(m) and int(m.group(1)) == 0,
                   ("%s claims left" % m.group(1)) if m else "-")
        self.phase("Duplicate test: send 5 incidents from client A, then resend the first one")
        ok = self.acts.do_dup_test(self.a, {"client": "A"})
        self.check(t, "a resent request is answered from the store, nothing planned twice", ok, self.a.get("summary") or "")

    # ---------------- driver
    def cleanup(self, stopped):
        steps = []
        if self.stressed:
            steps.append(lambda: self.acts.do_hog_off(self.a, {"node": self.stressed}))
        if self.cut:
            steps.append(lambda: self.acts.do_power_on(self.a, {"node": self.cut}))
        if self.faults_on:
            steps.append(lambda: self.acts.do_load_stop(self.a, {}))
            steps.append(lambda: self.acts.do_apply_faults(self.a, {"enabled": False}))
        elif stopped and self.load_on:
            steps.append(lambda: self.acts.do_load_stop(self.a, {}))
        if steps:
            self.phase("Cleaning up (CPU stress, powered-off PC, fault injector, load)")
        for fn in steps:
            try:
                fn()
            except Exception:  # noqa: BLE001
                traceback.print_exc()

    def run(self):
        d, lock = self.hub.demo, self.hub.lock
        tests = d["tests"]
        passed, stopped, aborted = 0, False, ""
        self.hub.event("action", "Demo run started: %d test(s)" % len(tests))
        try:
            for i, t in enumerate(tests):
                with lock:
                    d["current"] = i
                    t["status"], t["started"] = "running", now_ms()
                self.hub.event("info", "Demo run %d/%d: %s" % (i + 1, len(tests), t["title"]))
                ok = False
                try:
                    getattr(self, "t_" + t["id"])(t)
                    scored = [c for c in t["checks"] if c["pass"] is not None]
                    ok = bool(scored) and all(c["pass"] for c in scored)
                except DemoCancelled:
                    with lock:
                        t["status"], t["ended"] = "stopped", now_ms()
                    raise
                except Exception as e:  # noqa: BLE001
                    traceback.print_exc()
                    with lock:
                        t["note"] = str(e)
                with lock:
                    t["status"], t["ended"] = ("pass" if ok else "fail"), now_ms()
                self.hub.event("ready" if ok else "warn", "Demo run: %s %s" % (t["title"], "PASSED" if ok else "FAILED"))
                passed += ok
                if not ok and t["id"] == "health":
                    aborted = "the cluster is not ready (see Cluster health): fix it on the Setup page, then run again"
                    break
        except DemoCancelled:
            stopped = True
        finally:
            self.hub.demo_cancel.clear()
            self.cleanup(stopped)
            with lock:
                for t in tests:
                    if t["status"] == "pending":
                        t["status"] = "skipped"
                d["status"] = "stopped" if stopped else ("ok" if passed == len(tests) else "fail")
                d["summary"] = ("stopped by you after %d passed" % passed if stopped else
                                aborted or "%d of %d tests passed" % (passed, len(tests)))
                d["phase"], d["until"], d["ended"] = "", None, now_ms()
            self.hub.invalidate()
        self.hub.event("ready" if d["status"] == "ok" else "warn", "Demo run finished: " + d["summary"])
        self.a["summary"] = d["summary"]
        return d["status"] == "ok"


# ======================================================================== HTTP

HUB = None
ACTIONS = None

MIME = {".html": "text/html; charset=utf-8", ".js": "application/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png", ".json": "application/json"}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet
        pass

    def send_json(self, obj, code=200):
        body = json.dumps(obj, default=list).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n).decode("utf-8") or "{}") if n else {}

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/stream":
            return self.stream()
        if path == "/api/term/stream":
            return self.term_stream()
        if path == "/api/settings":
            return self.send_json(HUB.settings)
        if path == "/api/snapshot":
            return self.send_json(HUB.snapshot())
        if path == "/api/config":
            body = build_config(HUB.settings).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/":
            path = "/index.html"
        f = os.path.normpath(os.path.join(STATIC, path.lstrip("/")))
        if not f.startswith(STATIC) or not os.path.isfile(f):
            self.send_error(404)
            return
        with open(f, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(os.path.splitext(f)[1], "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        try:
            data = self.read_json()
            if path == "/api/settings":
                s = deep_merge(DEFAULT_SETTINGS, data)
                s["machines"] = [deep_merge(d, m) for d, m in zip(DEFAULT_SETTINGS["machines"], data.get("machines") or [])]
                with HUB.lock:
                    HUB.settings = s
                    HUB.reset_machine_state()
                save_settings(s)
                HUB.start_agents()
                return self.send_json({"ok": True})
            if path == "/api/term/open":
                t = term_open(data["key"], int(data.get("cols") or 100), int(data.get("rows") or 24))
                return self.send_json({"ok": True, "key": t.key})
            if path == "/api/term/input":
                t = TERMS.get(data["key"])
                if t:
                    t.write(data.get("data", "").encode("utf-8"))
                return self.send_json({"ok": bool(t)})
            if path == "/api/term/resize":
                t = TERMS.get(data["key"])
                if t:
                    t.resize(int(data["cols"]), int(data["rows"]))
                return self.send_json({"ok": bool(t)})
            if path == "/api/term/close":
                t = TERMS.pop(data["key"], None)
                if t:
                    t.close()
                return self.send_json({"ok": True})
            if path == "/api/action":
                aid = ACTIONS.start(data["name"], data.get("params") or {})
                return self.send_json({"ok": True, "id": aid})
            self.send_error(404)
        except Exception as e:  # noqa: BLE001
            self.send_json({"ok": False, "error": str(e)}, 400)

    def term_stream(self):
        from urllib.parse import urlparse, parse_qs
        key = (parse_qs(urlparse(self.path).query).get("key") or [""])[0]
        t = TERMS.get(key)
        if not t:
            return self.send_error(404)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        idx = t.base
        try:
            while True:
                with t.cv:
                    while idx >= t.base + len(t.chunks) and t.alive:
                        t.cv.wait(15)
                        if idx >= t.base + len(t.chunks) and t.alive:
                            self.wfile.write(b": ping\n\n")
                            self.wfile.flush()
                    idx = max(idx, t.base)
                    out = b"".join(t.chunks[idx - t.base:])
                    idx = t.base + len(t.chunks)
                    alive = t.alive
                if out:
                    self.wfile.write(("data: %s\n\n" % base64.b64encode(out).decode()).encode())
                    self.wfile.flush()
                if not alive:
                    self.wfile.write(b"event: closed\ndata: 1\n\n")
                    self.wfile.flush()
                    return
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            return

    def stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        try:
            while True:
                try:
                    body = json.dumps(HUB.snapshot(), default=list, separators=(",", ":"))
                except Exception as e:  # noqa: BLE001 - keep the stream alive, show the problem
                    traceback.print_exc()
                    body = json.dumps({"error": "snapshot failed: %s" % e})
                self.wfile.write(("data: %s\n\n" % body).encode("utf-8"))
                self.wfile.flush()
                time.sleep(1.0)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            return


def main():
    global HUB, ACTIONS
    ap = argparse.ArgumentParser(description="RescueMesh demo control panel")
    ap.add_argument("--port", type=int, default=8090)  # not 8080: SeaweedFS's volume port
    ap.add_argument("--bind", default="127.0.0.1")
    args = ap.parse_args()
    if not shutil.which("ssh"):
        print("WARNING: no `ssh` command found on this PC; the panel cannot reach the lab PCs.")
    HUB = Hub()
    ACTIONS = Actions(HUB)
    HUB.start_agents()
    srv = ThreadingHTTPServer((args.bind, args.port), Handler)
    srv.daemon_threads = True
    print("RescueMesh demo panel: http://%s:%d   (Ctrl+C to quit)" % ("localhost" if args.bind in ("127.0.0.1", "0.0.0.0") else args.bind, args.port))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        HUB.stop_agents()


if __name__ == "__main__":
    main()
