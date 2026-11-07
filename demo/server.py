#!/usr/bin/env python3
"""RescueMesh demo control panel (Phase 1 demo). Run it on a PC that can SSH to all 6 lab PCs:

    python3 demo/server.py            then open http://localhost:8080
    python3 demo/server.py --port 9000 --bind 0.0.0.0

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
    "cluster": {"node_port": 50051, "run_id": "demo", "faults_enabled": False, "faults_seed": 42,
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
                ev_("mute", "Node %d went silent for %.1f s (%s fault)" % (nid, dur / 1000.0, kind), nid, t)
            elif ev == "MUTE_END":
                f = open_failure(nid)
                if f and f["end"] is None:
                    f["end"] = t
                ev_("unmute", "Node %d is talking again and rejoins" % nid, nid, t)
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
                    ev_("ready", "Node %d caught up with the shared state and is back in elections (+%.2f s)"
                        % (nid, f["recovery"] / 1000.0), nid, t)
                else:
                    ev_("ready", "Node %d is ready (%s)" % (nid, fault_id), nid, t)
            elif ev == "PEER_DOWN" and peer:
                f = open_failure(peer)
                if f is not None and f["kind"] != "down" and f["detect"] is None:
                    f["detect"] = max(0, t - f["start"])
                    f["detected_by"] = nid
                    ev_("down", "Node %d detected that node %d is down after %.2f s" % (nid, peer, f["detect"] / 1000.0), peer, t)
                elif f is not None:
                    ev_("down", "Node %d also sees node %d down" % (nid, peer), peer, t)
                elif starting:
                    ev_("info", "Node %d does not hear node %d yet (start-up)" % (nid, peer), peer, t)
                else:
                    fails.append({"id": "down-%d-%d" % (peer, t), "node": peer, "fault_id": "", "kind": "down", "start": t,
                                  "planned": 0, "detect": None, "detected_by": nid, "end": None, "ready": None,
                                  "recovery": None})
                    ev_("down", "Node %d suspects node %d is down (no heartbeats)" % (nid, peer), peer, t)
            elif ev == "PEER_UP" and peer:
                f = open_failure(peer)
                if f and f["kind"] == "down" and f["end"] is None:
                    f["end"] = t
                ev_("up", "Node %d hears node %d again" % (nid, peer), peer, t)
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
        if st["muted"]:
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
nohup ./target/release/node --id @@id@@ --config @@cfg@@ --run-id @@run@@ @@extra@@ </dev/null >>logs/node@@id@@.log 2>&1 &
echo $! > logs/node.pid
sleep 1.5
if kill -0 "$(cat logs/node.pid)" 2>/dev/null; then
  echo "node @@id@@ started (pid $(cat logs/node.pid)) @@extra@@"
  tail -n 3 logs/node@@id@@.log | sed 's/\x1b\[[0-9;]*m//g'
else
  echo "ERROR: node @@id@@ stopped right away:"; tail -n 25 logs/node@@id@@.log | sed 's/\x1b\[[0-9;]*m//g'; exit 1
fi
''',
    "node_stop": r'''
P=$(pgrep -f "target/release/node --id @@id@@ ")
if [ -z "$P" ]; then echo "node @@id@@ was not running"; exit 0; fi
kill $P
for i in 1 2 3 4 5 6 7 8; do sleep 0.5; pgrep -f "target/release/node --id @@id@@ " >/dev/null || { echo "node @@id@@ stopped"; exit 0; }; done
kill -9 $P 2>/dev/null; echo "node @@id@@ killed (did not stop in 4 s)"
''',
    "mute": r'''
[ -x target/release/injector ] || { echo "ERROR: target/release/injector is missing: run cargo build --release"; exit 4; }
./target/release/injector --node @@id@@ --duration @@secs@@s --config @@cfg@@ 2>&1 | sed 's/\x1b\[[0-9;]*m//g'
exit ${PIPESTATUS[0]}
''',
    "hog_on": r'''
N=$(nproc)
for i in $(seq $N); do nohup timeout @@secs@@ sh -c ': rescuemesh-hog; while :; do :; done' </dev/null >/dev/null 2>&1 & done
echo "CPU stress started on all $N cores of $(hostname) for @@secs@@ s"
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
kill $P; sleep 1; pgrep -f "target/release/loadgen " >/dev/null && kill -9 $P 2>/dev/null
echo "loadgen stopped"
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
import hashlib, json, re, urllib.parse, urllib.request
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
    for k in sorted(cl):
        h.update(k.encode()); h.update(O.open(base + "/" + k, timeout=15).read())
    hr = hashlib.sha256("\n".join(sorted(rq)).encode()).hexdigest()[:12]
    print("STATE " + json.dumps({"resources": len(res), "claims": len(cl), "requests": len(rq), "resolved": len(rs),
                                 "claims_hash": h.hexdigest()[:12], "requests_hash": hr}))
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
                self.hub.event("action" if a["status"] == "ok" else "warn",
                               "%s: %s%s" % (a["title"], "done" if a["status"] == "ok" else "FAILED",
                                             (" - " + a["summary"]) if a["summary"] else ""))

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
            bins = "node injector" if m["role"] == "node" else "loadgen client"
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
                      lambda m: fill(SCRIPTS["node_start"], id=m["id"], clear=clear, extra="", **c), timeout=40)
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
        extra = "--planner-iterations %d" % it if it > 0 else ""
        ok = self.par(a, "Start node %d %s" % (nid, extra), [m],
                      lambda m: fill(SCRIPTS["node_start"], id=nid, clear="0", extra=extra, **c), timeout=40)
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
                      lambda m: fill(SCRIPTS["node_start"], id=m["id"], clear="0", extra="", **c), timeout=40)
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
        return self.par(a, "Stop loadgen", clients, lambda m: SCRIPTS["load_stop"], timeout=30)

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
                      lambda m: fill(SCRIPTS["hog_on"], secs=secs), timeout=30)
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
        if same:
            v = list(views.values())[0]
            lines.append("ALL 3 GATEWAYS AGREE: same %d resources, same %d claims (byte for byte), same %d requests"
                         % (v["resources"], v["claims"], v["requests"]))
        elif len(views) == 3:
            lines.append("The views differ: if load is running the state changes while we read it. "
                         "Stop the load and run the check again for an exact comparison.")
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
    "mute_leader": "Mute the busiest node", "hog_on": "CPU stress on", "hog_off": "CPU stress off",
    "dup_test": "Duplicate request test", "state_check": "Replicated state check", "reset_stats": "Reset counters",
    "collect": "Collect logs",
}


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
            if path == "/api/action":
                aid = ACTIONS.start(data["name"], data.get("params") or {})
                return self.send_json({"ok": True, "id": aid})
            self.send_error(404)
        except Exception as e:  # noqa: BLE001
            self.send_json({"ok": False, "error": str(e)}, 400)

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
    ap.add_argument("--port", type=int, default=8080)
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
