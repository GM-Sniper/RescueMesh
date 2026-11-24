"""RescueMesh demo agent: runs on ONE lab PC, started by demo/server.py over SSH.

    ssh <pc> python3 -u - <base64 settings>      (this file arrives on stdin)

It never changes anything on the PC. Once a second it prints one JSON line with what it
sees on this PC:
- processes: is our node / SeaweedFS / loadgen running, and their memory;
- the machine: CPU %, memory %, LAN bytes per second;
- new rows of the files the programs already write:
    node PC:   logs/elections-nodeN.csv, requests-nodeN.csv, faults-nodeN.csv,
               node_metrics-nodeN.csv and the node's text log;
    client PC: the newest results/demo-<X>/*.csv of loadgen and its .out file.

Files that already exist when the agent starts are read from their END (no replay of an old
run); a file that appears later, or is replaced, is read from its start.
Standard library only, Python 3.6+, Linux /proc.
"""
import base64
import json
import os
import re
import socket
import sys
import time

CFG = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))
ROLE = CFG["role"]  # "node" or "client"
REPO = os.path.expanduser(CFG["repo"])
SEAWEED_DIR = os.path.expanduser(CFG.get("seaweed_dir") or "~/rescuemesh-seaweed")
ANSI = re.compile(r"\x1b\[[0-9;]*m")
HEADERS = ("run_id,", "node_id,")


class Tail:
    """Reads the lines added to a file since the last call."""

    def __init__(self, path, from_end=True):
        self.path = path
        self.ino = None
        self.off = 0
        self.buf = b""
        self.first = True
        self.from_end = from_end

    def read(self, max_bytes=4000000):
        try:
            st = os.stat(self.path)
        except OSError:
            self.ino = None
            self.first = False
            return []
        if self.ino != st.st_ino or st.st_size < self.off:
            # A new file (or the same name replaced): start at its end only if it was
            # already there when the agent started.
            self.off = st.st_size if (self.first and self.from_end) else 0
            self.ino = st.st_ino
            self.buf = b""
        self.first = False
        if st.st_size <= self.off:
            return []
        with open(self.path, "rb") as f:
            f.seek(self.off)
            data = f.read(min(max_bytes, st.st_size - self.off))
        self.off += len(data)
        lines = (self.buf + data).split(b"\n")
        self.buf = lines.pop()  # an unfinished last line waits for the next call
        return [l.decode("utf-8", "replace") for l in lines if l and not l.startswith(b"run_id,") and not l.startswith(b"node_id,")]


# ---------- machine readings ----------

def cpu_times():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    idle = v[3] + (v[4] if len(v) > 4 else 0)
    return sum(v), idle


def net_bytes():
    rx = tx = 0
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            name, data = line.split(":", 1)
            if name.strip() == "lo":
                continue
            p = data.split()
            rx += int(p[0])
            tx += int(p[8])
    return rx, tx


def mem():
    d = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            d[k] = int(v.split()[0])
    total = d.get("MemTotal", 1)
    return 100.0 * (1 - d.get("MemAvailable", total) / float(total)), total // 1024


def rss_mb(pid):
    try:
        with open("/proc/%d/status" % pid) as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024.0, 1)
    except OSError:
        pass
    return 0.0


def processes():
    out = []
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            with open("/proc/%s/cmdline" % d, "rb") as f:
                parts = [x.decode("utf-8", "replace") for x in f.read().split(b"\0") if x]
        except OSError:
            continue
        if parts:
            out.append((int(d), parts))
    return out


def proc_status():
    node = None
    loadgen = None
    weed = {}
    weed_rss = 0.0
    hogs = 0
    for pid, parts in processes():
        # The program is argv[0], or argv[1] when started through an interpreter (sh, bash).
        i = 1 if len(parts) > 1 and os.path.basename(parts[0]) in ("sh", "bash", "dash") else 0
        exe = os.path.basename(parts[i])
        args = parts[i + 1:]
        line = " ".join(parts)
        if exe == "node" and "--id" in args:
            node = pid
        elif exe == "loadgen":
            loadgen = pid
        elif exe == "weed" and args:
            if args[0] in ("master", "volume", "filer", "s3"):
                weed[args[0]] = True
                weed_rss += rss_mb(pid)
        elif "rescuemesh-hog" in line and exe not in ("timeout", "pkill"):
            hogs += 1
    return {
        "node": node is not None,
        "node_pid": node,
        "node_rss": rss_mb(node) if node else 0.0,
        "loadgen": loadgen is not None,
        "loadgen_pid": loadgen,
        "weed": {r: bool(weed.get(r)) for r in ("master", "volume", "filer", "s3")},
        "weed_rss": round(weed_rss, 1),
        "hogs": hogs,
    }


# ---------- role-specific readers ----------

def node_reader(node_id):
    base = "logs/"
    el = Tail(base + "elections-node%d.csv" % node_id)
    rq = Tail(base + "requests-node%d.csv" % node_id)
    fl = Tail(base + "faults-node%d.csv" % node_id)
    mt = Tail(base + "node_metrics-node%d.csv" % node_id)
    lg = Tail(base + "node%d.log" % node_id)
    state = {"metrics": None}

    def tick(msg):
        rows = []
        last = None
        for line in el.read():
            p = line.split(",")
            if len(p) < 12:
                continue
            try:
                wall = int(p[11]) // 1000
                winner = int(p[5]) if p[5] else 0
                row = [wall, winner, int(p[6] or 0), 1 if p[7] == "true" else 0,
                       1 if p[9] == "true" else 0, int(p[3] or 0)]
            except ValueError:
                continue
            rows.append(row)
            last = {"req": p[2], "bids": p[4], "winner": winner, "round": row[5],
                    "dur_us": row[2], "wall": wall, "late": p[8], "calls": p[10]}
        msg["el"] = rows
        msg["el_last"] = last

        req = {"end": {}, "snap_n": 0, "snap_sum": 0, "snap_max": 0,
               "commit_n": 0, "commit_sum": 0, "commit_max": 0, "taken": 0}
        for line in rq.read():
            p = line.split(",")
            if len(p) < 7:
                continue
            ev, detail = p[3], p[4]
            if ev == "END":
                req["end"][detail] = req["end"].get(detail, 0) + 1
            elif ev in ("SNAPSHOT", "COMMIT"):
                try:
                    d = int(p[5] or 0)
                except ValueError:
                    continue
                k = "snap" if ev == "SNAPSHOT" else "commit"
                req[k + "_n"] += 1
                req[k + "_sum"] += d
                req[k + "_max"] = max(req[k + "_max"], d)
                if detail == "resource_taken":
                    req["taken"] += 1
        msg["req"] = req

        faults = []
        for line in fl.read():
            p = line.split(",")
            if len(p) < 7:
                continue
            try:
                faults.append([int(p[6]) // 1000, p[2], p[3], int(p[4]) if p[4] else 0,
                               int(p[5]) if p[5] else 0])
            except ValueError:
                continue
        msg["faults"] = faults

        for line in mt.read():
            p = line.split(",")
            if len(p) >= 6:
                try:
                    state["metrics"] = {"wall": int(p[1]) // 1000, "queue": int(p[2]),
                                        "cpu": float(p[3]), "service_ms": float(p[4]),
                                        "load": float(p[5])}
                except ValueError:
                    pass
        msg["metrics"] = state["metrics"]
        msg["log"] = [ANSI.sub("", l)[:400] for l in lg.read()][-60:]

    return tick


def client_reader(name):
    folder = "results/demo-%s" % name
    out = Tail("results/demo-%s.out" % name)
    state = {"path": None, "tail": None, "started": True}

    def newest():
        try:
            files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
        except OSError:
            return None
        return max(files, key=os.path.getmtime) if files else None

    def tick(msg):
        path = newest()
        if path != state["path"]:
            # The file that is there at start: from its end. Any later one: a new run, from 0.
            state["tail"] = Tail(path, from_end=state["started"]) if path else None
            state["path"] = path
        state["started"] = False
        rows = []
        if state["tail"]:
            for line in state["tail"].read():
                p = line.split(",")
                if len(p) < 12:
                    continue
                try:
                    st = {"ok": 0, "timeout": 1, "error": 2}.get(p[6], 2)
                    asg = {"true": 1, "false": 0}.get(p[7], -1)
                    oc = {"committed": 0, "duplicate": 1, "failed": 2}.get(p[11], 3)
                    rows.append([int(p[1]), int(p[4] or 0), int(p[5]) if p[5] else 0, st, asg,
                                 int(p[9] or 1), oc, int(p[8] or 0)])
                except ValueError:
                    continue
        msg["rows"] = rows[-30000:]
        msg["run_file"] = os.path.basename(path) if path else None
        msg["out"] = [l[:400] for l in out.read()][-40:]

    return tick


def main():
    try:
        os.chdir(REPO)
    except OSError:
        print(json.dumps({"t": int(time.time() * 1000), "role": ROLE, "fatal": "repo folder not found: " + REPO}), flush=True)
        return
    host = socket.gethostname()
    if ROLE == "node":
        tick = node_reader(int(CFG["id"]))
    else:
        tick = client_reader(CFG["name"])
    prev_cpu = cpu_times()
    prev_net = net_bytes()
    prev_t = time.time()
    while True:
        time.sleep(1.0)
        now = time.time()
        msg = {"t": int(now * 1000), "role": ROLE, "host": host}
        try:
            c = cpu_times()
            n = net_bytes()
            dt = max(now - prev_t, 0.001)
            busy = (c[0] - prev_cpu[0]) - (c[1] - prev_cpu[1])
            cpu = 100.0 * busy / max(c[0] - prev_cpu[0], 1)
            m, total = mem()
            msg["sys"] = {"cpu": round(cpu, 1), "mem": round(m, 1), "mem_total": total,
                          "rx": int((n[0] - prev_net[0]) / dt), "tx": int((n[1] - prev_net[1]) / dt),
                          "load1": os.getloadavg()[0], "cores": os.cpu_count()}
            prev_cpu, prev_net, prev_t = c, n, now
            msg["proc"] = proc_status()
            tick(msg)
        except Exception as e:  # noqa: BLE001 - report and keep going
            msg["err"] = "%s: %s" % (type(e).__name__, e)
        try:
            sys.stdout.write(json.dumps(msg, separators=(",", ":")) + "\n")
            sys.stdout.flush()
        except (BrokenPipeError, OSError):
            return  # the control panel went away


if __name__ == "__main__":
    main()
