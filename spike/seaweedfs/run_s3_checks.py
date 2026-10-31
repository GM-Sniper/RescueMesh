#!/usr/bin/env python3
"""SeaweedFS S3-gateway spike checks a-i. Spike only: see docs/p4-changes.md, "SeaweedFS spike 2".

Usage:  python run_s3_checks.py s3_cluster.local.toml [--only a,e,f] [--json results.json]

What it tests: create-if-absent (`If-None-Match: *`) and compare-and-swap (`If-Match: <etag>`)
through the S3 gateways, because that was the only route that gave exactly one winner in the
first spike. Reads the gateway and filer addresses from a TOML config, so the same checks can be
rerun on the real machines. Standard library only (Python 3.11+).

Checks:
  a  race: many concurrent PUTs of one key spread over all gateways; exactly one 200
  b  the same race while one volume server is killed (needs process control, see [control])
  c  the same race while one filer and its gateway are killed
  d  kill one master, then a second, and see what still works
  e  If-Match: two writers update one object with the same etag; exactly one wins
  f  read-after-commit staleness through the other gateways and filers
  g  a PUT that times out on the client: did the write land anyway? (proxy variant, and a
     paused-gateway variant that needs process control)
  i  restarting what b, c and d killed: does it rejoin, and is data from its absence readable?
(h, "do gateways work without credentials", is answered by every check here, which sends no
credentials; the credential experiment is in docs/p4-changes.md.)

STOP RULE: if any round has two or more winners, the script prints it and exits at once.
"""
import argparse
import http.client
import json
import socket
import statistics
import subprocess
import sys
import threading
import time
import tomllib
import uuid
from urllib.parse import urlparse


class TwoWinners(Exception):
    """A race produced two winners: the one result that invalidates the design."""


# ---------------------------------------------------------------- HTTP helpers

def http_request(base_url, method, path, body=None, headers=None, timeout=10):
    """One request on a fresh connection. Returns (status, response_headers, body, seconds).

    Status 0 means no HTTP answer (connection refused, reset or client-side timeout); the error
    text is in the body. A timeout is reported as status 0 with body b"timeout".
    """
    parts = urlparse(base_url)
    started = time.perf_counter()
    try:
        connection = http.client.HTTPConnection(parts.hostname, parts.port, timeout=timeout)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        data = response.read()
        result = (response.status, dict(response.getheaders()), data)
        connection.close()
    except TimeoutError:
        return 0, {}, b"timeout", time.perf_counter() - started
    except (OSError, http.client.HTTPException) as error:
        return 0, {}, str(error).encode(), time.perf_counter() - started
    return result[0], result[1], result[2], time.perf_counter() - started


def header(headers, name):
    """Case-insensitive header lookup (servers differ: "ETag" vs "Etag")."""
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


# Losers of a contended create wait on a lock inside SeaweedFS, roughly one second each, so
# the 24th contender can wait ~15 s. A short client timeout would report those as failures.
REQUEST_TIMEOUT_SECONDS = 60


def put_if_absent(gateway, bucket, key, body, timeout=REQUEST_TIMEOUT_SECONDS):
    return http_request(gateway, "PUT", f"/{bucket}/{key}", body, {"If-None-Match": "*"}, timeout)


def read_object(gateway, bucket, key, timeout=30):
    return http_request(gateway, "GET", f"/{bucket}/{key}", timeout=timeout)


def read_from_filer(filer, bucket, key, timeout=10):
    """Reads the object straight from a filer (S3 objects live under /buckets/<bucket>/)."""
    return http_request(filer, "GET", f"/buckets/{bucket}/{key}", timeout=timeout)


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


# ---------------------------------------------------------------- process control

class Control:
    """Runs the stop/start/pause/resume commands from the config's [control] table.

    Why commands from the config instead of code: on the real machines the way to stop a process
    differs (ssh, systemd, Task Scheduler). The local config points at proc_ctl.sh.
    """

    def __init__(self, config):
        control = config.get("control", {})
        self.template = control.get("command_template")
        self.names = control.get("names", {})

    @property
    def available(self):
        return bool(self.template)

    def run(self, action, role, index):
        name = self.names[role][index]
        command = self.template.format(action=action, name=name)
        subprocess.run(command, shell=True, check=True, timeout=60)
        print(f"   [control] {action} {name}")


# ---------------------------------------------------------------- race

def run_race(gateways, bucket, key, concurrent_puts):
    """Fires `concurrent_puts` create-if-absent PUTs of one key at the same instant,
    round-robin over the gateways. Returns a list of (gateway_index, writer_index, status)."""
    barrier = threading.Barrier(concurrent_puts)
    outcomes = [None] * concurrent_puts

    def worker(writer):
        gateway_index = writer % len(gateways)
        barrier.wait()  # all writers fire together
        status, _, _, seconds = put_if_absent(gateways[gateway_index], bucket, key, f"writer-{writer}".encode())
        outcomes[writer] = (gateway_index, writer, status, seconds)

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(concurrent_puts)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return outcomes


def summarize_race(outcomes):
    winners = [o for o in outcomes if o[2] == 200]
    return {
        "winners": len(winners),
        "winner_writer": winners[0][1] if len(winners) == 1 else None,
        "rejected_412": sum(o[2] == 412 for o in outcomes),
        "no_answer_0": sum(o[2] == 0 for o in outcomes),
        "other": sorted({o[2] for o in outcomes if o[2] not in (0, 200, 412)}),
        "winner_seconds": round(winners[0][3], 2) if len(winners) == 1 else None,
        "slowest_seconds": round(max(o[3] for o in outcomes), 2),
        "median_seconds": round(statistics.median(o[3] for o in outcomes), 2),
    }


def wait_until_all_read(readers, expected, timeout_seconds):
    """Polls every reader (a function returning (status, body)) until all return `expected`.
    Returns (seconds until all agreed, or None if they never did, last answers)."""
    started = time.perf_counter()
    answers = []
    while True:
        answers = [reader() for reader in readers]
        if all(status == 200 and body == expected for status, body in answers):
            return time.perf_counter() - started, answers
        if time.perf_counter() - started > timeout_seconds:
            return None, answers
        time.sleep(0.01)


def race_round(gateways, filers, bucket, concurrent_puts, label, live_gateways=None, live_filers=None):
    """One full round: race, then check that every live gateway AND filer reads the winner.

    `live_*` limit the readers when part of the cluster is deliberately down."""
    key = f"{label}-{uuid.uuid4().hex[:8]}"
    outcomes = run_race(gateways, bucket, key, concurrent_puts)
    summary = summarize_race(outcomes)
    if summary["winners"] > 1:
        print(f"!!! TWO OR MORE WINNERS on {key}: {summary}")
        raise TwoWinners(key)
    summary["key"] = key
    readers = []
    for gateway in (gateways if live_gateways is None else live_gateways):
        readers.append(lambda g=gateway: read_object(g, bucket, key)[:3:2])
    for filer in (filers if live_filers is None else live_filers):
        readers.append(lambda f=filer: read_from_filer(f, bucket, key)[:3:2])
    if summary["winners"] == 1:
        expected = f"writer-{summary['winner_writer']}".encode()
        delay, answers = wait_until_all_read(readers, expected, 15)
        summary["all_readers_agree_after_s"] = delay
        summary["readers_agree"] = delay is not None
        if delay is None:
            summary["last_answers"] = [(s, b[:30].decode(errors="replace")) for s, b in answers]
    else:
        # No winner: the write may still have landed (a 5xx can hide a successful write).
        time.sleep(2)
        status, _, body, _ = read_object(live_gateways[0] if live_gateways else gateways[0], bucket, key)
        summary["object_exists_anyway"] = status == 200
    return summary


def print_round(number, summary, extra=""):
    agree = summary.get("all_readers_agree_after_s")
    agree_text = "n/a" if agree is None and summary["winners"] != 1 else (
        "NEVER" if agree is None else f"{agree*1000:.0f} ms")
    print(f"round {number:2d}: 200={summary['winners']} 412={summary['rejected_412']} "
          f"no-answer={summary['no_answer_0']} other={summary['other']}  "
          f"time winner/median/slowest {summary['winner_seconds']}/{summary['median_seconds']}/{summary['slowest_seconds']} s  "
          f"readers agree after {agree_text}"
          f"{'  (object exists anyway: ' + str(summary['object_exists_anyway']) + ')' if 'object_exists_anyway' in summary else ''}"
          f"{extra}")


def verdict(rounds):
    one = sum(r["winners"] == 1 and r.get("readers_agree") for r in rounds)
    none = sum(r["winners"] == 0 for r in rounds)
    return {"rounds": len(rounds), "exactly_one_winner_and_all_agree": one,
            "zero_winners": none, "two_or_more_winners": 0,
            "disagreeing_after_15s": sum(r["winners"] == 1 and not r.get("readers_agree") for r in rounds)}


def ensure_bucket(gateway, bucket):
    status = http_request(gateway, "PUT", f"/{bucket}")[0]
    time.sleep(2)  # the bucket is a directory in the filers; give the others time to see it
    return status


# ---------------------------------------------------------------- checks

def check_a(config, control):
    print("\n== a) S3 race: concurrent PUTs of one key, spread over all gateways ==")
    c = config["cluster"]
    rounds = [race_round(c["gateway_urls"], c["filer_urls"], c["bucket"], config["race"]["concurrent_puts"], "a")
              for _ in range(config["race"]["rounds"])]
    for n, r in enumerate(rounds):
        print_round(n, r)
    result = verdict(rounds)
    result["slowest_loser_seconds_max"] = max(r["slowest_seconds"] for r in rounds)
    result["slowest_loser_seconds_median"] = statistics.median(r["slowest_seconds"] for r in rounds)
    print("-- the realistic case: 3 contenders (one request multicast to 3 nodes) --")
    three = [race_round(c["gateway_urls"], c["filer_urls"], c["bucket"], 3, "a3") for _ in range(10)]
    for n, r in enumerate(three):
        print_round(n, r, "  [3 contenders]")
    result["three_contenders"] = verdict(three)
    result["three_contenders"]["slowest_seconds_max"] = max(r["slowest_seconds"] for r in three)
    print("-- uncontended create latency (one writer, 30 fresh keys) --")
    latencies = []
    for i in range(30):
        key = f"a-solo-{uuid.uuid4().hex[:8]}"
        status, _, _, seconds = put_if_absent(c["gateway_urls"][i % len(c["gateway_urls"])], c["bucket"], key, b"x")
        latencies.append(seconds)
    result["uncontended_create_ms"] = {"median": round(statistics.median(latencies) * 1000, 1),
                                       "p95": round(percentile(latencies, 0.95) * 1000, 1),
                                       "max": round(max(latencies) * 1000, 1)}
    print("   ", result["uncontended_create_ms"])
    result["passed"] = (result["exactly_one_winner_and_all_agree"] == len(rounds)
                        and result["three_contenders"]["exactly_one_winner_and_all_agree"] == len(three))
    print("RESULT:", "PASS" if result["passed"] else "FAIL", result)
    return result


def killing_race(config, control, label, kill_role, kill_indices, delay_seconds, live_gateways, live_filers, rounds_count):
    """Runs race rounds back to back; a timer thread kills `kill_indices` of `kill_role`
    `delay_seconds` after the first round starts. Returns (rounds, kill_round_number)."""
    c = config["cluster"]
    killed_during = {"round": None}
    current = {"round": 0}

    def killer():
        time.sleep(delay_seconds)
        killed_during["round"] = current["round"]
        for role, index in kill_indices:
            control.run("stop", role, index)

    timer = threading.Thread(target=killer)
    timer.start()
    rounds = []
    for n in range(rounds_count):
        current["round"] = n
        # Readers: all of them before the kill, only the live ones afterwards.
        after_kill = killed_during["round"] is not None
        summary = race_round(c["gateway_urls"], c["filer_urls"], c["bucket"], config["race"]["concurrent_puts"], label,
                             live_gateways if after_kill else None, live_filers if after_kill else None)
        summary["killed_before_this_round"] = after_kill
        rounds.append(summary)
        print_round(n, summary, "  [after kill]" if after_kill else "")
    timer.join()
    return rounds, killed_during["round"]


def write_during_absence(gateways, bucket, count, label):
    """Creates `count` objects while part of the cluster is down. Returns {key: body} that succeeded
    and the number that failed."""
    written, failed = {}, 0
    for i in range(count):
        key = f"{label}-absent-{uuid.uuid4().hex[:8]}"
        body = f"during-absence-{i}".encode()
        status = put_if_absent(gateways[i % len(gateways)], bucket, key, body)[0]
        if status == 200:
            written[key] = body
        else:
            failed += 1
    return written, failed


def read_back_after(gateways, filers, bucket, objects, wait_limit_seconds):
    """After a restart: how long until EVERY object is readable (and right) through every
    gateway and filer in the lists? Returns seconds, or None if some never were."""
    readers = []
    for key, body in objects.items():
        for g in gateways:
            readers.append((lambda g=g, k=key: read_object(g, bucket, k)[:3:2], body))
        for f in filers:
            readers.append((lambda f=f, k=key: read_from_filer(f, bucket, k)[:3:2], body))
    started = time.perf_counter()
    while True:
        bad = sum(1 for reader, body in readers if reader() != (200, body))
        if bad == 0:
            return time.perf_counter() - started, 0
        if time.perf_counter() - started > wait_limit_seconds:
            return None, bad
        time.sleep(0.5)


def check_b_and_i_volume(config, control):
    print("\n== b) race while volume server 1 is killed, then i) bring it back ==")
    c = config["cluster"]
    rounds, kill_round = killing_race(config, control, "b", "volume", [("volume", 1)], 4.0,
                                      None, None, config["race"]["rounds"])
    result = verdict(rounds)
    result["killed_during_round"] = kill_round
    print("verdict:", result)
    print("-- writing while the volume server is still down --")
    written, failed = write_during_absence(c["gateway_urls"], c["bucket"], 20, "b")
    print(f"   {len(written)} of 20 creates succeeded, {failed} failed while volume1 was down")
    result["writes_during_absence"] = {"succeeded": len(written), "failed": failed}
    control.run("start", "volume", 1)
    print("-- i) volume1 restarted; waiting for objects written in its absence to be readable --")
    seconds, bad = read_back_after(c["gateway_urls"], c["filer_urls"], c["bucket"], written, 60)
    result["rejoin"] = {"all_absence_writes_readable_after_s": seconds, "unreadable_reads": bad}
    print(f"   all readable after {seconds if seconds is None else round(seconds, 1)} s (unreadable: {bad})")
    after, _ = [race_round(c["gateway_urls"], c["filer_urls"], c["bucket"], config["race"]["concurrent_puts"], "b-after")], None
    print_round(0, after[0], "  [after restart]")
    result["round_after_restart"] = after[0]
    result["passed"] = (result["two_or_more_winners"] == 0 and seconds is not None
                        and after[0]["winners"] == 1 and after[0].get("readers_agree"))
    return result


def check_c_and_i_filer(config, control):
    print("\n== c) race while filer 1 and its gateway are killed, then i) bring them back ==")
    c = config["cluster"]
    live_gateways = [g for i, g in enumerate(c["gateway_urls"]) if i != 1]
    live_filers = [f for i, f in enumerate(c["filer_urls"]) if i != 1]
    rounds, kill_round = killing_race(config, control, "c", "filer", [("gateway", 1), ("filer", 1)], 4.0,
                                      live_gateways, live_filers, config["race"]["rounds"])
    result = verdict(rounds)
    result["killed_during_round"] = kill_round
    print("verdict:", result)
    print("-- writing through the two live gateways while filer1 is down --")
    written, failed = write_during_absence(live_gateways, c["bucket"], 20, "c")
    print(f"   {len(written)} of 20 creates succeeded, {failed} failed")
    result["writes_during_absence"] = {"succeeded": len(written), "failed": failed}
    control.run("start", "filer", 1)
    time.sleep(3)
    control.run("start", "gateway", 1)
    started = time.perf_counter()
    print("-- i) filer1 and gateway1 restarted; waiting until THEY can read what was written meanwhile --")
    seconds, bad = read_back_after([c["gateway_urls"][1]], [c["filer_urls"][1]], c["bucket"], written, 90)
    result["rejoin"] = {"restarted_pair_reads_everything_after_s": seconds, "unreadable_reads": bad}
    print(f"   readable after {seconds if seconds is None else round(seconds, 1)} s (unreadable: {bad})")
    after = race_round(c["gateway_urls"], c["filer_urls"], c["bucket"], config["race"]["concurrent_puts"], "c-after")
    print_round(0, after, "  [after restart, all 3 gateways]")
    result["round_after_restart"] = after
    result["passed"] = seconds is not None and after["winners"] == 1 and after.get("readers_agree", False)
    return result


def try_create_and_read(config, label):
    """One create through each live gateway, then read back through the others. Returns a dict."""
    c = config["cluster"]
    key = f"{label}-{uuid.uuid4().hex[:8]}"
    create = put_if_absent(c["gateway_urls"][1], c["bucket"], key, b"probe", timeout=20)
    read = read_object(c["gateway_urls"][2], c["bucket"], key, timeout=20)
    return {"create_status": create[0], "create_seconds": round(create[3], 2),
            "read_status": read[0], "read_body": read[2][:20].decode(errors="replace")}


def check_d_and_i_masters(config, control):
    print("\n== d) kill one master, then a second, then i) bring them back ==")
    c = config["cluster"]
    result = {}
    # An object that exists BEFORE any master dies, to test reads later.
    existing = f"d-existing-{uuid.uuid4().hex[:8]}"
    put_if_absent(c["gateway_urls"][1], c["bucket"], existing, b"before")
    time.sleep(1)

    control.run("stop", "master", 0)  # index 0 is the initial leader
    print("   waiting 15 s for the remaining 2 masters to elect a leader")
    time.sleep(15)
    result["one_master_down"] = try_create_and_read(config, "d1")
    result["one_master_down"]["existing_object_read"] = read_object(c["gateway_urls"][2], c["bucket"], existing)[0]
    print("   2 of 3 masters alive:", result["one_master_down"])
    rounds = [race_round(c["gateway_urls"], c["filer_urls"], c["bucket"], config["race"]["concurrent_puts"], "d1")
              for _ in range(3)]
    for n, r in enumerate(rounds):
        print_round(n, r, "  [2 of 3 masters]")
    result["race_with_one_master_down"] = verdict(rounds)

    control.run("stop", "master", 1)
    time.sleep(10)
    result["two_masters_down"] = try_create_and_read(config, "d2")
    result["two_masters_down"]["existing_object_read"] = read_object(c["gateway_urls"][2], c["bucket"], existing, timeout=20)[0]
    print("   1 of 3 masters alive:", result["two_masters_down"])

    control.run("start", "master", 0)
    control.run("start", "master", 1)
    started = time.perf_counter()
    recovered = None
    while time.perf_counter() - started < 90:
        probe = try_create_and_read(config, "d3")
        if probe["create_status"] == 200 and probe["read_status"] == 200:
            recovered = time.perf_counter() - started
            break
        time.sleep(2)
    result["recovered_after_masters_restart_s"] = recovered
    print(f"   i) masters restarted; creates and reads work again after "
          f"{None if recovered is None else round(recovered, 1)} s")
    result["passed"] = (result["one_master_down"]["create_status"] == 200
                        and result["one_master_down"]["read_status"] == 200 and recovered is not None)
    return result


def check_e(config, control):
    print("\n== e) If-Match: two writers update one object with the same etag ==")
    c = config["cluster"]
    rounds = []
    for n in range(config["race"]["rounds"]):
        key = f"e-{uuid.uuid4().hex[:8]}"
        put_if_absent(c["gateway_urls"][0], c["bucket"], key, b"base")
        time.sleep(0.3)
        etag = header(http_request(c["gateway_urls"][1], "HEAD", f"/{c['bucket']}/{key}")[1], "ETag")
        writers = config["race"]["concurrent_puts"]
        barrier = threading.Barrier(writers)
        statuses = [None] * writers

        def worker(i):
            barrier.wait()
            statuses[i] = http_request(c["gateway_urls"][i % len(c["gateway_urls"])], "PUT", f"/{c['bucket']}/{key}",
                                       f"update-{i}".encode(), {"If-Match": etag}, REQUEST_TIMEOUT_SECONDS)[0]

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(writers)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        wins = [i for i, s in enumerate(statuses) if s == 200]
        if len(wins) > 1:
            print(f"!!! TWO WINNERS for If-Match on {key}: writers {wins}")
            raise TwoWinners(key)
        agree, _ = wait_until_all_read(
            [lambda g=g: read_object(g, c["bucket"], key)[:3:2] for g in c["gateway_urls"]],
            f"update-{wins[0]}".encode() if wins else b"base", 15)
        rounds.append({"winners": len(wins), "rejected_412": sum(s == 412 for s in statuses),
                       "other": sorted({s for s in statuses if s not in (200, 412)}),
                       "agree_after_s": agree})
        print(f"round {n:2d}: 200={len(wins)} 412={rounds[-1]['rejected_412']} other={rounds[-1]['other']} "
              f"readers agree after {None if agree is None else round(agree*1000)} ms")
    passed = all(r["winners"] == 1 and r["agree_after_s"] is not None for r in rounds)
    print("RESULT:", "PASS" if passed else "FAIL")
    return {"rounds": rounds, "passed": passed}


def check_f(config, control):
    print("\n== f) read-after-commit: commit via one gateway, read at once via the others ==")
    c = config["cluster"]
    gateways, filers, bucket = c["gateway_urls"], c["filer_urls"], c["bucket"]
    samples = config["staleness"]["samples"]
    fresh = {"total": 0, "not_found_404": 0, "other": 0}
    fresh_delays = []
    old = {"total": 0, "stale_old_version": 0, "other": 0}
    old_delays = []
    for n in range(samples):
        writer = n % len(gateways)
        readers = [("gateway", g) for i, g in enumerate(gateways) if i != writer] + [("filer", f) for f in filers]

        def read(kind, url, key):
            return (read_object if kind == "gateway" else read_from_filer)(url, bucket, key)[:3:2]

        # New key: the first read right after the 200 may be a 404.
        key = f"f-new-{uuid.uuid4().hex[:8]}"
        put_if_absent(gateways[writer], bucket, key, b"v1")
        committed_at = time.perf_counter()
        for kind, url in readers:
            fresh["total"] += 1
            status, body = read(kind, url, key)
            if status == 200 and body == b"v1":
                continue
            fresh["not_found_404" if status == 404 else "other"] += 1
            while True:
                status, body = read(kind, url, key)
                if status == 200 and body == b"v1" or time.perf_counter() - committed_at > 15:
                    break
                time.sleep(0.002)
            fresh_delays.append(time.perf_counter() - committed_at)

        # Existing key updated with If-Match: the first read may still return v1.
        key = f"f-upd-{uuid.uuid4().hex[:8]}"
        put_if_absent(gateways[writer], bucket, key, b"v1")
        wait_until_all_read([lambda k=kind, u=url: read(k, u, key) for k, u in readers], b"v1", 15)
        etag = header(http_request(gateways[writer], "HEAD", f"/{bucket}/{key}")[1], "ETag")
        http_request(gateways[writer], "PUT", f"/{bucket}/{key}", b"v2", {"If-Match": etag})
        committed_at = time.perf_counter()
        for kind, url in readers:
            old["total"] += 1
            status, body = read(kind, url, key)
            if status == 200 and body == b"v2":
                continue
            old["stale_old_version" if status == 200 and body == b"v1" else "other"] += 1
            while True:
                status, body = read(kind, url, key)
                if status == 200 and body == b"v2" or time.perf_counter() - committed_at > 15:
                    break
                time.sleep(0.002)
            old_delays.append(time.perf_counter() - committed_at)

    def stats(d):
        return {"count": len(d), "median_ms": round(statistics.median(d) * 1000, 1) if d else None,
                "max_ms": round(max(d) * 1000, 1) if d else None}
    result = {"new_key": {**fresh, "delay_until_correct": stats(fresh_delays)},
              "updated_key": {**old, "delay_until_correct": stats(old_delays)}}
    print("new key, first read right after the 200:", result["new_key"])
    print("updated key, first read right after the 200:", result["updated_key"])
    result["passed"] = None  # informational: the answer is the measured rates
    return result


class DelayedResponseProxy(threading.Thread):
    """A TCP proxy in front of a gateway that forwards requests at once but holds back the
    gateway's reply for `hold_seconds`. The client then times out while the gateway carries on
    and finishes the write: exactly the ambiguity check g is about.
    Why a proxy: it is portable (no process suspending) and timing-exact."""

    def __init__(self, listen_port, target_url, hold_seconds):
        super().__init__(daemon=True)
        parts = urlparse(target_url)
        self.target = (parts.hostname, parts.port)
        self.hold_seconds = hold_seconds
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("127.0.0.1", listen_port))
        self.server.listen(50)

    def run(self):
        while True:
            try:
                client, _ = self.server.accept()
            except OSError:
                return
            threading.Thread(target=self.handle, args=(client,), daemon=True).start()

    def handle(self, client):
        upstream = socket.create_connection(self.target)

        def forward_request():
            try:
                while data := client.recv(65536):
                    upstream.sendall(data)
            except OSError:
                pass

        threading.Thread(target=forward_request, daemon=True).start()
        try:
            time.sleep(self.hold_seconds)
            while data := upstream.recv(65536):
                client.sendall(data)
        except OSError:
            pass
        finally:
            client.close()
            upstream.close()

    def stop(self):
        self.server.close()


def check_g(config, control):
    print("\n== g) a PUT that times out on the client: did the write land anyway? ==")
    c = config["cluster"]
    bucket, gateway = c["bucket"], c["gateway_urls"][0]
    client_timeout = config["timeout"]["client_timeout_seconds"]
    result = {}

    proxy = DelayedResponseProxy(config["timeout"]["proxy_port"], gateway, client_timeout + 3)
    proxy.start()
    proxy_url = f"http://127.0.0.1:{config['timeout']['proxy_port']}"
    landed, timed_out, retries_412 = 0, 0, 0
    samples = config["timeout"]["samples"]
    for i in range(samples):
        key = f"g-proxy-{uuid.uuid4().hex[:8]}"
        status = put_if_absent(proxy_url, bucket, key, f"attempt-{i}".encode(), timeout=client_timeout)[0]
        timed_out += status == 0
        time.sleep(client_timeout + 4)  # let the held reply finish and the gateway settle
        exists = read_object(gateway, bucket, key)[0] == 200
        landed += exists
        # What the real retry (same request_id = same key) would see:
        retry = put_if_absent(gateway, bucket, key, b"retry")[0]
        retries_412 += retry == 412
    proxy.stop()
    result["reply_held_back"] = {"samples": samples, "client_timed_out": timed_out,
                                 "write_landed_anyway": landed, "retry_got_412": retries_412}
    print(f"   reply held back: {timed_out}/{samples} client timeouts; the write landed anyway in "
          f"{landed}; the retry (create-if-absent) got 412 in {retries_412}")

    if control.available:
        landed_late, client_timeouts, absent_while_paused, late_samples = 0, 0, 0, min(5, samples)
        for i in range(late_samples):
            key = f"g-pause-{uuid.uuid4().hex[:8]}"
            control.run("pause", "gateway", 0)
            status = put_if_absent(gateway, bucket, key, b"queued", timeout=client_timeout)[0]
            # Proof the pause worked: the client timed out and the object is not there yet.
            client_timeouts += status == 0
            absent_while_paused += read_object(c["gateway_urls"][1], bucket, key)[0] == 404
            control.run("resume", "gateway", 0)
            time.sleep(4)
            landed_late += read_object(c["gateway_urls"][1], bucket, key)[0] == 200
        result["gateway_paused_then_resumed"] = {"samples": late_samples, "client_timed_out": client_timeouts,
                                                 "absent_while_paused": absent_while_paused,
                                                 "write_landed_after_resume": landed_late}
        print(f"   gateway paused during the PUT: client timed out {client_timeouts}/{late_samples}, object absent "
              f"while paused {absent_while_paused}/{late_samples}, landed after resume {landed_late}/{late_samples}")
    else:
        print("   (paused-gateway variant skipped: no [control] in the config)")
    result["passed"] = None  # informational
    return result


CHECKS = {"a": check_a, "e": check_e, "f": check_f, "g": check_g,
          "b": check_b_and_i_volume, "c": check_c_and_i_filer, "d": check_d_and_i_masters}
# Order matters: the destructive checks run last and each restores what it killed.
DEFAULT_ORDER = ["a", "e", "f", "g", "b", "c", "d"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config")
    parser.add_argument("--only", help="comma-separated check letters, for example a,e,f")
    parser.add_argument("--json", help="also write the results to this file")
    arguments = parser.parse_args()
    with open(arguments.config, "rb") as handle:
        config = tomllib.load(handle)
    control = Control(config)
    order = arguments.only.split(",") if arguments.only else DEFAULT_ORDER
    c = config["cluster"]
    print(f"gateways: {c['gateway_urls']}\nfilers:   {c['filer_urls']}\nbucket:   {c['bucket']}")
    print("bucket create status:", ensure_bucket(c["gateway_urls"][0], c["bucket"]))

    results = {}
    try:
        for letter in order:
            if letter in "bcd" and not control.available:
                print(f"\n== {letter}) skipped: needs [control] in the config ==")
                results[letter] = {"passed": None, "skipped": True}
                continue
            results[letter] = CHECKS[letter](config, control)
    except TwoWinners as error:
        print(f"\nSTOPPED: two winners on {error}. Results so far are saved; do not continue.")
        results["stopped_on_two_winners"] = str(error)
        if arguments.json:
            json.dump(results, open(arguments.json, "w"), indent=2, default=str)
        return 2
    print("\n== Summary ==")
    for letter, outcome in results.items():
        passed = outcome.get("passed")
        print(f"{letter}: {'PASS' if passed else 'INFO/SKIPPED' if passed is None else 'FAIL'}")
    if arguments.json:
        json.dump(results, open(arguments.json, "w"), indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
