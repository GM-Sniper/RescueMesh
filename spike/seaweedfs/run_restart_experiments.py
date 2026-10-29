#!/usr/bin/env python3
"""SeaweedFS spike 3: how often do two writers win after a node comes back? Measurement only.

Usage:
  python run_restart_experiments.py s3_cluster.local.toml restart --runs 10 --log restart.jsonl
  python run_restart_experiments.py s3_cluster.local.toml mute    --runs 10 --log mute.jsonl
  python run_restart_experiments.py s3_cluster.local.toml masters --log masters.json

Experiments (numbers match the task list):
  restart  1 and 4: kill filer 1 and gateway 1, restart them, then run 24-writer races at fixed
           offsets after the restart. Every round is logged, two-winner rounds included (unlike
           run_s3_checks.py, this script never stops on one).
  mute     2: the same races after a "mute" instead of a kill. The SeaweedFS processes keep
           running but are suspended (NtSuspendProcess / SIGSTOP) for 10 or 20 s, then resumed.
  masters  3: kill one master, then a second, and probe creates, reads and old data.

Experiment 4 (writer-token read-back) is built into every race of `restart` and `mute`: each
writer's body is a unique token, and right after its own 200 the writer reads the key back
through the two OTHER gateways and compares.

Why suspending and not a firewall rule or proxy for `mute`: (a) the filers reach each other
through addresses registered with the masters, so a proxy would not sit on their path; (b) a
Windows firewall rule does not filter loopback traffic (UNVERIFIED, not tried). Suspending the
processes is the closest local equivalent: the node is alive, keeps its state and its sockets,
and answers nothing for a while.

Reuses helpers from run_s3_checks.py (same folder). Process control comes from the [control]
table of the config (proc_ctl.sh locally).
"""
import argparse
import json
import statistics
import sys
import threading
import time
import tomllib
import uuid

import run_s3_checks as s3


# ---------------------------------------------------------------- one race with tokens

def token_race(gateways, bucket, key, writers):
    """24 writers start together; writer i uses gateway i % 3 and a unique token as its body.

    Right after its own 200, a writer reads the key back through the other gateways
    (experiment 4). Returns a list of per-writer dicts."""
    barrier = threading.Barrier(writers)
    results = [None] * writers

    def worker(index):
        gateway_index = index % len(gateways)
        token = f"node{gateway_index + 1}-w{index}-{uuid.uuid4().hex[:8]}"
        barrier.wait()
        status, _, _, seconds = s3.put_if_absent(gateways[gateway_index], bucket, key, token.encode())
        entry = {"writer": index, "gateway": gateway_index, "token": token, "status": status,
                 "seconds": round(seconds, 2)}
        if status == 200:
            entry["readback"] = []
            for offset in (1, 2):  # the "second gateway" is offset 1; offset 2 is extra evidence
                other = (gateway_index + offset) % len(gateways)
                got_status, _, body, _ = s3.read_object(gateways[other], bucket, key)
                entry["readback"].append({"gateway": other, "status": got_status,
                                          "matches_own_token": got_status == 200 and body == token.encode()})
        results[index] = entry

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(writers)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return results


def read_everywhere(config, key):
    c = config["cluster"]
    out = {}
    for i, g in enumerate(c["gateway_urls"]):
        status, _, body, _ = s3.read_object(g, c["bucket"], key, timeout=20)
        out[f"gateway{i}"] = body.decode(errors="replace") if status == 200 else f"<{status}>"
    for i, f in enumerate(c["filer_urls"]):
        status, _, body, _ = s3.read_from_filer(f, c["bucket"], key, timeout=20)
        out[f"filer{i}"] = body.decode(errors="replace") if status == 200 else f"<{status}>"
    return out


def settle_and_judge(config, key, results):
    """Waits (at most 15 s) for every gateway and filer to return one value, then judges the round."""
    started = time.perf_counter()
    while True:
        final = read_everywhere(config, key)
        agree = len(set(final.values())) == 1 and not next(iter(final.values())).startswith("<")
        if agree or time.perf_counter() - started > 15:
            break
        time.sleep(0.3)
    filer_values = [v for k, v in final.items() if k.startswith("filer")]
    winners = [r for r in results if r["status"] == 200]
    caught_second = any(not r["readback"][0]["matches_own_token"] for r in winners)
    caught_either = any(not rb["matches_own_token"] for r in winners for rb in r["readback"])
    return {
        "winners": len(winners),
        "winner_tokens": [r["token"] for r in winners],
        "winner_gateways": [r["gateway"] for r in winners],
        "rejected_412": sum(r["status"] == 412 for r in results),
        "no_answer_0": sum(r["status"] == 0 for r in results),
        "other_statuses": sorted({r["status"] for r in results if r["status"] not in (0, 200, 412)}),
        "slowest_seconds": max(r["seconds"] for r in results),
        "all_agree": agree,
        "seconds_to_agree": round(time.perf_counter() - started, 2) if agree else None,
        "filers_agree": len(set(filer_values)) == 1,
        "final_values": final,
        # Experiment 4: would the writer's own read-back have noticed a double win?
        "readback_caught_via_second_gateway": caught_second,
        "readback_caught_via_either_other_gateway": caught_either,
        "readback_mismatch_in_single_winner_round": (len(winners) == 1 and caught_either),
    }


def scheduled_round(config, label, since_restart_clock, start_after_seconds, writers, sink, extra):
    """Sleeps until `start_after_seconds` after the restart clock, then runs and judges one round."""
    c = config["cluster"]
    delay = start_after_seconds - (time.perf_counter() - since_restart_clock["ready"])
    if delay > 0:
        time.sleep(delay)
    started_since_ready = time.perf_counter() - since_restart_clock["ready"]
    started_since_command = time.perf_counter() - since_restart_clock["command"]
    key = f"{label}-{uuid.uuid4().hex[:8]}"
    results = token_race(c["gateway_urls"], c["bucket"], key, writers)
    record = settle_and_judge(config, key, results)
    record.update(extra)
    record.update({"key": key, "planned_offset_s": start_after_seconds,
                   "started_s_after_gateway_ready": round(started_since_ready, 2),
                   "started_s_after_restart_command": round(started_since_command, 2)})
    sink(record)
    flag = "  <<< TWO OR MORE WINNERS" if record["winners"] > 1 else ""
    print(f"   offset {start_after_seconds:>3}s (started {started_since_ready:5.1f}s after ready): "
          f"200={record['winners']} 412={record['rejected_412']} none={record['no_answer_0']} "
          f"other={record['other_statuses']} filers_agree={record['filers_agree']} "
          f"all_agree={record['all_agree']} readback_caught={record['readback_caught_via_either_other_gateway']}{flag}",
          flush=True)


def wait_gateway_answers(gateway, bucket, limit=90):
    """Seconds until the gateway returns any HTTP answer for a read; None if it never does."""
    started = time.perf_counter()
    while time.perf_counter() - started < limit:
        status = s3.http_request(gateway, "GET", f"/{bucket}/readiness-probe", timeout=3)[0]
        if status in (200, 404):
            return time.perf_counter() - started
        time.sleep(0.1)
    return None


def cluster_healthy(config):
    c = config["cluster"]
    return all(s3.http_request(g, "GET", f"/{c['bucket']}/readiness-probe", timeout=5)[0] in (200, 404)
               for g in c["gateway_urls"])


# ---------------------------------------------------------------- experiments 1, 2, 4

def run_disturbance_experiment(config, control, arguments, mode):
    c = config["cluster"]
    offsets = [int(x) for x in arguments.offsets.split(",")]
    log = open(arguments.log, "a", encoding="utf-8")
    log_lock = threading.Lock()

    def sink(record):
        with log_lock:
            log.write(json.dumps(record) + "\n")
            log.flush()

    writers = config["race"]["concurrent_puts"]
    for run in range(1, arguments.runs + 1):
        print(f"\n=== {mode} run {run}/{arguments.runs} ===", flush=True)
        for _ in range(60):
            if cluster_healthy(config):
                break
            time.sleep(2)
        else:
            print("cluster not healthy; abandoning this run")
            continue
        mute_seconds = 10 if run % 2 else 20
        if mode == "restart":
            control.run("stop", "gateway", 1)
            control.run("stop", "filer", 1)
            # How long the node stays gone matters: after about a minute the masters have dropped
            # it from the cluster, so its return is a membership change. In spike 2 it was gone
            # for about 3 minutes while 20 objects were created through the other gateways.
            if arguments.writes_while_down:
                s3.ensure_bucket(c["gateway_urls"][0], c["bucket"])
                live = [g for i, g in enumerate(c["gateway_urls"]) if i != 1]
                for n in range(arguments.writes_while_down):
                    s3.put_if_absent(live[n % 2], c["bucket"], f"down{run}-{uuid.uuid4().hex[:6]}", b"while-down")
            time.sleep(arguments.down_seconds)
            control.run("start", "filer", 1)
            time.sleep(3)
            control.run("start", "gateway", 1)
        else:
            control.run("pause", "gateway", 1)
            control.run("pause", "filer", 1)
            time.sleep(mute_seconds)
            control.run("resume", "filer", 1)
            control.run("resume", "gateway", 1)
        clock = {"command": time.perf_counter()}
        ready_after = wait_gateway_answers(c["gateway_urls"][1], c["bucket"])
        clock["ready"] = time.perf_counter()
        print(f"   gateway 1 answers after {None if ready_after is None else round(ready_after, 1)} s", flush=True)
        if ready_after is None:
            continue
        extra = {"mode": mode, "run": run, "down_seconds": arguments.down_seconds if mode == "restart" else None, "mute_seconds": mute_seconds if mode == "mute" else None}
        threads = []
        for offset in offsets:
            for repeat in range(arguments.rounds_per_offset):
                t = threading.Thread(target=scheduled_round, args=(
                    config, f"{mode[0]}{run}", clock, offset, writers, sink, {**extra, "repeat": repeat}))
                t.start()
                threads.append(t)
        [t.join() for t in threads]
        time.sleep(5)


# ---------------------------------------------------------------- experiment 3

def probe_state(config, label, existing_key, timeout=25):
    """What works right now? New creates through each gateway, reads of old data through each
    gateway and each filer, with times."""
    c = config["cluster"]
    out = {"creates": [], "reads_through_gateways": [], "reads_through_filers": []}
    for i, g in enumerate(c["gateway_urls"]):
        key = f"{label}-new-{uuid.uuid4().hex[:8]}"
        status, _, body, seconds = s3.put_if_absent(g, c["bucket"], key, b"probe", timeout=timeout)
        out["creates"].append({"gateway": i, "status": status, "seconds": round(seconds, 2),
                               "error": body[:80].decode(errors="replace") if status not in (200, 412) else None})
    for i, g in enumerate(c["gateway_urls"]):
        status, _, body, seconds = s3.read_object(g, c["bucket"], existing_key, timeout=timeout)
        out["reads_through_gateways"].append({"gateway": i, "status": status, "seconds": round(seconds, 2),
                                              "right_value": status == 200 and body == b"existing"})
    for i, f in enumerate(c["filer_urls"]):
        status, _, body, seconds = s3.read_from_filer(f, c["bucket"], existing_key, timeout=timeout)
        out["reads_through_filers"].append({"filer": i, "status": status, "seconds": round(seconds, 2),
                                            "right_value": status == 200 and body == b"existing"})
    return out


def summarize_probe(p):
    return (f"creates {[x['status'] for x in p['creates']]} "
            f"({[x['seconds'] for x in p['creates']]} s); "
            f"gateway reads {[x['status'] for x in p['reads_through_gateways']]}; "
            f"filer reads {[x['status'] for x in p['reads_through_filers']]}")


def run_masters_experiment(config, control, arguments):
    c = config["cluster"]
    result = {}
    existing = f"masters-existing-{uuid.uuid4().hex[:8]}"
    print("creating an object before any master dies:", s3.put_if_absent(c["gateway_urls"][0], c["bucket"], existing, b"existing")[0])
    # A larger object too: small ones may be stored inside the filer entry itself, not on a volume
    # server, which would hide a dependency on the masters (UNVERIFIED that 4.48 does that).
    big = f"masters-big-{uuid.uuid4().hex[:8]}"
    print("creating a 1 MB object:", s3.put_if_absent(c["gateway_urls"][0], c["bucket"], big, b"b" * (1 << 20))[0])
    time.sleep(2)
    result["before"] = probe_state(config, "m0", existing)
    print("0 masters down:", summarize_probe(result["before"]), flush=True)

    def big_read():
        status, _, body, seconds = s3.read_object(c["gateway_urls"][2], c["bucket"], big, timeout=25)
        return {"status": status, "seconds": round(seconds, 2), "right_size": len(body) == 1 << 20}

    def leader():
        for url in ("http://127.0.0.1:9333", "http://127.0.0.1:9334", "http://127.0.0.1:9335"):
            status, _, body, _ = s3.http_request(url, "GET", "/cluster/status", timeout=3)
            if status == 200:
                try:
                    return json.loads(body).get("Leader")
                except ValueError:
                    pass
        return None

    control.run("stop", "master", 0)
    for waited in (5, 15, 30):
        time.sleep(waited if waited == 5 else waited - (5 if waited == 15 else 15))
        probe = probe_state(config, f"m1-{waited}", existing)
        probe["big_object_read"] = big_read()
        probe["leader_seen"] = leader()
        result[f"one_master_down_after_{waited}s"] = probe
        print(f"1 master down (master0 stopped), {waited}s later: {summarize_probe(probe)}; "
              f"1 MB read {probe['big_object_read']}; leader {probe['leader_seen']}", flush=True)
    race = [token_race(c["gateway_urls"], c["bucket"], f"m1race-{uuid.uuid4().hex[:8]}", config["race"]["concurrent_puts"])
            for _ in range(3)]
    result["races_with_one_master_down"] = [sum(r["status"] == 200 for r in rr) for rr in race]
    print("winners per 24-writer race with 1 master down:", result["races_with_one_master_down"], flush=True)

    control.run("stop", "master", 1)
    for waited in (5, 30, 90):
        time.sleep(5 if waited == 5 else (25 if waited == 30 else 60))
        probe = probe_state(config, f"m2-{waited}", existing)
        probe["big_object_read"] = big_read()
        result[f"two_masters_down_after_{waited}s"] = probe
        print(f"2 masters down, {waited}s later: {summarize_probe(probe)}; 1 MB read {probe['big_object_read']}", flush=True)
    # Does the filer still know the small object without any master? Read the file straight from a
    # filer, and list the bucket (a metadata-only operation).
    listing = s3.http_request(c["filer_urls"][0], "GET", f"/buckets/{c['bucket']}/?limit=3", headers={"Accept": "application/json"}, timeout=25)
    result["two_masters_down_filer_listing_status"] = listing[0]

    control.run("start", "master", 0)
    control.run("start", "master", 1)
    started = time.perf_counter()
    recovered = None
    while time.perf_counter() - started < 180:
        probe = probe_state(config, "m3", existing, timeout=15)
        ok = all(x["status"] == 200 for x in probe["creates"]) and all(x["right_value"] for x in probe["reads_through_gateways"])
        if ok:
            recovered = round(time.perf_counter() - started, 1)
            result["after_restart_probe"] = probe
            break
        time.sleep(2)
    result["recovered_after_s"] = recovered
    print("both masters restarted; creates and reads fully work again after", recovered, "s", flush=True)
    if recovered is not None:
        rr = [token_race(c["gateway_urls"], c["bucket"], f"m3race-{uuid.uuid4().hex[:8]}", config["race"]["concurrent_puts"])
              for _ in range(3)]
        result["races_after_recovery_winners"] = [sum(r["status"] == 200 for r in x) for x in rr]
        result["old_object_after_recovery"] = probe_state(config, "m4", existing)["reads_through_gateways"]
        print("winners per race after recovery:", result["races_after_recovery_winners"], flush=True)
    with open(arguments.log, "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2)


# ---------------------------------------------------------------- summary of a jsonl log

def summarize_log(path):
    rounds = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
    runs = sorted({r["run"] for r in rounds})
    print(f"\n=== summary of {path}: {len(rounds)} rounds in {len(runs)} runs ===")
    two = [r for r in rounds if r["winners"] > 1]
    zero = [r for r in rounds if r["winners"] == 0]
    print(f"rounds with 2+ winners: {len(two)}/{len(rounds)}; with 0 winners: {len(zero)}")
    runs_with_two = {r["run"] for r in two}
    print(f"runs with any two-winner round: {len(runs_with_two)}/{len(runs)}")
    print("by planned offset after the restart (rounds, two-winner rounds, filers disagreeing, runs affected):")
    for offset in sorted({r["planned_offset_s"] for r in rounds}):
        group = [r for r in rounds if r["planned_offset_s"] == offset]
        bad = [r for r in group if r["winners"] > 1]
        disagree = sum(not r["filers_agree"] for r in group)
        print(f"  {offset:>3}s: {len(group):3d} rounds, {len(bad):2d} with 2+ winners, {disagree:2d} with filers disagreeing, "
              f"runs {sorted({r['run'] for r in bad})}")
    print("experiment 4, read-back on rounds with 2+ winners:")
    caught2 = sum(r["readback_caught_via_second_gateway"] for r in two)
    caughte = sum(r["readback_caught_via_either_other_gateway"] for r in two)
    missed_agree = sum((not r["readback_caught_via_either_other_gateway"]) and (not r["filers_agree"]) for r in two)
    print(f"  caught via the second gateway: {caught2}/{len(two)}; via either other gateway: {caughte}/{len(two)}; "
          f"missed while filers still disagreed at the end: {missed_agree}")
    single = [r for r in rounds if r["winners"] == 1]
    print(f"  false alarms (single-winner rounds where a read-back mismatched): "
          f"{sum(r['readback_mismatch_in_single_winner_round'] for r in single)}/{len(single)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config")
    parser.add_argument("experiment", choices=["restart", "mute", "masters", "summary"])
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--offsets", default="0,2,5,10,30,60", help="seconds after the node is back")
    parser.add_argument("--down-seconds", type=float, default=5, help="restart mode: how long the node stays down")
    parser.add_argument("--writes-while-down", type=int, default=0, help="restart mode: creates through the live gateways while it is down")
    parser.add_argument("--rounds-per-offset", type=int, default=2)
    parser.add_argument("--log", required=True, help="output file (jsonl for restart/mute, json for masters)")
    arguments = parser.parse_args()
    with open(arguments.config, "rb") as handle:
        config = tomllib.load(handle)
    control = s3.Control(config)
    if arguments.experiment != "summary":
        s3.ensure_bucket(config["cluster"]["gateway_urls"][0], config["cluster"]["bucket"])
    if arguments.experiment in ("restart", "mute"):
        run_disturbance_experiment(config, control, arguments, arguments.experiment)
        summarize_log(arguments.log)
    elif arguments.experiment == "masters":
        run_masters_experiment(config, control, arguments)
    else:
        summarize_log(arguments.log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
