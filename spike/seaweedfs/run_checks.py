#!/usr/bin/env python3
"""SeaweedFS spike checks. Spike only: see docs/p4-changes.md, "SeaweedFS spike".

Usage:  python run_checks.py cluster.local.toml [--json results.json]

Reads the filer URLs from a TOML config, so the SAME checks can be rerun on the real machines
by writing a config with their addresses. Standard library only (needs Python 3.11+ for tomllib),
so there is nothing to install on the lab machines.

Checks (numbers match the task list):
  1. create-if-absent: PUT with `If-None-Match: *` twice; the second should be 412.
  2. race: many concurrent creates of one path, spread over DIFFERENT filers; exactly one must win.
  3. metadata sharing: write on filer A, time how long until B and C return it.
  4. latency and limits: write/read timings and a few object sizes.
  5. (extra, only if the config has [s3]) the same create-if-absent and race checks through
     SeaweedFS's S3 gateway, which documents conditional writes. Not part of the original task
     list; it was added after checks 1 and 2 failed on the plain filer API.
"""
import argparse
import http.client
import json
import statistics
import sys
import threading
import time
import tomllib
import uuid
from urllib.parse import urlparse


def request(base_url, method, path, body=None, headers=None, timeout=60):
    """One HTTP request on a fresh connection. Returns (status, response_body, seconds).

    A fresh connection per call (no keep-alive) so each timing includes connection setup, which
    is what a node that calls the filer only now and then would pay. Returns status 0 and the
    error text instead of raising, so a down filer is reported instead of crashing the run.
    """
    parts = urlparse(base_url)
    started = time.perf_counter()
    try:
        connection = http.client.HTTPConnection(parts.hostname, parts.port, timeout=timeout)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        data = response.read()
        status = response.status
        connection.close()
    except (OSError, http.client.HTTPException) as error:
        return 0, str(error).encode(), time.perf_counter() - started
    return status, data, time.perf_counter() - started


def create_if_absent(filer_url, path, body):
    return request(filer_url, "PUT", path, body, {"If-None-Match": "*"})


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def check_1_conditional_put(filers, base):
    print("\n== Check 1: create-if-absent with If-None-Match: * on one filer ==")
    path = f"{base}/check1.txt"
    first = create_if_absent(filers[0], path, b"first")
    second = create_if_absent(filers[0], path, b"second")
    content = request(filers[0], "GET", path)
    print(f"PUT {path}  If-None-Match: *  (first)   -> {first[0]}")
    print(f"PUT {path}  If-None-Match: *  (second)  -> {second[0]}   (412 expected)")
    print(f"GET {path} -> {content[0]}, body={content[1][:40]!r}")
    passed = first[0] in (200, 201) and second[0] == 412
    print("RESULT:", "PASS" if passed else "FAIL (the second create was not rejected)")
    return {"first_status": first[0], "second_status": second[0],
            "body_after": content[1].decode(errors="replace"), "passed": passed}


def check_2_race(filers, base, concurrent_creates, rounds):
    print(f"\n== Check 2: {concurrent_creates} concurrent creates of one path over {len(filers)} filers, {rounds} rounds ==")
    results = []
    for round_number in range(rounds):
        path = f"{base}/race-{round_number}.txt"
        barrier = threading.Barrier(concurrent_creates)
        outcomes = [None] * concurrent_creates

        def worker(index):
            filer = filers[index % len(filers)]  # round-robin: neighbours hit different filers
            barrier.wait()                       # all workers fire at the same moment
            status, _, _ = create_if_absent(filer, path, f"writer-{index}".encode())
            outcomes[index] = (index % len(filers), status)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrent_creates)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        successes = [o for o in outcomes if o[1] in (200, 201)]
        rejected = [o for o in outcomes if o[1] == 412]
        other = [o for o in outcomes if o[1] not in (200, 201, 412)]

        time.sleep(3)  # let filers finish replicating metadata, then see who agrees with whom
        final = [request(f, "GET", path)[1].decode(errors="replace") for f in filers]
        converged = len(set(final)) == 1
        passed = len(successes) == 1
        print(f"round {round_number}: success={len(successes)} rejected(412)={len(rejected)} "
              f"other={len(other)}  filers end with {final}  "
              f"{'PASS' if passed else 'FAIL'}{'' if converged else ' (filers DISAGREE)'}")
        results.append({"successes": len(successes), "rejected": len(rejected),
                        "other": len(other), "final_content_per_filer": final,
                        "filers_converged": converged, "passed": passed})
    passed_rounds = sum(r["passed"] for r in results)
    print(f"RESULT: exactly one winner in {passed_rounds}/{rounds} rounds")
    return {"rounds": results, "passed": passed_rounds == rounds}


def check_3_metadata_sharing(filers, base, samples, timeout_seconds):
    print(f"\n== Check 3: write on filer A, wait until the others return it ({samples} samples) ==")
    if len(filers) < 2:
        print("RESULT: skipped, needs at least 2 filers")
        return {"passed": None}
    delays = []  # seconds until the file was readable on each other filer
    never_appeared = 0
    for sample in range(samples):
        path = f"{base}/visible-{sample}.txt"
        payload = f"sample-{sample}".encode()
        writer = filers[sample % len(filers)]
        readers = [f for f in filers if f != writer]
        request(writer, "PUT", path, payload)
        written_at = time.perf_counter()
        for reader in readers:
            while True:
                status, data, _ = request(reader, "GET", path)
                if status == 200 and data == payload:
                    delays.append(time.perf_counter() - written_at)
                    break
                if time.perf_counter() - written_at > timeout_seconds:
                    never_appeared += 1
                    break
                time.sleep(0.002)
    if delays:
        print(f"appeared on {len(delays)} reads; delay ms: min={min(delays)*1000:.1f} "
              f"median={statistics.median(delays)*1000:.1f} max={max(delays)*1000:.1f}")
    print(f"never appeared within {timeout_seconds}s: {never_appeared}")
    passed = never_appeared == 0
    print("RESULT:", "PASS" if passed else "FAIL")
    return {"delay_ms_min": min(delays) * 1000 if delays else None,
            "delay_ms_median": statistics.median(delays) * 1000 if delays else None,
            "delay_ms_max": max(delays) * 1000 if delays else None,
            "never_appeared": never_appeared, "passed": passed}


def check_4_latency_and_limits(filers, base, samples):
    print(f"\n== Check 4: latency ({samples} sequential ops per filer) and sizes ==")
    out = {"per_filer": {}, "sizes": []}
    for filer in filers:
        writes, reads, failures = [], [], 0
        for _ in range(samples):
            path = f"{base}/latency-{uuid.uuid4().hex}.txt"
            status, _, seconds = request(filer, "PUT", path, b"x" * 512)
            writes.append(seconds)
            failures += status not in (200, 201)
            status, _, seconds = request(filer, "GET", path)
            reads.append(seconds)
            failures += status != 200
        line = {"write_ms_median": statistics.median(writes) * 1000,
                "write_ms_p95": percentile(writes, 0.95) * 1000,
                "read_ms_median": statistics.median(reads) * 1000,
                "read_ms_p95": percentile(reads, 0.95) * 1000, "failures": failures}
        out["per_filer"][filer] = line
        print(f"{filer}: write median {line['write_ms_median']:.1f} ms (p95 {line['write_ms_p95']:.1f}), "
              f"read median {line['read_ms_median']:.1f} ms (p95 {line['read_ms_p95']:.1f}), failures {failures}")
    for label, size in [("1 KB", 1024), ("1 MB", 1 << 20), ("16 MB", 16 << 20), ("64 MB", 64 << 20)]:
        status, _, seconds = request(filers[0], "PUT", f"{base}/size-{size}.bin", b"y" * size)
        out["sizes"].append({"size": label, "status": status, "seconds": seconds})
        print(f"PUT {label}: status {status} in {seconds*1000:.0f} ms")
    # A burst of parallel writes, to see whether the filer throttles or errors under load.
    burst = 100
    statuses = []
    lock = threading.Lock()

    def burst_worker(i):
        status, _, _ = request(filers[i % len(filers)], "PUT", f"{base}/burst-{uuid.uuid4().hex}.txt", b"z")
        with lock:
            statuses.append(status)

    started = time.perf_counter()
    threads = [threading.Thread(target=burst_worker, args=(i,)) for i in range(burst)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    elapsed = time.perf_counter() - started
    bad = [s for s in statuses if s not in (200, 201)]
    out["burst"] = {"requests": burst, "seconds": elapsed, "non_2xx": bad}
    print(f"burst of {burst} parallel small writes: {elapsed:.2f} s, non-2xx statuses: {bad or 'none'}")
    return out


def check_5_s3_conditional_create(endpoints, bucket, concurrent_creates, rounds):
    print(f"\n== Check 5 (extra): S3 gateway, If-None-Match: * on {len(endpoints)} gateways ==")
    status, _, _ = request(endpoints[0], "PUT", f"/{bucket}")
    print(f"create bucket {bucket!r} -> {status}")
    time.sleep(2)  # the bucket is a directory in the filer; give the other gateways time to see it
    key = f"/{bucket}/check5-{uuid.uuid4().hex[:8]}"
    first = create_if_absent(endpoints[0], key, b"first")
    second = create_if_absent(endpoints[0], key, b"second")
    print(f"single gateway: first create -> {first[0]}, second create -> {second[0]} (412 expected)")
    results = []
    for round_number in range(rounds):
        key = f"/{bucket}/race-{uuid.uuid4().hex[:8]}"
        barrier = threading.Barrier(concurrent_creates)
        statuses = [None] * concurrent_creates

        def worker(index):
            endpoint = endpoints[index % len(endpoints)]
            barrier.wait()
            statuses[index] = create_if_absent(endpoint, key, f"writer-{index}".encode())[0]

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrent_creates)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        wins = sum(s in (200, 201) for s in statuses)
        rejected = sum(s == 412 for s in statuses)
        time.sleep(2)
        final = [request(e, "GET", key)[1].decode(errors="replace") for e in endpoints]
        passed = wins == 1 and len(set(final)) == 1
        print(f"round {round_number}: success={wins} rejected(412)={rejected} "
              f"other={concurrent_creates - wins - rejected}  gateways end with {final}  "
              f"{'PASS' if passed else 'FAIL'}")
        results.append({"successes": wins, "rejected": rejected, "final": final, "passed": passed})
    single_ok = first[0] in (200, 201) and second[0] == 412
    passed = single_ok and all(r["passed"] for r in results)
    print("RESULT:", "PASS" if passed else "FAIL")
    return {"single_gateway_statuses": [first[0], second[0]], "rounds": results, "passed": passed}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config")
    parser.add_argument("--json", help="also write the results to this file")
    arguments = parser.parse_args()
    with open(arguments.config, "rb") as handle:
        config = tomllib.load(handle)
    filers = config["cluster"]["filer_urls"]
    base = f"{config['cluster']['base_path'].rstrip('/')}/run-{uuid.uuid4().hex[:8]}"
    print(f"filers: {filers}\nbase path: {base}")

    results = {"filers": filers, "base_path": base}
    results["check_1"] = check_1_conditional_put(filers, base)
    results["check_2"] = check_2_race(filers, base, config["race"]["concurrent_creates"], config["race"]["rounds"])
    results["check_3"] = check_3_metadata_sharing(filers, base, config["visibility"]["samples"],
                                                  config["visibility"]["timeout_seconds"])
    results["check_4"] = check_4_latency_and_limits(filers, base, config["latency"]["samples"])

    if "s3" in config:
        results["check_5"] = check_5_s3_conditional_create(
            config["s3"]["endpoint_urls"], config["s3"]["bucket"],
            config["race"]["concurrent_creates"], config["race"]["rounds"])

    print("\n== Summary ==")
    for name in [n for n in ("check_1", "check_2", "check_3", "check_5") if n in results]:
        passed = results[name]["passed"]
        print(f"{name}: {'PASS' if passed else 'SKIPPED' if passed is None else 'FAIL'}")
    print("check_4: informational")
    if arguments.json:
        with open(arguments.json, "w") as handle:
            json.dump(results, handle, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
