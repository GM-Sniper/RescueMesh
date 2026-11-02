#!/usr/bin/env python3
"""P1, Step C: after a lab run, check the shared state against what the clients saw.

    python3 scripts/check-bucket.py results/clientA/lab-c1.csv results/clientB/lab-c1.csv results/clientC/lab-c1.csv
    python3 scripts/check-bucket.py --endpoint http://<PC1-ip>:8333 results/client*/lab-c1.csv

Run it right after the run, before the bucket is reset. It reads every saved request in the
SeaweedFS bucket (`requests/<request_id>`, see crates/node/src/s3_store.rs) and checks:

1. every request a client got a plan for has exactly one saved record (a record's key is its
   request_id, so one key = one saved plan; two plans for one request cannot both be stored);
2. no record is missing a client row (an extra record = traffic from outside this run);
3. no resource is in two saved plans. Valid while resources are never freed (issue #36): once
   `resolve` exists, a resource may be reused after its incident is resolved.

A stand-in for P2's consistency check until that exists. Standard library only, Python 3.6+,
because lab PCs may have an older Python. Exit code 0 = all checks pass, 1 = a check failed.
"""
import argparse
import csv
import json
import re
import sys
import urllib.parse
import urllib.request
from collections import Counter

# The gateway is on the lab LAN: never send these calls through a system proxy.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get(url):
    with OPENER.open(url, timeout=10) as resp:
        return resp.read()


def list_keys(base, prefix):
    """All object keys under `prefix`, following S3's continuation tokens."""
    keys, token = [], None
    while True:
        url = "{}?list-type=2&max-keys=1000&prefix={}".format(base, urllib.parse.quote(prefix))
        if token:
            url += "&continuation-token=" + urllib.parse.quote(token)
        text = get(url).decode("utf-8")
        keys += re.findall(r"<Key>([^<]+)</Key>", text)
        more = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", text)
        if not more:
            return keys
        token = more.group(1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("csv", nargs="+", help="loadgen CSVs of this run, one per client PC")
    ap.add_argument("--endpoint", default="http://127.0.0.1:8333", help="an S3 gateway (default: this PC's)")
    ap.add_argument("--bucket", default="rescuemesh")
    args = ap.parse_args()

    rows = []
    for path in args.csv:
        with open(path, newline="") as f:
            rows += list(csv.DictReader(f))
    ids = [r["request_id"] for r in rows]
    outcomes = Counter(r["outcome"] for r in rows)
    answered = {r["request_id"] for r in rows if r["outcome"] in ("committed", "duplicate")}
    print("clients: {} CSV file(s), {} requests ({} distinct ids), outcomes {}".format(
        len(args.csv), len(rows), len(set(ids)), dict(outcomes)))

    base = "{}/{}".format(args.endpoint.rstrip("/"), args.bucket)
    try:
        keys = list_keys(base, "requests/")
    except Exception as e:  # noqa: BLE001 - any failure here means we cannot check
        print("cannot read the bucket at {}: {}".format(base, e))
        return 1
    saved = {urllib.parse.unquote(k[len("requests/"):]): k for k in keys}
    used = Counter()
    for key in keys:
        record = json.loads(get("{}/{}".format(base, key)).decode("utf-8"))
        for a in record["plan"]["assignments"]:
            used[a["resource_id"]] += 1
    print("bucket: {} saved requests, {} resources in a plan".format(len(saved), len(used)))

    failed = False
    missing = sorted(answered - set(saved))
    extra = sorted(set(saved) - set(ids))
    twice = sorted(r for r, n in used.items() if n > 1)
    dup_rows = sorted(i for i, n in Counter(ids).items() if n > 1)
    checks = [
        ("every answered request has a saved plan", not missing, "{} without one, e.g. {}".format(len(missing), missing[:3])),
        ("no saved request outside this run", not extra, "{} extra, e.g. {} (bucket not reset, or another client)".format(len(extra), extra[:3])),
        ("no resource in two plans", not twice, "resources {}".format(twice)),
        ("no request_id twice in the CSVs", not dup_rows, "{} ids, e.g. {}".format(len(dup_rows), dup_rows[:3])),
    ]
    for name, ok, why in checks:
        print("  {} {}{}".format("PASS" if ok else "FAIL", name, "" if ok else ": " + why))
        failed |= not ok
    print("ALL CHECKS PASS" if not failed else "SOME CHECKS FAILED")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
