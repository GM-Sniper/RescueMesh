#!/usr/bin/env python3
"""Read back every object in the RescueMesh bucket through one S3 gateway (P5, for Fix 2 and 5).

Usage, on a PC whose SeaweedFS is still running, after stopping another PC's SeaweedFS:
    python3 scripts/s3-readback.py                        # this PC's gateway, bucket rescuemesh
    python3 scripts/s3-readback.py http://10.7.57.20:8333 rescuemesh

Lists every key (ListObjectsV2, 1000 per page), GETs each one, and prints how many were
readable per prefix (requests/, claims/, resources/), plus the first failures.
Standard library only; ignores HTTP(S)_PROXY like the node does.
"""
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

ENDPOINT = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8333").rstrip("/")
BUCKET = sys.argv[2] if len(sys.argv) > 2 else "rescuemesh"
TIMEOUT_S = 5
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # no proxy


def get(url):
    with opener.open(url, timeout=TIMEOUT_S) as resp:
        return resp.read()


def list_keys():
    keys, token = [], None
    while True:
        query = {"list-type": "2", "max-keys": "1000"}
        if token:
            query["continuation-token"] = token
        root = ET.fromstring(get(f"{ENDPOINT}/{BUCKET}?{urllib.parse.urlencode(query)}"))
        keys += [c.find(f"{NS}Key").text for c in root.findall(f"{NS}Contents")]
        token_el = root.find(f"{NS}NextContinuationToken")
        if token_el is None or not token_el.text:
            return keys
        token = token_el.text


def main():
    started = time.time()
    keys = list_keys()
    print(f"{len(keys)} objects listed from {ENDPOINT}/{BUCKET}")
    totals, failures = {}, []
    for key in keys:
        prefix = key.split("/", 1)[0] + "/"
        ok_count, all_count = totals.get(prefix, (0, 0))
        try:
            get(f"{ENDPOINT}/{BUCKET}/{urllib.parse.quote(key)}")
            totals[prefix] = (ok_count + 1, all_count + 1)
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            totals[prefix] = (ok_count, all_count + 1)
            failures.append((key, getattr(e, "code", None) or str(e)))
    for prefix, (ok_count, all_count) in sorted(totals.items()):
        pct = 100.0 * ok_count / all_count if all_count else 100.0
        print(f"  {prefix:<12} {ok_count}/{all_count} readable ({pct:.1f}%)")
    for key, why in failures[:20]:
        print(f"  FAILED {key}: {why}")
    if len(failures) > 20:
        print(f"  ... and {len(failures) - 20} more")
    print(f"done in {time.time() - started:.1f} s; {len(failures)} unreadable")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
