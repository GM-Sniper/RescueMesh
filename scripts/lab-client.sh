#!/usr/bin/env bash
# P1, Step C: run the load generator on ONE client PC of the lab (docs/lab-clients.md).
#
#   ./scripts/lab-client.sh <A|B|C> <run-id> <requests> <rate> [clients]
#
#   ./scripts/lab-client.sh A lab-c1 600 10        # client PC A: 600 requests at 10/s
#
# Why a script: with 3 client PCs, three things must differ per PC and one must be the same.
# - The seed: the incident is made from it (generate_incident(seed + n)), so with one seed
#   all 3 PCs would send the same incidents. A = 1, B = 1000001, C = 2000001.
# - The output folder: results/client<X>/, so collecting the 3 CSVs never overwrites one.
#   client_id restarts at 1 on every PC, so the folder is what tells the PCs apart.
# - The run ID: the SAME on all 6 PCs (the nodes' --run-id), so all logs join into one run.
#
# Settings (environment variables, all optional):
#   CONFIG       the filled-in node list      (default: config/nodes.lab.toml)
#   LOADGEN_BIN  path to loadgen              (default: ./target/release/loadgen, then ./loadgen)
set -euo pipefail

usage() {
    echo "usage: $0 <A|B|C> <run-id> <requests> <rate> [clients]" >&2
    echo "  e.g. $0 A lab-c1 600 10" >&2
    exit 2
}
[ $# -ge 4 ] || usage
PC=$1 RUN=$2 REQUESTS=$3 RATE=$4 CLIENTS=${5:-3}

case "$PC" in
    A) SEED=1 ;;
    B) SEED=1000001 ;;
    C) SEED=2000001 ;;
    *) echo "client PC must be A, B or C (got '$PC')" >&2; usage ;;
esac

CONFIG=${CONFIG:-config/nodes.lab.toml}
if [ -z "${LOADGEN_BIN:-}" ]; then
    if [ -x ./target/release/loadgen ]; then LOADGEN_BIN=./target/release/loadgen; else LOADGEN_BIN=./loadgen; fi
fi
[ -x "$LOADGEN_BIN" ] || { echo "loadgen not found at $LOADGEN_BIN (build it, or set LOADGEN_BIN)" >&2; exit 1; }
[ -f "$CONFIG" ] || { echo "config not found: $CONFIG (run from the RescueMesh folder)" >&2; exit 1; }
if grep -q "REPLACE-ME" "$CONFIG"; then
    echo "$CONFIG still has REPLACE-ME: copy the filled-in file from PC1 first" >&2; exit 1
fi

OUT=results/client$PC
mkdir -p "$OUT"
echo "client PC $PC: run $RUN, $REQUESTS requests at $RATE/s with $CLIENTS clients, seed $SEED, results in $OUT/"
set -x
"$LOADGEN_BIN" --config "$CONFIG" --run-id "$RUN" --out "$OUT" --seed "$SEED" \
    --requests "$REQUESTS" --rate "$RATE" --clients "$CLIENTS"
