#!/usr/bin/env bash
# Clean reset before an experiment run (P3, Step D). Run it on each node machine, from the
# folder that holds config/, NODE 1 FIRST:
#
#   ./scripts/reset-node.sh 1 ramp-run-3     # on node 1's machine: also clears the shared state
#   ./scripts/reset-node.sh 2 ramp-run-3     # then on node 2's
#   ./scripts/reset-node.sh 3 ramp-run-3     # then on node 3's
#
# Anything after the run name goes to the node, e.g. the load knob for the imbalance run:
#   ./scripts/reset-node.sh 2 imbalance-1 --planner-iterations 30000
#
# What it does: stops this machine's node, clears the shared state (node 1 only), moves the
# last run's logs to logs/archive/<last run>/, saves the config and code version, starts a
# fresh node and waits until it listens.
#
# Settings: CONFIG (default config/nodes.lab.toml), NODE_BIN (default ./node, then
# target/release/node).
set -euo pipefail

CONFIG="${CONFIG:-config/nodes.lab.toml}"
START_WAIT_SECS=15 # S3Store::connect alone may retry for 10 s
STOP_WAIT_SECS=5

die() {
    echo "reset-node: $*" >&2
    exit 1
}

[ $# -ge 2 ] || die "usage: reset-node.sh <node id> <run name> [node flags...]"
ID="$1"
RUN_ID="$2"
shift 2
[[ "$ID" =~ ^[0-9]+$ ]] || die "node id must be a number, got '$ID'"
# The run name becomes a folder name and a CSV field.
[[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] || die "run name may only use letters, digits, . _ -"
[ -f "$CONFIG" ] || die "no $CONFIG here: run from the folder that holds config/"

if [ -n "${NODE_BIN:-}" ]; then
    BIN="$NODE_BIN"
elif [ -f ./node ] && [ -x ./node ]; then
    BIN=./node
else
    BIN=target/release/node
fi
[ -x "$BIN" ] || die "no node program at $BIN: build it first (cargo build --release)"

# Prints `key`'s value from the `[section]` table, without its quotes and comment.
toml_value() {
    awk -v section="[$1]" -v key="$2" '
        /^\[/ { inside = (index($0, section) == 1) }
        inside && $1 == key {
            sub(/^[^=]*=[ \t]*/, ""); sub(/[ \t]*#.*$/, ""); gsub(/"/, ""); print; exit
        }' "$CONFIG"
}

# Prints this node's port. Each `id =` line is followed by its own `addr =` line.
node_port() {
    awk -v id="$ID" '
        $1 == "id" { mine = ($3 == id) }
        mine && $1 == "addr" { gsub(/"/, "", $3); n = split($3, part, ":"); print part[n]; exit }
        ' "$CONFIG"
}

PORT="$(node_port)"
[ -n "$PORT" ] || die "node $ID is not listed in $CONFIG"

LOGS=logs
ARCHIVE="$LOGS/archive"
INFO="$LOGS/run-info-node$ID.txt"

last_run() {
    [ -f "$INFO" ] && sed -n 's/^run_id=//p' "$INFO" | head -n 1
    return 0
}

# Checked before anything is stopped: one run name means one folder of results (Step D rule).
if [ "$(last_run)" = "$RUN_ID" ] || [ -d "$ARCHIVE/$RUN_ID" ]; then
    die "run name '$RUN_ID' was already used on this machine: pick a new one"
fi

# 1. Stop this machine's node. Matches a node started by hand too.
node_pids() {
    pgrep -u "$(id -u)" -f "(^|/)node --id $ID( |$)" || true
}

pids="$(node_pids)"
if [ -n "$pids" ]; then
    echo "stopping node $ID (pid $(echo $pids))"
    kill $pids 2>/dev/null || true
    for _ in $(seq 1 $((STOP_WAIT_SECS * 10))); do
        [ -z "$(node_pids)" ] && break
        sleep 0.1
    done
    pids="$(node_pids)"
    if [ -n "$pids" ]; then
        echo "node $ID did not stop in ${STOP_WAIT_SECS} s: killing it"
        kill -9 $pids 2>/dev/null || true
        sleep 0.2
    fi
fi

# 2. Clear the shared state, once per run. Delete the bucket, never use a new name per run:
#    each bucket takes its own volumes (docs/p5-changes.md). The nodes refill it when they
#    start. With the "memory" backend the restart itself clears the state.
BACKEND="$(toml_value shared_state backend)"
if [ "$ID" = "1" ] && [ "$BACKEND" = "s3" ]; then
    ENDPOINT="$(toml_value shared_state endpoint)"
    BUCKET="$(toml_value shared_state bucket)"
    # No proxy: the same reason as S3Store::connect.
    code="$(curl --noproxy '*' --silent --output /dev/null --max-time 10 \
        --write-out '%{http_code}' -X DELETE "${ENDPOINT%/}/$BUCKET" || true)"
    case "$code" in
        2*) echo "shared state cleared ($BUCKET, HTTP $code)" ;;
        # Normal on the very first run. Said apart from "cleared" so a surprise is visible.
        404) echo "no bucket to clear ($BUCKET, HTTP 404): expected only on the first run" ;;
        000) die "no answer from the gateway at $ENDPOINT: is SeaweedFS running? Node 1 is stopped; nothing was started" ;;
        *) die "the gateway at $ENDPOINT answered HTTP $code when clearing $BUCKET: SeaweedFS may be partly down, check its log. Node 1 is stopped; nothing was started" ;;
    esac
fi

# 3. Move the last run's files away, so the new run starts with empty logs.
mkdir -p "$LOGS"
old_files=()
for f in "$LOGS"/*-node"$ID".csv "$LOGS/node$ID.log" "$LOGS/config-node$ID.toml" "$INFO"; do
    [ -e "$f" ] && old_files+=("$f")
done
if [ ${#old_files[@]} -gt 0 ]; then
    previous="$(last_run)"
    previous="${previous:-before-reset-$(date +%Y%m%d-%H%M%S)}" # started by hand: no run name known
    mkdir -p "$ARCHIVE/$previous"
    mv "${old_files[@]}" "$ARCHIVE/$previous/"
    echo "last run's logs moved to $ARCHIVE/$previous/"
fi

# 4. Record how this run was produced, next to its logs.
cp "$CONFIG" "$LOGS/config-node$ID.toml"
{
    echo "run_id=$RUN_ID"
    echo "node_id=$ID"
    echo "started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit=$(git rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "uncommitted_changes=$(git status --porcelain 2>/dev/null | grep -c -v '^??' || true)"
    echo "node_flags=$*"
} >"$INFO"

# 5. Start a fresh node. It keeps running after this shell closes.
nohup "$BIN" --id "$ID" --config "$CONFIG" --run-id "$RUN_ID" "$@" \
    >"$LOGS/node$ID.log" 2>&1 </dev/null &
pid=$!

# 6. Wait until it listens. READY comes later: the rejoin rule needs a peer running too.
for _ in $(seq 1 $((START_WAIT_SECS * 10))); do
    if ! kill -0 "$pid" 2>/dev/null; then
        tail -n 5 "$LOGS/node$ID.log" >&2
        rm -f "$INFO" # the run never started, so its name stays free for the retry
        die "node $ID stopped right after starting; full log: $LOGS/node$ID.log"
    fi
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        echo "node $ID up (pid $pid, port $PORT, run $RUN_ID). It turns READY once a peer is running."
        exit 0
    fi
    sleep 0.1
done
kill "$pid" 2>/dev/null || true
tail -n 5 "$LOGS/node$ID.log" >&2
rm -f "$INFO"
die "node $ID did not listen on port $PORT within ${START_WAIT_SECS} s; stopped it. Full log: $LOGS/node$ID.log"
