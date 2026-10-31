#!/usr/bin/env bash
# Starts a local SeaweedFS test cluster on one machine, each part on its own port:
#   3 masters (9333-9335), 3 volume servers (8080-8082, racks rack1-rack3),
#   3 filers (8888-8890, each with its OWN local store), 3 S3 gateways (8333-8335, one per filer).
# Spike only (docs/p4-changes.md, "SeaweedFS spike"). Not used by the product.
#
# Usage: WEED_BIN=/path/to/weed[.exe] RUN_DIR=/path/to/empty/dir ./start_local_cluster.sh
# Optional: REPLICATION=010 (default) or 020, HOST=127.0.0.1.
#
# Why 3 masters: the masters elect a leader with Raft, so 3 is the smallest number that
#   survives losing one.
# Why 3 volume servers on 3 racks: file CONTENT lives in volume servers. Replication is decided
#   per rack, so each server gets its own -rack to let a copy go to a different server.
# Why REPLICATION=010: SeaweedFS replication is three digits "xyz" = extra copies in other data
#   centers, other racks, and other servers on the same rack. "010" = one extra copy on a
#   different rack, so every object exists on 2 of the 3 servers. UNVERIFIED against the
#   SeaweedFS docs; confirmed here only by listing the volume locations (see the spike notes).
#   "020" would put a copy on every rack, but then a write needs ALL 3 servers alive.
# Why each filer has its own -defaultStoreDir: each uses its own local embedded store
#   (leveldb). The filers still agree through -filerGroup, which makes them replay each other's
#   metadata changes.
# Why Iceberg and Lance ports are 0: those S3-gateway listeners default to fixed ports, so a
#   second gateway on the same machine would crash on "address already in use".
set -euo pipefail

: "${WEED_BIN:?set WEED_BIN to the weed binary}"
: "${RUN_DIR:?set RUN_DIR to an empty directory for data and logs}"
HOST="${HOST:-127.0.0.1}"
REPLICATION="${REPLICATION:-010}"
MASTERS="$HOST:9333,$HOST:9334,$HOST:9335"
mkdir -p "$RUN_DIR"/{logs,pids,args}

# start <name> <weed args...>: runs weed in the background and remembers the arguments so
# proc_ctl.sh can restart the same process later.
start() {
  local name="$1"; shift
  printf '%q ' "$@" >"$RUN_DIR/args/$name.args"
  "$WEED_BIN" "$@" >>"$RUN_DIR/logs/$name.log" 2>&1 &
  echo $! >"$RUN_DIR/pids/$name.pid"
  echo "started $name (pid $!)"
}

for i in 0 1 2; do
  mkdir -p "$RUN_DIR/master$i"
  start "master$i" master -ip="$HOST" -port=$((9333 + i)) -mdir="$RUN_DIR/master$i" \
    -peers="$MASTERS" -defaultReplication="$REPLICATION"
done
sleep 6   # let the masters elect a leader before anything else registers

for i in 0 1 2; do
  mkdir -p "$RUN_DIR/volume$i"
  start "volume$i" volume -ip="$HOST" -port=$((8080 + i)) -dir="$RUN_DIR/volume$i" -max=30 \
    -dataCenter=dc1 -rack="rack$((i + 1))" -master="$MASTERS"
done
sleep 4

for i in 0 1 2; do
  mkdir -p "$RUN_DIR/filer$i"
  start "filer$i" filer -ip="$HOST" -port=$((8888 + i)) -master="$MASTERS" -filerGroup=spike \
    -defaultStoreDir="$RUN_DIR/filer$i"
done
sleep 5

# Gateway i talks to filer i only, so killing "filer i + gateway i" removes one whole entry point.
for i in 0 1 2; do
  start "s3gateway$i" s3 -port=$((8333 + i)) -port.grpc=$((18333 + i)) -port.iceberg=0 \
    -port.lance=0 -filer="$HOST:$((8888 + i))"
done
sleep 6
echo "filers:   http://$HOST:8888 http://$HOST:8889 http://$HOST:8890"
echo "gateways: http://$HOST:8333 http://$HOST:8334 http://$HOST:8335"
