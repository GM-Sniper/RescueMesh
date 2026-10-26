#!/usr/bin/env bash
# Starts 3 SeaweedFS masters, 1 volume server and 3 filers on localhost (different ports).
# Spike only (docs/p4-changes.md, "SeaweedFS spike"). Not used by the product.
#
# Usage: WEED_BIN=/path/to/weed[.exe] RUN_DIR=/path/to/empty/dir ./start_local_cluster.sh
#
# Why 3 masters: the masters hold the cluster layout and elect a leader with Raft, so 3 is the
#   smallest number that survives losing one.
# Why 1 volume server: file CONTENT lives in volume servers; the question under test is about
#   filer METADATA, so one is enough. (A real deployment needs one per machine.)
# Why no -defaultStoreDir sharing: each filer gets its OWN directory, so each uses its own local
#   embedded store (leveldb). Check 3 of the spike asks whether they still agree.
# Why -filerGroup: the documented way to make filers "share metadata with other filers".
set -euo pipefail

: "${WEED_BIN:?set WEED_BIN to the weed binary}"
: "${RUN_DIR:?set RUN_DIR to an empty directory for data and logs}"
HOST="${HOST:-127.0.0.1}"
MASTERS="$HOST:9333,$HOST:9334,$HOST:9335"
mkdir -p "$RUN_DIR"/{logs,pids}

start() {  # start <name> <args...>
  local name="$1"; shift
  "$WEED_BIN" "$@" >"$RUN_DIR/logs/$name.log" 2>&1 &
  echo $! >"$RUN_DIR/pids/$name.pid"
  echo "started $name (pid $!)"
}

for i in 0 1 2; do
  port=$((9333 + i))
  mkdir -p "$RUN_DIR/master$i"
  start "master$i" master -ip="$HOST" -port=$port -mdir="$RUN_DIR/master$i" -peers="$MASTERS" -defaultReplication=000
done
sleep 6   # let the masters elect a leader before anything else registers

mkdir -p "$RUN_DIR/volume0"
start volume0 volume -ip="$HOST" -port=8080 -dir="$RUN_DIR/volume0" -max=20 -master="$MASTERS"
sleep 3

for i in 0 1 2; do
  port=$((8888 + i))
  mkdir -p "$RUN_DIR/filer$i"
  start "filer$i" filer -ip="$HOST" -port=$port -master="$MASTERS" -filerGroup=spike -defaultStoreDir="$RUN_DIR/filer$i"
done
sleep 5

# Extra probe (check 5): two S3 gateways, each in front of a DIFFERENT filer. The S3 API is the
# part of SeaweedFS that documents conditional writes (If-None-Match), so we test it too.
# The Iceberg and Lance listeners are switched off (port 0): they default to fixed ports, so a
# second gateway on the same machine would crash on "address already in use".
if [ "${WITH_S3:-1}" = 1 ]; then
  start s3a s3 -port=8333 -port.grpc=18333 -port.iceberg=0 -port.lance=0 -filer="$HOST:8888"
  start s3b s3 -port=8334 -port.grpc=18334 -port.iceberg=0 -port.lance=0 -filer="$HOST:8889"
  sleep 6
fi
echo "filers: http://$HOST:8888 http://$HOST:8889 http://$HOST:8890"
