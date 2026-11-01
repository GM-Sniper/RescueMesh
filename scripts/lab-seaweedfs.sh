#!/usr/bin/env bash
# P5, Step C: start or stop SeaweedFS on ONE lab PC. Run it on each of the 3 PCs.
#
#   ./scripts/lab-seaweedfs.sh start <this-pc-ip> <ip1,ip2,ip3>
#   ./scripts/lab-seaweedfs.sh status
#   ./scripts/lab-seaweedfs.sh stop
#   ./scripts/lab-seaweedfs.sh wipe        # stop and delete all SeaweedFS data on this PC
#
# Starts, on this PC: a master (port 9333), a volume server (8080), a filer (8888) and the
# S3 gateway (8333) that our nodes use (DESIGN_LOG.md, "P5: Shared-state backend").
#
# Why each choice:
# - A master on every PC: the masters elect a leader with Raft and need 2 of 3 up, so
#   the cluster survives one PC being off (lab-setup.md, section 6C).
# - defaultReplication=001: every file is kept on 2 different volume servers, so a saved
#   plan can still be read with one PC off. (000, the default, keeps a single copy.)
# - One filer per PC in one filerGroup, each with its own local store: no central database.
# - The S3 gateway lists this PC's filer first, then the others, so it keeps working if the
#   local filer dies. It binds to 0.0.0.0, so the node reaches it at 127.0.0.1:8333.
# - AWS_* variables are removed: if SeaweedFS sees them it turns S3 auth on and our nodes,
#   which send no credentials, get HTTP 403 on every call.
#
# Settings (environment variables, all optional):
#   WEED_BIN     path to the weed binary            (default: ./weed)
#   SEAWEED_DIR  where data, logs and pids go         (default: $HOME/rescuemesh-seaweed)
#   S3_BIND      address the S3 gateway listens on    (default: 0.0.0.0)
set -euo pipefail

WEED="${WEED_BIN:-./weed}"
DIR="${SEAWEED_DIR:-$HOME/rescuemesh-seaweed}"
S3_BIND="${S3_BIND:-0.0.0.0}"

usage() { sed -n '2,7p' "$0"; exit 1; }

run() {  # run <name> <weed args...>: start in the background, without AWS variables
  local name="$1"; shift
  env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN -u AWS_PROFILE \
    nohup "$WEED" "$@" >"$DIR/logs/$name.log" 2>&1 &
  echo $! >"$DIR/pids/$name.pid"
  echo "started $name (pid $!), log: $DIR/logs/$name.log"
}

stop_all() {
  for name in s3 filer volume master; do
    local f="$DIR/pids/$name.pid"
    if [ -f "$f" ]; then
      kill "$(cat "$f")" 2>/dev/null && echo "stopped $name" || echo "$name was not running"
      rm -f "$f"
    fi
  done
}

case "${1:-}" in
  start)
    [ $# -eq 3 ] || usage
    ME="$2"; ALL="$3"
    [ -x "$WEED" ] || { echo "weed binary not found at $WEED (set WEED_BIN)"; exit 1; }
    case ",$ALL," in *",$ME,"*) ;; *) echo "this PC's ip $ME is not in the list $ALL"; exit 1;; esac
    IFS=, read -r -a IPS <<<"$ALL"
    [ ${#IPS[@]} -eq 3 ] || { echo "expected 3 ips, got ${#IPS[@]}"; exit 1; }

    MASTERS=$(printf '%s:9333,' "${IPS[@]}"); MASTERS=${MASTERS%,}
    FILERS="$ME:8888"
    for ip in "${IPS[@]}"; do [ "$ip" = "$ME" ] || FILERS="$FILERS,$ip:8888"; done

    mkdir -p "$DIR"/{master,volume,filer,logs,pids}
    run master master -ip="$ME" -port=9333 -mdir="$DIR/master" -peers="$MASTERS" \
      -defaultReplication=001 -volumeSizeLimitMB=256
    echo "waiting 8 s for the masters to elect a leader (start all 3 PCs within this time)"
    sleep 8
    run volume volume -ip="$ME" -port=8080 -dir="$DIR/volume" -max=50 -master="$MASTERS"
    sleep 3
    run filer filer -ip="$ME" -port=8888 -master="$MASTERS" -filerGroup=rescuemesh \
      -defaultStoreDir="$DIR/filer" -localSocket="$DIR/filer.sock"
    sleep 4
    run s3 s3 -ip.bind="$S3_BIND" -ip="$ME" -port=8333 -filer="$FILERS" \
      -port.iceberg=0 -port.lance=0 -localSocket="$DIR/s3.sock"
    sleep 4
    echo "check: curl -s -o /dev/null -w '%{http_code}\n' http://$ME:8333/   (want 200)"
    ;;
  status)
    for name in master volume filer s3; do
      f="$DIR/pids/$name.pid"
      if [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null; then echo "$name: running"; else echo "$name: NOT running"; fi
    done
    ;;
  stop) stop_all ;;
  wipe)
    stop_all
    sleep 1
    rm -rf "$DIR"
    echo "deleted $DIR"
    ;;
  *) usage ;;
esac
