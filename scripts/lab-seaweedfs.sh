#!/usr/bin/env bash
# P5, Step C: start or stop SeaweedFS on ONE lab PC. Run it on each of the 3 PCs.
#
#   ./scripts/lab-seaweedfs.sh start <this-pc-ip> <ip1,ip2,ip3>    # also after a crash of this PC
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
#   The S3 gateway lists this PC's filer first, then the others. (It also learns all filers
#   of the group from the master and then picks one itself, so which filer a gateway uses is
#   not fixed by this list; the filers copy every change to each other in the background.)
#   The gateway binds to 0.0.0.0, so the node reaches it at 127.0.0.1:8333.
#   Known limit (Nov 7 lab): that background copying is not crash-safe in SeaweedFS 4.48. After
#   a power cut of one PC, a few entries written around the cut can be missing on one filer.
#   The demo panel's "Repair storage" copies them back (saved requests and resolve markers
#   never change once written, so copying is safe) and the nodes' sweeper frees leftover claims.
#   Do NOT start a filer on an emptied store while the others run: the peers then re-import
#   its whole (seconds old) copy as a "new peer", which brings deleted claims back.
# - Each process runs under a small supervisor that restarts it if it exits: SeaweedFS 4.48's
#   filer crashed in the lab (nil pointer while forwarding a write lock, Nov 7, usually right
#   after another PC lost power), and a missing filer or gateway on one PC otherwise stays
#   missing until someone notices.
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
# Marker in every supervisor's command line, so a power cut (kill -9) can find them too.
SUP_TAG=rescuemesh-weed-supervisor
# Open-file limit: 1024 by default on the lab PCs. The S3 gateway gets up to 128 connections
# from each of the 3 nodes, and the filer and volume server keep files and sockets open too.
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

usage() { sed -n '2,8p' "$0"; exit 1; }

# run <name> <weed args...>: start <name> under a supervisor, in the background, without AWS
# variables. pids/<name>.pid = the supervisor, pids/<name>.weed.pid = the weed process now.
run() {
  local name="$1"; shift
  : >"$DIR/logs/$name.log"
  env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN -u AWS_PROFILE \
    nohup bash -c '
      name=$1; dir=$2; shift 2
      child=
      trap '"'"'[ -n "$child" ] && kill "$child" 2>/dev/null; exit 0'"'"' TERM INT
      n=0
      while :; do
        rm -f "$dir/$name.sock"
        "$@" >>"$dir/logs/$name.log" 2>&1 &
        child=$!
        echo "$child" >"$dir/pids/$name.weed.pid"
        rc=0; wait "$child" || rc=$?
        n=$((n + 1))
        echo "[supervisor] weed $name exited (code $rc) at $(date +%T); restart $n in 2 s" >>"$dir/logs/$name.log"
        sleep 2
      done' "$SUP_TAG" "$name" "$DIR" "$WEED" "$@" >/dev/null 2>&1 &
  echo $! >"$DIR/pids/$name.pid"
  echo "started $name (supervisor pid $!), log: $DIR/logs/$name.log"
}

weed_alive() {  # weed_alive <name>: is the weed process itself running right now?
  local pid
  pid=$(cat "$DIR/pids/$1.weed.pid" 2>/dev/null || true)
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

last_error() { grep -E '^[FE][0-9]{4}|^panic:' "$DIR/logs/$1.log" | tail -n 1 | cut -c1-160 || true; }

# wait_up <name> <settle seconds>: wait until <name> runs; give up after 3 tries.
# Retrying matters for the filer: on a fresh cluster each filer bootstraps its metadata from
# the other filers, and all 3 PCs start their filers at the same moment, so one can ask a peer
# that is not serving yet and exit with "bootstrap from ...: traverse bfs metadata: rpc".
# The supervisor restarts it; this only waits for that and reports.
wait_up() {
  local name="$1" settle="$2" try
  for try in 1 2 3; do
    sleep "$settle"
    weed_alive "$name" && return 0
    echo "$name exited during startup (attempt $try of 3): $(last_error "$name")"
    sleep 2
  done
  echo "ERROR: $name does not stay up. Last lines of $DIR/logs/$name.log:"
  grep -v '^\s' "$DIR/logs/$name.log" | tail -n 6 || true
  exit 1
}

# Every weed process for this data folder, and every supervisor, has "$DIR/" and "weed" in
# its command line. Matching on that also finds processes whose pid file is gone or wrong,
# e.g. left over from an earlier run.
strays() { pgrep -f -- "weed .*${DIR}/|${SUP_TAG}.* ${DIR} " || true; }

stop_all() {
  # Supervisors first, or they would restart what we stop.
  for name in s3 filer volume master; do
    local f="$DIR/pids/$name.pid"
    if [ -f "$f" ]; then
      kill "$(cat "$f")" 2>/dev/null && echo "stopped $name" || echo "$name was not running"
      rm -f "$f" "$DIR/pids/$name.weed.pid"
    fi
  done
  sleep 0.5
  local left
  left=$(strays)
  if [ -n "$left" ]; then
    echo "stopping leftover SeaweedFS processes not in the pid files: $(echo $left)"
    kill $left 2>/dev/null || true
    for _ in 1 2 3 4 5 6; do sleep 0.5; [ -z "$(strays)" ] && break; done
    left=$(strays)
    [ -n "$left" ] && { kill -9 $left 2>/dev/null || true; echo "killed (kill -9): $(echo $left)"; }
  fi
  return 0
}

ports_free() {  # fails, naming the holder, if a SeaweedFS port is still taken
  local busy
  busy=$(ss -ltnpH 2>/dev/null | awk '{print $4, $6}' | grep -E ':(9333|19333|8080|18080|8888|18888|8333) ' || true)
  if [ -n "$busy" ]; then
    echo "ERROR: SeaweedFS ports are still in use on this PC:"; echo "$busy"; exit 1
  fi
}

case "${1:-}" in
  start | rejoin)   # "rejoin" (after a crash of this PC) is the same as start: keep the store
    [ $# -ge 3 ] || usage
    ME="$2"; ALL="$3"
    [ -x "$WEED" ] || { echo "weed binary not found at $WEED (set WEED_BIN)"; exit 1; }
    case ",$ALL," in *",$ME,"*) ;; *) echo "this PC's ip $ME is not in the list $ALL"; exit 1;; esac
    IFS=, read -r -a IPS <<<"$ALL"
    [ ${#IPS[@]} -eq 3 ] || { echo "expected 3 ips, got ${#IPS[@]}"; exit 1; }

    MASTERS=$(printf '%s:9333,' "${IPS[@]}"); MASTERS=${MASTERS%,}
    FILERS="$ME:8888"
    for ip in "${IPS[@]}"; do [ "$ip" = "$ME" ] || FILERS="$FILERS,$ip:8888"; done

    # A second copy cannot bind the ports: it would die at once and overwrite the pid files,
    # leaving the old copy running with no pid file. So clear out anything already running.
    if [ -n "$(strays)" ]; then
      echo "SeaweedFS is already running on this PC: stopping it first"
      stop_all
      sleep 1
    fi
    ports_free

    mkdir -p "$DIR"/{master,volume,filer,logs,pids}
    # -electionTimeout=3s (default 10s): when the PC of the Raft leader dies, every write waits
    # for a new leader (then for the volume servers to re-register with it). On one lab LAN 3 s
    # is still far above any heartbeat delay, and it cuts that pause by ~7 s.
    run master master -ip="$ME" -port=9333 -mdir="$DIR/master" -peers="$MASTERS" \
      -defaultReplication=001 -volumeSizeLimitMB=256 -electionTimeout=3s
    echo "waiting 8 s for the masters to elect a leader (start all 3 PCs within this time)"
    wait_up master 8
    run volume volume -ip="$ME" -port=8080 -dir="$DIR/volume" -max=50 -master="$MASTERS"
    wait_up volume 3
    run filer filer -ip="$ME" -port=8888 -master="$MASTERS" -filerGroup=rescuemesh \
      -defaultStoreDir="$DIR/filer" -localSocket="$DIR/filer.sock"
    wait_up filer 4
    run s3 s3 -ip.bind="$S3_BIND" -ip="$ME" -port=8333 -filer="$FILERS" \
      -port.iceberg=0 -port.lance=0 -localSocket="$DIR/s3.sock"
    wait_up s3 4
    echo "S3 gateway filers: $FILERS"
    echo "check: curl -s -o /dev/null -w '%{http_code}\n' http://$ME:8333/   (want 200)"
    ;;
  status)
    for name in master volume filer s3; do
      if weed_alive "$name"; then
        echo "$name: running"
      elif [ -f "$DIR/pids/$name.pid" ] && kill -0 "$(cat "$DIR/pids/$name.pid")" 2>/dev/null; then
        echo "$name: NOT running (its supervisor is restarting it; last error: $(last_error "$name"))"
      else
        echo "$name: NOT running"
      fi
    done
    restarts=$(cat "$DIR"/logs/*.log 2>/dev/null | grep -c '^\[supervisor\]' || true)
    [ "${restarts:-0}" != 0 ] && echo "note: the supervisors restarted a crashed process $restarts time(s); see [supervisor] lines in $DIR/logs"
    tracked=$(cat "$DIR"/pids/*.pid 2>/dev/null | tr '\n' ' ')
    for p in $(strays); do
      case " $tracked " in *" $p "*) ;; *) echo "WARNING: leftover SeaweedFS process $p has no pid file (Stop or Wipe clears it)";; esac
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
