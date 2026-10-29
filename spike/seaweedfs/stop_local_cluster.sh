#!/usr/bin/env bash
# Stops everything start_local_cluster.sh started (reads the pid files in $RUN_DIR/pids).
set -u
: "${RUN_DIR:?set RUN_DIR to the directory used by start_local_cluster.sh}"
for pidfile in "$RUN_DIR"/pids/*.pid; do
  [ -e "$pidfile" ] || continue
  pid=$(cat "$pidfile")
  # taskkill is for Git Bash on Windows, where `kill` cannot stop a native Windows process.
  kill "$pid" 2>/dev/null || taskkill //PID "$pid" //F >/dev/null 2>&1 || true
  rm -f "$pidfile"
done
echo "stopped"
