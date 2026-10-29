#!/usr/bin/env bash
# Stops, starts, pauses or resumes one process of the local test cluster by name.
# Spike only. The fault checks (run_fault_checks.py) call this through commands in the config,
# so a config for the real machines can put its own commands (for example ssh) in their place.
#
# Usage: WEED_BIN=... RUN_DIR=... ./proc_ctl.sh <stop|start|pause|resume> <name>
#   names: master0-2, volume0-2, filer0-2, s3gateway0-2 (as started by start_local_cluster.sh)
set -euo pipefail

: "${RUN_DIR:?set RUN_DIR}"
action="$1"; name="$2"
pidfile="$RUN_DIR/pids/$name.pid"

# The pid file holds the shell's pid. On Windows (Git Bash) the real Windows process has a
# different id (WINPID in `ps`), which pause/resume need. UNVERIFIED on Linux: there `kill -STOP`
# on the shell's pid is the same process.
windows_pid() { ps -p "$1" 2>/dev/null | awk 'NR==2 {print $4}'; }

case "$action" in
  stop)
    pid=$(cat "$pidfile")
    # `kill` reaches weed in Git Bash (taskkill with the shell pid does not).
    kill "$pid" 2>/dev/null || true
    # Wait until it is really gone, so a following "start" does not hit a busy port.
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    ;;
  start)
    # Same arguments as the original start; append to the same log.
    eval "\"${WEED_BIN:?set WEED_BIN}\" $(cat "$RUN_DIR/args/$name.args") >>\"$RUN_DIR/logs/$name.log\" 2>&1 &"
    echo $! >"$pidfile"
    ;;
  pause|resume)
    pid=$(cat "$pidfile")
    if command -v powershell.exe >/dev/null 2>&1; then
      # Windows has no SIGSTOP; NtSuspendProcess / NtResumeProcess do the same job.
      native=$(windows_pid "$pid")
      fn=NtSuspendProcess; [ "$action" = resume ] && fn=NtResumeProcess
      powershell.exe -NoProfile -Command "
        Add-Type -Namespace Native -Name Nt -MemberDefinition '[DllImport(\"ntdll.dll\")] public static extern uint $fn(IntPtr handle);';
        \$p = [System.Diagnostics.Process]::GetProcessById($native);
        [Native.Nt]::$fn(\$p.Handle) | Out-Null"
    else
      sig=STOP; [ "$action" = resume ] && sig=CONT
      kill -$sig "$pid"
    fi
    ;;
  *) echo "unknown action $action" >&2; exit 2 ;;
esac
