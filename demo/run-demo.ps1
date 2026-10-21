# RescueMesh P1 demo: 3 nodes, real requests, load, fault injection, crash, fault log.
#
# Run from the repo root in PowerShell:
#   powershell -ExecutionPolicy Bypass -File demo\run-demo.ps1
# Each node opens in its own window; press Enter here to move to the next step.
#
#   -Auto   no windows and no pauses: nodes log to logs\demo\, printed at the end
#           (used to check that the demo works before showing it).

param([switch]$Auto)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:Path = "$env:USERPROFILE\.cargo\bin;$env:Path"
$bin = Join-Path $root "target\release"
$logDir = Join-Path $root "logs\demo"
$resultsDir = Join-Path $logDir "results"
# One name for this run: written in every fault-log row and every loadgen file.
$runId = "demo-$(Get-Date -Format yyyyMMdd-HHmmss)"
# Node 1 also prints one line per request it applies ("submit applied").
$node1Log = "info,node::service=debug"
$nodes = @{}

function Say($text) { Write-Host ""; Write-Host $text -ForegroundColor Cyan }
function Note($text) { Write-Host "  $text" -ForegroundColor Gray }
function NowUs { [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000 }

function Pause-Step($seconds) {
    if ($Auto) { Start-Sleep -Seconds $seconds }
    else { Read-Host "  [Enter] to continue" | Out-Null }
}

function Start-Node($id) {
    $level = if ($id -eq 1) { $node1Log } else { "info" }
    if ($Auto) {
        $env:RUST_LOG = $level # inherited by the process started next
        $nodes[$id] = Start-Process -FilePath "$bin\node.exe" -ArgumentList "--id", $id, "--run-id", $runId `
            -RedirectStandardOutput "$logDir\node$id-$(Get-Random).log" `
            -RedirectStandardError "$logDir\node$id-$(Get-Random).err" -PassThru -WindowStyle Hidden
        Remove-Item Env:RUST_LOG
    } else {
        $cmd = "`$host.UI.RawUI.WindowTitle = 'RescueMesh node $id'; `$env:RUST_LOG = '$level'; " +
               "& '$bin\node.exe' --id $id --run-id $runId"
        $nodes[$id] = Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd -PassThru
    }
}

function Stop-Node($id) {
    # In window mode, stop the node.exe inside the window, not the window itself.
    if (-not $Auto) {
        Get-CimInstance Win32_Process -Filter "ParentProcessId = $($nodes[$id].Id) AND Name = 'node.exe'" |
            ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
    } else {
        Stop-Process -Id $nodes[$id].Id -Force
    }
}

# Reads loadgen's CSV and prints the counts and the latency percentiles.
function Show-LoadSummary($name) {
    $rows = Import-Csv (Join-Path $resultsDir "$name.csv")
    $ok = @($rows | Where-Object { $_.status -eq "ok" })
    $lat = @($ok | ForEach-Object { [double]$_.latency_us / 1000 } | Sort-Object)
    $pct = { param($p) $lat[[math]::Min($lat.Count - 1, [math]::Floor($p * $lat.Count))] }
    $executors = ($ok | Group-Object executor_node | ForEach-Object { "node $($_.Name): $($_.Count)" }) -join ", "
    Note ("{0} requests: {1} ok, {2} not ok   (executor: {3})" -f $rows.Count, $ok.Count, ($rows.Count - $ok.Count), $executors)
    if ($lat.Count -gt 0) {
        Note ("latency: p50 {0:N2} ms, p99 {1:N2} ms, max {2:N2} ms" -f (& $pct 0.50), (& $pct 0.99), $lat[-1])
    }
}

# ---- 0. Build ----------------------------------------------------------------
Say "Step 0: building everything (release, so the timings are real)"
cargo build -q --release -p node -p injector -p client -p loadgen
if ($LASTEXITCODE -ne 0) { throw "build failed" }
New-Item -ItemType Directory -Force $logDir, $resultsDir | Out-Null
# Auto mode prints logs\demo\*.log at the end: start from an empty folder.
if ($Auto) { Remove-Item "$logDir\*.log", "$logDir\*.err" -ErrorAction SilentlyContinue }
Note "Run name: $runId"

try {
    # ---- 1. Start the cluster ------------------------------------------------
    Say "Step 1: starting 3 nodes (ports 50051-50053, config/nodes.toml)"
    Note "Each node prints 'node listening ... peers=[...]'. No master: all three are equal."
    Note "Every node sends a heartbeat to each peer every 500 ms. A healthy cluster prints"
    Note "nothing more: no news is good news."
    1..3 | ForEach-Object { Start-Node $_ }
    Pause-Step 3

    # ---- 2. One real request ---------------------------------------------------
    Say "Step 2: a client sends 5 emergencies to node 1"
    Note "Node 1 checks each one, takes a snapshot of the free resources, runs the planner,"
    Note "stores the plan and replies. Node 1's window prints 'submit applied assigned=1'."
    Note "Then the client resends the first request with the SAME ID: the stored plan comes"
    Note "back (duplicate=true) and nothing is planned or stored twice."
    & "$bin\client.exe" --count 5 --seed 100
    Pause-Step 1

    # ---- 3. Load -----------------------------------------------------------------
    Say "Step 3: load test - 10 clients send 1000 requests to node 1 as fast as they can"
    Note "Only 10 resources exist and none are released in Step A: the first ones get a"
    Note "resource, the rest get an empty plan ('nothing free'). No resource is ever given"
    Note "twice: a request that loses a race for a resource is planned again."
    & "$bin\loadgen.exe" --requests 1000 --clients 10 --out $resultsDir --run-id "$runId-load" | Select-Object -First 2
    Show-LoadSummary "$runId-load"
    Pause-Step 1

    # ---- 4. A fault while serving ----------------------------------------------
    Say "Step 4: node 2 goes silent for 6 s WHILE node 1 serves 500 requests (100 per second)"
    Note "Node 2 confirms, then stops answering and stops sending. Nobody tells the others:"
    Note "nodes 1 and 3 print 'WARN peer suspected down peer=2' about 1.5 s later, on their own."
    Note "Node 1 keeps answering clients the whole time."
    & "$bin\inject.exe" --node 2 --duration 6s
    & "$bin\loadgen.exe" --requests 500 --clients 10 --rate 100 --out $resultsDir --run-id "$runId-fault" | Select-Object -First 2
    Show-LoadSummary "$runId-fault"
    Note "When the 6 s are over node 2 comes back by itself: 'INFO peer alive again peer=2'."
    if ($Auto) { Start-Sleep -Seconds 3 } else { Pause-Step 0 }

    # ---- 5. A real crash -----------------------------------------------------
    Say "Step 5: a real crash - killing node 3's process"
    Note "Watch: nodes 1 and 2 print 'peer suspected down peer=3' after about 1.5 s."
    $killUs = NowUs
    Stop-Node 3
    Pause-Step 3
    Say "Step 5b: restarting node 3"
    Note "Its own heartbeats announce it: nodes 1 and 2 print 'peer alive again peer=3'"
    Note "within a fraction of a second."
    Start-Node 3
    Pause-Step 2

    # ---- 6. Fault log ------------------------------------------------------------
    Say "Step 6: the fault log - every node writes logs\faults-node<N>.csv (P5's format)"
    $rows = 1..3 | ForEach-Object { Import-Csv "logs\faults-node$_.csv" } |
        Where-Object { $_.run_id -eq $runId } | Sort-Object { [int64]$_.wall_ts_us }
    $t0 = [int64]$rows[0].wall_ts_us
    foreach ($r in $rows) {
        $what = if ($r.peer_id) { "peer $($r.peer_id)" } elseif ($r.duration_ms) { "$($r.duration_ms) ms" } else { "" }
        Note ("+{0,6:N2} s   node {1}  {2,-10}  {3}" -f (([int64]$r.wall_ts_us - $t0) / 1e6), $r.node_id, $r.event, $what)
    }
    # Detection time = from the fault (mute start / process killed) to each peer's PEER_DOWN.
    $muteUs = [int64]($rows | Where-Object { $_.event -eq "MUTE_START" } | Select-Object -First 1).wall_ts_us
    foreach ($case in @(@{ peer = "2"; from = $muteUs; name = "mute of node 2" },
                        @{ peer = "3"; from = $killUs; name = "crash of node 3" })) {
        $times = $rows | Where-Object { $_.event -eq "PEER_DOWN" -and $_.peer_id -eq $case.peer -and [int64]$_.wall_ts_us -gt $case.from } |
            ForEach-Object { "node $($_.node_id) after {0:N2} s" -f (([int64]$_.wall_ts_us - $case.from) / 1e6) }
        Note ("Detection, {0}: {1}" -f $case.name, ($times -join ", "))
    }
    Pause-Step 1

    # ---- 7. Evidence -----------------------------------------------------------
    Say "Step 7: the automated tests (Submit, mute, failure detection, broadcast, planner, ...)"
    cargo test -q --workspace 2>&1 | Select-String "test result: .* [1-9]\d* passed"
} finally {
    Say "Stopping all nodes"
    foreach ($id in @($nodes.Keys)) {
        try { Stop-Node $id } catch {}
        if (-not $Auto) { try { Stop-Process -Id $nodes[$id].Id -Force } catch {} }
    }
}

if ($Auto) {
    Say "Node logs (Auto mode; node 1's 'submit applied' lines left out)"
    Get-ChildItem $logDir -Filter *.log | Sort-Object LastWriteTime | ForEach-Object {
        Write-Host "--- $($_.Name)" -ForegroundColor Yellow
        Get-Content $_.FullName | Where-Object { $_ -notmatch "submit applied" } |
            ForEach-Object { $_ -replace '^\d{4}-\d\d-\d\dT', '' }
    }
}
