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
# Temporary (demo branch, agreed with P4): an assigned resource is busy for this long,
# then free again. Scaled-down time: 30 ms stands in for a real mission.
$missionMs = 30
$statsFile = Join-Path $root "logs\submit-stats-node1.txt"
$nodes = @{}

function Say($text) { Write-Host ""; Write-Host $text -ForegroundColor Cyan }
function Note($text) { Write-Host "  $text" -ForegroundColor Gray }
function NowUs { [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000 }

function Pause-Step($seconds) {
    if ($Auto) { Start-Sleep -Seconds $seconds }
    else { Read-Host "  [Enter] to continue" | Out-Null }
}

function Start-Node($id) {
    if ($Auto) {
        $nodes[$id] = Start-Process -FilePath "$bin\node.exe" `
            -ArgumentList "--id", $id, "--run-id", $runId, "--mission-ms", $missionMs `
            -RedirectStandardOutput "$logDir\node$id-$(Get-Random).log" `
            -RedirectStandardError "$logDir\node$id-$(Get-Random).err" -PassThru -WindowStyle Hidden
    } else {
        $cmd = "`$host.UI.RawUI.WindowTitle = 'RescueMesh node $id'; " +
               "& '$bin\node.exe' --id $id --run-id $runId --mission-ms $missionMs"
        $nodes[$id] = Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd -PassThru
    }
}

# Node 1's counters (logs\submit-stats-node1.txt, rewritten every second), as a hashtable.
function Read-Stats {
    $s = @{}
    if (Test-Path $statsFile) {
        Get-Content $statsFile | ForEach-Object { $k, $v = $_ -split "=", 2; $s[$k] = [int64]$v }
    }
    $s
}

# Runs loadgen, then prints one readable block: requests, resources, races, latency.
function Run-Load($name, $requests, $rate) {
    $before = Read-Stats
    & "$bin\loadgen.exe" --requests $requests --clients 10 --rate $rate --out $resultsDir --run-id $name |
        Where-Object { $_ -notmatch "^wrote" }
    Start-Sleep -Milliseconds 1500 # node 1 rewrites its stats file once a second
    $after = Read-Stats
    $d = @{}
    foreach ($k in "assigned", "none_free", "repeats", "replans", "aborted") { $d[$k] = [int64]$after[$k] - [int64]$before[$k] }

    $rows = Import-Csv (Join-Path $resultsDir "$name.csv")
    $ok = @($rows | Where-Object { $_.status -eq "ok" })
    $lat = @($ok | ForEach-Object { [double]$_.latency_us / 1000 } | Sort-Object)
    $pct = { param($p) $lat[[math]::Min($lat.Count - 1, [math]::Floor($p * $lat.Count))] }
    $executors = ($ok | Group-Object executor_node | ForEach-Object { "node $($_.Name)" }) -join ", "

    Write-Host ""
    Write-Host ("  Requests   {0} sent, {1} answered (executed by {2}), {3} failed" -f $rows.Count, $ok.Count, $executors, ($rows.Count - $ok.Count)) -ForegroundColor White
    Write-Host ("  Resources  {0} got one, {1} found none free at that moment" -f $d.assigned, $d.none_free) -ForegroundColor Green
    Write-Host ("  Races      {0} times a request lost a resource to another and planned again;" -f $d.replans) -ForegroundColor Yellow
    Write-Host ("             {0} gave up after 3 tries (the client may retry); never double-booked" -f $d.aborted) -ForegroundColor Yellow
    if ($lat.Count -gt 0) {
        Write-Host ("  Latency    typical {0:N1} ms, 99% under {1:N1} ms, slowest {2:N1} ms" -f (& $pct 0.50), (& $pct 0.99), $lat[-1]) -ForegroundColor White
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

# ---- 0. Build ----------------------------------------------------------------
Say "Step 0: building everything (release, so the timings are real)"
cargo build -q --release -p node -p injector -p client -p loadgen
if ($LASTEXITCODE -ne 0) { throw "build failed" }
New-Item -ItemType Directory -Force $logDir, $resultsDir | Out-Null
# Auto mode prints logs\demo\*.log at the end: start from an empty folder.
if ($Auto) { Remove-Item "$logDir\*.log", "$logDir\*.err" -ErrorAction SilentlyContinue }
Remove-Item $statsFile -ErrorAction SilentlyContinue # node 1's counters start from zero
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
    Note "stores the plan and replies. Then the client resends the first request with the"
    Note "SAME ID: the stored plan comes back (duplicate=true), nothing is planned twice."
    Note "Node 1's window prints a one-line summary each second while requests arrive."
    & "$bin\client.exe" --count 5 --seed 100
    Pause-Step 2

    # ---- 3. Load -----------------------------------------------------------------
    Say "Step 3a: normal load - 10 clients send 1000 requests to node 1, 200 per second"
    Note "10 resources (ambulances, fire trucks, ...). Each assignment is a mission: the"
    Note "resource is busy for $missionMs ms (scaled-down time), then free again. Many requests"
    Note "arrive at the same moment and want the same best resource: the store re-checks"
    Note "every plan when saving, so the loser plans again on fresh data."
    Run-Load "$runId-normal" 1000 200
    Pause-Step 1

    Say "Step 3b: overload - the same 1000 requests at 600 per second"
    Note "10 resources x one $missionMs ms mission each can serve about 330 per second. Above"
    Note "that some requests find nothing free, and races get more frequent."
    Run-Load "$runId-overload" 1000 600
    Pause-Step 1

    # ---- 4. A fault while serving ----------------------------------------------
    Say "Step 4: node 2 goes silent for 6 s WHILE node 1 serves 500 requests (100 per second)"
    Note "Node 2 confirms, then stops answering and stops sending. Nobody tells the others:"
    Note "nodes 1 and 3 print 'WARN peer suspected down peer=2' about 1.5 s later, on their own."
    Note "Node 1 keeps answering clients the whole time."
    & "$bin\inject.exe" --node 2 --duration 6s
    Run-Load "$runId-fault" 500 100
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
    Say "Node logs (Auto mode)"
    Get-ChildItem $logDir -Filter *.log | Sort-Object LastWriteTime | ForEach-Object {
        Write-Host "--- $($_.Name)" -ForegroundColor Yellow
        Get-Content $_.FullName |
            ForEach-Object { $_ -replace '^\d{4}-\d\d-\d\dT', '' }
    }
}
