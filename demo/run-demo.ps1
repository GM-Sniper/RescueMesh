# RescueMesh P1 demo: 3 nodes, heartbeats, fault injection, crash detection.
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
$bin = Join-Path $root "target\debug"
$logDir = Join-Path $root "logs\demo"
$nodes = @{}

function Say($text) { Write-Host ""; Write-Host $text -ForegroundColor Cyan }
function Note($text) { Write-Host "  $text" -ForegroundColor Gray }

function Pause-Step($seconds) {
    if ($Auto) { Start-Sleep -Seconds $seconds }
    else { Read-Host "  [Enter] to continue" | Out-Null }
}

function Start-Node($id) {
    if ($Auto) {
        $nodes[$id] = Start-Process -FilePath "$bin\node.exe" -ArgumentList "--id", $id `
            -RedirectStandardOutput "$logDir\node$id-$(Get-Random).log" `
            -RedirectStandardError "$logDir\node$id-$(Get-Random).err" -PassThru -WindowStyle Hidden
    } else {
        $cmd = "`$host.UI.RawUI.WindowTitle = 'RescueMesh node $id'; & '$bin\node.exe' --id $id"
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

# ---- 0. Build ----------------------------------------------------------------
Say "Step 0: building the node and the fault injector"
cargo build -q -p node -p injector
if ($LASTEXITCODE -ne 0) { throw "build failed" }
New-Item -ItemType Directory -Force $logDir | Out-Null

try {
    # ---- 1. Start the cluster ------------------------------------------------
    Say "Step 1: starting 3 nodes (ports 50051-50053, config/nodes.toml)"
    Note "Each node prints 'node listening ... peers=[...]'."
    Note "Every node sends a heartbeat to each peer every 500 ms. A healthy cluster prints"
    Note "nothing more: no news is good news (RUST_LOG=debug would show every heartbeat)."
    1..3 | ForEach-Object { Start-Node $_ }
    Pause-Step 3

    # ---- 2. Fault injection --------------------------------------------------
    Say "Step 2: fault injection - P5's injector tells node 2 to go silent for 8 s"
    Note "Node 2 replies 'accepted', then stops answering AND stops sending, like a"
    Note "machine cut off from the network. Nobody tells nodes 1 and 3: they must notice."
    Note "Watch: nodes 1 and 3 print 'WARN peer suspected down peer=2' about 1.5 s later,"
    Note "then 'INFO peer alive again peer=2' when the 8 s are over."
    & "$bin\inject.exe" --node 2 --duration 8s
    if ($Auto) { Start-Sleep -Seconds 11 } else { Pause-Step 0 }

    # ---- 3. A real crash -----------------------------------------------------
    Say "Step 3: a real crash - killing node 3's process"
    Note "Watch: nodes 1 and 2 print 'peer suspected down peer=3' after about 1.5 s."
    Stop-Node 3
    Pause-Step 3
    Say "Step 3b: restarting node 3"
    Note "Its own heartbeats announce it: nodes 1 and 2 print 'peer alive again peer=3'"
    Note "within a fraction of a second."
    Start-Node 3
    Pause-Step 2

    # ---- 4. Evidence -----------------------------------------------------------
    Say "Step 4: the automated tests (mute, failure detection, broadcast, config, ...)"
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
        Get-Content $_.FullName | ForEach-Object { $_ -replace '^\d{4}-\d\d-\d\dT', '' }
    }
}
