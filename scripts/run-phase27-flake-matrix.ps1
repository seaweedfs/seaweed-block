param(
    [string]$Runner = "C:\work\swblock.exe",
    [string]$Scenario = "testops\scenarios\helm-multi-volume-rf3-interleaved-failover-chain.yaml",
    [string]$ResultsDir = "results\phase27-flake-matrix",
    [int]$Iterations = 5,
    [string[]]$EnvOverride = @()
)

$ErrorActionPreference = "Stop"

if ($Iterations -lt 1) {
    throw "Iterations must be >= 1"
}
if (-not (Test-Path -LiteralPath $Runner)) {
    throw "Runner not found: $Runner"
}
if (-not (Test-Path -LiteralPath $Scenario)) {
    throw "Scenario not found: $Scenario"
}

New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null

$summaryTxt = Join-Path $ResultsDir "flake-summary.txt"
$summaryJson = Join-Path $ResultsDir "flake-summary.json"
$iterationsDir = Join-Path $ResultsDir "iterations"
New-Item -ItemType Directory -Force -Path $iterationsDir | Out-Null

$records = @()
$passRuns = 0
$failRuns = 0
$startedAt = (Get-Date).ToUniversalTime().ToString("o")

for ($i = 1; $i -le $Iterations; $i++) {
    $iterDir = Join-Path $iterationsDir ("iteration-{0:D2}" -f $i)
    New-Item -ItemType Directory -Force -Path $iterDir | Out-Null

    $args = @("run", "-results-dir", $iterDir)
    foreach ($e in $EnvOverride) {
        $args += @("-env", $e)
    }
    $args += $Scenario

    $started = Get-Date
    & $Runner @args
    $exitCode = $LASTEXITCODE
    $ended = Get-Date

    $runId = ""
    $statusFile = Get-ChildItem -Path $iterDir -Recurse -Filter "status.json" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($statusFile) {
        try {
            $status = Get-Content -LiteralPath $statusFile.FullName -Raw | ConvertFrom-Json
            if ($status.run_id) {
                $runId = [string]$status.run_id
            } elseif ($status.runId) {
                $runId = [string]$status.runId
            } else {
                $runId = Split-Path -Leaf (Split-Path -Parent $statusFile.FullName)
            }
        } catch {
            $runId = Split-Path -Leaf (Split-Path -Parent $statusFile.FullName)
        }
    }

    $result = if ($exitCode -eq 0) { "PASS" } else { "FAIL" }
    if ($result -eq "PASS") {
        $passRuns++
    } else {
        $failRuns++
    }

    $records += [pscustomobject]@{
        iteration = $i
        result = $result
        exit_code = $exitCode
        run_id = $runId
        results_dir = (Resolve-Path -LiteralPath $iterDir).Path
        duration_seconds = [math]::Round(($ended - $started).TotalSeconds, 3)
    }
}

$flakeRate = [math]::Round(($failRuns / [double]$Iterations) * 100.0, 3)
$finishedAt = (Get-Date).ToUniversalTime().ToString("o")

@(
    "phase27_flake_matrix_status=$(if ($failRuns -eq 0) { 'ok' } else { 'failed' })"
    "scenario=$Scenario"
    "started_at=$startedAt"
    "finished_at=$finishedAt"
    "target_runs=$Iterations"
    "pass_runs=$passRuns"
    "fail_runs=$failRuns"
    "flake_rate_percent=$flakeRate"
) + ($records | ForEach-Object {
    "iteration=$($_.iteration) result=$($_.result) exit_code=$($_.exit_code) run_id=$($_.run_id) duration_seconds=$($_.duration_seconds)"
}) | Set-Content -LiteralPath $summaryTxt -Encoding UTF8

[pscustomobject]@{
    phase27_flake_matrix_status = if ($failRuns -eq 0) { "ok" } else { "failed" }
    scenario = $Scenario
    started_at = $startedAt
    finished_at = $finishedAt
    target_runs = $Iterations
    pass_runs = $passRuns
    fail_runs = $failRuns
    flake_rate_percent = $flakeRate
    iterations = $records
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $summaryJson -Encoding UTF8

Get-Content -LiteralPath $summaryTxt

if ($failRuns -ne 0) {
    exit 1
}
