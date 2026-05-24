param(
    [string]$Runner = "C:\work\swblock.exe",
    [string]$ResultsDir = "results\phase28-productized-ops-close",
    [string[]]$EnvOverride = @()
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Runner)) {
    throw "Runner not found: $Runner"
}

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path

$gates = @(
    [pscustomobject]@{
        gate = "G1"
        name = "helm-first-volume-via-sw-block-cli"
        scenario = "testops\scenarios\helm-first-volume-via-sw-block-cli-chain.yaml"
    },
    [pscustomobject]@{
        gate = "G2"
        name = "helm-multi-volume-day1"
        scenario = "testops\scenarios\helm-multi-volume-day1-chain.yaml"
    },
    [pscustomobject]@{
        gate = "G3"
        name = "helm-support-bundle-diagnostics"
        scenario = "testops\scenarios\helm-support-bundle-diagnostics-chain.yaml"
    },
    [pscustomobject]@{
        gate = "G5"
        name = "cleanup-residue"
        scenario = "testops\scenarios\cleanup-residue-chain.yaml"
    }
)

foreach ($gate in $gates) {
    $scenarioPath = Join-Path $repoRoot $gate.scenario
    if (-not (Test-Path -LiteralPath $scenarioPath)) {
        throw "Scenario not found: $scenarioPath"
    }
}

New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null
$resolvedResults = (Resolve-Path -LiteralPath $ResultsDir).Path
$summaryTxt = Join-Path $resolvedResults "phase28-productized-ops-close-summary.txt"
$summaryJson = Join-Path $resolvedResults "phase28-productized-ops-close-summary.json"

$records = @()
$passRuns = 0
$failRuns = 0
$startedAt = (Get-Date).ToUniversalTime().ToString("o")

foreach ($gate in $gates) {
    $gateDir = Join-Path $resolvedResults $gate.gate
    New-Item -ItemType Directory -Force -Path $gateDir | Out-Null

    $args = @("run", "-results-dir", $gateDir)
    foreach ($e in $EnvOverride) {
        $args += @("-env", $e)
    }
    $args += (Join-Path $repoRoot $gate.scenario)

    $started = Get-Date
    & $Runner @args
    $exitCode = $LASTEXITCODE
    $ended = Get-Date

    $runId = ""
    $statusFile = Get-ChildItem -Path $gateDir -Recurse -Filter "status.json" -ErrorAction SilentlyContinue |
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
        gate = $gate.gate
        name = $gate.name
        scenario = $gate.scenario
        result = $result
        exit_code = $exitCode
        run_id = $runId
        results_dir = (Resolve-Path -LiteralPath $gateDir).Path
        duration_seconds = [math]::Round(($ended - $started).TotalSeconds, 3)
    }
}

$operatorSnapshotStatus = "not_checked"
$operatorSnapshotPath = ""
$operatorSnapshotReason = ""

$snapshotFile = Get-ChildItem -Path (Join-Path $resolvedResults "G1") -Recurse -Filter "operator-snapshot.json" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if (-not $snapshotFile) {
    $operatorSnapshotStatus = "failed"
    $operatorSnapshotReason = "operator-snapshot.json not found in G1 artifacts"
    $failRuns++
} else {
    $operatorSnapshotPath = $snapshotFile.FullName
    try {
        $snapshot = Get-Content -LiteralPath $snapshotFile.FullName -Raw | ConvertFrom-Json
        if ($snapshot.api_version -ne "block.seaweedfs.com/v1alpha1") {
            throw "api_version=$($snapshot.api_version)"
        }
        if ($snapshot.kind -ne "ReadOnlyOperatorFoundationSnapshot") {
            throw "kind=$($snapshot.kind)"
        }
        if ($snapshot.read_only -ne $true) {
            throw "read_only=$($snapshot.read_only)"
        }
        if ($snapshot.mutation.mutation_allowed -ne $false) {
            throw "mutation_allowed=$($snapshot.mutation.mutation_allowed)"
        }
        if ($snapshot.crd_contract.group -ne "block.seaweedfs.com") {
            throw "crd_contract.group=$($snapshot.crd_contract.group)"
        }
        $badAction = $false
        foreach ($volume in @($snapshot.volumes)) {
            foreach ($action in @($volume.allowed_actions)) {
                if ($action.mutation_allowed -eq $true) {
                    $badAction = $true
                }
                if (($action.mode -ne "read_only") -and ($action.mode -ne "dry_run")) {
                    $badAction = $true
                }
            }
        }
        if ($badAction) {
            throw "operator snapshot exposes mutating or unsupported action mode"
        }
        $operatorSnapshotStatus = "PASS"
    } catch {
        $operatorSnapshotStatus = "failed"
        $operatorSnapshotReason = [string]$_.Exception.Message
        $failRuns++
    }
}

$finishedAt = (Get-Date).ToUniversalTime().ToString("o")
$closeStatus = if ($failRuns -eq 0 -and $operatorSnapshotStatus -eq "PASS") { "ok" } else { "failed" }

@(
    "phase28_productized_ops_close_status=$closeStatus"
    "started_at=$startedAt"
    "finished_at=$finishedAt"
    "scenario_runs=$($records.Count)"
    "pass_runs=$passRuns"
    "fail_runs=$failRuns"
    "operator_snapshot_status=$operatorSnapshotStatus"
    "operator_snapshot_path=$operatorSnapshotPath"
    "operator_snapshot_reason=$operatorSnapshotReason"
) + ($records | ForEach-Object {
    "gate=$($_.gate) result=$($_.result) exit_code=$($_.exit_code) run_id=$($_.run_id) duration_seconds=$($_.duration_seconds) scenario=$($_.scenario)"
}) | Set-Content -LiteralPath $summaryTxt -Encoding UTF8

[pscustomobject]@{
    phase28_productized_ops_close_status = $closeStatus
    started_at = $startedAt
    finished_at = $finishedAt
    pass_runs = $passRuns
    fail_runs = $failRuns
    operator_snapshot_status = $operatorSnapshotStatus
    operator_snapshot_path = $operatorSnapshotPath
    operator_snapshot_reason = $operatorSnapshotReason
    gates = $records
} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $summaryJson -Encoding UTF8

Get-Content -LiteralPath $summaryTxt

if ($closeStatus -ne "ok") {
    exit 1
}
