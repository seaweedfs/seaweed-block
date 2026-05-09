param(
    [string]$ProductRepoRoot = (Get-Location).Path,
    [string]$RunnerRoot = "C:\work\seaweedfs\learn\sw-test-runner-standalone",
    [string]$RemoteProductRoot = "/tmp/seaweed-block-nvme-p4l",
    [string]$SshKey = "C:\work\dev_server\testdev_key",
    [string]$ArtifactRoot = "",
    [string]$RunId = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = "protocol-release-gate-" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
}
if ([string]::IsNullOrWhiteSpace($ArtifactRoot)) {
    $ArtifactRoot = Join-Path $ProductRepoRoot "results\$RunId"
}

New-Item -ItemType Directory -Force -Path $ArtifactRoot | Out-Null
$SuiteLog = Join-Path $ArtifactRoot "suite.log"
$ProductCommit = "unknown"
$RunnerCommit = "unknown"
try { $ProductCommit = (& git -C $ProductRepoRoot rev-parse HEAD).Trim() } catch {}
try {
    if (Test-Path $RunnerRoot) {
        $RunnerCommit = (& git -C $RunnerRoot rev-parse HEAD).Trim()
    }
} catch {}

function Write-SuiteLog {
    param([string]$Message)
    $line = "[protocol-gate] $Message"
    Write-Host $line
    Add-Content -Path $SuiteLog -Value $line
}

function Write-SuiteResult {
    param(
        [string]$Status,
        [string]$Summary,
        [string]$CurrentPhase = ""
    )
    $order = @(
        "iscsi-p6-alua-failover",
        "nvme-p4-multipath-failover",
        "nvme-p5-csi-protocol",
        "iscsi-p8-compat-soak"
    )
    $steps = @()
    foreach ($step in $order) {
        $stepDir = Join-Path $ArtifactRoot $step
        $childRunPath = Join-Path $stepDir "child-run.txt"
        if (!(Test-Path $childRunPath)) {
            $steps += [ordered]@{
                name = $step
                status = $(if ($step -eq $CurrentPhase) { "running" } else { "pending" })
                run_id = $null
                artifact_dir = $stepDir
                run_dir = $null
                phases_done = $null
                phases_total = $null
            }
            continue
        }
        $childRun = (Get-Content -Raw $childRunPath).Trim()
        $runDir = Join-Path (Join-Path $stepDir "runs") $childRun
        $statusPath = Join-Path $runDir "status.json"
        $childStatus = "error"
        $phasesDone = $null
        $phasesTotal = $null
        if (Test-Path $statusPath) {
            $data = Get-Content -Raw $statusPath | ConvertFrom-Json
            $childStatus = $data.state
            $phasesDone = $data.phases_done
            $phasesTotal = $data.phases_total
        }
        $steps += [ordered]@{
            name = $step
            status = $childStatus
            run_id = $childRun
            artifact_dir = $stepDir
            run_dir = $runDir
            phases_done = $phasesDone
            phases_total = $phasesTotal
        }
    }
    $result = [ordered]@{
        schema_version = "1.0"
        run_id = $RunId
        scenario = "protocol-release-gate-suite"
        source_commit = $ProductCommit
        product_commit = $ProductCommit
        runner_commit = $RunnerCommit
        remote_product_root = $RemoteProductRoot
        status = $Status
        summary = $Summary
        phase_results = $steps
        artifact_dir = $ArtifactRoot
        artifacts = [ordered]@{
            suite_log = $SuiteLog
        }
        non_claims = @(
            "Single-node lab release gate over existing runner-native chains.",
            "Does not claim multi-node Kubernetes, RoCE, long soak, or production HA.",
            "Each child chain owns its own product-level assertions and artifacts."
        )
    }
    $result | ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $ArtifactRoot "result.json")

    $doneStates = @("pass", "fail", "cancelled", "error")
    $phasesDone = @($steps | Where-Object { $doneStates -contains $_.status }).Count
    $statusState = if ($Status -eq "pass") { "pass" } elseif ($Status -eq "running") { "running" } else { "fail" }
    $statusDoc = [ordered]@{
        schema_version = 1
        run_id = $RunId
        scenario = "protocol-release-gate-suite"
        state = $statusState
        current_phase = $CurrentPhase
        phases_total = $order.Count
        phases_done = $phasesDone
        phases = $steps
        product_commit = $ProductCommit
        runner_commit = $RunnerCommit
        remote_product_root = $RemoteProductRoot
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        artifact_dir = $ArtifactRoot
        error_summary = $(if ($Status -eq "pass" -or $Status -eq "running") { $null } else { $Summary })
    }
    $statusDoc | ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $ArtifactRoot "status.json")
}

function Invoke-Chain {
    param(
        [string]$Step,
        [string]$Scenario
    )
    $stepDir = Join-Path $ArtifactRoot $Step
    $stepResults = Join-Path $stepDir "runs"
    New-Item -ItemType Directory -Force -Path $stepResults | Out-Null
    $stdout = Join-Path $stepDir "swblock.stdout.log"
    $stderr = Join-Path $stepDir "swblock.stderr.log"

    Write-SuiteLog "run step=$Step scenario=$Scenario"
    Write-SuiteResult -Status "running" -Summary "running $Step" -CurrentPhase $Step
    $scenarioPath = Join-Path $ProductRepoRoot $Scenario
    $swblockArgs = @(
        "run",
        "--env", "product_root=$RemoteProductRoot",
        "--env", "ssh_key=$SshKey",
        "--results-dir", $stepResults,
        $scenarioPath
    )
    Set-Content -Path (Join-Path $stepDir "swblock.command.txt") -Value ("swblock " + ($swblockArgs -join " "))
    $rc = 0
    try {
        if (Test-Path $RunnerRoot) {
            Push-Location $RunnerRoot
            try {
                & go run ./cmd/swblock @swblockArgs > $stdout 2> $stderr
                $rc = $LASTEXITCODE
            } finally {
                Pop-Location
            }
        } else {
            & swblock @swblockArgs > $stdout 2> $stderr
            $rc = $LASTEXITCODE
        }
    } catch {
        $rc = 1
        $_ | Out-String | Set-Content -Path $stderr
    }
    Set-Content -Path (Join-Path $stepDir "exit_code.txt") -Value $rc

    $latestPath = Join-Path $stepResults "latest"
    $childRun = "unknown"
    if (Test-Path $latestPath) {
        $childRun = (Get-Content -Raw $latestPath).Trim()
        Set-Content -Path (Join-Path $stepDir "child-run.txt") -Value $childRun
    }

    if ($rc -ne 0) {
        Write-SuiteLog "FAIL step=$Step rc=$rc child_run=$childRun"
        Write-SuiteResult -Status "fail" -Summary "release gate failed at $Step" -CurrentPhase $Step
        exit $rc
    }
    Write-SuiteLog "PASS step=$Step child_run=$childRun"
    Write-SuiteResult -Status "running" -Summary "completed $Step" -CurrentPhase ""
}

Write-SuiteResult -Status "running" -Summary "protocol release gate queued" -CurrentPhase ""
Write-SuiteLog "run_id=$RunId"
Write-SuiteLog "product_repo_root=$ProductRepoRoot"
Write-SuiteLog "runner_root=$RunnerRoot"
Write-SuiteLog "product_commit=$ProductCommit"
Write-SuiteLog "runner_commit=$RunnerCommit"
Write-SuiteLog "artifact_root=$ArtifactRoot"
Write-SuiteLog "remote_product_root=$RemoteProductRoot"

Invoke-Chain -Step "iscsi-p6-alua-failover" -Scenario "testops\scenarios\iscsi-p6-alua-failover-chain.yaml"
Invoke-Chain -Step "nvme-p4-multipath-failover" -Scenario "testops\scenarios\nvme-p4-multipath-failover-chain.yaml"
Invoke-Chain -Step "nvme-p5-csi-protocol" -Scenario "testops\scenarios\nvme-p5-csi-protocol-chain.yaml"
Invoke-Chain -Step "iscsi-p8-compat-soak" -Scenario "testops\scenarios\iscsi-p8-compat-soak-chain.yaml"

Write-SuiteResult -Status "pass" -Summary "protocol release gate passed"
Write-SuiteLog "PASS: protocol release gate"
Write-SuiteLog "artifacts=$ArtifactRoot"
