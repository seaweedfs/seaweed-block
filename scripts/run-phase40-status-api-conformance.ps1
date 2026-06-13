param(
    [string]$ProductRoot = (Get-Location).Path,
    [string]$ArtifactDir = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ArtifactDir)) {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $ArtifactDir = Join-Path $ProductRoot "results\phase40-status-api-conformance-$stamp"
}

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

$summary = Join-Path $ArtifactDir "phase40-status-api-conformance-summary.txt"
$result = Join-Path $ArtifactDir "result.json"
$log = Join-Path $ArtifactDir "gate.log"
Set-Content -Path $summary -Value "phase40_status_api_conformance_status=running"
Set-Content -Path $log -Value ""

$status = "ok"

function Invoke-GateStep {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$Arguments
    )
    $stdout = Join-Path $ArtifactDir "$Name.stdout.txt"
    $stderr = Join-Path $ArtifactDir "$Name.stderr.txt"
    Add-Content -Path $log -Value ("[{0}] {1}" -f (Get-Date).ToUniversalTime().ToString("HH:mm:ss"), $Name)
    $process = Start-Process -FilePath $FilePath -ArgumentList $Arguments -WorkingDirectory $ProductRoot -NoNewWindow -Wait -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    if ($process.ExitCode -eq 0) {
        Add-Content -Path $summary -Value "$Name=ok"
    } else {
        Add-Content -Path $summary -Value "$Name=failed"
        $script:status = "failed"
    }
}

Invoke-GateStep `
    -Name "status_conformance_go_test" `
    -FilePath "go" `
    -Arguments @(
        "test",
        "./core/ops",
        "-count=1",
        "-run",
        "TestPhase40D1KubernetesStatusClientConformsToCRDSchemaAndRBAC|TestPhase40D1StatusConformanceRejectsSchemaAndRBACDrift|TestOperatorStatusReconcilerProjectsDeleteSafetyWithoutFinalizerMutation|TestOperatorStatusReconcilerDeleteSafetyDoesNotContaminateOtherVolumes|TestPhase40D2VolumeStatusClearsStaleDeleteSafety"
    )

Invoke-GateStep `
    -Name "helm_operator_status_render" `
    -FilePath "helm" `
    -Arguments @(
        "template",
        "sw-block",
        "charts/seaweed-block",
        "--namespace",
        "kube-system",
        "--include-crds",
        "--set",
        "operatorStatus.create=true",
        "--set",
        "operatorStatus.dryRun=false"
    )

Add-Content -Path $summary -Value "phase40_status_api_conformance_status=$status"
Add-Content -Path $summary -Value "casing_drift_gate=$status"
Add-Content -Path $summary -Value "enum_drift_gate=$status"
Add-Content -Path $summary -Value "wrong_endpoint_gate=$status"
Add-Content -Path $summary -Value "rbac_boundary_gate=$status"
Add-Content -Path $summary -Value "delete_safety_status_gate=$status"
Add-Content -Path $summary -Value "operator_status_mutation_scope=status_events_only"
Add-Content -Path $summary -Value "finalizer_mutation_allowed=false"

[ordered]@{
    schema_version = "1.0"
    scenario = "phase40-status-api-conformance"
    status = $status
    summary = "Phase 40 status API conformance gate $status"
    artifact_dir = $ArtifactDir
} | ConvertTo-Json -Depth 4 | Set-Content -Path $result

if ($status -ne "ok") {
    exit 1
}
