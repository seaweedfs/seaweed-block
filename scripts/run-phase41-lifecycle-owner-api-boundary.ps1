param(
    [string]$ProductRoot = (Get-Location).Path,
    [string]$ArtifactDir = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ArtifactDir)) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $ArtifactDir = Join-Path $ProductRoot "results\phase41-lifecycle-owner-api-boundary-$stamp"
}

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null
$summary = Join-Path $ArtifactDir "phase41-lifecycle-owner-api-boundary-summary.txt"
Set-Content -Path $summary -Value "phase41_lifecycle_owner_api_boundary_status=running"

Push-Location $ProductRoot
try {
    $testPattern = "TestPhase40D1KubernetesStatusClientConformsToCRDSchemaAndRBAC|TestPhase40D1StatusConformanceRejectsSchemaAndRBACDrift|TestPhase41D2LifecycleOwnerFinalizerBoundary"
    $log = Join-Path $ArtifactDir "go-test-core-ops.log"
    & go test ./core/ops -count=1 -run $testPattern *>&1 | Tee-Object -FilePath $log
    if ($LASTEXITCODE -ne 0) {
        Add-Content -Path $summary -Value "go_test_core_ops=failed"
        Add-Content -Path $summary -Value "phase41_lifecycle_owner_api_boundary_status=failed"
        exit $LASTEXITCODE
    }

    Add-Content -Path $summary -Value "go_test_core_ops=ok"
    Add-Content -Path $summary -Value "operator_status_main_patch_allowed=false"
    Add-Content -Path $summary -Value "lifecycle_owner_finalizer_patch_allowed=true"
    Add-Content -Path $summary -Value "lifecycle_owner_spec_patch_allowed=false"
    Add-Content -Path $summary -Value "lifecycle_owner_unrelated_metadata_patch_allowed=false"
    Add-Content -Path $summary -Value "finalizers_endpoint_allowed=false"
    Add-Content -Path $summary -Value "phase41_lifecycle_owner_api_boundary_status=ok"
} finally {
    Pop-Location
}
