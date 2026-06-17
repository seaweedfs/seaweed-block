param(
    [string]$ProductRoot = (Get-Location).Path,
    [string]$ArtifactDir = ""
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($ArtifactDir)) {
    $ArtifactDir = Join-Path $ProductRoot "results\phase42-delete-safety-decision-gate"
}

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null
$Summary = Join-Path $ArtifactDir "phase42-delete-safety-decision-gate-summary.txt"
Set-Content -Path $Summary -Value ""

function Add-Summary([string]$Line) {
    Add-Content -Path $Summary -Value $Line
}

Add-Summary "phase42_delete_safety_decision_status=running"
Add-Summary "cleanup_execution_attempted=false"

Push-Location $ProductRoot
try {
    $testPattern = "TestEvaluateSwBlockVolumeDeleteSafety|TestObservationBundle_DeleteSafety|TestOperatorStatusReconcilerProjectsDeleteSafetyWithoutFinalizerMutation|TestOperatorStatusReconcilerDeleteSafetyDoesNotContaminateOtherVolumes|TestPhase40D2VolumeStatusClearsStaleDeleteSafety"
    $log = Join-Path $ArtifactDir "go-test-core-ops.log"
    & go test ./core/ops -count=1 -run $testPattern *>&1 | Tee-Object -FilePath $log
    if ($LASTEXITCODE -ne 0) {
        Add-Summary "go_test_core_ops=failed"
        Add-Summary "phase42_delete_safety_decision_status=failed"
        exit $LASTEXITCODE
    }
} finally {
    Pop-Location
}

Add-Summary "go_test_core_ops=ok"
Add-Summary "clean_delete_safety_decision=allowed"
Add-Summary "blocked_delete_safety_decision=rejected"
Add-Summary "missing_delete_safety_decision=unknown"
Add-Summary "stale_delete_safety_decision=unknown"
Add-Summary "lifecycle_owner_action_type=safe_k8s.release_swblockvolume_finalizer"
Add-Summary "lifecycle_owner_action_mode=dry_run"
Add-Summary "lifecycle_owner_action_mutation_allowed=false"
Add-Summary "finalizer_patch_count=0"
Add-Summary "no_finalizer_mutation_events=true"
Add-Summary "multi_volume_delete_safety_isolation=true"
Add-Summary "stale_delete_safety_cleared_when_absent=true"
Add-Summary "phase42_delete_safety_decision_status=ok"
