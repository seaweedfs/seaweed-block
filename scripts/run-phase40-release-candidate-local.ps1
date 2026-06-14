param(
    [string]$ProductRoot = (Get-Location).Path,
    [string]$ArtifactDir = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ArtifactDir)) {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $ArtifactDir = Join-Path $ProductRoot "results\phase40-release-candidate-local-$stamp"
}

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

$summary = Join-Path $ArtifactDir "phase40-release-candidate-local-summary.txt"
$result = Join-Path $ArtifactDir "result.json"
$log = Join-Path $ArtifactDir "gate.log"
Set-Content -Path $summary -Value "phase40_release_candidate_local_status=running"
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
    -Name "go_test_release_scope" `
    -FilePath "go" `
    -Arguments @("test", "./core/ops", "./cmd/sw-block", "./cmd/blockcsi", "./scripts")

Invoke-GateStep `
    -Name "helm_lint" `
    -FilePath "helm" `
    -Arguments @("lint", "charts/seaweed-block")

Invoke-GateStep `
    -Name "helm_operator_status_template" `
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

Add-Content -Path $log -Value ("[{0}] helm_published_image_compat_template" -f (Get-Date).ToUniversalTime().ToString("HH:mm:ss"))
$compatStdout = Join-Path $ArtifactDir "helm_published_image_compat_template.stdout.txt"
$compatStderr = Join-Path $ArtifactDir "helm_published_image_compat_template.stderr.txt"
$compatRender = & helm template sw-block charts/seaweed-block --namespace kube-system 2>$compatStderr
$compatExit = $LASTEXITCODE
$compatRender | Set-Content -Path $compatStdout
$compatText = $compatRender -join "`n"
if ($compatExit -eq 0 -and ($compatText -notmatch "--launcher-durable-impl")) {
    Add-Content -Path $summary -Value "helm_published_image_compat_template=ok"
} else {
    Add-Content -Path $summary -Value "helm_published_image_compat_template=failed"
    $status = "failed"
}

$conformanceDir = Join-Path $ArtifactDir "status-api-conformance"
Invoke-GateStep `
    -Name "status_api_conformance_gate" `
    -FilePath "powershell" `
    -Arguments @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        (Join-Path $ProductRoot "scripts\run-phase40-status-api-conformance.ps1"),
        "-ProductRoot",
        $ProductRoot,
        "-ArtifactDir",
        $conformanceDir
    )

Invoke-GateStep `
    -Name "git_diff_check" `
    -FilePath "git" `
    -Arguments @("diff", "--check")

Add-Content -Path $summary -Value "phase40_release_candidate_local_status=$status"
Add-Content -Path $summary -Value "fresh_helm_install_lab_required=true"
Add-Content -Path $summary -Value "first_pvc_writer_reader_lab_required=true"
Add-Content -Path $summary -Value "operator_status_crd_live_lab_required=true"
Add-Content -Path $summary -Value "negative_status_live_lab_required=true"
Add-Content -Path $summary -Value "cleanup_zero_residue_lab_required=true"
Add-Content -Path $summary -Value "status_api_conformance_artifact=$conformanceDir"

[ordered]@{
    schema_version = "1.0"
    scenario = "phase40-release-candidate-local"
    status = $status
    summary = "Phase 40 local release-candidate gate $status"
    artifact_dir = $ArtifactDir
    lab_qa_required = $true
} | ConvertTo-Json -Depth 4 | Set-Content -Path $result

if ($status -ne "ok") {
    exit 1
}
