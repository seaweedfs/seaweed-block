param(
    [string]$ProductRoot = (Get-Location).Path,
    [string]$ArtifactDir = "",
    [string]$ReleaseImage = $env:SW_BLOCK_RELEASE_IMAGE,
    [string]$ReleaseCSIImage = $env:SW_BLOCK_CSI_RELEASE_IMAGE
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ArtifactDir)) {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $ArtifactDir = Join-Path $ProductRoot "results\operation-milestone-release-readiness-$stamp"
}

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

$summary = Join-Path $ArtifactDir "operation-milestone-release-readiness-summary.txt"
$result = Join-Path $ArtifactDir "result.json"
Set-Content -Path $summary -Value "operation_milestone_release_readiness_status=running"

function Add-Summary {
    param([string]$Line)
    Add-Content -Path $summary -Value $Line
}

function Parse-ImageRef {
    param([string]$Image)
    if ($Image -match '^(?<repo>.+)@(?<digest>sha256:[0-9a-fA-F]+)$') {
        return [ordered]@{
            repository = $Matches.repo
            tag = ""
            digest = $Matches.digest
        }
    }
    $lastColon = $Image.LastIndexOf(':')
    $lastSlash = $Image.LastIndexOf('/')
    if ($lastColon -le $lastSlash) {
        throw "image must include an explicit tag or digest: $Image"
    }
    return [ordered]@{
        repository = $Image.Substring(0, $lastColon)
        tag = $Image.Substring($lastColon + 1)
        digest = ""
    }
}

function Invoke-Step {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$Arguments
    )
    $stdout = Join-Path $ArtifactDir "$Name.stdout.txt"
    $stderr = Join-Path $ArtifactDir "$Name.stderr.txt"
    $process = Start-Process -FilePath $FilePath -ArgumentList $Arguments -WorkingDirectory $ProductRoot -NoNewWindow -Wait -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    if ($process.ExitCode -eq 0) {
        Add-Summary "$Name=ok"
        return $true
    }
    Add-Summary "$Name=failed"
    return $false
}

if ([string]::IsNullOrWhiteSpace($ReleaseImage) -or [string]::IsNullOrWhiteSpace($ReleaseCSIImage)) {
    Add-Summary "operation_milestone_release_readiness_status=blocked_missing_release_images"
    Add-Summary "required_env=SW_BLOCK_RELEASE_IMAGE,SW_BLOCK_CSI_RELEASE_IMAGE"
    Add-Summary "example_SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>"
    Add-Summary "example_SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>"
    [ordered]@{
        schema_version = "1.0"
        scenario = "operation-milestone-release-readiness"
        status = "blocked"
        reason = "missing_release_images"
        artifact_dir = $ArtifactDir
    } | ConvertTo-Json -Depth 4 | Set-Content -Path $result
    exit 2
}

$status = "ok"
$image = Parse-ImageRef $ReleaseImage
$csiImage = Parse-ImageRef $ReleaseCSIImage

Add-Summary "release_image=$ReleaseImage"
Add-Summary "release_csi_image=$ReleaseCSIImage"

if (-not (Invoke-Step -Name "docker_manifest_release_image" -FilePath "docker" -Arguments @("manifest", "inspect", $ReleaseImage))) {
    $status = "failed"
}
if (-not (Invoke-Step -Name "docker_manifest_release_csi_image" -FilePath "docker" -Arguments @("manifest", "inspect", $ReleaseCSIImage))) {
    $status = "failed"
}
if (-not (Invoke-Step -Name "go_test_release_scope" -FilePath "go" -Arguments @("test", "./core/ops", "./cmd/sw-block", "./cmd/blockcsi", "./cmd/blockmaster", "./cmd/blockvolume", "./scripts", "-count=1"))) {
    $status = "failed"
}
if (-not (Invoke-Step -Name "helm_lint" -FilePath "helm" -Arguments @("lint", "charts/seaweed-block"))) {
    $status = "failed"
}

$templateArgs = @(
    "template", "sw-block", "charts/seaweed-block",
    "--namespace", "kube-system",
    "--include-crds",
    "--set", "image.repository=$($image.repository)",
    "--set", "csiImage.repository=$($csiImage.repository)",
    "--set", "image.pullPolicy=Always",
    "--set", "csiImage.pullPolicy=Always",
    "--set", "operatorStatus.create=true",
    "--set", "operatorStatus.dryRun=false",
    "--set", "lifecycleOwner.create=true",
    "--set", "lifecycleOwner.dryRun=false",
    "--set", "lifecycleOwner.admission.create=true"
)
if ($image.digest) {
    $templateArgs += @("--set", "image.digest=$($image.digest)")
} else {
    $templateArgs += @("--set", "image.tag=$($image.tag)")
}
if ($csiImage.digest) {
    $templateArgs += @("--set", "csiImage.digest=$($csiImage.digest)")
} else {
    $templateArgs += @("--set", "csiImage.tag=$($csiImage.tag)")
}

if (-not (Invoke-Step -Name "helm_template_operation_components" -FilePath "helm" -Arguments $templateArgs)) {
    $status = "failed"
}
if (-not (Invoke-Step -Name "validate_day1_published_image_scenario" -FilePath "C:\work\swblock.exe" -Arguments @("validate", "testops\scenarios\helm-first-volume-via-sw-block-cli-chain.yaml"))) {
    $status = "failed"
}
if (-not (Invoke-Step -Name "validate_phase98_operation_close_scenario" -FilePath "C:\work\swblock.exe" -Arguments @("validate", "testops\scenarios\failback-frontend-workload-close-chain.yaml"))) {
    $status = "failed"
}
if (-not (Invoke-Step -Name "git_diff_check" -FilePath "git" -Arguments @("diff", "--check"))) {
    $status = "failed"
}

Add-Summary "published_image_day1_lab_required=true"
Add-Summary "published_image_operator_lifecycle_lab_required=true"
Add-Summary "source_gated_phase98_regression_required=true"
Add-Summary "nvme_not_in_release_claim=true"
Add-Summary "operation_milestone_release_readiness_status=$status"

[ordered]@{
    schema_version = "1.0"
    scenario = "operation-milestone-release-readiness"
    status = $status
    release_image = $ReleaseImage
    release_csi_image = $ReleaseCSIImage
    artifact_dir = $ArtifactDir
    lab_qa_required = $true
} | ConvertTo-Json -Depth 4 | Set-Content -Path $result

if ($status -ne "ok") {
    exit 1
}
