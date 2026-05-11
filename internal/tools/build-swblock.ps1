param(
  [string]$RunnerRoot = "",
  [string]$OutputDir = "",
  [string]$RepoUrl = "https://github.com/pingqiu/sw-test-runner.git",
  [switch]$NoClone
)

$ErrorActionPreference = "Stop"

function RepoRoot {
  return (Split-Path -Parent (Split-Path -Parent $PSScriptRoot))
}

function IsWindowsHost {
  if ($env:OS -eq "Windows_NT") {
    return $true
  }
  try {
    return [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
      [System.Runtime.InteropServices.OSPlatform]::Windows)
  } catch {
    return $false
  }
}

function HasSwblockSource([string]$Path) {
  return $Path -and (Test-Path (Join-Path $Path "cmd/swblock/main.go"))
}

function RequireCommand([string]$Name) {
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "missing required command: $Name"
  }
}

$productRoot = RepoRoot
if (-not $OutputDir) {
  $OutputDir = Join-Path $productRoot ".tools"
}

$candidates = @()
if ($RunnerRoot) {
  $candidates += $RunnerRoot
}
if ($env:SW_TEST_RUNNER_ROOT) {
  $candidates += $env:SW_TEST_RUNNER_ROOT
}
$candidates += (Join-Path (Split-Path -Parent $productRoot) "sw-test-runner")
$candidates += "C:\work\sw-test-runner"
$candidates += "C:\work\sw-test-runner-standalone"
$candidates += "C:\work\seaweedfs\learn\sw-test-runner-standalone"

$resolvedRunner = ""
foreach ($candidate in $candidates) {
  if (HasSwblockSource $candidate) {
    $resolvedRunner = (Resolve-Path $candidate).Path
    break
  }
}

if (-not $resolvedRunner) {
  if ($NoClone) {
    throw "sw-test-runner checkout not found. Set -RunnerRoot or SW_TEST_RUNNER_ROOT."
  }
  RequireCommand git
  $cloneTarget = if ($RunnerRoot) { $RunnerRoot } else { Join-Path (Split-Path -Parent $productRoot) "sw-test-runner" }
  if (Test-Path $cloneTarget) {
    throw "candidate runner path exists but does not contain cmd/swblock/main.go: $cloneTarget"
  }
  Write-Host "[swblock-build] cloning $RepoUrl -> $cloneTarget"
  git clone $RepoUrl $cloneTarget
  $resolvedRunner = (Resolve-Path $cloneTarget).Path
}

RequireCommand go
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$exeName = if (IsWindowsHost) { "swblock.exe" } else { "swblock" }
$outPath = Join-Path $OutputDir $exeName

Write-Host "[swblock-build] runner_root=$resolvedRunner"
Write-Host "[swblock-build] output=$outPath"

Push-Location $resolvedRunner
try {
  go build -o $outPath ./cmd/swblock
} finally {
  Pop-Location
}

Set-Content -Path (Join-Path $OutputDir "swblock.path") -Value $outPath
Write-Output $outPath
