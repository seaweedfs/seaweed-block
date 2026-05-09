param(
    [string]$ProductRepoRoot = (Get-Location).Path,
    [string]$RunnerRoot = "C:\work\seaweedfs\learn\sw-test-runner-standalone",
    [string]$ArtifactRoot = "",
    [string]$SearchRoot = "",
    [string]$ExpectCommit = "",
    [switch]$Json
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($SearchRoot)) {
    $SearchRoot = Join-Path $ProductRepoRoot "results"
}
if ([string]::IsNullOrWhiteSpace($ExpectCommit)) {
    try { $ExpectCommit = (& git -C $ProductRepoRoot rev-parse HEAD).Trim() } catch {}
}

function Find-LatestProtocolGateArtifact {
    param([string]$Root)
    if (!(Test-Path $Root)) {
        throw "search root does not exist: $Root"
    }
    $candidates = Get-ChildItem -Path $Root -Directory -Recurse -ErrorAction SilentlyContinue |
        Where-Object { Test-Path (Join-Path $_.FullName "result.json") } |
        Sort-Object LastWriteTime -Descending
    foreach ($dir in $candidates) {
        try {
            $doc = Get-Content -Raw (Join-Path $dir.FullName "result.json") | ConvertFrom-Json
            if ($doc.scenario -eq "protocol-release-gate-suite") {
                return $dir.FullName
            }
        } catch {
            continue
        }
    }
    throw "no protocol-release-gate-suite artifact found under $Root"
}

function ConvertTo-NativeArgument {
    param([string]$Argument)
    if ($null -eq $Argument) { return '""' }
    if ($Argument -notmatch '[\s"]') { return $Argument }
    $escaped = $Argument -replace '(\\*)"', '$1$1\"'
    $escaped = $escaped -replace '(\\+)$', '$1$1'
    return '"' + $escaped + '"'
}

function Invoke-Native {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$WorkingDirectory
    )
    $cmd = Get-Command $FilePath -ErrorAction Stop
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $cmd.Source
    $psi.Arguments = (($Arguments | ForEach-Object { ConvertTo-NativeArgument $_ }) -join " ")
    if (![string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        $psi.WorkingDirectory = $WorkingDirectory
    }
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $psi
    if (!$p.Start()) { throw "failed to start $FilePath" }
    $stdoutTask = $p.StandardOutput.ReadToEndAsync()
    $stderrTask = $p.StandardError.ReadToEndAsync()
    $p.WaitForExit()
    if ($stdoutTask.Result) { [Console]::Out.WriteLine($stdoutTask.Result.TrimEnd()) }
    if ($stderrTask.Result) { [Console]::Error.WriteLine($stderrTask.Result.TrimEnd()) }
    return $p.ExitCode
}

if ([string]::IsNullOrWhiteSpace($ArtifactRoot)) {
    $ArtifactRoot = Find-LatestProtocolGateArtifact -Root $SearchRoot
}

$argsList = @("validate-bundle", "--profile", "protocol-release-gate")
if (![string]::IsNullOrWhiteSpace($ExpectCommit)) {
    $argsList += @("--expect-commit", $ExpectCommit)
}
if ($Json) {
    $argsList += "--json"
}
$argsList += $ArtifactRoot

Write-Host "[protocol-gate-validate] artifact_root=$ArtifactRoot"
Write-Host "[protocol-gate-validate] expect_commit=$ExpectCommit"
if (Test-Path $RunnerRoot) {
    $rc = Invoke-Native -FilePath "go" -Arguments (@("run", "./cmd/swblock") + $argsList) -WorkingDirectory $RunnerRoot
    exit $rc
}
$rc = Invoke-Native -FilePath "swblock" -Arguments $argsList -WorkingDirectory $ProductRepoRoot
exit $rc
