# QA Assignment: iSCSI OS Windows Initiator Validation

Status: pending.
Scope: validate the same V3 iSCSI target with the Windows built-in iSCSI
Initiator and filesystem stack.

This is the second-bar check for the current
`iSCSI OS-Initiator Compatibility Closure` plan. Linux/open-iscsi is already
green at `8e220e5`; this assignment decides whether Windows support can be
claimed now or must stay an explicit non-claim.

## Preconditions

- Windows host with administrator PowerShell.
- Windows iSCSI Initiator service available.
- m02 reachable from the Windows host.
- m02 checkout on the branch under test.
- No active sw-block iSCSI sessions before the run.
- Use a non-production test target. The validation formats a disk.

## Start V3 Target On m02

Run on m02 from the product checkout:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-windows-target"
PORT=36260
ART="/mnt/smb/work/share/g15d-k8s/${RUN_ID}"
WORK="/tmp/sw-block-iscsi-windows-target-${RUN_ID}"

SW_BLOCK_ARTIFACT_DIR="$ART" \
SW_BLOCK_ISCSI_WORK_DIR="$WORK" \
SW_BLOCK_ISCSI_PORT="${PORT}" \
SW_BLOCK_ISCSI_TARGET_ONLY=1 \
SW_BLOCK_ISCSI_TARGET_HOLD_SECONDS=900 \
SW_BLOCK_DURABLE_BLOCKS=65536 \
SW_BLOCK_ISCSI_STRESS=none \
bash scripts/run-iscsi-os-smoke.sh "$PWD"
```

Expected target-side setup:

- `run.log` prints `target-only mode`.
- `run.log` prints portal `127.0.0.1:<PORT>`.
- `blockvolume.log` has the iSCSI listener and no startup error.
- Target holds for the configured time, then cleans itself up.

Keep the target loopback-only. The product intentionally refuses
unauthenticated external binds. For Windows validation, use an SSH local
forward from the Windows controller:

```powershell
ssh -i C:/work/dev_server/testdev_key `
  -N -L 36260:127.0.0.1:36260 `
  testdev@192.168.1.184
```

Leave that SSH process running while executing the Windows initiator steps.

## Windows Initiator Steps

Run from administrator PowerShell. Adjust `$Portal` and `$Port` if the target
command used different values.

```powershell
$Portal = "127.0.0.1"
$Port = 36260
$IQN = "iqn.2026-05.io.seaweedfs:os-smoke-v1"
$Mount = "S"

Start-Service MSiSCSI
New-IscsiTargetPortal -TargetPortalAddress $Portal -TargetPortalPortNumber $Port
$target = Get-IscsiTarget | Where-Object { $_.NodeAddress -eq $IQN }
if (-not $target) { throw "target not discovered: $IQN" }
Connect-IscsiTarget -NodeAddress $IQN -IsPersistent $false

$disk = Get-Disk | Where-Object { $_.BusType -eq "iSCSI" -and $_.OperationalStatus -eq "Online" } |
  Sort-Object Number -Descending |
  Select-Object -First 1
if (-not $disk) { throw "no online iSCSI disk found" }

if ($disk.PartitionStyle -eq "RAW") {
  Initialize-Disk -Number $disk.Number -PartitionStyle GPT
}
$partition = New-Partition -DiskNumber $disk.Number -UseMaximumSize -DriveLetter $Mount
Format-Volume -DriveLetter $Mount -FileSystem NTFS -Confirm:$false

$path = "${Mount}:\payload.bin"
$copy = "${Mount}:\payload.copy.bin"
$bytes = New-Object byte[] (4MB)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
[IO.File]::WriteAllBytes($path, $bytes)
Copy-Item $path $copy
$h1 = (Get-FileHash $path -Algorithm SHA256).Hash
$h2 = (Get-FileHash $copy -Algorithm SHA256).Hash
if ($h1 -ne $h2) { throw "checksum mismatch" }

Remove-Item $path, $copy -Force
Remove-Partition -DriveLetter $Mount -Confirm:$false
Disconnect-IscsiTarget -NodeAddress $IQN -Confirm:$false
Remove-IscsiTargetPortal -TargetPortalAddress $Portal -TargetPortalPortNumber $Port -Confirm:$false
```

## Expected Result

- Windows discovers the target IQN.
- Connect succeeds.
- Windows sees exactly one new iSCSI disk for the test target.
- NTFS format succeeds.
- 4 MiB write/copy/read checksum succeeds.
- Disconnect succeeds.
- m02 target exits cleanly after the hold window or after manual stop.
- No lingering Windows iSCSI session for the test IQN.
- No lingering m02 `blockmaster` or `blockvolume` process after target cleanup.

## Evidence To Report

- Product branch and commit.
- Runner/script commit if TestOps is involved.
- Windows version.
- m02 kernel.
- Target artifact path.
- `run.log`.
- `blockvolume.log`.
- Windows PowerShell transcript or copied command output.
- `Get-IscsiSession` output after cleanup.
- m02 process cleanup output.

## Failure Classification

If the run fails, classify the first failure point:

- discovery failure,
- login/connect failure,
- disk materialization failure,
- initialize/partition/format failure,
- write/read/checksum failure,
- disconnect/cleanup failure.

Attach Windows event log snippets only if they add a concrete error code.

## Non-Claims

- This is not a performance benchmark.
- This is not MPIO.
- This is not CHAP.
- This is not failover.
- This is not a broad Windows Server matrix.
- This validates one Windows host against one V3 target profile.
