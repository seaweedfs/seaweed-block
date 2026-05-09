# V3 TestOps Scenarios

This directory holds product-owned scenario content for the standalone
`sw-test-runner` stack.

The intended boundary is:

- Product repo owns the test intent: which scripts or checks represent the
  product gate, what artifacts prove each hop, and what cleanup must be true.
- Standalone runner owns orchestration: target cleanup, command execution,
  build/import coordination, progress checks, run-bundle layout, and artifact
  collection.
- QA can run the same YAML locally or from CI; developers can run it before
  pushing to avoid stale-image / stale-process / scattered-artifact loops.

Example:

```bash
swblock run testops/scenarios/nvme-p5-csi-protocol-chain.yaml \
  --env product_root=/path/to/seaweed_block/on/m02 \
  --env ssh_key=/path/to/testdev_key/on/controller
```

The same runner-native shape is used for the longer iSCSI P8 soak:

```bash
swblock run testops/scenarios/iscsi-p8-compat-soak-chain.yaml \
  --env product_root=/path/to/seaweed_block/on/m02 \
  --env ssh_key=/path/to/testdev_key/on/controller
```

The scenario still shells out to existing bash payloads. That is deliberate:
the first migration step is to move orchestration and evidence collection into
the runner without rewriting the product smoke tests.

## Protocol Release Gate

The product-owned release gate composes the current protocol chains:

- `iscsi-p6-alua-failover-chain`
- `nvme-p4-multipath-failover-chain`
- `nvme-p5-csi-protocol-chain`
- `iscsi-p8-compat-soak-chain`

The primary operator path is the runner-native suite command. Run it from the
controller, pointing it at the remote product checkout on m02:

```powershell
swblock suite `
  --results-dir V:/share/g15d-k8s/testops-runs/protocol-release-gate-native `
  --env product_root=/tmp/seaweed-block-nvme-p4l `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/suites/protocol-release-gate.yaml
```

Validate the bundle before manual artifact review:

```powershell
swblock validate-bundle --profile protocol-release-gate `
  V:/share/g15d-k8s/testops-runs/protocol-release-gate-native/<run-id>
```

QA close evidence:

- product commit: `033028e74c1ac3bc06f19c0563bc2e6a0495af59`,
- runner commit: `3c1b6603aefcf4c1bf0b22f9a9c081a67e786d8d`,
- suite run: `20260509-151531-9c6c`,
- result: PASS, validated by `swblock validate-bundle --profile
  protocol-release-gate`.

The older wrappers remain available for compatibility and local debugging. The
PowerShell wrapper supports both Windows PowerShell 5.1 and PowerShell 7+;
native runner stderr is captured into the child artifact directory instead of
being treated as a PowerShell exception.

```powershell
.\scripts\testops-run-protocol-release-gate.ps1 `
  -RunnerRoot C:\work\seaweedfs\learn\sw-test-runner-standalone `
  -RemoteProductRoot /tmp/seaweed-block-nvme-p4l `
  -SshKey C:\work\dev_server\testdev_key `
  -ArtifactRoot C:\work\tmp\protocol-release-gate-$(Get-Date -Format yyyyMMddTHHmmssZ)
```

Linux / Git Bash controllers can use the bash wrapper:

```bash
SWBLOCK_RUNNER_ROOT=/c/work/seaweedfs/learn/sw-test-runner-standalone \
SW_BLOCK_REMOTE_PRODUCT_ROOT=/tmp/seaweed-block-nvme-p4l \
SW_BLOCK_SSH_KEY='C:\work\dev_server\testdev_key' \
SW_BLOCK_ARTIFACT_DIR=/c/work/tmp/protocol-release-gate-$(date -u +%Y%m%dT%H%M%SZ) \
  bash scripts/testops-run-protocol-release-gate.sh "$PWD"
```

The suite writes a top-level `result.json` and `status.json` with
`started_at`, `ended_at`, `wall_clock_s`, child run IDs, per-child PASS/FAIL,
and artifact pointers. Each child bundle still owns its own `status.json`,
`result.json`, `scenario.yaml`, and collected remote artifacts under
`<suite-artifact>/<step>/runs/<child-run-id>/`.

After a run, validate the suite bundle offline before manual artifact review:

```powershell
.\scripts\testops-validate-latest-protocol-release-gate.ps1 `
  -RunnerRoot C:\work\seaweedfs\learn\sw-test-runner-standalone `
  -ArtifactRoot C:\work\tmp\protocol-release-gate-20260509T000000Z `
  -ExpectCommit a0175f8
```

Linux / Git Bash:

```bash
SWBLOCK_RUNNER_ROOT=/c/work/seaweedfs/learn/sw-test-runner-standalone \
SW_BLOCK_ARTIFACT_DIR=/c/work/tmp/protocol-release-gate-20260509T000000Z \
SW_BLOCK_EXPECT_COMMIT=a0175f8 \
  bash scripts/testops-validate-latest-protocol-release-gate.sh "$PWD"
```

The wrapper calls the platform validator profile:

```bash
swblock validate-bundle --profile protocol-release-gate \
  --expect-commit a0175f8 \
  /path/to/protocol-release-gate-artifact
```

The older product-specific Python checker remains available for local
debugging but is no longer the primary operator path:

```bash
python3 scripts/testops-validate-protocol-release-gate.py \
  /path/to/protocol-release-gate-artifact \
  --expect-product-commit a0175f8
```

Use `--allow-fail` when validating the schema of an intentionally failed or
cancelled run; without it the checker requires all four child chains to be
`pass` with complete phase counts.
