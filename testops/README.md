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

Run it from the Windows controller, pointing it at the standalone runner and
the remote product checkout on m02. The PowerShell wrapper supports both
Windows PowerShell 5.1 and PowerShell 7+; native runner stderr is captured into
the child artifact directory instead of being treated as a PowerShell exception.

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

```bash
python3 scripts/testops-validate-protocol-release-gate.py \
  /path/to/protocol-release-gate-artifact \
  --expect-product-commit a0175f8
```

Use `--allow-fail` when validating the schema of an intentionally failed or
cancelled run; without it the checker requires all four child chains to be
`pass` with complete phase counts.
