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

## Runner Binary

`swblock` is the `sw-test-runner` binary used for Seaweed Block / V3 tests.

- Source repo: `pingqiu/sw-test-runner`
- Source path: `cmd/swblock/main.go`
- Build command from the runner repo:

  ```bash
  go build -o swblock ./cmd/swblock
  ```

- Windows QA convention: `C:\work\swblock.exe`
- Linux controller convention: put `swblock` on `PATH`, or set a shell alias to
  the built binary.

`swblock` is not built from this product repo. If a developer only has
`seaweed_block` checked out, runner-native scenarios can still be authored and
statically reviewed here, but `swblock validate`, `swblock run`, and
`swblock suite` require the external runner binary.

Convenience build helpers are available when the runner source is accessible:

```powershell
.\internal\tools\build-swblock.ps1
```

```bash
bash internal/tools/build-swblock.sh
```

They build into `.tools/swblock(.exe)`, write `.tools/swblock.path`, and print
the final binary path.

Related runner binaries share the same engine but link different product/action
sets:

- `swblock`: V3 / Seaweed Block product pack.
- `weedblock`: V2 product pack.
- `cmd/sw-test-runner`: kitchen-sink/dev binary.

Recommended local check before running scenarios:

```bash
swblock list
swblock validate testops/scenarios/csi-rf1-durable-restart-chain.yaml
```

## Which Gate To Run

Use the smallest gate that can prove the contract you changed.

| Change type | Default gate | Why |
| --- | --- | --- |
| Package-local logic, parser, renderer, state-machine helper | `go test -count=1 ./<touched-package>` | Fastest feedback; no m02 or runner dependency. |
| CSI protocol propagation, lifecycle persistence, launcher render shape | `swblock run testops/scenarios/nvme-p5-protocol-component-gate.yaml` | Proves the known NVMe/iSCSI protocol-shape contracts with a runner bundle in seconds. |
| RF=1 restart contract, durable-root/status manifest shape | `swblock run testops/scenarios/csi-rf1-durable-restart-component-gate.yaml` | Proves restart/recovery contract shape without a full K8s restart. |
| Operations read-only volume status snapshot shape | `swblock run testops/scenarios/operations-volume-status-snapshot-component-gate.yaml` | Proves schema, frontend identity, unavailable fields, and residue copy behavior without starting product processes. |
| Kernel initiator, k3s, mounted filesystem, failover, or stale-session behavior | The matching child chain, for example `nvme-p5-csi-protocol-chain` or `csi-rf1-durable-restart-chain` | These require real Linux/K8s/initiator behavior. |
| Milestone readiness or cross-protocol release confidence | `swblock suite testops/suites/beta-hardening-gate.yaml` plus `swblock validate-bundle --profile beta-hardening` | Slow, evidence-heavy gate. Use for release/milestone claims, not first debugging. |

Default ownership:

- Developers run unit/component tests, fast runner-native component gates, and
  single-child chains while iterating.
- QA runs full-suite repeatability, long soak, ambiguous lab failures, and
  independent milestone validation.
- If QA would only execute a single known command and return stdout, run it
  yourself and reserve QA for scenario design or trust-critical validation.

Example:

```bash
swblock run testops/scenarios/nvme-p5-csi-protocol-chain.yaml \
  --env product_root=/path/to/seaweed_block/on/m02 \
  --env ssh_key=/path/to/testdev_key/on/controller
```

Fast component gates are available for developer loops before running the
heavier K8s or initiator paths. For example, the NVMe-P5 protocol component
gate pins CSI protocol extraction, lifecycle protocol planning, and launcher
protocol persistence/planning, plus launcher NVMe/iSCSI manifest shape without
launching k3s:

```bash
swblock run testops/scenarios/nvme-p5-protocol-component-gate.yaml \
  --env product_root=/path/to/seaweed_block/on/m02 \
  --env ssh_key=/path/to/testdev_key/on/controller
```

The RF=1 durable restart path also has a component gate that checks restart
wrapper shape, generated hostPath/status manifest behavior, master frontend
projection, and durable status lineage without doing the full K8s restart:

```bash
swblock run testops/scenarios/csi-rf1-durable-restart-component-gate.yaml \
  --env product_root=/path/to/seaweed_block/on/m02 \
  --env ssh_key=/path/to/testdev_key/on/controller
```

The first operations-layer snapshot contract has its own component gate. It is
read-only and does not start product processes:

```bash
swblock run testops/scenarios/operations-volume-status-snapshot-component-gate.yaml \
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
  -ExpectCommit 033028e74c1ac3bc06f19c0563bc2e6a0495af59
```

Linux / Git Bash:

```bash
SWBLOCK_RUNNER_ROOT=/c/work/seaweedfs/learn/sw-test-runner-standalone \
SW_BLOCK_ARTIFACT_DIR=/c/work/tmp/protocol-release-gate-20260509T000000Z \
SW_BLOCK_EXPECT_COMMIT=033028e74c1ac3bc06f19c0563bc2e6a0495af59 \
  bash scripts/testops-validate-latest-protocol-release-gate.sh "$PWD"
```

The wrapper calls the platform validator profile:

```bash
swblock validate-bundle --profile protocol-release-gate \
  --expect-commit 033028e74c1ac3bc06f19c0563bc2e6a0495af59 \
  /path/to/protocol-release-gate-artifact
```

The older product-specific Python checker remains available for local
debugging but is no longer the primary operator path:

```bash
python3 scripts/testops-validate-protocol-release-gate.py \
  /path/to/protocol-release-gate-artifact \
  --expect-product-commit 033028e74c1ac3bc06f19c0563bc2e6a0495af59
```

Use `--allow-fail` when validating the schema of an intentionally failed or
cancelled run; without it the checker requires all four child chains to be
`pass` with complete phase counts.

## Beta Hardening Gate

`testops/suites/beta-hardening-gate.yaml` is the seed suite for the current
beta-hardening plan. It composes the protocol release children plus the new
component/restart/reintegration gates:

- `iscsi-p6-alua-failover-chain`
- `nvme-p4-multipath-failover-chain`
- `nvme-p5-csi-protocol-chain`
- `iscsi-p8-compat-soak-chain`
- `csi-lifecycle-component-gate`
- `csi-rf1-durable-restart-chain`
- `operations-status-diagnostics-chain`
- `returned-replica-component-gate`
- `iscsi-returned-replica-chain`
- `cleanup-residue-chain`

Run shape:

```powershell
swblock suite `
  --results-dir V:/share/g15d-k8s/testops-runs/beta-hardening-gate `
  --env product_root=/tmp/seaweed-block-plan-roadmap-refresh-devrun `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/suites/beta-hardening-gate.yaml
```

Repeatability evidence:

- product commit:
  `8822f20e91c2b88727ead9e49f9bf75eec28c791`
- runner commit:
  `cf65daaf2ce5cf500e1efa48b411f7cb66dbac0b`
- run 1: `20260511-031605-8258`, PASS, `21m46s`,
  `validate-bundle --profile beta-hardening` VALID.
- run 2: `20260511-040412-ac38`, PASS, `22m55s`,
  `validate-bundle --profile beta-hardening` VALID.
- no manual cleanup between runs; final residue audit clean.

Use this suite for milestone readiness or release-candidate confidence. Do not
use it as the first debug loop for a package-level change.
