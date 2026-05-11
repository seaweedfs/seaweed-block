# TestOps Runner Binary Contract

Status: active reference for beta-hardening work.

Purpose: keep the product repo, runner repo, developer workstation, and QA lab
aligned on what binary executes runner-native scenarios.

## Binary Identity

`swblock` is the standalone `sw-test-runner` binary for Seaweed Block / V3.

- Runner repo: `pingqiu/sw-test-runner`
- Main package: `cmd/swblock/main.go`
- Build command:

```bash
go build -o swblock ./cmd/swblock
```

`swblock` links the shared runner engine plus the V3 product action set:

- core actions,
- iSCSI actions,
- NVMe actions,
- Kubernetes actions,
- V3-specific product pack actions.

Sibling binaries use the same engine with different linked action sets:

- `weedblock`: V2 product pack.
- `cmd/sw-test-runner`: broad development / kitchen-sink binary.

## Path Convention

QA currently keeps a Windows build at:

```text
C:\work\swblock.exe
```

Developer machines should either put the binary on `PATH` as `swblock` or call
it by full path. Linux controllers should do the same with a Linux build of the
binary.

The product repo should not vendor the runner binary. The product repo owns
scenario content; the runner repo owns execution.

## Product-Side Build Helper

This product repo includes a helper for developer/QA convenience. It does not
vendor the binary; it builds from a local `sw-test-runner` checkout or clones
the runner repo when allowed.

Windows:

```powershell
.\internal\tools\build-swblock.ps1
```

Linux / Git Bash:

```bash
bash internal/tools/build-swblock.sh
```

Useful options:

```powershell
.\internal\tools\build-swblock.ps1 -RunnerRoot C:\work\sw-test-runner
.\internal\tools\build-swblock.ps1 -NoClone
```

```bash
bash internal/tools/build-swblock.sh --runner-root /path/to/sw-test-runner
bash internal/tools/build-swblock.sh --no-clone
```

Default output:

```text
<product-root>/.tools/swblock.exe   # Windows
<product-root>/.tools/swblock       # Linux
<product-root>/.tools/swblock.path  # file containing the built binary path
```

## Expected Commands

Validate a product-owned scenario:

```bash
swblock validate testops/scenarios/csi-rf1-durable-restart-chain.yaml
```

Run a product-owned scenario against m02:

```bash
swblock run testops/scenarios/csi-rf1-durable-restart-chain.yaml \
  --env product_root=/tmp/seaweed_block \
  --env ssh_key=C:/work/dev_server/testdev_key
```

Run the full protocol release gate:

```bash
swblock suite \
  --results-dir V:/share/g15d-k8s/testops-runs/protocol-release-gate-native \
  --env product_root=/tmp/seaweed-block-nvme-p4l \
  --env ssh_key=C:/work/dev_server/testdev_key \
  C:/work/seaweed_block/testops/suites/protocol-release-gate.yaml
```

Validate a finished protocol release gate bundle:

```bash
swblock validate-bundle --profile protocol-release-gate \
  V:/share/g15d-k8s/testops-runs/protocol-release-gate-native/<run-id>
```

## Responsibility Boundary

Product repo:

- scenario YAML,
- product smoke scripts,
- field-level assertions,
- QA assignment docs,
- expected evidence and non-claims.

Runner repo:

- action registry,
- SSH/control execution,
- run-control status schema,
- result bundle layout,
- artifact collection,
- bundle validation.

QA:

- builds or receives the correct runner binary,
- runs product-owned scenarios/suites,
- reports result bundle path and validation output.

Developer:

- writes product-owned scenario content,
- keeps gates narrow and evidence-based,
- uses `swblock validate`/`run` locally when the binary is available.

## QA Assignment Policy

Use developer self-run for short feedback loops:

- `swblock validate` on a new or edited scenario,
- one child scenario run when the target lab is reachable,
- a focused re-run after a small scenario/product fix.

Assign QA when independence or lab discipline matters:

- milestone validation,
- release-gate validation,
- long soak / repeatability runs,
- runner feature validation such as status, cancellation, collection, or bundle
  validation,
- complex scenario design where the product behavior and lab evidence still
  need shaping.

This keeps TestOps productive: developers use the runner for immediate feedback,
while QA owns authoritative evidence and complex validation.
