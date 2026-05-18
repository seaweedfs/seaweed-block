# Release Note Draft — Phase 20 Day-1 Activation / First Volume

## Summary

Seaweed Block now has an alpha Day-1 Kubernetes path:

```text
activate stack
-> create first PVC-backed volume
-> writer pod verifies data
-> reader pod verifies persisted data
-> collect product-owned cluster evidence
-> generate a local read-only status report
-> cleanup leaves no sessions or product processes
```

This is a usability and packaging milestone. It wraps already proven block,
CSI, recovery, and observation work into a path a PM, QA engineer, or early
operator can run without reading source code.

## User-Visible Additions

- `scripts/activate-k8s-alpha.sh` installs the alpha stack and writes an
  activation summary with image mode, selected network mode, node readiness,
  protocol, ACK profile, StorageClass, next commands, and non-claims.
- On multi-node labs, activation uses external iSCSI/status addresses and CHAP
  instead of loopback-only publish targets.
- `scripts/run-basic-app-example.sh` creates the example PVC, runs writer and
  reader checksum pods, collects status/inventory evidence, generates a local
  report, and cleans the example resources.
- `sw-block ops report` renders a static read-only status page from either live
  master evidence or a saved support bundle.

## Evidence

- Activation + first volume: `20260517-205252-04e9` PASS, 5/5 phases, 27/27
  actions.
- D4/D5 report slice: `20260517-212358-bfba` PASS, 5/5 phases, 27/27 actions.
- Report artifacts from the D4/D5 run:
  - `status/report/index.html`
  - `status/report/cluster-evidence.json`
  - `status/report/timeline.jsonl`
  - `status/report/summary.txt`

## Current Claim

Seaweed Block provides an alpha install-to-first-volume Kubernetes loop:

- install the stack on a supported k3s/Kubernetes lab,
- create a PVC-backed block volume through Kubernetes,
- write and read data through app pods,
- inspect cluster/volume/timeline evidence,
- collect a local read-only status report,
- uninstall and leave the lab clean.

## Explicit Non-Claims

- Not production-ready.
- No Helm/operator lifecycle yet.
- No hosted dashboard yet; `sw-block ops report` is a static local read-only
  report.
- No backup/snapshot/restore workflow.
- No upgrade/rollback safety claim.
- No mutating admin workflows such as promote, repair, rebuild, failback, or
  delete buttons.
- No broad performance, RTO, or compatibility SLO.
- No transparent Kubernetes node-loss claim beyond already gated recovery
  plans.

## Release Validation Guidance

For QA/PM release validation, prefer immutable published images:

```bash
export SW_BLOCK_ACTIVATION_IMAGE_MODE=published
export SW_BLOCK_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>
export SW_BLOCK_CSI_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<commit>
bash scripts/activate-k8s-alpha.sh "$PWD"
bash scripts/run-basic-app-example.sh "$PWD"
```

Use mutable `:alpha` only as a casual smoke tag; it can drift from the checked
out source tree.
