# QA Report - Phase 28 D13 Release Packaging

Verdict: **PASS**

Date: 2026-05-24

Image source commit:

```text
6260e46fd3beb0ba07784c7e7a637efd59aeee6f
```

Publish workflow:

```text
https://github.com/seaweedfs/seaweed-block/actions/runs/26370891528
```

## Published Images

```text
ghcr.io/seaweedfs/seaweed-block:sha-6260e46fd3be
ghcr.io/seaweedfs/seaweed-block-csi:sha-6260e46fd3be
```

Published digests:

```text
seaweed-block
  index:       sha256:ef9c60f82c36f22360b10faafd32caf807f98ac0ea86c0365c0d0836e5f67110
  linux/amd64: sha256:36481cbc1fc98fafdfa386823e0e5906785cb6f35748ef698ff1cec39bb40464

seaweed-block-csi
  index:       sha256:b160ceee874dc6743074ef6b6735ccf05914c1de5951972922f6d3779bc73592
  linux/amd64: sha256:82e41b7ef92ad8db38b6927e334cc1d564b1012ad916e9bde2e882cece680be8
```

## Release-Path QA

Scenario:

```text
testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml
```

Run:

```text
20260524-124413-829a
```

Result:

```text
PASS, 34/34 actions
```

Validated path:

- `sw-block ops generate-helm-values` wrote Helm values using the immutable
  GHCR image tags.
- `helm install` deployed `sw-blockmaster`, `sw-block-csi-controller`, and
  `sw-block-csi-node`.
- Kubernetes pulled:
  - `ghcr.io/seaweedfs/seaweed-block:sha-6260e46fd3be`
  - `ghcr.io/seaweedfs/seaweed-block-csi:sha-6260e46fd3be`
- First PVC bound.
- Writer verified `/data/demo.bin`.
- Reader verified persisted `/data/demo.bin`.
- `sw-block ops report` wrote `operator-snapshot.json`.
- `sw-block ops dashboard` served `/operator-snapshot.json` with:
  - `read_only=true`
  - `mutation_allowed=false`
- Helm uninstall and cleanup left no active iSCSI sessions or sw-block
  processes.

Key summary:

```text
first_volume_status=ok
pvc_phase=Bound
writer_verified=true
reader_verified=true
inventory_status=ok
status_report=status/report/index.html
operator_snapshot=status/report/operator-snapshot.json
cleanup_status=ok
```

## Boundary

D13 validates release packaging and the published-image first-volume path. It
does not add new functional claims beyond the Phase 28 D12 close report.
Multi-volume, support-bundle, operator-snapshot, dashboard, and cleanup claims
remain backed by the D12 close report:

```text
internal/docs/qa-assignments/phase28-productized-operations-close-report.md
```
