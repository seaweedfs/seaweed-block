# Alpha Release QA Assignment

Status: active assignment  
Owner: QA  
Reviewer: maintainer/dev lead  
Scope: Kubernetes alpha user experience and storage-path smoke gates

This assignment turns the current alpha into repeatable release evidence.

QA owns the run scripts, artifacts, and PR evidence. The reviewer checks the
evidence and decides whether the alpha release gate is satisfied.

## Goal

Prove that an early Kubernetes user can try `seaweed-block` without knowing the
internal development history:

1. run a demo on a test cluster,
2. mount a normal PVC from an app pod,
3. write and read data through the PVC,
4. clean up without dangling iSCSI sessions or Kubernetes workloads,
5. capture enough artifacts to debug failures.

## Non-Goals

- Do not claim production readiness.
- Do not claim multi-node failover.
- Do not claim performance SLOs.
- Do not claim durable data across blockvolume pod restart while the alpha
  manifest still uses pod-local state.
- Do not rewrite product code in QA PRs unless explicitly approved.

## Required Gates

### Gate 1: GHCR Fresh-User Demo

Purpose: prove the easiest public user path.

Preconditions:

- A Linux Kubernetes test cluster is available.
- Privileged CSI node pods are allowed.
- `iscsi_tcp` is loadable on the node.
- The alpha GHCR images are public:
  - `ghcr.io/seaweedfs/seaweed-block:alpha`
  - `ghcr.io/seaweedfs/seaweed-block-csi:alpha`

Command:

```bash
bash scripts/run-k8s-demo-ghcr.sh "$PWD"
```

Pass criteria:

- Final line contains:

```text
[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete
```

- `writer.log` contains `/data/demo.bin: OK`.
- `reader.log` contains `/data/demo.bin: OK`.
- `iscsiadm -m session` shows no active Seaweed Block session after cleanup.
- `kubectl get all -A | grep sw-block` has no live residue after teardown settles.

Review points:

- Confirm the run used GHCR images, not local `sw-block:local`.
- Confirm no manual workaround was needed.
- Confirm artifact directory includes daemon logs and pod logs.

### Gate 2: Install Then Use Own App

Purpose: prove a user can install the alpha stack and run their own PVC/app
workflow, not only the bundled demo script.

Commands:

```bash
bash scripts/install-k8s-alpha.sh "$PWD"
kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml
bash scripts/apply-k8s-alpha-blockvolumes.sh
kubectl apply -f examples/kubernetes/basic-app/writer-pod.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-example-writer --timeout=240s
kubectl logs sw-block-example-writer
kubectl delete pod sw-block-example-writer
kubectl apply -f examples/kubernetes/basic-app/reader-pod.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-example-reader --timeout=240s
kubectl logs sw-block-example-reader
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

Pass criteria:

- install script reports PASS.
- generated blockvolume apply script reports PASS.
- writer pod log contains `/data/demo.bin: OK`.
- reader pod log contains `/data/demo.bin: OK`.
- uninstall script reports PASS.
- no active iSCSI session remains.
- no live `sw-block` Kubernetes workload remains after teardown settles.

Review points:

- Confirm this is not just `run-k8s-demo.sh`; it must use the install/apply/app
  sequence above.
- Confirm the reader pod is created after the writer pod is deleted.
- Confirm the current manual `apply-k8s-alpha-blockvolumes.sh` step is called
  out as temporary operator debt.

### Gate 3: Larger Volume Smoke

Purpose: prevent regressions in iSCSI multi-PDU write handling.

Command:

```bash
bash scripts/run-k8s-alpha-large.sh "$PWD"
```

Pass criteria:

- 256 MiB PVC path completes.
- Pod checksum verification passes.
- Server log shows large write handling beyond the historical tiny-write path.
- No iSCSI session remains after cleanup.
- No live `sw-block` Kubernetes workload remains after teardown settles.

Review points:

- Capture the maximum SCSI WRITE `transferLen` observed in the blockvolume log.
- Confirm there are zero `session error` entries in the blockvolume log.
- Do not report throughput as a product benchmark.

### Gate 4: FIO Stability Smoke

Purpose: exercise sustained filesystem I/O without making performance claims.

Command:

Use the existing alpha fio scenario or QA TestOps equivalent.

Suggested parameters:

```text
PVC size: 256Mi
fio runtime: 60s
rw: randrw
bs: 128k
iodepth: 1
```

Pass criteria:

- fio exits successfully.
- fio reports zero failed I/O.
- blockvolume log has zero session errors.
- cleanup leaves no active iSCSI session.

Review points:

- Record read/write totals and runtime.
- State explicitly that this is stability evidence, not a performance SLO.

### Gate 5: PostgreSQL / pgbench Smoke

Purpose: prove that a small DB-like workload can run on the alpha PVC for
experimentation.

Command:

Use the existing pgbench scenario or QA TestOps equivalent.

Pass criteria:

- PostgreSQL initializes on the mounted PVC.
- `pgbench` completes.
- failed transaction count is zero.
- cleanup leaves no active iSCSI session and no live `sw-block` workload.

Review points:

- Record transaction count, failed transaction count, TPS, and runtime.
- State explicitly that production database use is not claimed.

## Artifact Requirements

Each gate must produce a run directory containing:

- `run.log` or equivalent command transcript,
- pod logs,
- pod describe output on failure,
- blockmaster log,
- blockvolume log,
- CSI controller log,
- CSI provisioner / attacher logs when applicable,
- `kubectl get pods,deploy,sc,pv,pvc -A -o wide` before and after cleanup,
- `iscsiadm -m session` before and after cleanup when available.

The PR must include the artifact path and the exact commit SHA tested.

## PR Requirements

QA should open one PR named:

```text
test(k8s): record alpha release gate evidence
```

The PR may include:

- QA instruction documents,
- TestOps scenario registrations,
- harness improvements,
- evidence summary documents.

The PR must not include unrelated product behavior changes.

PR body must include:

```text
## Summary
- gate results
- tested commit SHA
- lab host / cluster

## Evidence
- artifact paths
- exact commands

## Non-Claims
- no production readiness
- no failover-under-mounted-PVC
- no performance SLO

## Reviewer Checklist
- [ ] GHCR fresh-user path uses GHCR images
- [ ] install-own-app path uses the install/apply/app sequence
- [ ] larger-volume path covers multi-PDU iSCSI writes
- [ ] fio is reported as stability, not benchmark
- [ ] pgbench is reported as alpha DB smoke, not production DB support
- [ ] cleanup evidence is present for every gate
```

## Reviewer Decision

Reviewer should approve only if:

1. all required gates pass or failures are explicitly documented as release
   blockers,
2. evidence is reproducible from the PR instructions,
3. cleanup is proven,
4. claims stay within alpha scope.

If any gate fails, QA should keep the PR open and mark the failed gate as
`BLOCKED`, with the artifact path and root-cause summary.
