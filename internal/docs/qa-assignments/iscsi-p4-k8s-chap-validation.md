# QA Assignment: iSCSI P4 K8s CHAP Validation

Status: ready for QA.
Branch: `iscsi/frontend-completeness`.
Scope: Kubernetes dynamic PVC path with target-side CHAP enabled through a
Kubernetes Secret and CSI `NodeStage` secrets.

## What This Verifies

- The alpha runner creates a Kubernetes Secret when CHAP env vars are set.
- The generated `blockvolume` Deployment references that Secret and starts the
  iSCSI target with CHAP enabled.
- The StorageClass carries `csi.storage.k8s.io/node-stage-secret-*` refs.
- The CSI node configures `iscsiadm` CHAP before login.
- The pod writes and reads through the mounted PVC.
- Cleanup leaves no iSCSI session, PVC, pod, or generated blockvolume
  Deployment.

## Preconditions

- Host: M02 or equivalent Linux Kubernetes node with k3s/kubectl, Docker,
  `iscsiadm`, and loadable `iscsi_tcp`.
- Worktree: `iscsi/frontend-completeness` at or after the commit containing
  K8s CHAP wiring.
- Build local images from the same worktree before the run:

```bash
bash scripts/build-alpha-images.sh "$PWD"
```

## Test 1: Default Non-CHAP Regression

Run first to prove the default alpha path still works.

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p4-k8s-default"
SW_BLOCK_ARTIFACT_DIR="/tmp/sw-block-artifacts/$RUN_ID" \
  bash scripts/run-k8s-alpha.sh "$PWD"
```

Expected:

- final line contains:
  `[alpha] PASS: dynamic PVC create/delete completed checksum write/read and cleanup`
- `generated-blockvolume.yaml` does not contain `--iscsi-chap-`.
- `iscsi-sessions.after-delete.txt` reports no active sessions.
- no generated `sw-blockvolume` Deployment remains.

## Test 2: K8s CHAP Dynamic PVC

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p4-k8s-chap"
SW_BLOCK_ARTIFACT_DIR="/tmp/sw-block-artifacts/$RUN_ID" \
  SW_BLOCK_ISCSI_CHAP_USERNAME=swchap \
  SW_BLOCK_ISCSI_CHAP_SECRET=swchap-secret \
  bash scripts/run-k8s-alpha.sh "$PWD"
```

Expected:

- run log includes `chap_enabled=1`.
- `dynamic-pvc-pod.rendered.yaml` contains:
  - `csi.storage.k8s.io/node-stage-secret-name`,
  - `csi.storage.k8s.io/node-stage-secret-namespace`.
- `generated-blockvolume.yaml` does not contain `--iscsi-chap-`.
- `generated-blockvolume.yaml` contains:
  - `secretKeyRef`,
  - `SW_BLOCK_ISCSI_CHAP_USERNAME`,
  - `SW_BLOCK_ISCSI_CHAP_SECRET`,
  - `chapUsername`,
  - `chapSecret`.
- pod log contains `/data/payload.bin: OK`.
- final line contains:
  `[alpha] PASS: dynamic PVC create/delete completed checksum write/read and cleanup`.
- `iscsi-sessions.after-delete.txt` reports no active sessions.
- no generated `sw-blockvolume` Deployment remains.

## Report Format

QA should report:

- branch and commit SHA,
- host and kernel,
- exact commands,
- result for each test,
- artifact path for each run,
- final PASS line,
- snippets proving StorageClass secret refs and generated Deployment
  `secretKeyRef`,
- `blockcsi-controller.log` error count for `chap`,
- `blockvolume-generated.log` auth/session errors if any,
- cleanup state.

## Non-Claims

- No CHAP mutual authentication.
- No Kubernetes Secret rotation.
- No per-volume user management beyond the single Secret used in this alpha
  run.
- No ALUA, MPIO, NVMe, or mounted failover claim.
