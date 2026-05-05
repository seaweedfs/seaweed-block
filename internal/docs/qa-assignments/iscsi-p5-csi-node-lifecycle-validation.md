# QA Assignment: iSCSI P5 CSI Node Lifecycle Validation

Status: ready for QA.
Branch: `iscsi/csi-node-lifecycle`.
Scope: Kubernetes CSI node restart while a writer pod is holding the PVC mount,
then replacement app pod reads the same PVC.

## What This Verifies

- The app writer pod can write and verify data through a dynamic PVC.
- `sw-block-csi-node` can be restarted while the writer pod keeps the PVC
  mounted.
- The writer pod can be deleted after the CSI node restart.
- A replacement reader pod can mount the same PVC and verify the existing data.
- Cleanup leaves no sw-block iSCSI sessions, PVCs, pods, or generated
  blockvolume Deployment.

## Preconditions

- Linux Kubernetes lab node with k3s/kubectl, Docker, `iscsiadm`, and
  loadable `iscsi_tcp`.
- Local images built from the same branch:

```bash
bash scripts/build-alpha-images.sh "$PWD"
```

## Test 1: CSI Node Restart Between Writer And Reader

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p5-csi-node-restart"
SW_BLOCK_ARTIFACT_DIR="/tmp/sw-block-artifacts/$RUN_ID" \
  bash scripts/run-k8s-csi-node-restart.sh "$PWD"
```

Expected:

- run log includes `restart_csi_node_before_reader=1`.
- run log includes `demo-app-pvc-writer-hold.yaml`.
- `writer.log` includes `holding pod so CSI node restart happens while PVC is mounted`.
- `restart-csi-node-status.log` shows the DaemonSet rollout completed.
- `writer.log` contains `/data/demo.bin: OK`.
- `reader.log` contains `/data/demo.bin: OK`.
- final line contains:
  `[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete`.
- `iscsi-sessions.after-delete.txt` contains no `iqn.2026-05.io.seaweedfs`.
- no generated `sw-blockvolume` Deployment remains.

## Report Format

QA should report:

- branch and commit SHA,
- host and kernel,
- exact command,
- result,
- artifact path,
- final PASS line,
- `restart-csi-node-status.log`,
- writer and reader checksum lines,
- cleanup state,
- any CSI node, kubelet, or iSCSI errors.

## Non-Claims

- No blockvolume process restart durability claim.
- No mounted failover claim.
- No ALUA, MPIO, NVMe, or multi-node claim.
- No performance claim.
