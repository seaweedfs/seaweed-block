# QA Assignment: CSI RF=1 Durable Blockvolume Restart Validation

Status: draft, waiting for first lab run.
Branch: `plan-roadmap-refresh`.

Scope: prove a single-replica dynamic PVC survives a generated `blockvolume`
pod restart when the launcher renders durable host state. This is the first
RF=1 reliable-restart gate for the beta-hardening plan.

## Preconditions

- Host: M02 k3s lab or equivalent single-node Kubernetes lab.
- Required host tools:
  - `kubectl`,
  - Docker image build/load path for `sw-block:local` and
    `sw-block-csi:local`,
  - iSCSI initiator tools for the default protocol path.
- Build and import fresh alpha images before running, preferably through the
  existing pin-build/TestOps path.
- This gate must run with durable generated workload state:

```bash
export SW_BLOCK_LAUNCHER_STATE_HOSTPATH=/var/lib/sw-block
```

Do not mark the gate green if the generated `blockvolume` manifest still uses
`emptyDir` for the state volume.

## Test 1: RF=1 Dynamic PVC Survives Blockvolume Restart

Script command:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-csi-rf1-blockvolume-restart"
SW_BLOCK_ALPHA_IMAGES_ENV="/path/to/pin-build/alpha-images.env" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
  bash scripts/run-k8s-blockvolume-restart.sh "$PWD"
```

Expected:

- `run.log` contains:
  - `launcher_state_hostpath=/var/lib/sw-block`,
  - `restart_blockvolume_before_reader=1`,
  - `demo-app-pvc-writer-hold-root.yaml`,
  - `delete writer pod but keep PVC`,
  - `restart generated blockvolume Deployment before replacing the app pod`,
  - `start reader pod on the same PVC`,
  - final line:
    `[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete`.
- `block-stack.rendered.yaml` contains:
  - `--launcher-state-hostpath=/var/lib/sw-block`.
- `generated-blockvolume.yaml` contains:
  - `hostPath:`,
  - `path: /var/lib/sw-block`,
  - `type: DirectoryOrCreate`,
  - no `emptyDir:` under the generated `state` volume,
  - `--durable-root=/var/lib/sw-block/<volume>/<replica>`.
- `writer.log` contains:
  - `[app-writer] wrote and verified /data/demo.bin`.
- `restart-blockvolume-status.log` shows the generated `blockvolume`
  Deployment completed rollout after the writer checksum.
- `blockvolume-pods.before-restart.txt` and
  `blockvolume-pods.after-restart.txt` show pod replacement.
- `lifecycle-volumes.after-blockvolume-restart.json` exists and still contains
  the PVC volume spec.
- `reader.log` contains:
  - `/data/demo.bin: OK`.
- Cleanup:
  - no generated `sw-blockvolume` Deployment remains,
  - demo PVC is gone,
  - no `iqn.2026-05.io.seaweedfs` iSCSI session remains.

## What This Proves

- The launcher can render durable generated workload state for a dynamic PVC.
- A generated `blockvolume` pod can restart and recover the same durable root.
- `blockmaster` remains in the loop: the restarted workload must re-observe and
  keep enough lifecycle/frontend state for CSI to rediscover and reattach.
- A replacement app pod can read data written before the restart.

## What This Does Not Prove

- No RF=2/RF=3 promotion or returned-replica catch-up claim.
- No node reboot claim.
- No multi-node scheduling claim.
- No production storage claim for `hostPath`.
- No NVMe multipath claim.

## Report Format

QA should report:

- branch and commit SHA,
- host and kernel,
- exact command,
- result,
- artifact path,
- final PASS line,
- generated manifest state-volume evidence,
- writer and reader checksum lines,
- blockvolume rollout evidence,
- master/lifecycle evidence after restart,
- cleanup state,
- any CSI, kubelet, iSCSI, or blockvolume errors.
