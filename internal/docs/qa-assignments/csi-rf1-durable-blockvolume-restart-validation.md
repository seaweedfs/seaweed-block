# QA Assignment: CSI RF=1 Durable Blockvolume Restart Validation

Status: ready for QA repeatability validation after dev run
`20260512-211604-1339` passed at product commit `e90ce49`.
Branch: `docs/post-merge-plan`.

Scope: prove a single-replica dynamic PVC survives a generated `blockvolume`
pod restart when the launcher renders durable host state, and prove the
operator can read the post-restart durable state through `sw-block ops
inventory` and nested `sw-block ops status` bundles.

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
export SW_BLOCK_LAUNCHER_STATE_HOSTPATH=/var/lib/sw-block/testops-${RUN_ID}
```

Do not mark the gate green if the generated `blockvolume` manifest still uses
`emptyDir` for the state volume. Use a run-scoped hostPath under
`/var/lib/sw-block/testops-*` so repeat runs do not reuse stale durable roots.

## Test 1: RF=1 Dynamic PVC Survives Blockvolume Restart

Script command:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-csi-rf1-blockvolume-restart"
SW_BLOCK_ALPHA_IMAGES_ENV="/path/to/pin-build/alpha-images.env" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_LAUNCHER_STATE_HOSTPATH="/var/lib/sw-block/testops-${RUN_ID}" \
  bash scripts/run-k8s-blockvolume-restart.sh "$PWD"
```

Expected:

- `run.log` contains:
  - `launcher_state_hostpath=/var/lib/sw-block/testops-${RUN_ID}`,
  - `restart_blockvolume_before_reader=1`,
  - `collect_inventory_after_restart=1`,
  - `demo-app-pvc-writer-hold-root.yaml`,
  - `delete writer pod but keep PVC`,
  - `restart generated blockvolume Deployment before replacing the app pod`,
  - `start reader pod on the same PVC`,
  - final line:
    `[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete`.
- `block-stack.rendered.yaml` contains:
  - `--launcher-state-hostpath=/var/lib/sw-block/testops-${RUN_ID}`.
- `generated-blockvolume.yaml` contains:
  - `hostPath:`,
  - `path: /var/lib/sw-block/testops-${RUN_ID}`,
  - `type: DirectoryOrCreate`,
  - no `emptyDir:` under the generated `state` volume,
  - `--durable-root=/var/lib/sw-block/<volume>/<replica>`.
- `writer.log` contains:
  - `[app-writer] wrote and verified /data/demo.bin`.
- `iscsi-sessions.before-blockvolume-restart.txt` exists and contains no
  `iqn.2026-05.io.seaweedfs` session. This proves the writer pod unmounted and
  CSI logged out before the `blockvolume` process restart.
- `restart-blockvolume-status.log` shows the generated `blockvolume`
  Deployment completed rollout after the writer checksum.
- `blockvolume-pod-ids.before-restart.tsv` and
  `blockvolume-pod-ids.after-restart.tsv` are both non-empty and contain
  different pod UIDs. This proves the serving `blockvolume` pod was replaced.
- `lifecycle-volumes.after-blockvolume-restart.json` exists and still contains
  the PVC volume spec with `protocol: "iscsi"`.
- `reader.log` contains:
  - `/data/demo.bin: OK`.
- `reader.describe.before-delete.txt` shows:
  - `Status:           Succeeded`,
  - `SuccessfulAttachVolume`.
- `blockvolume-generated.after-restart.log` contains:
  - `phase":"iscsi-listening"`,
  - `durable primary lineage ensured`.
  Together with the reader checksum, this proves the restarted target recovered
  and the replacement reader pod reattached/read after restart. Do not require a
  live iSCSI session after the one-shot reader exits; Kubernetes may unstage and
  logout immediately after the pod reaches `Succeeded`.
- `ops-inventory-after-restart/volume-inventory-summary.txt` exists and
  contains:
  - a `volume:` row for `pvc=sw-block-demo-pvc`,
  - `protocols=iscsi`,
  - a `replica:` row with `lifecycle_owner=pvc-owner-ref
    owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc`,
  - `support_bundle=volumes/<volume>/<replica>`.
- `ops-inventory-after-restart/nested-ops-status-bundles.json` contains:
  - `"command": "sw-block ops status"`.
- At least one nested
  `ops-inventory-after-restart/volumes/<volume>/<replica>/volume-status-summary.txt`
  contains:
  - `durable_entry:`,
  - `latched=true`,
  - `operational=true`.
- Cleanup:
  - no generated `sw-blockvolume` Deployment remains,
  - demo PVC is gone,
  - no `iqn.2026-05.io.seaweedfs` iSCSI session remains.
  - the run-scoped hostPath `/var/lib/sw-block/testops-${RUN_ID}` is gone.

## What This Proves

- The launcher can render durable generated workload state for a dynamic PVC.
- A generated `blockvolume` pod can restart and recover the same durable root.
- `blockmaster` remains in the loop: the restarted workload must re-observe and
  keep enough lifecycle/frontend state for CSI to rediscover and reattach.
- A replacement app pod can read data written before the restart.
- Inventory maps the restarted PVC to its generated workload and support
  bundle.
- The nested status bundle shows the durable entry is latched and operational
  after restart.

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
- inventory summary and nested status durable-entry evidence,
- cleanup state,
- any CSI, kubelet, iSCSI, or blockvolume errors.
