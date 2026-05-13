# Seaweed Block V1 Alpha Operations Manual

This manual is the operator-facing path for the current light-use alpha. It is
not a production HA guide. It gives one supported path first:

```text
single-node Kubernetes -> iSCSI -> walstore -> ReadWriteOnce PVC
```

Use this when you want to install the alpha stack, create a first PVC, inspect
the backing `blockvolume`, delete it, and collect useful evidence if something
fails.

## Scope And Non-Claims

Claimed in this manual:

- one single-node Kubernetes alpha cluster,
- one or more RF=1 PVCs on the supported alpha path,
- product-owned reconciliation of generated `blockvolume` Deployments,
- durable restart of a generated RF=1 `blockvolume` when an explicit
  launcher hostPath is configured,
- read-only cluster inventory with per-replica support bundles,
- scoped cleanup checks for the demo PVC and generated workload.

Not claimed:

- production HA,
- node loss or host-disk failure,
- multi-node scheduling,
- live RF=2/RF=3 Kubernetes lifecycle,
- upgrade or broad uninstall safety,
- repair, rebuild, promote, backup, or restore commands,
- performance SLOs,
- UI or operator-grade reconciliation.

## 1. Preflight

Run preflight before install. It separates environment failures from storage
failures.

```bash
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
bash scripts/preflight-k8s-alpha.sh --local-k3s
```

Expected shape:

```text
[preflight] checked name=kubectl status=PASS ...
[preflight] checked name=iscsiadm status=PASS ...
[preflight] unchecked name=ghcr_pull reason="local-k3s path selected"
[preflight] summary status=PASS checked=... failed=0 unchecked=... mode=local-k3s
```

If the summary is not `status=PASS`, stop and fix the environment first.

## 2. Build Or Select Images

Default local k3s path:

```bash
SW_BLOCK_IMPORT_K3S=1 \
SW_BLOCK_ARTIFACT_DIR=/tmp/sw-block-alpha-build \
  bash scripts/build-alpha-images.sh "$PWD"
```

Check the image provenance:

```bash
cat /tmp/sw-block-alpha-build/alpha-images.env
```

If you use a registry instead, set `SW_BLOCK_IMAGE` and
`SW_BLOCK_CSI_IMAGE`, push both images, then use the same install and demo
commands below.

## 3. Install The Alpha Stack

```bash
bash scripts/install-k8s-alpha.sh "$PWD"
```

Expected final line:

```text
[alpha-install] PASS: seaweed-block alpha stack installed
```

Verify component readiness:

```bash
kubectl -n kube-system get deploy sw-blockmaster -o jsonpath='{.status.readyReplicas}/{.status.replicas}{"\n"}'
kubectl -n kube-system get deploy sw-block-csi-controller -o jsonpath='{.status.readyReplicas}/{.status.replicas}{"\n"}'
kubectl -n kube-system rollout status ds/sw-block-csi-node
kubectl get sc sw-block-dynamic -o jsonpath='{.provisioner}{"\n"}'
```

Expected output:

```text
1/1
1/1
daemon set "sw-block-csi-node" successfully rolled out
block.csi.seaweedfs.com
```

## 4. Create A First PVC And Prove I/O

The fastest full check is the demo script:

```bash
bash scripts/run-k8s-demo.sh "$PWD"
```

Expected final line:

```text
[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete
```

The important boundaries are:

```bash
ARTIFACT_DIR="$(ls -td /tmp/sw-block-app-demo-* | head -1)"
grep 'PASS:' "$ARTIFACT_DIR/run.log"
grep '/data/demo.bin: OK' "$ARTIFACT_DIR/writer.log"
grep '/data/demo.bin: OK' "$ARTIFACT_DIR/reader.log"
grep -- '--volume-id=' "$ARTIFACT_DIR/generated-blockvolume.yaml"
cat "$ARTIFACT_DIR/apply-generated-blockvolume.log"
```

`apply-generated-blockvolume.log` should say the product-owned lifecycle path
is active. The demo keeps the old manual apply fallback behind
`SW_BLOCK_DEMO_MANUAL_APPLY_BLOCKVOLUMES=1`, but that is not the normal user
path.

## 5. Prove Durable Blockvolume Restart

Use this path when you want to prove the generated `blockvolume` can restart
and the same PVC can still read the bytes written before the restart. This is
the first durable alpha path. It is still single-node and RF=1.

Use a run-scoped host path unless you intentionally want to retain data after
the demo:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export SW_BLOCK_LAUNCHER_STATE_HOSTPATH="/var/lib/sw-block/testops-${RUN_ID}-restart"
bash scripts/run-k8s-blockvolume-restart.sh "$PWD"
```

Expected final line:

```text
[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete
```

The restart wrapper enables the same product-owned lifecycle path as the normal
demo, but it also:

- injects the launcher state hostPath into the generated `blockvolume`
  workload,
- restarts the generated `blockvolume` Deployment after the writer pod exits,
- waits for durable status to become ready after the restart,
- starts a replacement reader pod on the same PVC and verifies the checksum.

Check the concrete restart evidence:

```bash
ARTIFACT_DIR="$(ls -td /tmp/sw-block-app-demo-* | head -1)"

grep 'restart_blockvolume_before_reader=1' "$ARTIFACT_DIR/run.log"
grep 'hostPath:' "$ARTIFACT_DIR/generated-blockvolume.yaml"
grep 'type: DirectoryOrCreate' "$ARTIFACT_DIR/generated-blockvolume.yaml"
grep -- '--durable-root=/var/lib/sw-block/' "$ARTIFACT_DIR/generated-blockvolume.yaml"
grep '/data/demo.bin: OK' "$ARTIFACT_DIR/reader.log"
grep '"Latched"[[:space:]]*:[[:space:]]*true' "$ARTIFACT_DIR/status-durable-after-blockvolume-restart.json"
grep '"Operational"[[:space:]]*:[[:space:]]*true' "$ARTIFACT_DIR/status-durable-after-blockvolume-restart.json"
```

Useful restart artifacts:

- `blockvolume-pod-ids.before-restart.tsv`
- `blockvolume-pod-ids.after-restart.tsv`
- `restart-blockvolume.log`
- `restart-blockvolume-status.log`
- `status-durable-after-blockvolume-restart.json`
- `blockvolume-generated.after-restart.log`
- `lifecycle-volumes.after-blockvolume-restart.json`

For a support bundle after the restart, run inventory while the cluster is
still reachable:

```bash
kubectl -n kube-system port-forward svc/blockmaster 9333:9333
```

In another terminal:

```bash
sw-block ops inventory \
  --namespace default \
  --master 127.0.0.1:9333 \
  --out "$ARTIFACT_DIR/ops-inventory-after-restart"

cat "$ARTIFACT_DIR/ops-inventory-after-restart/volume-inventory-summary.txt"
```

If inventory records a `support_bundle=volumes/<volume>/<replica>` path, inspect
the nested durable status summary:

```bash
cat "$ARTIFACT_DIR/ops-inventory-after-restart/volumes/<volume>/<replica>/volume-status-summary.txt"
```

Cleanup semantics:

- Run-scoped paths under `/var/lib/sw-block/testops-*` are treated as test-owned
  and are removed by the restart wrapper cleanup.
- A stable host path such as `/var/lib/sw-block/sw-block-alpha-restart` is
  treated as user-owned retained data. Remove it only when you intentionally
  want a clean lab:

```bash
sudo rm -rf -- "$SW_BLOCK_LAUNCHER_STATE_HOSTPATH"
```

Do not point `SW_BLOCK_LAUNCHER_STATE_HOSTPATH` at a shared or production data
directory in this alpha path. This is a restart durability proof, not upgrade,
node-loss, backup, or restore safety.

## 6. Inspect Cluster Inventory

Port-forward blockmaster:

```bash
kubectl -n kube-system port-forward svc/blockmaster 9333:9333
```

In another terminal:

```bash
sw-block ops inventory \
  --namespace default \
  --master 127.0.0.1:9333 \
  --out /tmp/sw-block-inventory
```

Read the summary:

```bash
cat /tmp/sw-block-inventory/volume-inventory-summary.txt
```

Healthy one-volume shape:

```text
inventory_status: ok
volumes: total=1 ok=1 unhealthy=0 invalid=0
volume: id=pvc-... namespace=default pvc=sw-block-demo-pvc pv=pvc-... rf=1 desired=1 observed=1 primary=r1 status=ok protocols=iscsi replicas=1
replica: volume=pvc-... replica=r1 ... observed=true status=ok lifecycle_owner=pvc-owner-ref owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc ... support_bundle=volumes/pvc-.../r1
issues: none
```

The per-replica status bundle is under:

```text
/tmp/sw-block-inventory/volumes/<volume_id>/<replica_id>/
```

Attach the entire inventory directory to issues. It contains:

- `volume-inventory.json`,
- `volume-inventory-summary.txt`,
- `ops-inventory-bundle.json`,
- nested `sw-block ops status` artifacts when status endpoints are reachable.

## 7. Delete And Verify Scoped Cleanup

For the demo resources:

```bash
kubectl -n default delete pod sw-block-demo-reader sw-block-demo-writer --ignore-not-found=true
kubectl -n default delete pvc sw-block-demo-pvc --ignore-not-found=true
```

Verify the PVC and generated workload are gone:

```bash
kubectl -n default get pvc sw-block-demo-pvc
kubectl -n default get deploy -l app=sw-blockvolume
sudo iscsiadm -m session
```

Expected output:

```text
Error from server (NotFound): persistentvolumeclaims "sw-block-demo-pvc" not found
No resources found in default namespace.
iscsiadm: No active sessions.
```

Run inventory again. Empty cluster is a valid result:

```text
inventory_status: ok
volumes: total=0 ok=0 unhealthy=0 invalid=0
issues: none
```

Do not use broad cleanup such as `kubectl delete deploy -A -l
app=sw-blockvolume` in a shared cluster. Product-owned cleanup and PVC
owner-reference cleanup should only affect matching Seaweed Block workloads.

## 8. Failure Collection

If the PVC or generated `blockvolume` exists, collect inventory first:

```bash
sw-block ops inventory \
  --namespace default \
  --master 127.0.0.1:9333 \
  --out "$ARTIFACT_DIR/ops-inventory"
```

If inventory shows a `support_bundle=volumes/<volume>/<replica>` path, inspect
that nested bundle:

```bash
cat "$ARTIFACT_DIR/ops-inventory/volumes/<volume>/<replica>/volume-status-summary.txt"
```

Common issue classes:

```text
generated_deployment_missing
orphan-blockvolume-deploy=<deployment>
blockvolume-process-without-placement=<server>
status_endpoint_unavailable
status_endpoint_unreachable=<addr>
ops_status=unhealthy reason=authority_not_assigned ...
```

If no volume identity was reached, record that explicitly:

```text
ops-status-unavailable: no volume id/status address reached
```

Then attach the install/demo artifact directory anyway. The failure is then in
install, PVC binding, or blockvolume generation rather than replica status.

## 9. Retry After Interruption

If a run is interrupted, use the alpha uninstall before retrying:

```bash
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

Then verify the retry boundary:

```bash
kubectl -n default get pvc sw-block-demo-pvc
kubectl -n default get deploy -l app=sw-blockvolume
sudo iscsiadm -m session
```

Expected:

```text
Error from server (NotFound): persistentvolumeclaims "sw-block-demo-pvc" not found
No resources found in default namespace.
iscsiadm: No active sessions.
```

Then rerun:

```bash
bash scripts/run-k8s-demo.sh "$PWD"
```

## 10. Full Alpha Uninstall

```bash
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

This removes the alpha stack and the demo PVC-scoped generated workload. It
does not claim broad uninstall safety and it does not automatically erase all
persistent state under `/var/lib/sw-block/`.

If you need a clean lab, inspect retained state first and remove it only when
you know no other run owns it:

```bash
sudo find /var/lib/sw-block -maxdepth 3 -type f -o -type d
```

## 11. What To Report

For a useful issue report, include:

- the command that failed,
- `/tmp/sw-block-alpha-build/alpha-images.env`,
- the latest `/tmp/sw-block-app-demo-*` directory,
- `/tmp/sw-block-inventory` if inventory was collected,
- `status-durable-after-blockvolume-restart.json` when using the durable
  restart path,
- whether `sudo iscsiadm -m session` shows any active Seaweed Block session,
- any explicit `ops-status-unavailable` marker.
