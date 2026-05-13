# Kubernetes Quick Start

This guide is for Kubernetes users who already have a test cluster and want to
see what the current alpha can do.

For the longer install, inventory, cleanup, retry, and support-bundle path, see
[`operations-v1.md`](operations-v1.md).

The current demo path is intentionally small:

```text
app pod -> PVC -> CSI -> iSCSI -> blockvolume -> WAL-backed storage
```

It proves that a normal application pod can mount a `seaweed-block` PVC, write
data, exit, and then a second pod can mount the same PVC and read the data back.
It does not claim production durability or failover-under-mount yet. For the
explicit blockvolume restart durability path, see
[`operations-v1.md`](operations-v1.md#5-prove-durable-blockvolume-restart).

## What Runs

The demo deploys:

- `blockmaster`: the control-plane service that tracks desired volumes and
  publishes assignments.
- `block-csi` controller: the CSI controller plugin plus Kubernetes CSI
  sidecars.
- `block-csi` node: the privileged CSI node plugin that runs `iscsiadm` and
  mounts the device for kubelet.
- one launcher-generated `blockvolume` Deployment for the PVC.
- two ordinary BusyBox app pods: writer first, reader second.

The writer and reader are not simultaneous. The PVC is `ReadWriteOnce`; the demo
uses pod replacement to show that data is on the volume, not just inside the
first pod.

## First Volume In 10 Minutes

This quick start is intentionally a same-node Kubernetes alpha path. On a
single-node cluster that is automatic. On a multi-node-capable cluster, the app
pod and generated `blockvolume` must be pinned to the same selected node because
the alpha iSCSI frontend is loopback (`127.0.0.1:<port>`). Use it to prove the
first-volume workflow, not production readiness.

Recommended path: local k3s build. This avoids relying on public GHCR alpha
packages and records the exact image IDs used by the cluster.

```bash
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

# Optional. Defaults to the first Kubernetes node.
export SW_BLOCK_ALPHA_NODE_NAME="${SW_BLOCK_ALPHA_NODE_NAME:-m02}"
export SW_BLOCK_DEMO_APP_NODE_NAME="${SW_BLOCK_DEMO_APP_NODE_NAME:-$SW_BLOCK_ALPHA_NODE_NAME}"

bash scripts/preflight-k8s-alpha.sh --local-k3s

SW_BLOCK_IMPORT_K3S=1 \
SW_BLOCK_ARTIFACT_DIR=/tmp/sw-block-alpha-build \
  bash scripts/build-alpha-images.sh "$PWD"

bash scripts/run-k8s-demo.sh "$PWD"
```

Expected final line:

```text
[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete
```

This default demo proves PVC write/read through pod replacement. To also prove
that the generated `blockvolume` can restart and reattach with data intact, use
the durable restart workflow in
[`operations-v1.md`](operations-v1.md#5-prove-durable-blockvolume-restart).

The preflight command emits structured lines:

```text
[preflight] checked name=kubectl status=PASS ...
[preflight] checked name=iscsiadm status=PASS ...
[preflight] unchecked name=ghcr_pull reason="local-k3s path selected"
[preflight] summary status=PASS checked=... failed=0 unchecked=... mode=local-k3s
```

If any preflight command fails, fix that first. Otherwise the storage failure
will be mixed with environment setup failure.

Boundary checks:

```bash
ARTIFACT_DIR="$(ls -td /tmp/sw-block-app-demo-* | head -1)"

grep 'PASS:' "$ARTIFACT_DIR/run.log"
grep '/data/demo.bin: OK' "$ARTIFACT_DIR/writer.log"
grep '/data/demo.bin: OK' "$ARTIFACT_DIR/reader.log"
grep -- '--volume-id=' "$ARTIFACT_DIR/generated-blockvolume.yaml"
grep 'app_node=' "$ARTIFACT_DIR/run.log"
grep 'nodeSelector:' "$ARTIFACT_DIR/demo-app.rendered.yaml"
grep 'nodeSelector:' "$ARTIFACT_DIR/demo-app-reader.rendered.yaml"
cat "$ARTIFACT_DIR/app-storage.after-delete.txt"
cat "$ARTIFACT_DIR/iscsi-sessions.after-delete.txt"
```

If you want to watch each boundary directly while the demo is running, use
these checks. The expected output is intentionally small and copyable:

| Boundary | Command | Expected output line |
|---|---|---|
| CSI controller Ready | `kubectl -n kube-system get deploy sw-block-csi-controller -o jsonpath='{.status.readyReplicas}/{.status.replicas}{"\n"}'` | `1/1` |
| CSI node DaemonSet rolled out | `kubectl -n kube-system rollout status ds/sw-block-csi-node` | `daemon set "sw-block-csi-node" successfully rolled out` |
| StorageClass present | `kubectl get sc sw-block-dynamic -o jsonpath='{.provisioner}{"\n"}'` | `block.csi.seaweedfs.com` |
| PVC Bound | `kubectl get pvc sw-block-demo-pvc -o jsonpath='{.status.phase}{"\n"}'` | `Bound` |
| Generated blockvolume Deployment Ready | `kubectl -n default get deploy -l app=sw-blockvolume -o jsonpath='{.items[0].status.readyReplicas}/{.items[0].status.replicas}{"\n"}'` | `1/1` |
| Writer pod Succeeded | `kubectl get pod sw-block-demo-writer -o jsonpath='{.status.phase}{"\n"}'` | `Succeeded` |
| Reader checksum OK | `kubectl logs sw-block-demo-reader` | `/data/demo.bin: OK` |
| Delete returned clean | `kubectl get pvc sw-block-demo-pvc` | `Error from server (NotFound): persistentvolumeclaims "sw-block-demo-pvc" not found` |
| Residue absent | `sudo iscsiadm -m session` | `iscsiadm: No active sessions.` |

The build artifact directory records image IDs and binary versions. The demo
artifact directory records the first-volume evidence.

```bash
cat /tmp/sw-block-alpha-build/alpha-images.env
ARTIFACT_DIR="$(ls -td /tmp/sw-block-app-demo-* | head -1)"
cat "$ARTIFACT_DIR/run.log"
```

## Inspect The Cluster

After the alpha stack is installed, use the read-only inventory command to see
the Seaweed Block volumes Kubernetes can currently discover:

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

The command does not need a TestOps artifact directory or a known volume id. It
discovers PVC/PV ownership and generated `blockvolume` Deployments from
Kubernetes. When `--master` is supplied and a replica has `--status-addr`, it
also writes that replica's normal `sw-block ops status` bundle under
`/tmp/sw-block-inventory/volumes/<volume_id>/<replica_id>/`.

Expected one-volume shape while the demo PVC is live. The lifecycle fields show
whether the generated `blockvolume` Deployment is owned by the PVC-owner-ref
path or a legacy launcher-managed fallback:

```text
inventory_status: ok
volumes: total=1 ok=1 unhealthy=0 invalid=0
volume: id=pvc-... namespace=default pvc=sw-block-demo-pvc pv=pvc-... rf=1 desired=1 observed=1 primary=r1 status=ok protocols=iscsi replicas=1
replica: volume=pvc-... replica=r1 server=... node=... observed=true status=ok lifecycle_owner=pvc-owner-ref owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc role=primary replication=none healthy=true ... frontend=127.0.0.1:3260 status_addr=127.0.0.1:... support_bundle=volumes/pvc-.../r1
issues: none
artifacts: volume-inventory.json volume-inventory-summary.txt ops-inventory-bundle.json
```

Expected empty shape after the demo PVC is deleted:

```text
inventory_status: ok
volumes: total=0 ok=0 unhealthy=0 invalid=0
issues: none
```

Expected partial/failure shape if the PVC exists but no generated
`blockvolume` Deployment is present:

```text
inventory_status: unhealthy
volume: id=pvc-... namespace=default pvc=<name> ... observed=0 primary=unavailable status=unhealthy protocols= replicas=0
issues:
- volume pvc-... generated_deployment_missing
```

Expected residue shape if a generated `blockvolume` Deployment remains after
its PVC/PV disappeared:

```text
inventory_status: unhealthy
volume: id=pvc-... namespace=default pvc=unavailable pv=unavailable ... observed=1 status=unhealthy protocols=iscsi replicas=1
issues:
- volume pvc-... orphan-blockvolume-deploy=sw-blockvolume-pvc-...-r1
- volume pvc-... heartbeat-without-placement=<server> state=unadmitted-by-master reason=no-matching-pvc-or-pv
```

Expected residue shape if a host-local `blockvolume` process exists without a
matching PVC/PV placement:

```text
inventory_status: unhealthy
volume: id=pvc-... namespace=default pvc=unavailable pv=unavailable ... observed=1 status=unhealthy
issues:
- volume pvc-... blockvolume-process-without-placement=<server>
- volume pvc-... heartbeat-without-placement=<server> state=unadmitted-by-master reason=local-process-without-pvc-or-pv
```

If a replica pod is observed as ready but the nested product status has not
confirmed authority assignment yet, inventory should name that as degraded
readiness, not as a contradictory health failure:

```text
replica: volume=pvc-... replica=r1 ... observed=true status=unhealthy role=primary healthy=true epoch=0 endpoint_version=0 ...
issues:
- volume pvc-... replica_degraded=r1 status=unhealthy
- volume pvc-... replica r1 ops_status=unhealthy reason=authority_not_assigned assigned=false epoch=0 endpoint_version=0
```

Attach the whole inventory directory when filing an issue. The top-level
inventory answers "what exists?", while each nested replica status bundle
answers "what did this replica report?" using the same schema as
`sw-block ops status`.

Inventory non-claims are explicit in every bundle:

- `read-only-observation`: inventory does not mutate product state.
- `single-cluster-alpha-scope`: discovery is scoped to one alpha Kubernetes cluster.
- `best-effort-partial-discovery`: missing inputs are reported as issues or unchecked evidence, not inferred as healthy.
- `no-mutating-admin`: inventory is not repair, cleanup, failover, backup, or restore.
- `no-multi-node-scheduling`: inventory observes placement; it does not schedule or rebalance replicas.
- `rf2-rf3-live-kubernetes-operation`: live RF=2/RF=3 Kubernetes operation is not claimed unless a runner gate explicitly proves it.

## Alternate Image Paths

Use these only after the local k3s path is understood.

### Published Alpha Images

Use this when your cluster can pull public images from GHCR:

```bash
bash scripts/preflight-k8s-alpha.sh --ghcr
bash scripts/run-k8s-demo-ghcr.sh "$PWD"
```

The script uses:

```text
ghcr.io/seaweedfs/seaweed-block:alpha
ghcr.io/seaweedfs/seaweed-block-csi:alpha
```

If the images are not public or your cluster cannot pull them, app pods or
Seaweed Block pods may show `ImagePullBackOff`. Inspect with:

```bash
kubectl -n kube-system describe pod -l app=sw-blockmaster
kubectl -n kube-system describe pod -l app=sw-block-csi-controller
```

Remediation: use the local k3s path above, or push images to a registry your
cluster can pull.

### Existing Cluster With Your Registry

Use this when your cluster pulls images from a registry.

```bash
export SW_BLOCK_IMAGE=registry.example.com/storage/sw-block:alpha
export SW_BLOCK_CSI_IMAGE=registry.example.com/storage/sw-block-csi:alpha

bash scripts/build-alpha-images.sh "$PWD"
docker push "$SW_BLOCK_IMAGE"
docker push "$SW_BLOCK_CSI_IMAGE"

bash scripts/run-k8s-demo.sh "$PWD"
```

The runner renders the Kubernetes manifests with those image names before
applying them. The source manifests stay simple and still default to
`sw-block:local` for local labs.

## First-Volume Evidence Ladder

A successful run should show the same ladder regardless of which image path you
used:

```text
preflight ok
blockmaster available
CSI controller available
CSI node DaemonSet rolled out
StorageClass/PVC applied
blockvolume manifest generated by blockmaster
blockvolume Deployment available
writer pod checksum OK
reader replacement pod checksum OK
PVC deleted
generated blockvolume cleaned up
no sw-block iSCSI session remains
```

The demo records the useful files under `/tmp/sw-block-app-demo-*`:

| Artifact | What it proves |
|---|---|
| `run.log` | Phase progress and final PASS line. |
| `apply-block-stack.log` | Control-plane manifest applied. |
| `apply-csi-controller.log`, `apply-csi-node.log` | CSI components applied. |
| `generated-blockvolume.yaml` | Master generated the per-PVC blockvolume workload. |
| `demo-app.rendered.yaml` | Writer pod rendered with the selected app-node `nodeSelector`. |
| `demo-app-reader.rendered.yaml` | Reader pod rendered with the selected app-node `nodeSelector`. |
| `apply-generated-blockvolume.log` | Generated blockvolume workload was reconciled by blockmaster, or applied manually when the fallback env is enabled. |
| `writer.log` | First app pod wrote and verified `/data/demo.bin`. |
| `reader.log` | Replacement app pod read and verified the same data. |
| `status-durable-after-blockvolume-restart.json` | Present only in the durable restart workflow; durable entry was latched and operational after generated `blockvolume` restart. |
| `app-storage.txt` | StorageClass/PV/PVC/pod state during the run. |
| `iscsi-sessions.after-reader.txt` | Host iSCSI session state after the replacement reader verifies data. |
| `app-storage.after-delete.txt` | App/PVC state after delete. |
| `blockvolume-namespace-pods-deploys.after-delete.txt` | Generated blockvolume cleanup evidence. |
| `iscsi-sessions.after-delete.txt` | Host iSCSI session residue check. |
| `cleanup.log` | Test guardrail cleanup actions. |

Cleanup attribution:

```text
pvc:sw-block-demo-pvc state=deleted deleted_by=demo-script-kubectl-delete evidence=demo/delete-pvc.log
blockmaster-manifest:<volume-id> state=removed waited_by=demo-script-after-DeleteVolume evidence=demo/poll.log
blockvolume-deploy:<name> namespace=default state=deleted deleted_by=pvc-owner-ref-or-demo-guard evidence=demo/blockvolume-namespace-pods-deploys.after-delete.txt
iscsi-session:<iqn> state=absent released_by=csi-node-unstage evidence=demo/iscsi-sessions.after-delete.txt
iscsi-node-db:<iqn> state=present_before_guardrail cleaned_by=testops-guardrail evidence=iscsi-nodes.after-demo.txt
```

Product/Kubernetes cleanup and TestOps guardrail cleanup are intentionally
separate. An active iSCSI session must be gone after delete. A non-active iSCSI
node database entry may remain until guardrail cleanup removes it.

## What To Show In A Demo

The most understandable demo is:

```bash
bash scripts/run-k8s-demo.sh "$PWD"
```

Then show these artifacts:

```bash
cat /tmp/sw-block-app-demo-*/writer.log
cat /tmp/sw-block-app-demo-*/reader.log
sudo iscsiadm -m session || true
kubectl get all -A | grep sw-block || true
```

The proof is that both app logs contain:

```text
/data/demo.bin: OK
```

The first pod wrote the file. The second pod mounted the same PVC later and
verified it.

## If The Demo Fails

Start with the artifact directory printed in `run.log`:

```bash
ARTIFACT_DIR="$(ls -td /tmp/sw-block-app-demo-* | head -1)"
cat "$ARTIFACT_DIR/run.log"
ls "$ARTIFACT_DIR"
```

Common boundary checks:

```bash
kubectl -n kube-system get pods,deploy -o wide
kubectl get sc,pv,pvc,pod -o wide
cat "$ARTIFACT_DIR/app-storage.txt" 2>/dev/null || true
cat "$ARTIFACT_DIR/generated-blockvolume.err" 2>/dev/null || true
cat "$ARTIFACT_DIR/blockmaster.log" 2>/dev/null || true
cat "$ARTIFACT_DIR/blockvolume-generated.log" 2>/dev/null || true
```

If a `volume-id` and blockvolume status address are available from
`generated-blockvolume.yaml`, collect a product status bundle:

```bash
VOLUME_ID="$(sed -n 's/.*--volume-id=\([^"[:space:]]*\).*/\1/p' "$ARTIFACT_DIR/generated-blockvolume.yaml" | head -1)"
STATUS_ADDR="$(sed -n 's/.*--status-addr=\([^"[:space:]]*\).*/\1/p' "$ARTIFACT_DIR/generated-blockvolume.yaml" | head -1)"
MASTER_ADDR="${SW_BLOCK_MASTER_ADDR:-127.0.0.1:9333}"

if [ -n "$VOLUME_ID" ] && [ -n "$STATUS_ADDR" ]; then
  sw-block ops status \
    --volume "$VOLUME_ID" \
    --master "$MASTER_ADDR" \
    --status-addr "$STATUS_ADDR" \
    --out "$ARTIFACT_DIR/ops-status"
else
  echo "ops-status-unavailable: no volume id/status address reached"
fi
```

On a local single-node k3s lab, `MASTER_ADDR=127.0.0.1:9333` works only when
the blockmaster service is reachable from the shell where `sw-block` runs. If it
is not reachable, port-forward it first in another terminal:

```bash
kubectl -n kube-system port-forward svc/blockmaster 9333:9333
```

Attach the whole artifact directory when filing an issue. If no volume identity
was reached, attach the directory anyway; the useful boundary is then install,
PVC, or manifest generation rather than data-path status.

If the cluster is still reachable, also collect the inventory bundle:

```bash
sw-block ops inventory \
  --namespace default \
  --master 127.0.0.1:9333 \
  --out "$ARTIFACT_DIR/ops-inventory"
```

## Use Your Own App

Install the alpha stack:

```bash
bash scripts/install-k8s-alpha.sh "$PWD"
```

Create a StorageClass and PVC:

```bash
kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml
```

The installed blockmaster owns the generated `blockvolume` workload. Wait for
it to reconcile the Deployment:

```bash
kubectl get deploy -n default -l app=sw-blockvolume -o jsonpath='{.items[0].status.readyReplicas}/{.items[0].status.replicas}{"\n"}'
```

Expected output:

```text
1/1
```

If you are running an older alpha stack without product-owned reconciliation,
the internal fallback remains:

```bash
bash scripts/apply-k8s-alpha-blockvolumes.sh
```

After the fallback apply, verify the generated workload is Ready:

```bash
kubectl get deploy -n default -l app=sw-blockvolume -o jsonpath='{.items[0].status.readyReplicas}/{.items[0].status.replicas}{"\n"}'
```

Then start an app pod that mounts the PVC:

```bash
kubectl apply -f examples/kubernetes/basic-app/writer-pod.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-example-writer --timeout=240s
kubectl logs sw-block-example-writer
```

To prove a replacement pod can read the same PVC:

```bash
kubectl delete pod sw-block-example-writer
kubectl apply -f examples/kubernetes/basic-app/reader-pod.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-example-reader --timeout=240s
kubectl logs sw-block-example-reader
```

For the copyable YAML, see
[`examples/kubernetes/basic-app`](../examples/kubernetes/basic-app).

## Current Alpha Limitations

- The generated `blockvolume` workload is reconciled by blockmaster on the
  supported alpha path. This is a small product-owned lifecycle loop, not a
  full production operator.
- By default, generated `blockvolume` Deployments carry a PVC owner reference,
  so Kubernetes garbage collection removes them after the PVC is deleted.
- The demo uses same-node loopback attach. Remote-node attach to a
  loopback-published `blockvolume` is not claimed.
- The default quickstart is a first-volume path. Durable generated
  `blockvolume` restart is supported only when
  `SW_BLOCK_LAUNCHER_STATE_HOSTPATH` is configured as described in
  `operations-v1.md`; it is not a production durability, node-loss, backup, or
  restore claim.
- Failover while a PVC remains mounted is not claimed.
- NVMe-oF is not part of this alpha path.
- Operator-grade reconciliation is not claimed.
- Upgrade and uninstall safety are not claimed.
- Performance numbers from this demo are not a product SLO.

## Cleanup

The runner performs cleanup automatically and records artifacts under
`/tmp/sw-block-app-demo-*`.

If you need to clean up manually:

```bash
kubectl -n default delete pod sw-block-demo-writer sw-block-demo-reader --ignore-not-found=true
kubectl -n default delete pvc sw-block-demo-pvc --ignore-not-found=true
kubectl -n default delete deploy -l app=sw-blockvolume --ignore-not-found=true
kubectl delete -f deploy/k8s/alpha/csi-driver.yaml --ignore-not-found=true
kubectl delete -f deploy/k8s/alpha/rbac.yaml --ignore-not-found=true
kubectl delete -f /tmp/sw-block-stack.yaml --ignore-not-found=true
sudo iscsiadm -m session || true
```

Do not use global cleanup commands such as `kubectl delete deploy -A -l
app=sw-blockvolume` in a shared cluster. Broad sweeps are TestOps guardrails,
not user-facing cleanup.

For a full alpha stack uninstall:

```bash
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

This removes the demo app resources, the demo PVC-scoped generated
`blockvolume`, CSI resources, and the blockmaster stack. It does not broadly
delete every `app=sw-blockvolume` Deployment unless
`SW_BLOCK_UNINSTALL_DELETE_ALL_BLOCKVOLUMES=1` is set by a TestOps guardrail.

Some alpha or test paths can leave persistent blockvolume state under
`/var/lib/sw-block/`. This quickstart does not claim upgrade or uninstall
safety; remove those paths only when you need a clean lab and know no other run
owns that state.

## Retry After A Partial Run

If you interrupt the demo after the PVC or generated `blockvolume` appears,
clean up before retrying:

```bash
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

Then verify the retry boundary is clean:

```bash
kubectl -n default get pvc sw-block-demo-pvc
kubectl -n default get deploy -l app=sw-blockvolume
sudo iscsiadm -m session
```

Expected result:

```text
Error from server (NotFound): persistentvolumeclaims "sw-block-demo-pvc" not found
No resources found in default namespace.
iscsiadm: No active sessions.
```

After that, rerun the same demo command:

```bash
bash scripts/run-k8s-demo.sh "$PWD"
```

The retry behavior is gated by
`testops/scenarios/light-use-first-volume-retry-chain.yaml`: it intentionally
stops after the generated `blockvolume` is ready, leaves the PVC and
`blockvolume` Deployment in place, runs the uninstall path, and then proves the
same first-volume demo passes on retry.

The scripted demo applies rendered manifests under its artifact directory. If
you used custom image names, delete those rendered files instead of the source
YAML:

```bash
kubectl delete -f "$ARTIFACT_DIR/csi-controller.rendered.yaml" --ignore-not-found=true
kubectl delete -f "$ARTIFACT_DIR/csi-node.rendered.yaml" --ignore-not-found=true
```
