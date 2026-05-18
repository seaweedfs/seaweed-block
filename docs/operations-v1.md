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
- same-node RF=1 attach on a multi-node-capable alpha cluster when the app pod
  and generated `blockvolume` are pinned to the same selected node,
- one or more RF=1 PVCs on the supported alpha path,
- product-owned reconciliation of generated `blockvolume` Deployments,
- durable restart of a generated RF=1 `blockvolume` when an explicit
  launcher hostPath is configured,
- read-only cluster inventory with per-replica support bundles,
- RF=2 `best-effort` controlled recovery as a development/TestOps demo profile,
- RF=3 `sync-quorum` mounted recovery through CSI/pod recreate as the Stage 1
  beta recovery target,
- gated RF=3 `sync-quorum` Kubernetes node-loss recovery through CSI/pod
  recreate when the Node-Loss Survival gate artifacts are present,
- scoped cleanup checks for the demo PVC and generated workload.

Not claimed:

- transparent production HA without pod recreate,
- transparent node-loss, physical-host loss, or host-disk failure,
- remote-node attach to a loopback-published `blockvolume`,
- automatic multi-node scheduling, rescheduling, or rebalancing,
- RF=2 quorum HA after primary failure,
- transparent in-place mounted I/O continuation,
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

### Same-Node Placement Control

The alpha iSCSI frontend is loopback by default:

```text
--iscsi-listen=127.0.0.1:<port>
```

That means the app pod must run on the same Kubernetes node as the generated
`blockvolume`. The scripts enforce this on the demo path by rendering writer
and reader pods with a `nodeSelector` matching the selected alpha node.

Default node selection uses the first Kubernetes node. To choose the node
explicitly:

```bash
export SW_BLOCK_ALPHA_NODE_NAME=m02
bash scripts/install-k8s-alpha.sh "$PWD"
```

For the demo, the app node defaults to the same value:

```bash
export SW_BLOCK_ALPHA_NODE_NAME=m02
export SW_BLOCK_DEMO_APP_NODE_NAME=m02
bash scripts/run-alpha-app-demo.sh "$PWD"
```

Keep `SW_BLOCK_DEMO_PIN_APP_NODE=1` for the supported happy path. Setting it to
`0`, or setting `SW_BLOCK_DEMO_APP_NODE_NAME` to a different node while the
frontend is loopback, is a negative-fixture path and should produce an
`unsupported_cross_node_loopback_attach` bundle instead of a timeout.

## 4. Create A First PVC And Prove I/O

The fastest full check is the demo script:

```bash
bash scripts/run-alpha-app-demo.sh "$PWD"
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
grep 'app_node=' "$ARTIFACT_DIR/run.log"
grep 'nodeSelector:' "$ARTIFACT_DIR/demo-app.rendered.yaml"
grep 'nodeSelector:' "$ARTIFACT_DIR/demo-app-reader.rendered.yaml"
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

### AI-Readable Control-Plane Status

The read-only inventory bundle above remains the safest support artifact for
replica-level details. The newer control-plane observation path adds a
product-owned cluster snapshot and timeline that a user, support engineer,
dashboard, CI job, or AI assistant can read without reconstructing events from
TestOps logs.

Port-forward the running `blockmaster` Deployment:

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
```

Then export the master-owned cluster evidence:

```bash
sw-block ops cluster --master-api 127.0.0.1:9333 -o json \
  > /tmp/sw-block-cluster-evidence.json
```

The JSON contains:

- `nodes`: Kubernetes nodes and product server identities known to
  `blockmaster`,
- `volumes`: PVC/volume/replica placement, primary, publish target, epoch, and
  endpoint version,
- `events`: product-owned timeline events with stable reason codes,
  master-minted `event_id`, and `event_time`.

For support, attach `/tmp/sw-block-cluster-evidence.json` together with the
inventory directory. The cluster evidence answers what the control plane
believes happened; the inventory bundle answers what Kubernetes and each
replica reported at collection time.

User-facing commands follow this shape:

```bash
sw-block ops cluster
sw-block ops cluster --master-api 127.0.0.1:9333 -o json
sw-block ops volumes
sw-block ops describe volume <volume-id>
sw-block ops timeline volume <volume-id>
sw-block ops timeline volume <volume-id> -o jsonl
sw-block ops explain volume <volume-id>
sw-block ops report --master-api 127.0.0.1:9333 --out /tmp/sw-block-report
sw-block ops report --from-bundle <bundle-dir> --out /tmp/sw-block-report
sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out /tmp/sw-block-inventory
```

`sw-block ops cluster --master-api` reads the product-owned master API.
`sw-block ops inventory` and bundle-backed `describe`/`timeline`/`explain`
commands remain useful when diagnosing from saved artifacts.
`sw-block ops report` writes a local read-only status page plus JSON/JSONL
artifacts from the same observation core; it is a dashboard-shaped artifact, not
a mutating admin UI.

The long-term design is one shared observation core with three consumers:

- `sw-block ops ...` text for users, support, QA, and AI,
- `sw-block ops ... -o json|jsonl` for automation and support bundles,
- master read-only API for a future dashboard.

The dashboard view should answer five questions without requiring internal
knowledge:

- Which PVC and volume are affected?
- Which replica is primary, and on which Kubernetes node?
- Is the volume `ok`, `degraded`, `recovering`, or `blocked`?
- If recovery happened, which frontend did CSI attach to before and after?
- What is the next operator action?

Example healthy shape:

```text
volume pvc-... status=ok rf=3 ack=sync-quorum
pvc default/mysql-data
primary r1 on m01 frontend=192.168.1.181:3260
replicas desired=3 observed=3
r1 m01 primary ready durable_lsn=44
r2 m02 replica_ready candidate_ready=true durable_lsn=44
r3 tp01 replica_ready candidate_ready=true durable_lsn=44
next action: none
```

Example recovering shape:

```text
volume pvc-... status=recovering reason=primary_node_lost
old primary r1 on m01 unavailable
promoted primary r2 on m02 epoch=2 endpoint_version=1
CSI target changed 192.168.1.181:3260 -> 192.168.1.184:3260
reattach method: pod_recreate
next action: wait for app pod readiness, then collect support bundle if stuck
```

Example blocked shape:

```text
volume pvc-... status=blocked reason=no_promotion_ready_candidate
r2 candidate_ready=false reason=durable_frontier_missing
r3 candidate_ready=false reason=candidate_frontier_behind
next action: collect support bundle; do not force promote without support review
```

Example attach/install blocked shape:

```text
volume pvc-... status=blocked reason=csi_node_image_pull_failed
node m02 missing image sw-block-csi:local
pod kube-system/sw-block-csi-node-... waiting=ImagePullBackOff
impact: PVC attach cannot proceed on workloads scheduled to m02
next action: import the image to m02 or use a registry reachable by all nodes
```

The first dashboard and AI assistant path must stay read-only. It should not
expose promote, repair, rebuild, backup, restore, or cleanup buttons until
those actions have separate strict gates.

Support bundles for attach/install failures should include Kubernetes runtime
evidence in addition to Seaweed Block inventory: `kubectl get pods -A -o wide`,
recent namespace events, `kubectl describe pod` for `sw-blockmaster`,
`sw-block-csi-*`, generated `sw-blockvolume` pods, CSI logs, blockmaster logs,
and per-node product image presence.

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
observed_replicas=0 desired_replicas=2
replica_slot_missing=unknown
orphan-blockvolume-deploy=<deployment>
blockvolume-process-without-placement=<server>
status_endpoint_unavailable
status_endpoint_unreachable=<addr>
unsupported_cross_node_loopback_attach
ops_status=unhealthy reason=authority_not_assigned ...
```

### Mounted Recovery ACK Profiles

Stage 1 mounted recovery has two validated ACK profiles. Keep them separate
when reporting results or setting user expectations.

| Profile | Claim label | Validated meaning |
| --- | --- | --- |
| RF=2 `best-effort` | `controlled-best-effort-demo` | Development/TestOps recovery demonstration. It proves the mounted app path, scoped primary stop, master promotion, CSI/pod recreate, and reader checksum for the gated bytes. It is not a quorum durability claim. |
| RF=3 `sync-quorum` | `beta-recovery` | Stage 1 durable mounted recovery target. It proves candidate promotion only after the sync ACK frontier is covered, single-primary authority publication, CSI/pod recreate re-stage to the promoted frontend, and post-failure reader checksum. |

Stage 1 recovery still uses CSI/node reattach through pod recreate. It does not
claim transparent in-place I/O continuation. Transparent host-path switching
requires the later multipath line: iSCSI ALUA plus dm-multipath, or NVMe ANA
plus native multipath.

Reserved Stage 2 claim profile:

| Profile | Claim label | Validation status |
| --- | --- | --- |
| RF=3 `sync-quorum` + iSCSI ALUA | `stage2-iscsi-alua-multipath` | Reserved for the dm-multipath path where the mounted workload stays in place and recovery is proven through ALUA path-state evidence plus a post-failure data check. It is not valid with `best-effort`; the close gate must prove it before it becomes an external recovery claim. |

#### Stage 2 iSCSI ALUA / dm-multipath Mounted Failover

Stage 2 is the transparent mounted-host-path recovery line. It is different
from Stage 1: the workload pod is not recreated, CSI does not re-stage the
volume, and the Linux host must keep one dm-multipath device online while ALUA
path state moves from the failed primary path to the promoted path.

The validated Stage 2 contract is:

```text
protocol=iscsi
host_multipath=dm-multipath
ack_profile=sync-quorum
claim_profile=stage2-iscsi-alua-multipath
replication=RF3
recovery=mounted workload verifies data without pod recreate
```

Host prerequisites for this path:

```text
iscsiadm present
multipath present
sg_rtpg or equivalent RTPG reader present
sg_inq present
kernel modules: iscsi_tcp, dm_multipath, scsi_dh_alua
udev device state available under /run/udev
```

The support bundle must show:

```text
pod_recreate_used=false
transparent_failover_claimed=true
before_primary_replica=<rN>
promoted_replica=<rM>
post_failure_primary_count=1
old_primary_stale_io_success_count=0
data_check_after_failover=mounted_workload_checksum_passed
bounded_waits=pass
```

Host-path evidence must include one shared iSCSI IQN with multiple portals, one
dm-multipath device with multiple paths, and RTPG/ALUA state showing the
primary path changed after promotion. A recovery that only succeeds after pod
recreate belongs to Stage 1 and must not be reported as Stage 2.

Stage 2 still does not claim node loss, NVMe ANA Kubernetes recovery, Windows
MPIO, broad distro compatibility, performance/RTO SLOs, or automatic
repair/rebuild/failback.

#### Stage 3 Node-Loss Survival Gate

Stage 3 is the Kubernetes node-loss recovery line. It is different from Stage
2: the first node-loss close uses CSI/pod recreate reattach, not transparent
mounted I/O continuation. The storage control plane must promote a surviving
RF=3 `sync-quorum` replica, and the replacement pod must attach to the promoted
non-loopback frontend on a surviving Kubernetes node.

The active gate contract is:

```text
protocol=iscsi
replication=RF3
ack_profile=sync-quorum
frontends=non-loopback
topology=3 Kubernetes nodes; physical-host/fault-domain sharing disclosed
failure=controlled primary Kubernetes node loss or equivalent node isolation
recovery=CSI/pod recreate reattach to surviving promoted frontend
transparent_failover_claimed=false
node_loss_recovery_claimed=true
```

The current lab validation is TCP/iSCSI over the LAN (`192.168.1.x`). It does
not use the RoCE/RDMA fabric (`10.0.0.x`), and it does not support NVMe/RDMA or
performance claims.

The support bundle must show:

```text
node-loss-recovery-summary.txt
replicas_on_distinct_nodes=true
frontends_non_loopback=true
before_primary_replica=<rN>
before_primary_node=<node-a>
failed_replica=<same rN>
failed_node=<same node-a>
promoted_replica=<rM>
promoted_replica_node=<node-b>
post_failure_primary_count=1
before_publish_target_frontend=<node-a-ip:port>
after_publish_target_frontend=<node-b-ip:port>
pod_recreate_used=true
transparent_failover_claimed=false
node_loss_recovery_claimed=true
data_check_after_node_loss=reader_checksum_passed
old_primary_stale_io_success_count=0
bounded_waits=pass
physical_host_loss_claimed=false
```

This gate does not claim transparent node-loss, NVMe ANA node-loss, arbitrary
network partition tolerance, rebuild/failback, RTO/SLO, or production HA
outside the tested topology. If three Kubernetes nodes share fewer physical
machines, the report must keep `physical_host_loss_claimed=false`; that is a
Kubernetes-node-loss proof, not a full physical-host-loss proof.

#### RF=2 Mounted Failover Status

RF=2 Kubernetes lifecycle has three validated alpha boundaries:

1. On the default single-logical-server alpha topology, an RF=2 PVC is refused
   safely. The PVC may bind, but the product must not launch a partial
   one-replica `blockvolume` and call it healthy.
2. On the development/TestOps two-logical-server alpha topology, the mounted
   app path can write and read through an RF=2 PVC while inventory sees two
   generated replicas. Controlled primary failure is still a safe-refusal path
   when the peer replica is not promotion-ready.
3. On the same development/TestOps topology with `ack_profile=best-effort` and
   `claim_profile=controlled-best-effort-demo`, controlled primary failure can
   promote a ready peer and recover through CSI/pod recreate with a reader
   checksum. This is a demo profile only; it must not be described as quorum
   HA.

The safe-refusal behavior remains valid when the peer is not promotion-ready:

```text
writer writes and verifies data
before-failure inventory identifies primary=r1
r2 is visible but not promotion-ready
r1 blockvolume is stopped in a scoped way
product emits failover_status: refused
data_check_after_failover=not_claimed
reason=candidate_not_ready_for_primary
```

The product must not start a replacement reader and claim checksum recovery
unless a separate gate proves a ready candidate, authority movement, and
reattach/readback.

Default single-logical-server refusal shape:

The supported operator check is inventory:

```text
inventory_status: unhealthy
volume: ... rf=2 desired=2 observed=0 ...
issues:
- volume ... generated_deployment_missing
- volume ... observed_replicas=0 desired_replicas=2
- volume ... replica_slot_missing=unknown
```

That means the product preserved the requested RF=2 intent and refused to create
an unsafe partial failover topology. It is not a recovery success; it is an
honest safe-refusal state.

For development and TestOps only, the alpha installer can opt into two logical
Seaweed Block server identities on the same Kubernetes node:

```bash
SW_BLOCK_ALPHA_LOGICAL_SERVERS=2 \
SW_BLOCK_ALPHA_EXPECTED_SLOTS_PER_VOLUME=2 \
  bash scripts/install-k8s-alpha.sh "$PWD"
```

This is a placement/failover-lab shape, not a node-failure HA shape. The
generated `blockvolume` pods are scheduled to the same Kubernetes node, while
their `server_id`s, data/control ports, frontend ports, and status ports remain
distinct. It is useful for proving replica placement and process-level failover
mechanics before broad multi-node scheduling exists.

In that two-logical-server shape, the currently validated RF=2 primary-failure
safe-refusal evidence is:

```text
failover_status: refused
ack_profile: best-effort
failure_class=primary-blockvolume-controlled-stop
before_primary_replica=r1
failed_replica=r1
candidate_ready=false
candidate_evidence=replica: ... replica=r2 ... replication=not_ready ...
data_check_after_failover=not_claimed
reason=candidate_not_ready_for_primary
target_ready_replicas=0
after_issue_evidence=- volume ... replica_degraded=...
```

Read this as: the writer path worked before the failure; the primary stop was
scoped and observable; the peer was not ready to become primary; recovery was
not claimed.

The RF=2 controlled recovery demo shape, when the peer is promotion-ready, is:

```text
ack_profile: best-effort
claim_profile=controlled-best-effort-demo
failover_status: promotion_pending -> promoted
data_check_after_failover: pending_reader -> reader_checksum_passed
reader_verified: true
```

Read this as: the exact gated demo bytes were recovered after promotion and
pod recreate. Do not read it as a sync-quorum or sync-all durability guarantee.

#### RF=3 Sync-Quorum Mounted Recovery Status

RF=3 `sync-quorum` is the Stage 1 beta-facing durable recovery profile. The
validated path is:

```text
writer writes and verifies data
inventory identifies primary=r1 and promotion-ready candidate=r2
candidate frontier covers the required sync ACK frontier
r1 blockvolume is stopped in a scoped way
master publishes r2 as the only primary
CSI/node re-stages the recreated pod to r2's frontend
reader verifies /data/demo.bin after failure
```

The expected recovery marker is:

```text
ack_profile: sync-quorum
claim_profile=beta-recovery
failover_status: promotion_pending -> promoted
before_primary_replica=r1
failed_replica=r1
promoted_replica=r2
frontier_covered=true
post_failure_primary_count=1
data_check_after_failover: pending_reader -> reader_checksum_passed
reader_verified=true
```

This is a Stage 1 Kubernetes mounted recovery claim, not a transparent
multipath claim. The reader succeeds after pod recreate / CSI re-stage, not by
continuing in-place I/O through the original mounted path.

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
bash scripts/run-alpha-app-demo.sh "$PWD"
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
