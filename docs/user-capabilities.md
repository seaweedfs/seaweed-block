# Seaweed Block User Capabilities

This page describes what a user can do with the current Seaweed Block alpha,
what evidence is available, and what is still outside the product boundary.
For the exact release evidence, see [`releases/README.md`](releases/README.md).

## Primary User Loop

The supported alpha loop is:

```text
Helm install
-> create a Kubernetes PVC
-> mount the PVC in a writer pod
-> verify the same data from a replacement reader pod
-> inspect status/report/dashboard/support evidence
-> uninstall and verify zero residue
```

The quickstart command path is in
[`quickstart-kubernetes.md`](quickstart-kubernetes.md).

## Core Capabilities

### Day-1 Helm Install And First PVC

Users can generate Helm values from the current Kubernetes cluster, install the
Seaweed Block alpha stack, create the first PVC, run a writer pod, run a
replacement reader pod, and verify persisted data.

Expected summary fields:

```text
first_volume_status=ok
writer_verified=true
reader_verified=true
inventory_status=ok
status_report=status/report/index.html
cleanup_status=ok
```

### Standard Kubernetes PVC Workflow

The normal volume creation path is Kubernetes PVC creation:

```text
kubectl apply -f pvc.yaml
-> CSI dynamic provisioning
-> iSCSI target
-> app pod mounts the PVC through persistentVolumeClaim
```

The current default filesystem path is ext4 through the CSI mount flow.

### Multi-Volume Lab Path

The gated lab path validates three RF=3 PVC-backed volumes at the same time:

- three PVCs bind and mount,
- each volume has its own primary and publish target,
- ManagedVolume reports each volume independently,
- cross-volume authority mixups are rejected by the gate.

This is a lab-gated capability, not a broad scale claim.

### Recovery And Failover Gates

The current alpha has gated recovery evidence for:

- CSI reattach recovery: primary blockvolume stops, authority promotes a
  surviving replica, a replacement workload pod reads the same PVC data.
- Mounted transparent failover on the proven Stage-2 path: the same mounted
  writer pod continues through iSCSI ALUA/dm-multipath when the primary path
  fails.
- Interleaved multi-volume failover: two volumes fail over independently while
  an untouched third volume remains stable.

The broad claim remains narrow: these are supported lab gates, not production
HA or broad RTO/SLO commitments.

### Restart Persistence

With the hostPath-backed alpha persistence path, QA gates verify:

- data written before restart remains readable after k3s/product restart,
- a promoted RF=3 primary does not roll back to an old primary after restart,
- epoch and publish target do not regress,
- multiple volumes remain distinct after restart.

## Read-Only Operations

Port-forward blockmaster when reading live state:

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
```

| Command | Use |
|---|---|
| `sw-block ops cluster --master-api 127.0.0.1:9333 -o json` | Cluster snapshot. |
| `sw-block ops volumes --master-api 127.0.0.1:9333` | Volume list. |
| `sw-block ops status --volume <id> --master <addr> --status-addr <addr> --out <dir>` | Focused live status bundle. |
| `sw-block ops describe volume <id> --namespace default --master 127.0.0.1:9333` | Volume detail. |
| `sw-block ops timeline volume --from-bundle <dir> <id> -o jsonl` | Volume event timeline from a bundle. |
| `sw-block ops explain volume --from-bundle <dir> <id>` | Human-readable explanation and reason code. |
| `sw-block ops report --master-api 127.0.0.1:9333 --out <dir>` | Static HTML/JSON/text report. |
| `sw-block ops dashboard --master-api 127.0.0.1:9333 --listen 127.0.0.1:9334` | Local read-only dashboard. |
| `sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out <dir>` | Replica-level support inventory. |

Report output includes:

```text
index.html
summary.txt
cluster-evidence.json
timeline.jsonl
operator-snapshot.json
```

`operator-snapshot.json` is a read-only status projection for future operator
work. It is not a running controller manager and it does not permit mutations.

## Support Bundle Replay

Support artifacts can be collected and replayed offline:

```bash
bash scripts/collect-helm-support-bundle.sh "$PWD"
sw-block ops report --from-bundle <bundle-or-artifact-dir> --out /tmp/sw-block-report
sw-block ops explain volume <volume-id> --from-bundle <bundle-or-artifact-dir>
sw-block ops dashboard --from-bundle <bundle-or-artifact-dir> --listen 127.0.0.1:9334
```

This is intended to avoid SSH log spelunking. A cold reviewer should be able to
understand cluster state, volume state, conditions, reason codes, and suggested
read-only/dry-run actions from the bundle.

## Status Vocabulary

The current status model is negative-first:

- `Ready=True reason=first_volume_verified` means current evidence supports a
  ready claim.
- `Blocked=True reason=csi_node_image_pull_failed` and similar reason codes
  identify known blockers.
- `EvidenceStale=True` or `Ready=Unknown` means evidence is stale, missing, or
  still reconverging; the product should not claim ready.
- Cluster counters include `ready_volume_count`, `blocked_volume_count`, and
  `stale_volume_count`.

## Cleanup

The documented cleanup path is:

```bash
helm uninstall sw-block --namespace kube-system
bash scripts/uninstall-k8s-alpha.sh "$PWD"
bash scripts/verify-helm-cleanup.sh
```

The verifier checks:

- Helm and Kubernetes resources,
- iSCSI sessions,
- iSCSI node DB records,
- dm-multipath maps,
- `dmsetup` devices,
- product processes,
- hostPath residue.

Expected clean shape:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
multipath_residue_count=0
process_residue_count=0
hostpath_residue_count=0
```

## Recommended Learning Path

1. Run the quickstart: install, first PVC, writer/reader, cleanup.
2. Open the local read-only dashboard.
3. Create multiple PVCs and inspect `ManagedVolume` entries.
4. Collect a support bundle and replay it offline.
5. Run restart or failover gates in a lab/TestOps environment if you are
   evaluating recovery behavior.
6. Intentionally break a non-production lab path, such as a bad CSI image tag,
   and verify the status surface reports `Blocked` instead of false `Ready`.

## Explicit Non-Claims

- Not production-ready.
- No production-grade operator or controller-manager lifecycle.
- No mutating admin/operator/dashboard actions.
- No promote, repair, rebuild, failback, delete, backup, restore, or cleanup
  mutation through UI/API/operator.
- No backup/snapshot/restore workflow.
- No returned-replica rebuild or automated failback.
- No transparent Kubernetes node-loss failover without pod recreate.
- No NVMe ANA parity for the transparent failover path.
- No broad distro/kernel/initiator compatibility matrix.
- No broad upgrade/rollback safety beyond existing gated smoke paths.
- No performance, RTO, RPO, or SLO claim.
