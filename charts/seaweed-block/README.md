# Seaweed Block Helm Chart

This chart installs the Seaweed Block alpha Kubernetes CSI stack:

- `sw-blockmaster`
- CSI controller
- CSI node DaemonSet
- optional `StorageClass`
- optional CHAP secret for cross-node iSCSI

The chart is read/write only through Kubernetes PVCs. It does not add a
mutating dashboard, operator reconciliation, backup/restore, or upgrade safety.

## Quick Install

Single-node or local-dev loopback mode:

```bash
helm install sw-block charts/seaweed-block \
  --namespace kube-system
```

Multi-node Day-1 mode should use non-loopback node IPs and CHAP:

```bash
SW_BLOCK_HELM_VALUES_OUT=values.day1.yaml \
  bash scripts/generate-helm-values-day1.sh "$PWD"

helm install sw-block charts/seaweed-block \
  --namespace kube-system \
  -f values.day1.yaml
```

Example `values.day1.yaml`:

```yaml
image:
  tag: sha-<commit>
csiImage:
  tag: sha-<commit>

network:
  externalISCSI: true
  externalStatus: true
  rejectLoopbackPublishTargets: true

compat:
  launcherRejectLoopbackFlag: false

chap:
  enabled: true
  username: swblock
  secret: replace-with-a-real-secret

storageClass:
  replicationFactor: 1

blockNodes:
  - name: m01
    kubernetesNode: m01
    internalIP: 192.168.1.181
  - name: m02
    kubernetesNode: m02
    internalIP: 192.168.1.184
  - name: tp01
    kubernetesNode: tp01
    internalIP: 192.168.1.188
```

`blockNodes[*].kubernetesNode` must match a real Kubernetes node name. The
`internalIP` must be reachable by workloads that may mount the PVC.

`network.rejectLoopbackPublishTargets` records the intended safety boundary.
The matching blockmaster flag is gated by
`compat.launcherRejectLoopbackFlag` because older alpha images do not accept
`--launcher-reject-loopback-publish-targets`. Keep the compat flag false unless
the selected image is known to support it.

## RF=3 Sync-Quorum Profile

For the gated RF=3 recovery shape:

```yaml
storageClass:
  replicationFactor: 3
replication:
  ackProfile: sync-quorum
  expectedSlotsPerVolume: 3
network:
  externalISCSI: true
  externalStatus: true
  rejectLoopbackPublishTargets: true
chap:
  enabled: true
  username: swblock
  secret: replace-with-a-real-secret
```

## Stage 2 Multipath Baseline

Stage 2 iSCSI ALUA/dm-multipath requires host prerequisites and is intentionally
opt-in:

```yaml
stage2Multipath:
  enabled: true
```

This only enables chart wiring. The host still needs iSCSI, multipath, and ALUA
support configured.

## Current Boundary

- Use immutable image tags such as `sha-<commit>` for QA and release checks.
- `:alpha` is a convenience tag and can drift from local source.
- Dynamic node discovery/onboarding is not yet handled by this chart. For now,
  update `blockNodes` and run `helm upgrade`.
- Operator-managed node discovery, repair, and upgrade flows are future work.
