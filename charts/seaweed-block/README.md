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

Use generated values for the supported alpha install path. The generator reads
the live Kubernetes nodes and writes the node names/IPs that the launcher uses
for placement. On a single-node lab it keeps loopback/local-consumer mode; on a
multi-node lab it switches to non-loopback iSCSI with CHAP.

```bash
SW_BLOCK_HELM_VALUES_OUT=values.day1.yaml \
  bash scripts/generate-helm-values-day1.sh "$PWD"

helm install sw-block charts/seaweed-block \
  --namespace kube-system \
  -f values.day1.yaml
```

The raw chart defaults are render/development defaults only. Do not treat a
plain `helm install sw-block charts/seaweed-block` as a release-gated install:
it does not run preflight, does not discover the live Kubernetes node set, and
can leave placement/launcher evidence incomplete on real labs.

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
  launcherDurableImplFlag: false
  launcherReplicationAckFlag: false
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
Some blockmaster launcher flags are gated by `compat.*` settings because older
published alpha images do not accept every v0.3 flag. Keep
`compat.launcherDurableImplFlag`, `compat.launcherReplicationAckFlag`, and
`compat.launcherRejectLoopbackFlag` false unless the selected image is known to
support the corresponding flag. The default durable implementation is still
`walstore` because that is the blockmaster binary default when the
`--launcher-durable-impl` flag is omitted.

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

## Optional Failback Runtime Wiring

Returned-replica failback execution is disabled by default. The chart only
renders the blockmaster failback RPC and executor runtime flags when all
execution switches are explicit:

```yaml
blockmaster:
  failbackRuntimeRPC: true

failbackExecutor:
  create: true
  dryRun: false
  execution:
    enabled: true
    policy: true
    failbackRuntimeGrpcAddr: blockmaster.kube-system.svc:9333
```

The chart rejects unsafe combinations such as execution with `dryRun: true`,
missing execution policy, or both HTTP and gRPC runtime addresses. This wiring
does not enable automatic failback by default.

## Current Boundary

- Use immutable image tags such as `sha-<commit>` for QA and release checks.
- `:alpha` is a convenience tag and can drift from local source.
- Dynamic node discovery/onboarding is not yet handled by this chart. For now,
  update `blockNodes` and run `helm upgrade`.
- Operator-managed node discovery, repair, and upgrade flows are future work.
