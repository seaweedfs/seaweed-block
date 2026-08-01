# Seaweed Block Helm Chart

This chart installs the Seaweed Block alpha Kubernetes CSI stack:

- `sw-blockmaster`
- CSI controller
- CSI node DaemonSet
- optional `StorageClass`
- optional CHAP secret for cross-node iSCSI

The default chart is read/write only through Kubernetes PVCs. A source-gated,
disabled-by-default single-volume snapshot/restore path is available for
development validation; it does not add a mutating dashboard, automatic
backup policy, cross-cluster DR, or upgrade safety.

## Quick Install

Use generated values for the supported alpha install path. The generator reads
the live Kubernetes nodes and writes the node names/IPs that the launcher uses
for placement. On a single-node lab it keeps loopback/local-consumer mode; on a
multi-node lab it switches to non-loopback iSCSI with CHAP. For the
experimental NVMe/TCP path, pass `--protocol nvme`; this switches to
non-loopback NVMe/TCP plus external status, without enabling iSCSI CHAP.

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
  externalNVMe: false
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
    frontendIP: 10.0.0.181
    frontendNetworkClass: 100gbe_tcp
  - name: m02
    kubernetesNode: m02
    internalIP: 192.168.1.184
    frontendIP: 10.0.0.184
    frontendNetworkClass: 100gbe_tcp
  - name: tp01
    kubernetesNode: tp01
    internalIP: 192.168.1.188
    frontendIP: 10.0.0.188
    frontendNetworkClass: 100gbe_tcp
```

`blockNodes[*].kubernetesNode` must match a real Kubernetes node name. The
`internalIP` is the management/control-plane address. `frontendIP` is optional;
when set, blockvolume `data_addr` and NVMe/TCP or iSCSI publish targets use
that address while `ctrl_addr` remains on `internalIP`. Use
`frontendNetworkClass: 100gbe_tcp` for a TCP frontend on the lab 100GbE data
network.

The source-gated NVMe/RDMA path is explicit and remains off by default:

```yaml
storageClass:
  protocol: nvme
  nvmeTransport: rdma
network:
  externalNVMe: true
  frontendNetworkClass: 100gbe_roce
  blockNodes:
    - name: m02
      kubernetesNode: m02
      internalIP: 192.168.1.184
      frontendIP: 10.0.0.3
```

RDMA requires a non-loopback RoCE frontend address and host `nvme-rdma`,
`nvmet-rdma`, NBD, configfs, and RDMA-device prerequisites. This supported-lab
path does not claim RDMA multipath, failover, performance improvement, or an
SLO.

`network.rejectLoopbackPublishTargets` records the intended safety boundary.
Some blockmaster launcher flags are gated by `compat.*` settings because older
published alpha images do not accept every v0.3 flag. Keep
`compat.launcherDurableImplFlag`, `compat.launcherReplicationAckFlag`, and
`compat.launcherRejectLoopbackFlag` false unless the selected image is known to
support the corresponding flag. The default durable implementation is still
`walstore` because that is the blockmaster binary default when the
`--launcher-durable-impl` flag is omitted.

For source-gated NVMe/TCP write-path experiments, `nvme.maxH2CDataLength` can
be set explicitly. The chart default is `32768`; `65536` has a supported-lab
gate as an opt-in candidate. This is not a default change, NVMe/RDMA/RoCE
claim, or performance SLO.

For source-gated WAL write-path experiments, multi-block WAL records can be
enabled explicitly:

```yaml
blockmaster:
  durableWALMultiBlockRecords: true
```

This is a lab-only optimization boundary backed by Phase 151/152/155 gates. It
is not a release-image claim until a matching-image smoke runs the explicit
opt-in recovery/status path. The default remains `false`. Do not set
`durableWALRecoveryTestDisableFlusher` outside recovery-test gates; that flag is
scaffolding used to force WAL replay evidence and is not a production tuning
knob.

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
  externalNVMe: false
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

## Optional VolumeSnapshot Path

The chart does not install the cluster-wide Kubernetes snapshot CRDs or
snapshot-controller. Install a compatible external-snapshotter controller
stack first and verify the prerequisite APIs:

```bash
kubectl api-resources --api-group=snapshot.storage.k8s.io
kubectl -n kube-system get deploy | grep snapshot-controller
```

Then supply a durable, hostname-pinned blockmaster and an externally managed
Secret:

```yaml
blockmaster:
  replicas: 1
  stateHostPath: /var/lib/sw-block
  nodeSelector:
    kubernetes.io/hostname: m02

snapshot:
  enabled: true
  runtimeSecretName: sw-block-snapshot-runtime
  class:
    create: true
    name: sw-block-snapshot
    deletionPolicy: Delete
```

The Secret has distinct credentials for the blockvolume runtime and the CSI
SnapshotService client:

```text
ca.crt  tls.crt  tls.key  client.crt  client.key  token
api-server.crt  api-server.key  api-server-ca.crt
api-client-ca.crt  api-client.crt  api-client.key  api-token
```

`tls.crt` covers every advertised blockvolume node IP. `api-server.crt` is
signed by `api-server-ca.crt` and covers
`blockmaster.<namespace>.svc.cluster.local`; `api-client.crt` is signed by
`api-client-ca.crt`. The CSI pod receives only the API server CA, API client
identity, and API token. The blockmaster SnapshotService remains isolated from
the plaintext control listener by mTLS plus bearer authentication.

When enabled, this chart adds the `csi-snapshotter` sidecar, its snapshot API
RBAC, and the configured `VolumeSnapshotClass`. The requested PVC capacity
range must contain the snapshot size; the restored volume initially uses that
exact geometry. Larger restored volumes, application-consistent quiescing,
group snapshots, incremental backup, and in-place revert are not yet
supported.

## Current Boundary

- Use immutable image tags such as `sha-<commit>` for QA and release checks.
- `:alpha` is a convenience tag and can drift from local source.
- Dynamic node discovery/onboarding is not yet handled by this chart. For now,
  update `blockNodes` and run `helm upgrade`.
- Operator-managed node discovery, repair, and upgrade flows are future work.
