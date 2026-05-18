# Kubernetes Quick Start

This guide is the alpha Day-1 path for Seaweed Block on Kubernetes.

It shows a normal user loop:

```text
preflight
-> install the alpha stack
-> create a PVC-backed volume
-> write data from one app pod
-> read the same data from a replacement app pod
-> collect status/report evidence
-> clean up
```

The current quickstart is for a supported test cluster, not production.
For deeper operational detail, see [`operations-v1.md`](operations-v1.md).

## What This Proves

The quickstart proves:

- Kubernetes can install the alpha blockmaster and CSI components.
- A PVC can dynamically provision a Seaweed Block-backed PV.
- A writer pod can mount the PVC and write `/data/demo.bin`.
- A replacement reader pod can mount the same PVC and verify the same data.
- Product-owned status evidence and a local read-only report are collected.
- The example resources are cleaned up.

It does not prove production HA, backup/restore, upgrade safety, broad platform
compatibility, or a hosted dashboard.

## Prerequisites

Run from a Linux host with access to your Kubernetes cluster:

- `kubectl` configured for the target cluster.
- `sudo` access for iSCSI checks and cleanup.
- `iscsiadm` available on nodes that will stage volumes.
- `go` and Docker/container tools for the local dev path, unless you use
  published images.

Check the cluster:

```bash
kubectl get nodes -o wide
sudo iscsiadm -m session || true
```

Expected before starting:

```text
at least one Ready node
iscsiadm: No active sessions.
```

On a multi-node cluster, the activation script uses a non-loopback node
InternalIP plus CHAP-backed iSCSI for the Day-1 path. On a single-node cluster,
it may use the simpler local path.

## Step 1 — Activate Seaweed Block

From the repository root:

```bash
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
bash scripts/activate-k8s-alpha.sh "$PWD"
```

The script runs preflight, builds/imports local images into k3s, applies the
alpha manifests, waits for blockmaster and CSI readiness, creates the default
StorageClass, and writes an activation summary.

Read the summary:

```bash
cat "$(ls -td /tmp/sw-block-activation-* | head -1)/activation-summary.txt"
```

Expected fields include:

```text
activation_status=ok
image_mode=local
protocol=iscsi
ack_profile=best-effort
ready_kubernetes_nodes=<N>
master_ready_replicas=1
csi_controller_ready_replicas=1
csi_node_ready=<N>/<N>
storageclass=sw-block-dynamic
next_create_volume=...
next_status=...
non_claims=...
```

If preflight fails, fix the environment first. Do not debug storage behavior
until `activation_status=ok`.

## Step 2 — Create And Verify The First Volume

Run the first-volume helper:

```bash
bash scripts/run-basic-app-example.sh "$PWD"
```

The helper applies the example StorageClass/PVC, waits for the PVC to bind,
runs a writer pod, deletes it, runs a reader pod, collects status evidence,
generates a local report, and cleans the example resources by default.

Expected final line:

```text
[basic-app] PASS: basic app PVC writer/reader loop complete
```

Read the first-volume summary:

```bash
cat "$(ls -td /tmp/sw-block-basic-app-* | head -1)/first-volume-summary.txt"
```

Expected fields:

```text
first_volume_status=ok
pvc=sw-block-example-pvc
pvc_phase=Bound
writer_verified=true
reader_verified=true
inventory_status=ok
status_evidence=status/cluster-evidence.json,status/inventory
cluster_evidence=status/cluster-evidence.json
inventory_bundle=status/inventory
status_report=status/report/index.html
cleanup_status=ok
```

The writer and reader are ordinary app pods. The reader is a replacement pod,
so this proves the data is on the PVC-backed volume, not only in the writer
container.

## Step 3 — Inspect The Read-Only Report

The first-volume helper writes a status directory under the latest basic-app
artifact directory:

```bash
APP_DIR="$(ls -td /tmp/sw-block-basic-app-* | head -1)"
ls "$APP_DIR/status/report"
cat "$APP_DIR/status/report/summary.txt"
```

Expected files:

```text
index.html
cluster-evidence.json
timeline.jsonl
summary.txt
```

`index.html` is a local static report. It is read-only. It has no promote,
repair, rebuild, delete, failback, or cleanup controls.

If you want to generate the same report from the live master:

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
```

In another terminal:

```bash
sw-block ops cluster --master-api 127.0.0.1:9333
sw-block ops report --master-api 127.0.0.1:9333 --out /tmp/sw-block-report
```

If `sw-block` is not installed in `PATH`, run the same commands from the
repository as `go run ./cmd/sw-block ops ...`.

For replica-level support evidence:

```bash
sw-block ops inventory \
  --namespace default \
  --master 127.0.0.1:9333 \
  --out /tmp/sw-block-inventory
```

## Published Images

For development, local build/import is the fastest path and guarantees the
running image matches the source tree.

For QA/PM or release-candidate validation, use immutable published images:

```bash
export SW_BLOCK_ACTIVATION_IMAGE_MODE=published
export SW_BLOCK_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>
export SW_BLOCK_CSI_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<commit>

bash scripts/activate-k8s-alpha.sh "$PWD"
bash scripts/run-basic-app-example.sh "$PWD"
```

Mutable `:alpha` images are acceptable for casual smoke tests, but they can
drift from the checked-out source tree. Do not use `:alpha` as release
evidence unless the publish commit is known.

## Use Your Own PVC

The standard volume creation path is Kubernetes PVC creation. Start from the
example:

```bash
kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml
kubectl get pvc sw-block-example-pvc
```

Then mount the PVC from your own pod, or use the example writer and reader:

```bash
kubectl apply -f examples/kubernetes/basic-app/writer-pod.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-example-writer --timeout=240s
kubectl logs sw-block-example-writer

kubectl delete pod sw-block-example-writer
kubectl apply -f examples/kubernetes/basic-app/reader-pod.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-example-reader --timeout=240s
kubectl logs sw-block-example-reader
```

The generated `blockvolume` workload is reconciled by blockmaster. You should
not need to manually apply generated blockvolume YAML on the supported alpha
path.

## Troubleshooting

Start with the latest basic-app artifact directory:

```bash
APP_DIR="$(ls -td /tmp/sw-block-basic-app-* | head -1)"
cat "$APP_DIR/first-volume-summary.txt" 2>/dev/null || true
find "$APP_DIR" -maxdepth 3 -type f | sort
```

Common evidence:

```bash
cat "$APP_DIR/status/cluster-evidence.json" 2>/dev/null || true
cat "$APP_DIR/status/inventory/volume-inventory-summary.txt" 2>/dev/null || true
cat "$APP_DIR/diagnostics/writer/writer-describe.txt" 2>/dev/null || true
cat "$APP_DIR/diagnostics/reader/reader-describe.txt" 2>/dev/null || true
```

If a pod cannot mount the PVC, `writer-describe.txt` or `reader-describe.txt`
should include the Kubernetes event explaining why, such as image pull failure,
iSCSI connection failure, missing CHAP secret, or scheduling mismatch.

If the cluster is still reachable, collect fresh live evidence:

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
```

In another terminal:

```bash
sw-block ops cluster --master-api 127.0.0.1:9333 -o json \
  > /tmp/sw-block-cluster-evidence.json
sw-block ops report --master-api 127.0.0.1:9333 --out /tmp/sw-block-report
sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out /tmp/sw-block-inventory
```

If `sw-block` is not installed in `PATH`, prefix those commands with
`go run ./cmd/sw-block` from the repository root.

Attach the artifact directory and `/tmp/sw-block-report` when filing an issue.

## Cleanup

The first-volume helper cleans the example resources by default. To uninstall
the whole alpha stack:

```bash
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

Verify:

```bash
kubectl get sc | grep sw-block || echo "no sw-block StorageClass"
kubectl get deploy -A | grep sw-block || echo "no sw-block deployments"
sudo iscsiadm -m session || true
```

Expected clean state:

```text
no sw-block StorageClass
no sw-block deployments
iscsiadm: No active sessions.
```

Do not run broad cluster cleanup commands in a shared cluster unless you know no
other run owns the resources.

## Current Alpha Limitations

- This is an alpha installer/script path, not a production Helm chart or
  operator.
- The local `sw-block ops report` is not a hosted dashboard.
- Mutating admin workflows are not exposed: no promote, repair, rebuild,
  failback, delete, backup, or restore button.
- Upgrade and rollback safety are not claimed.
- Backup, snapshot, and restore are not claimed.
- Broad distro/kernel/initiator compatibility is not claimed.
- Performance, RTO, and SLO numbers are not claimed.
- Transparent Kubernetes node-loss without pod recreate is not claimed by this
  quickstart.
