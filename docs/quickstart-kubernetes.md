# Kubernetes Quick Start

This guide is the v0.3 / v0.3.1 alpha path for Seaweed Block on Kubernetes.

It shows the user loop:

```text
generate Helm values
-> helm install
-> create a PVC-backed volume
-> write data from one app pod
-> read the same data from a replacement app pod
-> inspect read-only report/dashboard evidence
-> clean up
```

The current quickstart is for supported test clusters, not production. For
deeper operational detail, see [`operations-v1.md`](operations-v1.md).

## What This Proves

The quickstart proves:

- Helm can install the alpha blockmaster and CSI components.
- A PVC can dynamically provision a Seaweed Block-backed PV.
- A writer pod can mount the PVC and write `/data/demo.bin`.
- A replacement reader pod can mount the same PVC and verify the same data.
- Product-owned status evidence, a local read-only report, and a local
  read-only dashboard are available.
- The example resources and host-side residue are cleaned up.

It does not prove production HA, backup/restore, broad upgrade safety, broad
platform compatibility, operator lifecycle, mutating admin workflows, or
production UI.

## Prerequisites

Run from a Linux host with access to your Kubernetes cluster:

- `kubectl` configured for the target cluster.
- `helm` installed.
- `sudo` access for iSCSI checks and cleanup.
- `iscsiadm` available on nodes that will stage volumes.
- `sw-block` binary in `PATH`, or run `go run ./cmd/sw-block ...` from this
  repository.

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

## Step 0 - Build The CLI

If `sw-block` is not already installed in `PATH`, build it from this
repository:

```bash
go build -o sw-block ./cmd/sw-block
export PATH="$PWD:$PATH"
sw-block --version
```

The release walkthrough uses `sw-block ops generate-helm-values`. `go run
./cmd/sw-block ...` is available as a fallback, but building the CLI first
matches what a user normally runs.

## Step 1 — Generate Helm Values

From the repository root:

```bash
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
sw-block ops generate-helm-values \
  --out values.day1.yaml \
  --image ghcr.io/seaweedfs/seaweed-block:sha-28a99ce4f644 \
  --csi-image ghcr.io/seaweedfs/seaweed-block-csi:sha-28a99ce4f644
```

The `KUBECONFIG` fallback above is the common k3s path. Non-k3s users should
keep their existing `KUBECONFIG`. Fresh k3s installs may keep
`/etc/rancher/k3s/k3s.yaml` readable only by root unless k3s was started with a
readable kubeconfig mode.

If `sw-block` is not installed:

```bash
go run ./cmd/sw-block ops generate-helm-values --out values.day1.yaml
```

What this command does:

- reads Kubernetes nodes through `kubectl get nodes -o wide`,
- selects Ready schedulable nodes with non-loopback InternalIP,
- writes `values.day1.yaml` for the Helm chart.

Single-node behavior:

- one selected node,
- loopback iSCSI/status mode,
- CHAP disabled.

Multi-node behavior:

- multiple selected nodes,
- non-loopback iSCSI/status addresses,
- CHAP enabled,
- loopback publish-target rejection recorded in values.

Inspect the generated values:

```bash
grep -E "externalISCSI|externalStatus|replicationFactor|ackProfile|enabled:|internalIP" values.day1.yaml
```

For release validation, use immutable images:

```bash
sw-block ops generate-helm-values \
  --out values.day1.yaml \
  --image ghcr.io/seaweedfs/seaweed-block:sha-<commit> \
  --csi-image ghcr.io/seaweedfs/seaweed-block-csi:sha-<commit>
```

Current validated v0.3 Day-1 walkthrough image tag:
`sha-28a99ce4f644`. The v0.3.1 lifecycle-hardening gates used the Phase 26
branch build; use the v0.3.1 immutable GHCR SHA once it is published.

Mutable `:alpha` is a smoke/demo tag only. Do not use it as release evidence
unless the publish commit is known.

## Step 2 — Install Seaweed Block With Helm

```bash
helm install sw-block charts/seaweed-block \
  --namespace kube-system \
  --create-namespace \
  -f values.day1.yaml \
  --wait \
  --timeout 10m
```

Check readiness:

```bash
kubectl -n kube-system get deploy,ds,pods -l app.kubernetes.io/instance=sw-block -o wide
kubectl get storageclass
```

Expected:

```text
sw-blockmaster ready
sw-block-csi-controller ready
sw-block-csi-node ready on selected nodes
StorageClass present
```

## Step 3 — Create And Verify The First Volume

Run the first-volume helper in Helm mode:

```bash
SW_BLOCK_INSTALL_MODE=helm \
SW_BLOCK_HELM_RELEASE=sw-block \
SW_BLOCK_HELM_NAMESPACE=kube-system \
SW_BLOCK_HELM_VALUES_FILE=values.day1.yaml \
  bash scripts/run-basic-app-example.sh "$PWD"
```

The `"$PWD"` argument is the repository root. The helper uses it to locate the
example manifests and chart metadata.

The helper applies the example StorageClass/PVC, waits for the PVC to bind,
runs a writer pod, deletes it, runs a reader pod, collects status evidence,
generates a local report, and cleans the example resources by default.

Current helper scope: the example app resources use the `default` namespace.
Use the documented example as-is for the v0.3 alpha gate. Cross-namespace
helpers are a later usability enhancement; Kubernetes can still consume the
installed CSI driver from other namespaces through normal PVC manifests.

Expected final line:

```text
[basic-app] PASS: basic app PVC writer/reader loop complete
```

Read the first-volume summary:

```bash
APP_DIR="$(ls -td /tmp/sw-block-basic-app-* | head -1)"
cat "$APP_DIR/first-volume-summary.txt"
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

## Step 4 — Inspect Report Or Dashboard

The first-volume helper writes a status report:

```bash
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

`sw-block ops report` writes static artifacts. `sw-block ops dashboard` serves
the same evidence locally over HTTP.

Serve the collected bundle:

```bash
sw-block ops dashboard --from-bundle "$APP_DIR" --listen 127.0.0.1:9334
```

`$APP_DIR` is the basic-app artifact root. The dashboard/report loader finds
the nested `status/report/`, `status/cluster-evidence.json`, and inventory
artifacts under that root.

Open:

```text
http://127.0.0.1:9334/
```

The dashboard is read-only. It has no promote, repair, rebuild, delete,
failback, backup, restore, or cleanup controls.

To generate live evidence from blockmaster:

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
```

In another terminal:

```bash
sw-block ops cluster --master-api 127.0.0.1:9333
sw-block ops report --master-api 127.0.0.1:9333 --out /tmp/sw-block-report
sw-block ops dashboard --master-api 127.0.0.1:9333 --listen 127.0.0.1:9334
```

## Use Your Own PVC

The standard volume creation path is Kubernetes PVC creation. Start from the
example, which is written for the `default` namespace:

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
not manually apply generated blockvolume YAML on the supported alpha path.

## Script Fallback Path

The script path remains available for development, local images, and fallback
diagnostics:

```bash
bash scripts/activate-k8s-alpha.sh "$PWD"
bash scripts/run-basic-app-example.sh "$PWD"
```

The `"$PWD"` argument is the repository root for both helper scripts.

Use Helm for the v0.3 alpha release path unless you are specifically testing
local script activation.

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

If a pod cannot mount the PVC, `writer-describe.txt` or
`reader-describe.txt` should include the Kubernetes event explaining why, such
as image pull failure, iSCSI connection failure, missing CHAP secret, or
scheduling mismatch.

Attach the artifact directory and any `/tmp/sw-block-report` output when filing
an issue.

## Cleanup

The first-volume helper cleans the example resources by default. To uninstall
the alpha stack:

```bash
helm uninstall sw-block --namespace kube-system
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

- This is an alpha Helm path, not a production installer or operator.
- The dashboard is local and read-only, not a production hosted UI.
- Mutating admin workflows are not exposed: no promote, repair, rebuild,
  failback, delete, backup, restore, or cleanup button.
- Broad upgrade and rollback safety are not claimed beyond the gated alpha
  smoke path.
- Backup, snapshot, and restore are not claimed.
- Broad distro/kernel/initiator compatibility is not claimed.
- Performance, RTO, and SLO numbers are not claimed.
- Transparent Kubernetes node-loss without pod recreate is not claimed by this
  quickstart.
