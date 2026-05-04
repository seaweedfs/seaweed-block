# Seaweed Block Kubernetes Alpha

This directory contains the current alpha Kubernetes deployment path for
`seaweed-block`.

It is intentionally small:

- one `blockmaster`
- one CSI controller Deployment
- one CSI node DaemonSet
- dynamic PVC provisioning through CSI `CreateVolume`
- launcher-generated `blockvolume` Deployment
- iSCSI mount into a pod
- CSI `DeleteVolume` cleanup path exercised by the alpha smoke harness

The verified lab path is M02 single-node k3s. Treat this as an alpha MVP for
evaluation and contributor onboarding, not production storage.

## Current Storage Path

```text
Pod -> PVC -> CSI controller/node -> iscsiadm -> blockvolume -> walstore
```

Current defaults:

- frontend protocol: iSCSI
- durable implementation: `walstore`
- launcher-generated blockvolume state: `emptyDir`
- replication factor in demo StorageClass: `1`
- Kubernetes topology: single-node lab

## Prerequisites

- Linux Kubernetes node
- privileged CSI node plugin is allowed
- `iscsi_tcp` kernel module is loadable
- `kubectl` can reach the cluster
- container images are available to the cluster:
  - `sw-block:local`
  - `sw-block-csi:local`

For a default k3s install:

```bash
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
```

For local k3s/kind-style testing, build images with:

```bash
bash scripts/build-alpha-images.sh "$PWD"
```

For k3s, import them into containerd:

```bash
docker save sw-block:local | sudo k3s ctr images import -
docker save sw-block-csi:local | sudo k3s ctr images import -
```

## Deploy

The blockmaster manifest contains a `__NODE_NAME__` placeholder for the lab
node. Render it first:

```bash
NODE_NAME="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
sed "s/__NODE_NAME__/${NODE_NAME}/g" \
  deploy/k8s/alpha/block-stack.yaml > /tmp/sw-block-stack.yaml
```

Apply the service stack:

```bash
kubectl apply -f /tmp/sw-block-stack.yaml
kubectl apply -f deploy/k8s/alpha/rbac.yaml
kubectl apply -f deploy/k8s/alpha/csi-driver.yaml
kubectl apply -f deploy/k8s/alpha/csi-controller.yaml
kubectl apply -f deploy/k8s/alpha/csi-node.yaml
```

Wait for readiness:

```bash
kubectl -n kube-system wait --for=condition=available deploy/sw-blockmaster --timeout=120s
kubectl -n kube-system wait --for=condition=available deploy/sw-block-csi-controller --timeout=120s
kubectl -n kube-system rollout status ds/sw-block-csi-node --timeout=120s
```

## Run The Demo PVC

```bash
kubectl apply -f deploy/k8s/alpha/demo-dynamic-pvc-pod.yaml
```

The first dynamic PVC causes the CSI controller to ask blockmaster to create a
lifecycle volume. Blockmaster writes a generated blockvolume Deployment manifest
under `/manifests`; until a real operator exists, apply that generated manifest
manually:

```bash
kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- \
  sh -c 'cat /manifests/*.yaml' > /tmp/sw-block-generated-blockvolume.yaml
kubectl apply -f /tmp/sw-block-generated-blockvolume.yaml
kubectl -n kube-system wait --for=condition=available deploy -l app=sw-blockvolume --timeout=120s
```

Wait for the demo pod:

```bash
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-dynamic-smoke --timeout=240s
kubectl logs sw-block-dynamic-smoke
```

Expected log:

```text
/data/payload.bin: OK
```

## Cleanup

The current alpha harness performs an explicit cleanup sweep for the generated
blockvolume Deployment. A future operator should own that reconciliation instead
of the smoke script.

```bash
kubectl delete pod sw-block-dynamic-smoke --ignore-not-found=true
kubectl delete pvc sw-block-dynamic-v1 --ignore-not-found=true
kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true
kubectl delete -f deploy/k8s/alpha/demo-dynamic-pvc-pod.yaml --ignore-not-found=true
kubectl delete -f deploy/k8s/alpha/csi-node.yaml --ignore-not-found=true
kubectl delete -f deploy/k8s/alpha/csi-controller.yaml --ignore-not-found=true
kubectl delete -f deploy/k8s/alpha/csi-driver.yaml --ignore-not-found=true
kubectl delete -f deploy/k8s/alpha/rbac.yaml --ignore-not-found=true
kubectl delete -f /tmp/sw-block-stack.yaml --ignore-not-found=true
```

Check iSCSI cleanup:

```bash
sudo iscsiadm -m session || true
```

## Verified Evidence

The scripted version of this flow is:

```bash
bash scripts/run-k8s-alpha.sh "$PWD"
```

For a larger storage-path check that keeps the same stack but uses a 256 MiB
PVC and writes a 32 MiB checksum payload:

```bash
bash scripts/run-k8s-alpha-large.sh "$PWD"
```

For a small PostgreSQL workload check that runs `pgbench` for 60 seconds on a
Seaweed Block PVC:

```bash
bash scripts/run-k8s-alpha-pgbench.sh "$PWD"
```

To run the same PostgreSQL workload against the experimental `smartwal`
backend instead of the alpha default `walstore`:

```bash
bash scripts/run-k8s-alpha-pgbench-smartwal.sh "$PWD"
```

For a presentation-friendly app demo that uses the same storage stack but shows
two ordinary app pods sharing one PVC over time:

```bash
bash scripts/run-alpha-app-demo.sh "$PWD"
```

Known green:

- create/write/read: `a3d1e6a`
- create/write/read/delete cleanup: `ddec28c`

## Non-Claims

This alpha MVP does not yet claim:

- production-ready durability
- multi-node Kubernetes operation
- failover while a pod remains mounted
- real operator reconciliation
- backend data shredding on delete
- snapshots, clone, resize, block mode, topology spread, or NVMe-oF
- performance or soak readiness

