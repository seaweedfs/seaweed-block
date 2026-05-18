# Phase 20 D2/D3 Ref: First Volume User Loop (Before Helm)

Status: proposed reference for next implementation slice.

## Why This Slice

Phase 20 D1 is now green for activation. The next product gap is not Helm yet;
it is the first user volume loop after install:

```text
install
-> create PVC
-> wait ready
-> writer/reader verify
-> status evidence
-> cleanup
```

If this loop is not stable and self-explaining, Helm only moves the failure
surface and hides root causes behind packaging.

## Product Question

Can a Kubernetes user, using published immutable images, complete one first
volume loop and produce a compact evidence summary without internal help?

## Scope

This D2/D3 slice should ship:

1. A runner-native first-volume scenario on published immutable images.
2. `examples/kubernetes/basic-app/` as canonical user volume flow.
3. Explicit status evidence capture after PVC creation:
   - `kubectl get pvc`
   - `kubectl get deploy -A -l app=sw-blockvolume`
   - `sw-block ops cluster -o json`
   - `sw-block ops inventory --out ...`
4. A compact `first-volume-summary.txt` in artifacts with:
   - `pvc=<name>`
   - `volume_id=<id>`
   - `writer_verified=true|false`
   - `reader_verified=true|false`
   - `cluster_evidence=status/cluster-evidence.json`
   - `inventory_bundle=status/inventory`
   - `cleanup_status=ok|failed`
5. QA run on immutable GHCR tags.
6. Multi-node activation must avoid loopback publish targets by default:
   activation auto-selects a Ready node InternalIP, enables external
   iSCSI/status with CHAP, and renders node-stage secret references into the
   StorageClass used by the first-volume example.

## Out Of Scope

- Helm chart packaging.
- New HA semantics.
- Mutating admin workflows.
- Backup/snapshot/restore.
- Performance/SLO claims.

## Canonical Commands

Install (published immutable):

```bash
SW_BLOCK_ACTIVATION_IMAGE_MODE=published \
SW_BLOCK_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit> \
SW_BLOCK_CSI_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<commit> \
bash scripts/activate-k8s-alpha.sh "$PWD"
```

Create first volume:

```bash
kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml
kubectl apply -f examples/kubernetes/basic-app/writer-pod.yaml
kubectl apply -f examples/kubernetes/basic-app/reader-pod.yaml
```

Status evidence:

```bash
kubectl get pvc
kubectl get deploy -A -l app=sw-blockvolume
sw-block ops cluster --master-api 127.0.0.1:9333 -o json
sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out /tmp/sw-block-inventory
```

The helper script prefers `$SW_BLOCK_CLI` or an installed `sw-block` binary and
falls back to `go run ./cmd/sw-block` only for source-tree dev use.
It cleans the example pod/PVC/StorageClass resources by default; set
`SW_BLOCK_BASIC_APP_CLEANUP=0` only when intentionally preserving them for
manual inspection.

Cleanup:

```bash
kubectl delete pod sw-block-example-reader --ignore-not-found=true
kubectl delete pod sw-block-example-writer --ignore-not-found=true
kubectl delete pvc sw-block-example-pvc --ignore-not-found=true
kubectl delete storageclass sw-block-example --ignore-not-found=true
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

## Evidence Contract

Required artifact shape:

```text
activation/
  activation-summary.txt
first-volume/
  writer.log
  reader.log
  kubectl-get-pvc.txt
  kubectl-get-blockvolume-deploy.txt
  cluster-evidence.json
  inventory/
  first-volume-summary.txt
cleanup/
  uninstall.log
  delete-storageclass.log
```

`first-volume-summary.txt` is the PM-facing verdict file for this slice. If
status evidence cannot be collected, it must say `first_volume_status=failed`
and `failed_phase=status_evidence` instead of silently passing.

## QA Matrix

Minimum matrix for this slice:

1. `published + immutable sha` (required close path).
2. `published + :alpha` (non-blocking drift smoke).
3. `local` mode remains dev regression path, not PM release attestation.

## Close Gate

D2/D3 closes when all are true:

1. Immutable published path passes end-to-end.
2. Writer and reader checksum both pass.
3. Status evidence files exist and are readable.
4. `first-volume-summary.txt` exists and is self-contained.
5. Cleanup leaves no active iSCSI sessions and no product residue.

## Suggested Claim After Close

Seaweed Block supports a documented first-volume Kubernetes loop on published
immutable images: install, create PVC-backed volume, verify app write/read,
capture status evidence, and clean teardown with supportable artifacts.
