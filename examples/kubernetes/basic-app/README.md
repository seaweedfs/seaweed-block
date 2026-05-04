# Basic Kubernetes App Example

This example shows the smallest application-facing shape:

```text
StorageClass -> PVC -> writer pod -> reader pod
```

It assumes the seaweed-block alpha stack is already installed in the cluster.
For the full install-and-demo flow, run:

```bash
bash scripts/run-k8s-demo.sh "$PWD"
```

To install the alpha stack and keep it running for this example:

```bash
bash scripts/install-k8s-alpha.sh "$PWD"
```

## Apply

```bash
kubectl apply -f storageclass-pvc.yaml
bash ../../../scripts/apply-k8s-alpha-blockvolumes.sh
kubectl apply -f writer-pod.yaml
```

Wait for the writer:

```bash
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-example-writer --timeout=240s
kubectl logs sw-block-example-writer
```

Expected:

```text
/data/demo.bin: OK
```

Delete the writer pod but keep the PVC:

```bash
kubectl delete pod sw-block-example-writer
```

Start the reader pod against the same PVC:

```bash
kubectl apply -f reader-pod.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/sw-block-example-reader --timeout=240s
kubectl logs sw-block-example-reader
```

Expected:

```text
/data/demo.bin: OK
```

The second pod reading the checksum proves the data came from the PVC, not from
the first pod's container filesystem.

## Cleanup

```bash
kubectl delete pod sw-block-example-reader --ignore-not-found=true
kubectl delete pod sw-block-example-writer --ignore-not-found=true
kubectl delete pvc sw-block-example-pvc --ignore-not-found=true
kubectl delete storageclass sw-block-example --ignore-not-found=true
```

The current alpha still needs the blockvolume launcher cleanup path from the
demo runner or operator work. If you are using this example manually, inspect
and clean generated `sw-blockvolume-*` Deployments as needed:

```bash
kubectl -n kube-system get deploy -l app=sw-blockvolume
```

## Alpha Notes

- `ReadWriteOnce` is the intended mode.
- The example uses a tiny `1Mi` PVC because this is a functional smoke, not a
  capacity or performance benchmark.
- Current alpha storage is not production durable.
