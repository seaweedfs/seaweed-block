# Kubernetes App Demo

This demo shows `seaweed-block` from the application's point of view.

The app is a normal Kubernetes pod using a normal PVC. The storage stack is the
`seaweed-block` alpha service underneath it.

```text
app pod image:        busybox:1.36
storage images:       sw-block:local, sw-block-csi:local
Kubernetes object:    PersistentVolumeClaim
storage path:         PVC -> CSI -> iSCSI -> blockvolume -> walstore
```

The demo does two app runs against the same PVC:

1. writer pod writes `/data/demo.bin` and `/data/demo.sha256`;
2. writer pod is deleted while the PVC remains;
3. reader pod mounts the same PVC and verifies the checksum.

This is easier to explain than the automated smoke because viewers can see that
ordinary app pods are using the volume.

## Prerequisites

Same as the README quick start:

- Docker,
- `kubectl`,
- a running Kubernetes cluster such as k3s,
- `iscsi_tcp` loadable on the node,
- `KUBECONFIG` set for the cluster, or a default k3s config at
  `/etc/rancher/k3s/k3s.yaml`.

Build and import local images first:

```bash
bash scripts/build-alpha-images.sh "$PWD"

docker save sw-block:local | sudo k3s ctr images import -
docker save sw-block-csi:local | sudo k3s ctr images import -
```

## Run

```bash
bash scripts/run-alpha-app-demo.sh "$PWD"
```

Expected final line:

```text
[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete
```

Useful proof points:

```bash
cat /tmp/sw-block-app-demo-*/writer.log
cat /tmp/sw-block-app-demo-*/reader.log
sudo iscsiadm -m session || true
kubectl get all -A | grep sw-block || true
```

The writer log should show:

```text
/data/demo.bin: OK
```

The reader log should also show:

```text
/data/demo.bin: OK
```

The second check proves the replacement app pod read data written by the first
app pod through the same PVC.

## Demo Talk Track

Use this short explanation:

> This is a normal Kubernetes app pod. It asks for a PVC. The CSI controller
> asks blockmaster to create a volume. Blockmaster launches a blockvolume
> workload. The CSI node logs into that blockvolume through iSCSI and mounts it
> for the pod. The writer pod writes data, exits, and is deleted. A reader pod
> mounts the same PVC and verifies the file. The current alpha harness still
> performs the generated blockvolume cleanup; a controller will own that in the
> production path.

## Non-Claims

This demo does not claim:

- multi-node production readiness,
- failover while the app remains mounted,
- durable state across blockvolume pod restart,
- real operator reconciliation,
- performance or soak readiness.
