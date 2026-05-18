# QA Assignment: Phase 20 D2/D3 Day-1 First Volume

## Product Question

After activation, can a user create one PVC-backed volume, write data through a
mounted pod, replace the pod, read the same data back, and collect status /
inventory evidence without internal help?

## Run

Use the runner-native chain with immutable published images:

```bash
swblock run \
  -env product_root=/tmp/seaweed_block \
  -env ssh_key=C:\work\dev_server\testdev_key \
  -env sw_block_image=ghcr.io/seaweedfs/seaweed-block:sha-3a916a120d10 \
  -env sw_block_csi_image=ghcr.io/seaweedfs/seaweed-block-csi:sha-3a916a120d10 \
  testops/scenarios/activation-day1-first-volume-chain.yaml
```

The product-facing command used inside the gate is:

```bash
bash scripts/run-basic-app-example.sh /tmp/seaweed_block
```

## Required Evidence

The `basic-app` artifact directory must contain:

- `first-volume-summary.txt`
- `writer.log`
- `reader.log`
- `status/cluster-evidence.json`
- `status/inventory/volume-inventory-summary.txt`
- `status/inventory/ops-inventory-bundle.json`

`first-volume-summary.txt` must include:

```text
first_volume_status=ok
pvc=sw-block-example-pvc
writer_verified=true
reader_verified=true
status_evidence=status/cluster-evidence.json,status/inventory
cluster_evidence=status/cluster-evidence.json
inventory_bundle=status/inventory
cleanup_status=ok
```

## Pass Criteria

- Activation succeeds using immutable published images.
- The example PVC reaches `Bound`.
- The writer pod logs `/data/demo.bin: OK`.
- The reader pod logs `/data/demo.bin: OK` after the writer pod is deleted.
- Product-owned cluster evidence is collected from `sw-block ops cluster`.
- Inventory evidence maps the PVC to the generated volume.
- The helper cleans up the example reader, writer, PVC, and StorageClass.
- Cleanup leaves no active `io.seaweedfs` iSCSI sessions.
- Cleanup leaves no `blockmaster`, `blockvolume`, `blockcsi`, or `iscsi-target`
  processes.

## Fail Conditions

- The path depends on local image build/import.
- The user example requires manual `apply-k8s-alpha-blockvolumes.sh`.
- Writer/reader checksum does not pass.
- Status/inventory evidence is missing or cannot name `sw-block-example-pvc`.
- Cleanup leaves product processes, sessions, or generated blockvolume
  Deployments.

## Non-Claims

This gate proves the first Day-1 volume loop only. It does not claim recovery,
transparent failover, backup/restore, rebuild, upgrade safety, performance SLO,
or mutating admin workflows.
