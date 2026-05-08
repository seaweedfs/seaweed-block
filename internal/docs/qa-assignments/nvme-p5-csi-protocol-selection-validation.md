# QA Assignment: NVMe P5 CSI Protocol Selection

Branch: `frontend/nvme-ana-parity-plan`.

Scope: prove Kubernetes dynamic provisioning can select the NVMe frontend with
the same PVC/app smoke shape used by alpha iSCSI. This is a CSI integration
gate, not a new NVMe protocol gate.

## Preconditions

- Host: M02 k3s lab or equivalent single-node k3s host.
- Required host tools:
  - `kubectl`,
  - `nvme-cli`,
  - loadable `nvme_tcp`,
  - Docker image build/load path for `sw-block:local` and
    `sw-block-csi:local`.
- Rebuild both images from the branch under test before running.

## Test 1: NVMe Dynamic PVC

Command:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p5-csi-dynamic"
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
  bash scripts/run-k8s-alpha-nvme.sh "$PWD"
```

Expected:

- `run.log` contains:
  - `[alpha-nvme] frontend_protocol=nvme`,
  - final line:
    `[alpha-nvme] PASS: dynamic PVC create/delete completed checksum write/read and cleanup`.
- `dynamic-pvc-pod.rendered.yaml` contains:
  - `protocol: "nvme"` under the StorageClass `parameters`.
- `generated-blockvolume.yaml` contains:
  - `--nvme-listen=`,
  - `--nvme-subsysnqn=`,
  - `--nvme-ns=1`,
  - no `--iscsi-listen`.
- `lifecycle-volumes.json` contains:
  - `"protocol": "nvme"`.
- `blockcsi-controller.log` or `blockmaster.log` contains a `CreateVolume`
  line with `protocol="nvme"`.
- CSI node evidence:
  - `csi-node.rendered.yaml` includes `modprobe nvme_tcp`,
  - CSI node image includes `nvme-cli`.
- Workload evidence:
  - `pod.log` shows checksum verification succeeded.
- Cleanup:
  - `nvme-list-subsys.after-delete.json` has no
    `nqn.2026-05.io.seaweedfs` test subsystem,
  - `app-storage.after-delete.txt` shows no PVC residue,
  - `blockvolume-namespace-pods-deploys.after-delete.txt` shows no
    generated `sw-blockvolume` deployment residue,
  - `processes` are clean if separately checked.

## Test 2: Default iSCSI Regression

Command:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p5-default-iscsi-regression"
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
  bash scripts/run-k8s-alpha.sh "$PWD"
```

Expected:

- `run.log` contains:
  - `[alpha] frontend_protocol=iscsi`,
  - final alpha PASS line.
- `dynamic-pvc-pod.rendered.yaml` does not contain `protocol: "nvme"`.
- `generated-blockvolume.yaml` contains `--iscsi-listen=` and `--iscsi-iqn=`.
- Cleanup leaves no PVC, generated Deployment, iSCSI session, or NVMe test
  subsystem residue.

## Non-Claims

- Not a new ANA / native multipath gate.
- Not mounted failover.
- Not performance evidence.
- Single-node k3s only.
