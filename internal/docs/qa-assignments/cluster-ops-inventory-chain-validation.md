# QA Assignment: Cluster Ops Inventory Chain Validation

Status: ready after dev lands `cluster-ops-inventory-chain.yaml`.

## Goal

Validate the D5 live runner-native gate for the active plan:

```text
Cluster Operations Inventory And Lifecycle Visibility MVP
```

This is not the final close gate. It proves the first live inventory boundary:
an operator can inspect two concurrent live alpha Kubernetes volumes from the
cluster itself and get nested per-replica `sw-block ops status` bundles through
`sw-block ops inventory`.

## Precondition

- Controller has a current `swblock` binary.
- m02 product tree is at the product commit under validation.
- m02 k3s lab passes `scripts/preflight-k8s-alpha.sh --local-k3s`.
- No manual TestOps artifact path should be supplied to `sw-block ops
  inventory`; the command must discover from Kubernetes plus `--master`.

## Command

From the Windows controller:

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/cluster-ops-inventory `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/cluster-ops-inventory-chain.yaml
```

Adjust `product_root` only if QA staged the product tree elsewhere on m02.

## Expected Result

The scenario should PASS:

```text
cluster-ops-inventory-chain PASS
```

All seven phases should reach terminal PASS:

- `pre_clean`
- `preflight`
- `pin_build_alpha_images`
- `live_volume_boundary`
- `second_volume_boundary`
- `inventory_live_cluster`
- `inventory_asserts`
- `collect_and_cleanup`

## Evidence To Report

Report these exact fields:

- product commit used on m02,
- runner commit / swblock build,
- run id,
- final scenario state,
- `volume-inventory-summary.txt` first 15 lines,
- `volume-inventory.json` contains `pvc_name=sw-block-demo-pvc`,
- `volume-inventory.json` contains `pvc_name=sw-block-demo-pvc-2`,
- replica row contains `support_bundle=volumes/<volume>/<replica>`,
- nested `ops-status-bundle.json` exists and has
  `"command": "sw-block ops status"`,
- final cleanup audit: no Seaweed Block iSCSI sessions, no sw-block
  processes, no blockmaster port-forward process.

## Failure Criteria

Fail the assignment if any of these happen:

- inventory needs a TestOps artifact path or generated YAML path to succeed,
- inventory exits non-zero while still writing a trustworthy unhealthy report,
- summary omits PVC, volume id, status endpoint, or support-bundle pointer,
- nested per-replica status bundle is missing,
- cleanup leaves active iSCSI sessions or sw-block processes,
- the scenario passes but the inventory is empty while the demo PVC is live.
- the scenario passes with only one volume row while both PVCs are live.

## Report Template

```text
QA Report -- Cluster Ops Inventory Chain Validation

Product commit:
Runner commit:
Run id:
Result:
Wall clock:

Phase table:
  pre_clean:
  preflight:
  pin_build_alpha_images:
  live_volume_boundary:
  second_volume_boundary:
  inventory_live_cluster:
  inventory_asserts:
  collect_and_cleanup:

Inventory evidence:
  inventory_status:
  volume_count:
  pvc_1:
  volume_id_1:
  pvc_2:
  volume_id_2:
  status_address:
  support_bundle:
  nested ops-status command:

Cleanup audit:
  iSCSI sessions:
  sw-block processes:
  blockmaster port-forward:

Findings:
  - ...

Verdict:
  PASS / FAIL
```
