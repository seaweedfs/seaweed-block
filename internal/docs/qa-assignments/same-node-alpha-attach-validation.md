# QA Assignment: Same-Node Alpha Attach Validation

## Purpose

Validate the D4 runner-native gate for the current
Multi-Node Attach And Placement MVP.

This is not a remote-node attach claim. It proves the supported alpha model:

```text
RF=1 PVC -> generated blockvolume on selected node -> writer/reader pods pinned
to that same node -> normal CSI attach/write/read -> inventory explains node and
loopback endpoint ownership
```

## Product / Runner Inputs

Use the current product branch/commit provided by dev.

Runner requirement: `swblock` from `pingqiu/sw-test-runner`, with runner-native
scenario support.

Expected lab: m02 k3s lab is sufficient for this gate. A true second Kubernetes
node is not required because this gate validates same-node loopback attach.

## Command

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/same-node-alpha-attach `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/same-node-alpha-attach-chain.yaml
```

Adjust `product_root` only if the product checkout on m02 is elsewhere.

## Expected PASS Shape

Scenario:

- state `pass`
- all phases terminal:
  - `pre_clean`
  - `preflight`
  - `pin_build_alpha_images`
  - `reader_verified_with_live_inventory`
  - `same_node_asserts`
  - `collect_and_cleanup`

Functional evidence:

- `demo/writer.log` contains `[app-writer] wrote and verified /data/demo.bin`.
- `demo/reader.log` contains `[app-reader] verified persisted /data/demo.bin`.
- `demo/run.log` contains:
  - `app_node=<node>`
  - `pin_app_node=1`
  - `[app-demo] controlled stop after reader verified`
- `demo/demo-app.rendered.yaml` contains a Pod `nodeSelector`.
- `demo/demo-app-reader.rendered.yaml` contains a Pod `nodeSelector`.
- `demo/generated-blockvolume.yaml` contains:
  - `kubernetes.io/hostname: <same node>`
  - `--iscsi-listen=127.0.0.1:<port>`

Inventory evidence:

- `demo/ops-inventory-reader-verified/volume-inventory-summary.txt` contains:
  - `pvc=sw-block-demo-pvc`
  - `node=<same app_node from run.log>`
  - `frontend=127.0.0.1:<port>`
  - `support_bundle=volumes/<volume>/r1`
- `demo/ops-inventory-reader-verified/nested-ops-status-bundles.json` contains
  `"command": "sw-block ops status"`.

Cleanup:

- no active iSCSI sessions,
- no `blockmaster`, `blockvolume`, `blockcsi`, `iscsi-target`, or blockmaster
  `kubectl port-forward` processes,
- no `app=sw-blockvolume` Deployment remains after cleanup.

## Failures To Report

Report as blocking if:

- writer/reader checksums do not pass,
- app manifests are not node-pinned by default,
- app node and inventory blockvolume node differ while frontend is loopback,
- inventory lacks PVC, node, frontend, or support-bundle evidence,
- nested per-replica status bundle is missing,
- cleanup leaves sessions/processes/resources.

Report as lab/precondition if:

- m02 k3s preflight fails before product install,
- `swblock` runner cannot execute runner-native scenarios.

## QA Needed After This

If this passes, D4 is validated. Dev can proceed to D5 negative fixture for an
unsupported cross-node/loopback placement bundle.
