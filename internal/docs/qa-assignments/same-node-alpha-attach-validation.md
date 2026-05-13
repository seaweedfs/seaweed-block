# QA Assignment: Same-Node Alpha Attach Validation

## Purpose

Validate the D4 runner-native same-node attach gate and D5 negative fixture for
the current Multi-Node Attach And Placement MVP.

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

Happy path:

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/same-node-alpha-attach `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/same-node-alpha-attach-chain.yaml
```

Negative fixture:

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/same-node-alpha-attach-negative `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/same-node-alpha-attach-negative-chain.yaml
```

Adjust `product_root` only if the product checkout on m02 is elsewhere.

## Expected Happy-Path PASS Shape

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

## Expected Negative-Fixture PASS Shape

Scenario `same-node-alpha-attach-negative-chain` should also end in state
`pass`. It deliberately makes the app node differ from the blockvolume node and
expects the demo script to stop before writer attach with exit code `45`.

Required evidence:

- `demo/run.log` contains `unsupported cross-node loopback attach`.
- `demo/unsupported-cross-node-loopback-attach.txt` contains:
  - `issue=unsupported_cross_node_loopback_attach`
  - `app_node=sw-block-not-the-blockvolume-node`
  - `blockvolume_node=<actual node>`
  - `frontend=127.0.0.1:<port>`
  - `reason=loopback frontend requires app pod and blockvolume on the same node`
- `demo/ops-inventory-unsupported-placement/volume-inventory-summary.txt`
  contains `pvc=sw-block-demo-pvc`.
- cleanup leaves no iSCSI sessions, sw-block processes, or port-forwards.

Report as blocking if this fixture turns into a pod scheduling timeout without
the issue file and inventory bundle.

## QA Needed After This

If both scenarios pass, D4 and D5 are validated. Dev can proceed to the
operations manual update and close-gate prep.
