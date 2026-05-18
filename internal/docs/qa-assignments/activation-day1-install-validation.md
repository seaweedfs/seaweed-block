# QA Assignment: Phase 20 D1 Activation / Day-1 Install

## Product Question

Can a new Kubernetes user run one documented activation command and get a
usable alpha install summary without manually stitching together preflight,
image build/import, install, rollout checks, and StorageClass setup?

## Run

Default dev/QA work-tree path:

Use the runner-native chain:

```bash
swblock run \
  -env product_root=/tmp/seaweed_block \
  -env ssh_key=C:\work\dev_server\testdev_key \
  testops/scenarios/activation-day1-install-chain.yaml
```

The scenario executes:

```bash
bash scripts/activate-k8s-alpha.sh /tmp/seaweed_block
```

It then uninstalls with:

```bash
bash scripts/uninstall-k8s-alpha.sh /tmp/seaweed_block
```

PM/user-path published-image smoke, after images are pushed:

```bash
SW_BLOCK_ACTIVATION_IMAGE_MODE=published \
SW_BLOCK_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit> \
SW_BLOCK_CSI_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<commit> \
  bash scripts/activate-k8s-alpha.sh /tmp/seaweed_block
```

Use the mutable `:alpha` tags only for exploratory smoke. Close validation
should record immutable `sha-<commit>` or release tags in
`activation-summary.txt`.

## Required Evidence

The activation bundle must contain:

- `activation-summary.txt`
- `readiness.txt`
- `build/alpha-images.env`
- `install/install.log`
- `storageclass.log`

`activation-summary.txt` must include:

```text
activation_status=ok
image_mode=local
master_ready_replicas=1
csi_controller_ready_replicas=1
csi_node_ready=<ready>/<desired>
storageclass=sw-block-dynamic
storageclass_provider=block.csi.seaweedfs.com
protocol=iscsi
ack_profile=best-effort
next_create_volume=kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml
non_claims=alpha_only,no_backup_restore,no_upgrade_safety,no_mutating_dashboard_actions,no_broad_performance_slo
```

For published-image smoke, `image_mode=published` must appear and the summary
must record the exact image tags used.

If published mode fails during install, the activation bundle must contain
`diagnostics/failure-context.txt`. If the pod crashes because the image is older
than this source tree, the file should include:

```text
activation_blocker=image_flag_mismatch
remediation=republish the image from this commit or use matching sha-<commit> image tags
```

## Pass Criteria

- The scenario passes all actions.
- The activation command builds/imports the local alpha images.
- `blockmaster`, CSI controller, and CSI node are Ready.
- The default alpha StorageClass exists and uses `block.csi.seaweedfs.com`.
- The summary gives a clear next command for creating a PVC-backed volume.
- Cleanup leaves no active `io.seaweedfs` iSCSI sessions.
- Cleanup leaves no `blockmaster`, `blockvolume`, `blockcsi`, or `iscsi-target`
  processes.
- `uninstall-k8s-alpha.sh` removes the alpha StorageClass.

## Fail Conditions

- The user-facing activation command requires an undocumented side script.
- The activation summary is missing or does not name the blocker.
- A component is not Ready but `activation_status=ok` is emitted.
- The StorageClass is absent after activation.
- Cleanup leaves product processes, active iSCSI sessions, or the alpha
  StorageClass behind.

## Non-Claims

This D1 gate does not prove app write/read, add-volume, dashboard, recovery, or
node-loss behavior. Those are later Phase 20 gates or already-closed capability
plans. This gate only validates the install-to-ready activation entry point.
