# Release Minimal New-User Cleanup Recheck Assignment

Date: 2026-05-26

Owner: QA.

Purpose: verify the B1 cleanup blocker from
`release-minimal-new-user-qa-signoff.md` is fixed.

## Source Commit Under Test

Use the dev commit that includes:

- `scripts/uninstall-k8s-alpha.sh` iSCSI node DB scrub,
- `scripts/uninstall_k8s_alpha_test.go`.

## Minimal Recheck

Run only Step 8 from the minimal new-user release validation after a normal
first-volume run, or reproduce with a known sw-block iSCSI node DB record.

Commands:

```bash
helm uninstall sw-block --namespace kube-system
bash scripts/uninstall-k8s-alpha.sh "$PWD"
bash scripts/verify-helm-cleanup.sh
```

Expected:

```text
cleanup_status=ok
iscsi_residue_count=0
k8s_residue_count=0
multipath_residue_count=0
process_residue_count=0
hostpath_residue_count=0
```

Also verify:

```bash
sudo iscsiadm -m node | grep -q io.seaweedfs && exit 1 || true
```

## Evidence To Record

- `iscsi-nodes.before-scrub.txt`
- `delete-iscsi-node-records.log`
- `iscsi-nodes.after-scrub.txt`
- `cleanup-summary.txt` from `verify-helm-cleanup.sh`

## Expected Sign-off Update

Append to:

```text
internal/docs/qa-assignments/release-minimal-new-user-qa-signoff.md
```

Required verdict:

```text
B1: RESOLVED
Minimal release new-user validation: PASS (strict)
```
