# Phase 175 D6 Kubernetes Snapshot Restore QA Sign-Off

Verdict: **PASS** for the real Kubernetes `VolumeSnapshot` to restored-PVC
mounted data path. This does not close D7 dirty failure or D9 release gates.

## Provenance

- Commit: `d02ad493e8ac2fa7439cf9f5e248a75bb91797b2`.
- Environment: m02 k3s Linux lab with matching fresh images.
- Evidence bundle:
  `V:\share\g15d-k8s\phase175-d6-d02ad49-PASS`.
- Evidence SHA-256:
  `4f8fa556be2f14fc3f35bfe1fb71409f5134e7b0958cd852d629a8c75b38f651`.

## User-Path Evidence

- The real CSI snapshot capability was advertised and a real
  `VolumeSnapshot` reached ready.
- Source NodeUnstage and iSCSI session cleanup completed before restore.
- Restore produced a distinct ext4 device at `/dev/sdc`; it was not a reused
  source mapping.
- Data written before the snapshot cut was present after mount.
- A sentinel written after the cut was absent from the restored volume.
- A new write to the restored filesystem was read back independently.
- Restore published frontier 15; the first post-restore write used LSN 16.
- No post-restore write timeout occurred.

## Verification

- Requested Linux race coverage passed for the five affected packages.
- Snapshot and restore identity, geometry, and mounted data evidence agreed.
- Supported uninstall followed by `verify-helm-cleanup.sh` reported
  `cleanup_status=ok` with zero residue.

## Finding

Raw `helm uninstall` left two inactive `io.seaweedfs` iSCSI node database
records. The supported product uninstall path scrubbed them and the final
verifier passed. The release/user documentation must direct users to the
supported uninstall path rather than claim raw Helm deletion is sufficient.

## Boundary

This gate proves the healthy Kubernetes snapshot/restore path. It does not
prove create crash/retry, restore restart, abort/discard, corrupt backup,
multi-volume isolation, or matching published-image release behavior.
