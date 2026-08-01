# Phase 175 D6 Restored PVC Mount Diagnostic

Status: FAIL - product defect identified

Validated exact commit `f1ba2520418963f399fa0c9df42cbbdb5e8cd26f` on the
`m02` k3s lab. This is diagnostic evidence, not a D6 pass.

## Classification

The restored image is correct. The failure is in post-restore replication
activation, not archive integrity, CSI device discovery, filesystem quiesce,
or source unstage timing.

## Evidence

- The source consumer was deleted and its `VolumeAttachment`, kubelet mount,
  and iSCSI session were absent before snapshot creation.
- Source and target devices were 1 MiB with 256 4096-byte blocks.
- Snapshot `snap-388ea3738654857c013c6bd7f828578b` contained 27 records,
  110592 data bytes, and source frontier 71.
- The restore marker was `activated` with 27 restored blocks and target
  frontier 27.
- Both source and target passed `blkid` and `e2fsck -fn`.
- The complete source and target devices had the same SHA-256:
  `0c4a5a583f4f82c475ab899e200004bdefd90ef4f2e003799cc888e6a1bdae1f`.
- The first ext4 write reached `StorageBackend.Write` at LSN 28, then timed
  out through iSCSI. The replication resequencer had been constructed before
  restore with `nextShipLSN=1` and waited for nonexistent LSN 1.

The product fix must advance the resequencer only from verified durable target
frontier evidence, before local readiness is released. It must not infer a
restore from an arbitrary high arriving LSN.

## Cleanup And Artifact

Cleanup finished with `cleanup_status=ok` and zero Helm, Kubernetes, iSCSI,
multipath, device-mapper, hostPath, process, or test-image residue.

Evidence bundle:
`/mnt/smb/work/share/g15d-k8s/phase175-d6-f1ba252-diagnostic.tar.gz`

Bundle SHA-256:
`6313b0a9bd2ab024d0c1332b7e08685e6263c82b3048064253e1305bee7c00f6`
