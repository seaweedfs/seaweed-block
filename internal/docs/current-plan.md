# Current Plan: Phase 101 Data Lifecycle MVP

Status: active plan; implementation not started.

## Why This Is Next

The operation layer now has a coherent beta-candidate loop:

```text
PVC / SwBlockVolume identity
  -> CRD status and Events
  -> evidence-backed decisions
  -> bounded lifecycle-owner finalizer mutation
  -> delete-safety hold/release
```

Phase 98 closed the returned-replica operation loop. Phase 100 closed the
supported-lab Kubernetes CSI NVMe multipath attach path. The next user-visible
gap is data lifecycle: users can create and operate block volumes, but they
cannot yet create a product-owned recovery point, export it as a backup, or
restore it into a new PVC-backed volume.

This phase must reuse the operation model rather than invent a separate control
plane:

```text
live facts -> judgment -> safe action -> evidence -> CRD/report/dashboard/explain
```

## Product Goal

Deliver a narrow, honest data-lifecycle MVP:

```text
source PVC-backed volume
  -> create a crash-consistent snapshot/checkpoint record
  -> export/record backup metadata and artifact location
  -> dry-run restore preflight with explicit blockers
  -> restore into a new PVC-backed volume only when evidence is sufficient
  -> writer/reader verifies restored data
  -> cleanup proves zero residue
```

The first milestone can be source-gated. Do not claim production backup, remote
DR, encryption/KMS, incremental forever, application consistency, or broad
retention policy support.

## D1: Data Lifecycle Contract

Define the product vocabulary and schema before wiring mutation:

- `SwBlockSnapshot` or equivalent snapshot record shape.
- `SwBlockBackup` or equivalent backup/export record shape.
- restore request/decision shape.
- status conditions:
  - `Ready`
  - `Blocked`
  - `EvidenceStale`
  - `SnapshotCreated`
  - `BackupAvailable`
  - `RestoreReady`
  - `RestoreBlocked`
- stable reasons:
  - `snapshot_source_not_ready`
  - `snapshot_checkpoint_created`
  - `backup_artifact_recorded`
  - `restore_source_missing`
  - `restore_target_exists`
  - `restore_preflight_passed`
  - `restore_data_verified`
- action records:
  - `data_lifecycle.create_snapshot`
  - `data_lifecycle.export_backup`
  - `data_lifecycle.restore_volume`

Success criteria:

- schema is camelCase and Kubernetes OpenAPI-valid;
- status/action records appear in report, dashboard, operator-snapshot, and
  explain;
- no action claims execution without evidence.

## D2: Snapshot Evidence Gate

Implement the first source-gated snapshot/checkpoint path.

Minimum acceptable scope:

- single source volume;
- quiesce/application-consistency is not claimed;
- snapshot is crash-consistent at the block layer only;
- evidence records source volume ID, PVC, authority epoch, primary, publish
  target, durable frontier/checkpoint identity, and captured time.

Success criteria:

- snapshot creation refuses when source volume is not Ready;
- snapshot evidence is stable enough for cold-bundle replay;
- no false `SnapshotCreated=True` when the checkpoint is missing or stale.

## D3: Backup Artifact Metadata Gate

Record backup/export metadata without overclaiming enterprise backup.

Minimum acceptable scope:

- local filesystem or configured artifact path only;
- backup artifact has identity, source snapshot, size/checksum if available,
  created time, and restore compatibility metadata;
- status surfaces show where the artifact is and whether it is complete.

Success criteria:

- backup metadata is reproducible from support bundle;
- missing/incomplete artifact surfaces `Blocked` or `EvidenceStale`, never
  `BackupAvailable=True`;
- cleanup verifies no temporary export residue.

## D4: Restore Preflight Gate

Before mutating a target volume, implement dry-run restore judgment.

Preflight must check:

- backup artifact exists and matches declared snapshot/source identity;
- target PVC/volume name is free or explicitly allowed;
- requested size is compatible;
- protocol/storage backend compatibility is explicit;
- restore action owner has the minimum required RBAC and no broad workload or
  storage mutation power.

Success criteria:

- clean backup -> `restore_preflight_passed`;
- missing artifact/source mismatch/target collision -> `RestoreBlocked`;
- CRD/report/dashboard/explain agree;
- action remains dry-run until the restore executor gate starts.

## D5: Restore Executor Close Gate

Implement one bounded restore path after D4 is green.

Minimum acceptable scope:

- restore one source backup into one new PVC-backed volume;
- verify mounted reader sees the restored data;
- status and Events record `restore_data_verified`;
- multi-volume isolation: restoring volume A cannot change volume B status,
  finalizers, authority, or frontend publication.

Success criteria:

- writer/reader proves restored data;
- failed restore leaves no false Ready and no hidden residue;
- cleanup verifier returns all zero residue counts;
- final sign-off states exact non-claims.

## Non-Claims

- no production backup/SLO/retention policy claim;
- no application-consistent snapshot claim;
- no remote DR or cross-cluster restore claim;
- no incremental forever/dedup/compression claim;
- no backup encryption/KMS claim;
- no broad UI claim.

## Release Relationship

Operation Layer v0.5 remains a large release candidate but still needs matching
published images and pinned-image smoke before being marked released.

Phase 101 is development after that operation milestone. It should not block the
operation release, and the operation release should not claim Phase 101 data
lifecycle features.
