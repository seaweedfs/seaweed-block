# ManagedVolume Operational Model Contract

Status: Phase 30 D2 tightened contract.

Purpose: make `ManagedVolume` the stable operations read model for CLI,
report, dashboard, support bundle, and future read-only operator status.

This contract applies the layered model from
`internal/docs/protocol/layered-participant-authority-master-executor-model.md`:

```text
Participant -> Fact Authority -> Master -> Executor -> Evidence
```

For fields, the executor slot is intentionally absent. `ManagedVolume` fields
are read-only facts and derived state. Executors only appear on
`ManagedVolumeAction` entries.

## Boundary

`ManagedVolume` is the product-state projection for one PVC-backed sw-block
volume.

It is:

- a read model,
- a condition source,
- a support-bundle vocabulary,
- the future operator status source,
- the place where cross-domain state is composed.

It is not:

- a replacement for Kubernetes PVC/PV,
- a second authority publisher,
- a CSI primary selector,
- a mutating repair/rebuild/failback executor,
- a dashboard-specific data shape.

## Role Contract

The implementation contract is encoded in:

- `core/ops/managed_volume_contract.go`
- `core/ops/managed_volume_contract_test.go`

Every stable field must name:

- path,
- stability,
- participant,
- Fact Authority,
- Master,
- aggregation mode,
- probe boundary when applicable,
- condition surface,
- required evidence.

Every allowed action must name:

- action type,
- deciding Master,
- mode,
- side-effect class,
- owner executor,
- policy gate,
- required facts,
- invariant refs when applicable,
- required evidence,
- whether mutation is allowed.

The current contract uses these aggregation modes:

| Mode | Meaning |
|---|---|
| `passive` | Steady-state fact stream is enough. |
| `bounded_probe` | Fact is only accepted from an explicit bounded probe. |
| `passive_plus_bounded_probe` | Passive stream normally used; active probe allowed at decision boundary. |

## Stable Field Authorities

| Path | Fact Authority | Master | Aggregation | Evidence |
|---|---|---|---|---|
| `identity.volume_id` | KubernetesObjectAuthority | ManagedVolumeMaster | passive | PV handle or inventory volume id |
| `identity.namespace` | KubernetesObjectAuthority | ManagedVolumeMaster | passive | PVC/PV object |
| `identity.pvc_name` | KubernetesObjectAuthority | ManagedVolumeMaster | passive | PVC object |
| `desired.replication_factor` | KubernetesObjectAuthority | ManagedVolumeMaster | passive | StorageClass parameter or Helm values |
| `desired.ack_profile` | KubernetesObjectAuthority | ManagedVolumeMaster | passive | StorageClass parameter or Helm values |
| `desired.claim_profile` | ObservationAuthority | ManagedVolumeMaster | passive | claim artifact or release contract |
| `desired.protocol` | KubernetesObjectAuthority | ManagedVolumeMaster | passive | StorageClass parameter or Helm values |
| `kubernetes.pvc_phase` | KubernetesObjectAuthority | ManagedVolumeMaster | passive + bounded probe | PVC status or describe |
| `placement.replica_node` | PlacementAuthority | ManagedVolumeMaster | passive | launcher plan or generated Deployment |
| `authority.primary_replica` | AuthorityLineAuthority | EngineMaster | passive + bounded probe | authority event or inventory primary |
| `authority.epoch` | AuthorityLineAuthority | EngineMaster | passive | authority event |
| `authority.endpoint_version` | AuthorityLineAuthority | EngineMaster | passive | authority event |
| `authority.publish_target` | AuthorityLineAuthority | EngineMaster | passive + bounded probe | authority event or inventory publish target |
| `replica.durable_frontier_lsn` | ReplicaDurabilityAuthority | EngineMaster | passive + bounded probe | status endpoint or promotion evidence |
| `csi.staged_target` | CSIAttachAuthority | ManagedVolumeMaster | passive + bounded probe | CSI event or node-stage log |
| `host_path.rtpg_aas` | HostPathAuthority | ManagedVolumeMaster | passive + bounded probe | `sg_rtpg` artifact |
| `host_path.stale_path_probe` | HostPathAuthority | ManagedVolumeMaster | bounded probe | direct stale-path I/O probe |
| `workload.reader_verified` | WorkloadEvidenceAuthority | ManagedVolumeMaster | bounded probe | reader checksum log |
| `cleanup.status` | CleanupAuthority | ManagedVolumeMaster | bounded probe | cleanup summary |
| `cleanup.k8s_residue_count` | CleanupAuthority | ManagedVolumeMaster | bounded probe | cleanup summary and kubectl artifact |
| `cleanup.iscsi_residue_count` | CleanupAuthority | ManagedVolumeMaster | bounded probe | cleanup summary and iscsiadm artifact |
| `cleanup.multipath_residue_count` | CleanupAuthority | ManagedVolumeMaster | bounded probe | cleanup summary and multipath artifact |
| `cleanup.process_residue_count` | CleanupAuthority | ManagedVolumeMaster | bounded probe | cleanup summary and process artifact |
| `cleanup.hostpath_residue_count` | CleanupAuthority | ManagedVolumeMaster | bounded probe | cleanup summary and hostpath artifact |
| `cleanup.failure_count` | CleanupAuthority | ManagedVolumeMaster | bounded probe | cleanup summary |
| `evidence.reason_code` | ObservationAuthority | ManagedVolumeMaster | passive | projection inputs |

Test-only field:

| Path | Why test-only |
|---|---|
| `workload.same_pod_uid` | Needed to prove transparent mounted failover gates; not a general user-facing Kubernetes status field yet. |

## Action Contract

Fields describe facts. Actions describe allowed next steps.

Phase 30 keeps all actions read-only or dry-run. No action in the current
contract may mutate storage, promote a replica, rebuild data, delete user data,
or perform cleanup without a future policy gate.

| Action | Master | Executor | Mode | Side effect | Policy gate |
|---|---|---|---|---|---|
| `observe.collect_bundle` | ManagedVolumeMaster | `ops` | `read_only` | `observe` | `read_only` |
| `observe.wait_for_pvc_bound` | ManagedVolumeMaster | `ops` | `dry_run` | `observe` | `dry_run` |
| `observe.inspect_mount_failure` | ManagedVolumeMaster | `ops` | `dry_run` | `observe` | `dry_run` |
| `observe.inspect_host_path` | ManagedVolumeMaster | `ops` | `dry_run` | `observe` | `dry_run` |
| `safe_k8s.reinstall_external_iscsi` | ManagedVolumeMaster | `installer_or_operator` | `dry_run` | `safe_k8s` | `dry_run` |
| `safe_k8s.import_csi_image` | ManagedVolumeMaster | `installer_or_operator` | `dry_run` | `safe_k8s` | `dry_run` |
| `authority.request_promotion` | EngineMaster | `authority_recovery_executor` | `dry_run` | `authority_mutating` | `disabled_until_operator_policy` |

`authority.request_promotion` is present only to reserve the contract shape.
It is not emitted as an executable operation in Phase 30.

Required rule:

```text
ManagedVolumeProjection may recommend observation or dry-run remediation.
Only a future operator policy can convert safe_k8s, authority, repair, cleanup,
or destructive actions into executable mutations.
```

## Dual-Mode Aggregation

The model keeps normal operation passive:

```text
heartbeat / event stream / watch cache / status report
```

Active probes are allowed only at decision boundaries:

- promotion decision,
- CSI reattach timeout,
- transparent failover claim,
- stale-primary fencing claim,
- cleanup close gate,
- support bundle for blocked state.

If a probe cannot complete, the ManagedVolume status must stay
`Blocked`, `Recovering`, or `Unknown`; it must not infer `Ready`.

## Condition Surface

The same projection must feed CLI/report/dashboard/operator status:

| Product state | Conditions |
|---|---|
| `ready` | `Ready=True` |
| `recovered` | `Ready=True`, `Recovered=True` |
| `recovering` | `Ready=False`, `Recovering=True` |
| `blocked` | `Ready=False`, `Blocked=True` |
| `invalid` / `unsafe` | `Ready=False`, `Invalid=True` |
| `unknown` | `Ready=Unknown` |

Reason codes must remain stable. New reason codes need tests and support-bundle
field mapping before they become user-facing.

## Current Golden Cases

D9 is anchored by current unit coverage in `core/ops`:

- healthy first-volume ready,
- loopback cross-node blocked,
- PVC pending blocked,
- writer mount failed blocked,
- CSI image pull blocked,
- RF3 node-loss CSI reattach recovered,
- iSCSI ALUA transparent mounted failover recovered,
- missing multipath blocks transparent claim,
- invalid dual-primary beats ready,
- NVMe ANA schema seam does not infer recovery,
- action invariant refs and non-claims derived from facts.

## Rules For Adding Fields

Before adding a field to `ManagedVolumeFacts` or `ManagedVolumeProjection`,
update `ManagedVolumeFactContract()` and answer:

1. Which Participant emits the observation?
2. Which Fact Authority publishes the authoritative fact?
3. Which Master consumes it?
4. Is it passive, bounded-probe, or dual-mode?
5. What decision boundary allows active probe?
6. Which Condition surface can expose it?
7. Which evidence artifact proves it?
8. Which test fails if it drifts?

If the answers are not clear, the field is probably a UI convenience or timing
workaround, not a stable product fact.

Before adding an action to `ManagedVolumeAction`, update
`ManagedVolumeActionContract()` and answer:

1. Which Master allows or refuses it?
2. Which executor is allowed to perform it?
3. Is the current mode `read_only`, `dry_run`, or disabled?
4. Which required facts must be present and fresh?
5. Which invariant prevents unsafe execution?
6. Which evidence proves the action is justified?
7. Which future policy gate can make it executable?

If the action needs mutation today, it is out of Phase 30 scope.

## Open Work

D2 is not fully closed until:

- `ManagedVolumeFactContract()` and `ManagedVolumeActionContract()` are tested,
- report/dashboard/operator surfaces keep using these field names,
- QA verifies one healthy bundle and one blocked bundle against the stable
  field/action vocabulary.
