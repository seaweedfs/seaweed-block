# ManagedVolume Operational Model Contract

Status: Phase 28 D9 working contract.

Purpose: make `ManagedVolume` the stable operations read model for CLI,
report, dashboard, support bundle, and future read-only operator status.

This contract applies the layered model from
`internal/docs/protocol/layered-participant-authority-master-executor-model.md`:

```text
Participant -> Fact Authority -> Master -> Executor -> Evidence
```

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

The current contract uses these aggregation modes:

| Mode | Meaning |
|---|---|
| `passive` | Steady-state fact stream is enough. |
| `bounded_probe` | Fact is only accepted from an explicit bounded probe. |
| `passive_plus_bounded_probe` | Passive stream normally used; active probe allowed at decision boundary. |

## Stable Field Authorities

| Path | Fact Authority | Master | Aggregation | Evidence |
|---|---|---|---|---|
| `identity.namespace` | KubernetesObjectAuthority | ManagedVolumeMaster | passive | PVC/PV object |
| `identity.pvc_name` | KubernetesObjectAuthority | ManagedVolumeMaster | passive | PVC object |
| `kubernetes.pvc_phase` | KubernetesObjectAuthority | ManagedVolumeMaster | passive + bounded probe | PVC status or describe |
| `placement.replica_node` | PlacementAuthority | ManagedVolumeMaster | passive | launcher plan or generated Deployment |
| `authority.primary_replica` | AuthorityLineAuthority | EngineMaster | passive + bounded probe | authority event or inventory primary |
| `authority.epoch` | AuthorityLineAuthority | EngineMaster | passive | authority event |
| `replica.durable_frontier_lsn` | ReplicaDurabilityAuthority | EngineMaster | passive + bounded probe | status endpoint or promotion evidence |
| `csi.staged_target` | CSIAttachAuthority | ManagedVolumeMaster | passive + bounded probe | CSI event or node-stage log |
| `host_path.rtpg_aas` | HostPathAuthority | ManagedVolumeMaster | passive + bounded probe | `sg_rtpg` artifact |
| `host_path.stale_path_probe` | HostPathAuthority | ManagedVolumeMaster | bounded probe | direct stale-path I/O probe |
| `workload.reader_verified` | WorkloadEvidenceAuthority | ManagedVolumeMaster | bounded probe | reader checksum log |
| `cleanup.multipath_residue_count` | CleanupAuthority | ManagedVolumeMaster | bounded probe | cleanup summary and multipath artifact |
| `evidence.reason_code` | ObservationAuthority | ManagedVolumeMaster | passive | projection inputs |

Test-only field:

| Path | Why test-only |
|---|---|
| `workload.same_pod_uid` | Needed to prove transparent mounted failover gates; not a general user-facing Kubernetes status field yet. |

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

## Open Work

D9 is not fully closed until:

- the contract is used by `sw-block ops report` / dashboard field rendering,
- the future CRD/Condition examples in D10 are generated from this vocabulary,
- QA verifies one healthy bundle and one blocked bundle against the stable
  field names.
