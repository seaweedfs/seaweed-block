# Phase 22 Plan: ManagedVolume Operations Model

## Status

Closed for Phase 22 scope, about 90% overall release confidence because
repository-wide regression still has unrelated failures outside this scope.

Closed in code so far: ManagedVolume spec seed, model tests over existing
gates, ops/report/explain JSON alignment, dry-run action contracts, K8s
blocked-state projection, iSCSI ALUA host-path projection, non-claim
derivation, and bundle artifact replay for node-loss and Stage 2 recovery
summaries.

Close artifacts:

- `internal/docs/qa-assignments/managed-volume-operations-model-close-report.md`
- `internal/docs/finished-plans/phase22_finishedplan_managed_volume_operations_model.md`

Remaining outside Phase 22 scope: track the unrelated full-repo failures in
`cmd/sparrow` and `core/frontend/iscsi` before making a repository-wide release
green claim.

## Product Question

Can Seaweed Block represent a Kubernetes PVC-backed block volume as one
product entity, so operations, dashboard, failover, CSI, iSCSI, and future NVMe
logic do not keep spreading across scripts, listeners, and unrelated small
state machines?

## Decision

Phase 22 is not a small observation polish phase. It is the first internal model
evolution after Helm:

```text
PVC / PV / StorageClass intent
-> ManagedVolume read model
-> authority, replica, frontend, CSI, host-path, recovery projections
-> user-facing ops/report/explain/dashboard surfaces
```

`ManagedVolume` is not a new user API and not a replacement for Kubernetes PVC.
It is Seaweed Block's internal product model for "the volume a user thinks they
created". It gives one place to explain:

- what Kubernetes requested,
- which sw-block replicas exist,
- who is primary,
- where the current frontend is,
- whether CSI has staged the right target,
- whether the host path is loopback, external iSCSI, multipath ALUA, or later
  NVMe ANA,
- whether failover/recovery is pending, blocked, or complete,
- what evidence proves writer/reader data checks.

It does not own lifecycle transitions. Ownership remains:

- Kubernetes owns PVC/PV lifecycle.
- CSI owns publish/stage lifecycle.
- blockmaster launcher or future operator owns generated `blockvolume`
  workload lifecycle.
- blockmaster authority/recovery owns primary, epoch, promotion, and
  fail-closed decisions.
- host initiator/kernel owns iSCSI/NVMe path mechanics.
- `ManagedVolume` owns correlation, projection, reason codes, evidence refs,
  and user-facing explanation.

## Why This Is Needed

Recent gates proved real product behavior:

- Helm Day-1 install and first PVC,
- RF3 sync-quorum recovery through CSI reattach,
- Stage 2 iSCSI ALUA/dm-multipath transparent failover,
- Kubernetes node-loss recovery through pod recreate,
- product-owned event stream and static report.

The implementation now has enough capability that the weak point is no longer a
single missing feature. The weak point is semantic drift:

- launcher placement has product facts,
- master authority has product facts,
- CSI publish/stage has product facts,
- inventory/report has product facts,
- TestOps and scripts capture blocked states,
- host commands capture iSCSI/multipath facts.

If those stay separate, every new surface will rediscover PVC state, replica
state, host-path state, and recovery state differently. NVMe ANA, rebuild, and
operator Conditions would multiply that problem.

## Non-Claims

Phase 22 does not claim:

- new HA behavior,
- operator lifecycle,
- mutating admin actions,
- repair/rebuild/failback,
- backup/snapshot/restore,
- hosted dashboard,
- production SLOs.

It creates the product state model and action contract that those later
surfaces must use. Initial execution remains read-only/dry-run, but action
preconditions should be defined here so future operator/controller execution
does not bypass global visibility.

## Model Boundary

`ManagedVolume` should compose facts from these domains without stealing their
authority:

| Domain | Owns Truth | ManagedVolume Uses It For |
|---|---|---|
| Kubernetes | PVC/PV/StorageClass, Pod scheduling, Node readiness | user intent, attach locality, blocked Day-1 diagnosis |
| CSI | ControllerPublish target, NodeStage/NodePublish observations | attach path, reattach evidence, pod recreate vs transparent path |
| Master authority | primary, epoch, endpoint_version, publish target | one-primary invariant, recovery story |
| Replica/blockvolume | durable frontier, role, status endpoint, frontend | promotion eligibility, replica health, data-plane reachability |
| Host path | iSCSI sessions, dm-multipath maps, ALUA state, future NVMe ANA | transparent failover proof and host-side blockers |
| Workload | writer/reader checksum, same pod UID, app logs | user-visible data verification |

The model can derive status, but it must not mint authority or own lifecycle.
Promotion still belongs to master/recovery. CSI still consumes authority.
Kubernetes still owns PVC/Pod lifecycle. Launcher/operator still owns generated
`blockvolume` workload lifecycle.

## Proposed Shape

```text
Collectors
  -> K8s, CSI, master, replica, host path, workload facts

ManagedVolume Store
  -> stable volume identity
  -> typed facts with source, observed_at, generation/epoch, confidence

Projection Engines
  -> Kubernetes intent/attach projection
  -> authority/recovery projection
  -> frontend/host-path projection
  -> workload data-check projection
  -> observation/report/explain projection

Action Contract
  -> allowed_actions / recommended_actions
  -> policy gate
  -> owning executor
  -> audit/evidence requirements

Read Surfaces
  -> sw-block ops cluster/report/explain
  -> future read-only dashboard
  -> future operator Conditions/Events
```

This is still "one global model, many local controllers". It is not one monster
engine.

## D1: ManagedVolume Spec

Define the stable internal entity and fields.

Core identity:

- `managed_volume_id`
- `kubernetes_namespace`
- `pvc_name`
- `pv_name`
- `volume_handle`
- `storage_class`
- `replication_factor`
- `ack_profile`
- `claim_profile`

Topology:

- `replicas[]`
- `replica_id`
- `server_id`
- `kubernetes_node`
- `physical_host` when known
- `frontend_address`
- `status_address`
- `protocol`

Authority/recovery:

- `primary_replica`
- `epoch`
- `endpoint_version`
- `required_frontier_lsn`
- `candidate_frontier_lsn`
- `candidate_ready`
- `promotion_blocker`

K8s/CSI:

- `pvc_phase`
- `pod_mounts[]`
- `controller_publish_target`
- `node_stage_target`
- `reattach_generation`
- `attach_blocker`

Host path:

- `iscsi_sessions[]`
- `multipath_map`
- `alua_states[]`
- future `nvme_paths[]`
- future `ana_states[]`

Evidence:

- `events[]`
- `reason_codes[]`
- `evidence_refs[]`
- `writer_verified`
- `reader_verified`
- `cleanup_status`

Acceptance:

- every field has an owner domain,
- every derived field cites source facts,
- the model can represent mixed/conflicting evidence,
- no projection field is used as control truth.
- every action hint names preconditions, owner executor, side-effect class, and
  evidence requirement.
- state dimensions and priority rules follow
  [`engine-design-guidelines.md`](./engine-design-guidelines.md), especially
  multi-state overlay and unsafe/blocked/recovering/degraded/ready priority.
- TDD: add table tests before or with each new fact/state/action.
- Internal review: confirm lifecycle ownership is not stolen from Kubernetes,
  CSI, master, launcher/operator, or host path.
- Regression check: `go test ./core/ops -count=1`.

## D2: Model Tests For Existing Gates

Add table-driven model tests that feed simulated facts and assert the composed
ManagedVolume state for:

1. Helm single-node first-volume healthy path.
2. Helm multi-node first-volume healthy path.
3. First-volume blocked by loopback publish target across nodes.
4. CSI node image pull blocked path.
5. RF3 CSI/pod-recreate recovery.
6. Stage 2 transparent iSCSI ALUA/multipath failover.
7. Kubernetes node-loss reattach recovery.

Acceptance:

- same fact set produces same projection independent of event order,
- one-primary invariant holds,
- stale-primary fence invariant holds,
- reason codes are stable,
- non-claims are derived from facts.
- TDD: each scenario starts as a red/green table test.
- Internal review: compare each test against an existing live gate and name the
  invariant rows it protects.
- Regression check: `go test ./core/ops -count=1`.

## D3: Operations Surface Alignment

Align read-only operations with ManagedVolume:

- `sw-block ops cluster`
- `sw-block ops report`
- `sw-block ops explain volume`
- `sw-block ops generate-helm-values`

Acceptance:

- outputs are human-readable by default,
- JSON/JSONL forms are stable enough for AI/dashboard,
- no SSH is required for the normal read path,
- blocked first-volume and recovery cases cite facts and evidence refs.
- action hints are emitted as `dry_run` / `not_executed` only.
- TDD: CLI/report tests assert both human text and JSON fields.
- Internal review: verify no mutating RPC, kubectl write, or host mutation is
  introduced.
- Regression check: `go test ./cmd/sw-block ./core/ops -count=1`.

## D3b: Action Contract Seed

Define action shapes without enabling mutating execution:

- `observe.collect_bundle`
- `safe_k8s.apply_missing_owned_object`
- `safe_k8s.update_condition`
- `disruptive_k8s.recreate_workload`
- `authority.request_promotion`
- `repair.start_rebuild`
- `cleanup.cleanup_residue`

Acceptance:

- every action has owner executor, required facts, invariant references, and
  side-effect class,
- Phase 22 emits only read-only or dry-run actions,
- no local component should perform a product-level action that is not
  representable in this contract.
- TDD: action suggestion tests must include missing-fact negative cases.
- Internal review: every action names an executor and policy gate.
- Regression check: `go test ./core/ops -count=1`.

## D4: K8s Adaptor Projection

Implement the K8s/PVC projection over facts, not shell output.

Initial states:

- `uninstalled`
- `installing`
- `ready`
- `first_volume_pending`
- `first_volume_ready`
- `reattach_expected`
- `recovered`
- `blocked`

Blocked reasons:

- `csi_node_image_pull_failed`
- `publish_target_loopback_cross_node`
- `writer_mount_failed`
- `pvc_unbound`
- `primary_unavailable_candidate_not_ready`
- `csi_reattach_timeout`

Acceptance:

- no authority minting,
- no promotion decision,
- emits timeline events and reason codes,
- bundle explains first-volume and node-loss blocked cases.
- TDD: blocked-state fixtures cover ImagePullBackOff, FailedMount, PVC Pending,
  and loopback cross-node target.
- Internal review: confirm K8s facts remain observations, not authority inputs
  that bypass master.
- Regression check: `go test ./core/ops ./cmd/sw-block -count=1`.

## D5: Host Path Projection

Formalize host path projection for iSCSI/ALUA/multipath, with an explicit seam
for NVMe ANA.

Current fields:

- iSCSI sessions per portal/IQN,
- dm-multipath map,
- path state,
- ALUA target port group state,
- stale path evidence,
- workload checksum evidence.

Future fields:

- NVMe subsystem/path identity,
- ANA state,
- namespace/device mapping,
- host multipath mode.

Acceptance:

- transparent failover requires same pod UID and host path transition,
- failed old path does not imply data loss by itself,
- no transparent claim without workload checksum,
- NVMe can plug into the same projection without adding parallel product logic.
- TDD: host-path projection tests cover iSCSI ALUA first and leave NVMe ANA as
  explicit schema seam.
- Internal review: confirm host-path facts do not infer authority or data
  recovery alone.
- Regression check: `go test ./core/ops -count=1`.

## D6: Ledger Stamp And Close Gate

Update [`invariant-ledger.md`](./invariant-ledger.md):

- promote `INV-MANAGED-VOLUME-READMODEL-001`,
- promote `INV-K8S-ADAPTOR-FACTS-001`,
- promote `INV-HOSTPATH-FACTS-001`,
- add any new rows discovered during D1-D5.

Current D6 checkpoint:

- `INV-MANAGED-VOLUME-READMODEL-001`: ACTIVE via model/evidence/report tests.
- `INV-CONTROL-CONTEXT-001`: ACTIVE via product event D5 plus bundle replay
  tests.
- `INV-K8S-ADAPTOR-FACTS-001`: ACTIVE via PVC pending, loopback, image-pull,
  mount-failure, and node-loss reattach model tests.
- `INV-HOSTPATH-FACTS-001`: ACTIVE via iSCSI ALUA transparent recovery and
  non-claim model tests.
- Regression: `go test ./cmd/sw-block ./core/ops -count=1` passes.

Close with:

```text
model tests
-> ops/report tests
-> Helm first-volume rerun
-> one blocked first-volume fixture
-> one recovery evidence replay
```

Every close report must include:

- TDD summary,
- internal review summary,
- regression command output,
- live/TestOps evidence if applicable,
- explicit non-claims.

Do not start with a full live multi-node gate. The purpose of Phase 22 is to
reduce dependence on slow live-only debugging.

## Expected Product Outcome

A user, support engineer, dashboard, or AI assistant should be able to ask:

```text
What volume did this PVC create?
Where is the current primary?
What target did CSI stage?
Why is my pod mount blocked?
Was this CSI reattach or transparent multipath?
Which replica/path is unhealthy?
What evidence proves data was verified?
What is the next safe action?
```

and get a fact-backed answer from product state, not from ad-hoc TestOps grep.
