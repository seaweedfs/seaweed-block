# Phase 30 Control-State Dependency Review

Date: 2026-05-24

Purpose: define the current control-state dependency map for the Kubernetes
block product before adding mutating operator actions, rebuild/failback, NVMe
ANA parity, or backup workflows.

This review is about model ownership. It does not add a new product claim.

## Control Model Rule

Phase 30 keeps the layered rule from the protocol docs:

```text
fact authority publishes facts
master/projection composes product state
executor performs allowed actions
evidence records why a state/action is valid
TestOps audits the whole path externally
```

Definitions:

- Fact authority: the component that can state a fact without another layer
  minting it.
- Master/projection: the component that composes facts across domains.
- Executor: the component allowed to perform a future action.
- Bounded probe: an on-demand fact refresh allowed when passive state is stale,
  missing, or needed for a safety decision.

## Dependency Matrix

| Domain | Stable facts | Fact authority | Passive source | Bounded probe | Master/projection | Future executor | Risk if scattered |
|---|---|---|---|---|---|---|---|
| Helm install intent | image refs, RF, ACK profile, network mode, CHAP, selected block nodes | Helm values generator + chart values | `values.yaml`, chart render, activation summary | `sw-block ops generate-helm-values` can query Kubernetes nodes before render | ManagedVolume / install report | Helm | Docs, chart, and scripts can drift on ports/network/security. |
| Kubernetes PVC/PV | namespace, PVC name, PV name, bound phase, StorageClass | Kubernetes API + CSI provisioner | PVC/PV objects | `kubectl get/describe pvc,pv` when Pending or timeout | ManagedVolume projection | Kubernetes user / CSI external provisioner | CLI/dashboard can misclassify Pending without pod/CSI context. |
| StorageClass parameters | RF, protocol, ACK profile, secrets | Kubernetes API + chart/examples | StorageClass object | read StorageClass during volume explain | ManagedVolume projection | Helm/user | Wrong RF/ACK claim can appear if report uses helper defaults instead of SC facts. |
| Launcher placement intent | replica slots, node placement, frontend/status ports, Deployment names | blockmaster launcher | launcher plan / generated Deployments | Kubernetes Deployment read when placement drift suspected | ManagedVolume projection | launcher / future operator | Generated Deployment drift becomes hidden state machine. |
| Generated blockvolume runtime | desired replicas, observed pods, ready replicas, args, node | Kubernetes API + launcher desired state | Deployment/Pod objects | `kubectl get/describe deploy,pod` on readiness/failover/cleanup gates | ManagedVolume projection | launcher / future operator | Helper waits can become product truth. |
| Authority identity | primary replica, epoch, endpoint_version, publish target | blockmaster authority engine | authority event stream, inventory/status endpoint | master may probe surviving replicas before promotion or when frontier unknown | engine master + ManagedVolume projection | authority/recovery executor | Any layer other than authority can accidentally mint primary truth. |
| Promotion readiness | candidate readiness, required frontier, candidate frontier, fail-closed reason | recovery/promotion evaluator | promotion evidence, inventory | bounded status/frontier probe on promotion decision | engine master | authority/recovery executor | Recovery UI can claim safe promotion from incomplete facts. |
| CSI publish/stage | ControllerPublish target, NodeStage target, reattach event, node name | CSI controller/node | CSI events ingested by master, CSI logs, publish context | query node-stage evidence or bundle logs on attach mismatch | ManagedVolume projection | CSI | Dashboard cannot distinguish CSI reattach from host multipath switch. |
| Host path | iSCSI sessions, dm-multipath maps, ALUA AAS, stale-path probe | host initiator + host-path probe | support artifacts, sg_rtpg, multipath, iscsiadm | bounded host probe when transparent failover or cleanup is claimed | ManagedVolume projection | CSI / host cleanup executor later | Transparent failover evidence can depend on ad-hoc grep. |
| Workload evidence | writer checksum, reader checksum, same pod UID | workload probe/TestOps/user app | writer/reader logs, app pod UID artifacts | workload read/write probe in test gates | ManagedVolume projection | user/TestOps; future operator only with policy | Storage health can be confused with app not scheduled or app image failure. |
| Cleanup evidence | cleanup status, residue counts, failure reasons | cleanup verifier + Kubernetes/host APIs | `cleanup-summary.txt`, residue artifacts | cleanup close-gate probe | ManagedVolume projection / report | Helm/helper now; future operator only after policy | Residue can be hidden by force cleanup before evidence capture. |
| Support/report artifacts | cluster evidence, timeline, summary, operator snapshot | observation/report engine | bundle files | replay bundle with `sw-block ops report --from-bundle` | observation engine | ops CLI/dashboard | Each surface can invent its own truth or reason codes. |

## Stable / Provisional / Test-Only Classification

### Stable

These can remain user-facing read-only facts:

- `volume_id`
- `namespace`
- `pvc_name`
- `pv_name`
- `replication_factor`
- `ack_profile`
- `primary_replica`
- `primary_node`
- `publish_target`
- `epoch`
- `endpoint_version`
- `status`
- `reason`
- `conditions`
- `candidate_ready`
- `frontier_covered`
- `required_frontier_lsn`
- `candidate_frontier_lsn`
- `cleanup_status`
- `k8s_residue_count`
- `iscsi_residue_count`
- `multipath_residue_count`
- `process_residue_count`
- `failure_count`

### Provisional

Useful for support/report but not yet public API:

- raw ALUA AAS values,
- raw iSCSI session rows,
- raw `multipath -ll` text,
- app node distribution counters,
- interleaved fault window,
- helper-local cleanup internals,
- exact selected scenario names.

### Test-Only

Keep in TestOps artifacts unless promoted deliberately:

- run IDs,
- local artifact paths,
- exact pod UIDs,
- generated manifest paths,
- local SSH target names,
- raw command stdout used only by scenario assertions.

## Bounded Probe Policy

Most state should be passively collected. Bounded probes are allowed when:

- a user-visible state is blocked or timed out,
- a promotion/failover decision needs fresh frontier or authority facts,
- a transparent failover claim needs host-path proof,
- cleanup close needs residue proof,
- report replay needs to validate bundle evidence.

Bounded probes must:

- have a timeout,
- record evidence,
- not mutate state unless the executor and policy explicitly allow it,
- not mint another domain's truth.

## Current Risk Hotspots

| Hotspot | Why it matters | Phase 30 treatment |
|---|---|---|
| Helper-derived lifecycle status | Helpers still summarize some product states. | D2/D3 should move durable field definitions into model/report tests. |
| CSI vs host-path recovery classification | CSI reattach and multipath transparent failover use different evidence chains. | D3 candidate for projection ownership cleanup. |
| Generated Deployment lifecycle | Launcher owns desired runtime, Kubernetes owns observed runtime, helpers wait. | D2 should freeze desired/observed/absent fields. |
| Cleanup facts | Phase 29 added evidence parity, but execution remains helper-owned. | Keep read-only; no mutating operator cleanup yet. |
| Action hints | Dry-run actions exist, but executor/policy boundaries must stay explicit. | D2 should assert action side-effect class and executor. |

## Recommended D2/D3 Focus

Recommended D2:

- update the ManagedVolume field contract to include cleanup fields and
  classify host-path/recovery fields as stable/provisional/test-only.

Recommended D3:

- pick one projection chain and remove ambiguity. Best candidate:

```text
cleanup-summary.txt -> CleanupEvidence -> report summary/dashboard/operator snapshot
```

This chain is now implemented, small, and easy to harden with tests. A second
candidate is CSI reattach vs transparent host-path recovery classification, but
that is riskier and should follow once field contracts are updated.

