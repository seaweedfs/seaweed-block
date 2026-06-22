# Seaweed Block Roadmap

> Document status: public roadmap summary. It is intentionally high level and
> may trail the internal execution plan between releases. For current
> user-facing claims, use [`quickstart-kubernetes.md`](quickstart-kubernetes.md)
> and [`releases/README.md`](releases/README.md).

This roadmap is intentionally practical. The goal is to make a Kubernetes block
service that is easy to try, review, and improve without hiding unfinished
storage semantics.

## Current Alpha MVP

Already demonstrated:

- CSI static PV path on Kubernetes.
- CSI dynamic PVC create path.
- CSI dynamic PVC delete/cleanup path.
- iSCSI mount into a real pod.
- Pod write/read checksum through the mounted volume.
- TestOps registry and CLI MVP.
- Deeper recovery/failover components tested below the K8s surface.

Current alpha constraints:

- single-node Kubernetes lab evidence
- iSCSI and NVMe-oF frontends are release-gated on a single-node lab
- `walstore` backend only
- launcher-generated blockvolume state uses `emptyDir`
- harness applies generated blockvolume manifests
- generated blockvolume Deployments can use PVC owner references for cleanup
- no production operator yet; only the bounded v0.5 beta-candidate
  lifecycle-owner mutates `SwBlockVolume.metadata.finalizers`

## Near-Term MVP Hardening

1. Package a clean Kubernetes install surface.

   - v0.2 alpha: script-based Day-1 activation path
   - v0.3 alpha: Helm chart for the same supported path
   - document image names, immutable release tags, prerequisites, and cleanup
   - remove gate-specific naming from user paths where possible

2. Replace `emptyDir` for blockvolume state.

   - add durable root configuration
   - support hostPath/local-path style lab persistence
   - keep `emptyDir` only for explicit throwaway smoke tests

3. Add a minimal operator/controller loop after Helm.

   - do not jump directly from scripts to an operator
   - first stabilize install values, chart ownership, and uninstall behavior
   - read-only CRDs, Conditions, Events, node readiness, support evidence refs,
     cleanup visibility, and safe next-step status are now gated as the first
     operator foundation
   - mutating lifecycle ownership, finalizers, upgrade execution, repair,
     rebuild, failback, and automatic cleanup remain separate future phases

4. Improve TestOps usability.

   - remote shell execution for K8s scenarios
   - stable result bundles
   - scenario registry index
   - stronger negative-path gates for blocked, stale, unreachable, corrupt
     evidence, and cleanup-residue cases

## Current Capability Snapshot

Seaweed Block can already demonstrate a narrow Kubernetes block-storage loop:

- Helm install on supported lab clusters.
- Dynamic PVC provisioning through CSI.
- App pod mount, write, read, and replacement-reader verification.
- iSCSI frontend with gated ALUA/dm-multipath failover evidence.
- RF=3 multi-volume lab path with independent volume identity and publish
  target.
- Restart persistence for supported hostPath gates.
- Read-only CLI/report/dashboard/support-bundle flows.
- Read-only CRD status and Events for `SwBlockCluster` and `SwBlockVolume`.
- Live node evidence for Kubernetes Ready/SchedulingDisabled, CSI registration,
  CSI image-pull blockers, host-prereq evidence replay, and loopback
  cross-node attach blockers.
- Negative-first status: blocked, stale, unreachable, or corrupt evidence must
  not become false `Ready=True`.
- Cleanup verification for Kubernetes, iSCSI, multipath, process, and hostPath
  residue.
- Executable lifecycle action contracts: dry-run actions can be allowed with
  evidence, unsafe/future-mutating actions are rejected with stable reasons, and
  CRD/report/dashboard/operator-snapshot surfaces agree on the decision.
- Delete-safety status and cleanup-required visibility without finalizer or
  cleanup mutation.
- Bounded `SwBlockVolume` finalizer lifecycle in the v0.5 beta-candidate path:
  CSI-created identity CRs, lifecycle-owner protection finalizer, evidence-driven
  hold/release, Events, and multi-volume isolation.
- Install drift status for current versus desired chart/app/image identity
  without upgrade execution.
- CRD/RBAC status-writer conformance coverage for the failures that previously
  escaped mock tests and only failed in live QA.

The product model is converging around one read-only control-plane pattern:
truth owners publish facts, the status layer aggregates judgment, executors stay
bounded, and evidence explains why the status is allowed. The next work should
preserve that model rather than add isolated scripts or separate status systems.

Recommended order from here:

1. Operator-status foundation release: complete in v0.4 beta. It claims
   status/events-only visibility, not lifecycle mutation.
2. Operation Layer v0.5 beta candidate: code and QA complete through Phase 44;
   release images still need publish/pinned-image smoke before marking it
   shipped. The team is intentionally skipping that release step for the next
   development slice, so v0.5 must not be marked released from roadmap wording.
   It claims a bounded `SwBlockVolume` protection-finalizer lifecycle, not
   automatic cleanup or broad operator automation.
   - Phase 41: lifecycle-owner foundation. **Closed 2026-06-14, QA PASS**
     (`internal/docs/finished-plans/phase41_finishedplan_lifecycle_owner_foundation.md`).
     Observer/lifecycle-owner/executor roles defined; delete-safety preconditions
     and a dry-run finalizer-release action shipped.
   - Phase 42: real API/admission proof. **Closed 2026-06-15, QA PASS**
     (`internal/docs/finished-plans/phase42_finishedplan_lifecycle_owner_admission_gate.md`).
     The lifecycle-owner patch boundary is proven with real Kubernetes
     ValidatingAdmissionPolicy.
   - Phase 43: first bounded lifecycle mutation. **Closed 2026-06-15, QA PASS**
     (`internal/docs/finished-plans/phase43_finishedplan_bounded_finalizer_lifecycle.md`).
     Finalizer add/release works as isolated gates.
   - Phase 44: delete lifecycle close gate. **Closed 2026-06-17, QA PASS**
     (`internal/docs/finished-plans/phase44_finishedplan_delete_lifecycle_close_gate.md`).
     The integrated PVC -> protected CR -> hold/release -> zero-residue path is
     validated end-to-end.
3. Phase 46: productize returned-replica rebuild/reintegration status and
   decisioning. **Closed 2026-06-19, QA PASS**
   (`internal/docs/finished-plans/phase46_finishedplan_returned_replica_reintegration_productization.md`).
   Returned replicas are now Kubernetes/product-surface visible, fenced from
   frontend/ACK eligibility until evidence supports reintegration, and
   volume-scoped across multi-volume reports. Automatic failback or broad
   rebuild execution remains a later executor phase.
4. Phase 47: returned-replica executor admission. **Closed 2026-06-20, QA PASS**
   (`internal/docs/finished-plans/phase47_finishedplan_returned_replica_executor_admission.md`).
   `authority.reintegrate_returned_replica` is allowed only as a dry-run,
   non-mutating action when exact fencing and frontier evidence is present.
   This is not automatic failback or rebuild execution.
5. Phase 48: returned-replica live evidence close. **Closed 2026-06-20, QA PASS**
   (`internal/docs/finished-plans/phase48_finishedplan_returned_replica_live_evidence.md`).
   The live iSCSI returned-replica scenario now emits same-run managed-volume
   evidence proving r1 remains fenced, r2 remains the primary, and the returned
   replica covers the required frontier before any future executor mutates
   authority or storage state.
6. Phase 49: returned-replica executor preflight. **Closed 2026-06-20, local PASS**
   (`internal/docs/finished-plans/phase49_finishedplan_returned_replica_executor_preflight.md`).
   The returned-replica dry-run action now has a typed preflight that is ready
   only when the target is unique, frontend/ACK fenced, and durable frontier
   evidence covers the required frontier. It remains non-mutating and does not
   claim automatic reintegration, rebuild, failback, or frontend publication.
7. Phase 50: returned-replica preflight status schema. **Closed 2026-06-20, local PASS**
   (`internal/docs/finished-plans/phase50_finishedplan_returned_replica_preflight_status_schema.md`).
   The preflight is now machine-readable in operator-snapshot JSON and
   SwBlockVolume `.status.executorPreflights[]`, with OpenAPI schema and
   status-writer coverage. It is still non-mutating.
8. Phase 51: returned-replica ACK evidence gate. **Closed 2026-06-21, QA PASS**
   (`internal/docs/finished-plans/phase51_finishedplan_returned_replica_ack_evidence_gate.md`).
   The executor preflight now distinguishes explicit ACK non-eligibility from
   missing ACK evidence. A future executor may not treat default
   `ack_eligible=false` as proof; preflight readiness requires
   `ack_eligibility_known=true` and `ack_eligible=false`.
9. Phase 52: returned-replica executor contract. **Closed 2026-06-21, QA PASS**
   (`internal/docs/finished-plans/phase52_finishedplan_returned_replica_executor_contract.md`).
   The future executor boundary is published as a non-mutating contract:
   execution remains disabled, only ACK eligibility is named as the future
   allowed mutation class, frontend publication/rebuild traffic/failback remain
   forbidden, and terminal evidence is required before any later executor can
   claim completion.
10. Phase 53: returned-replica authority executor skeleton. **Closed 2026-06-22, QA PASS**
    (`internal/docs/finished-plans/phase53_finishedplan_returned_replica_executor_skeleton.md`).
    The executor process boundary is now disabled-by-default with
    read-only `SwBlockVolume` access. It consumes executor contracts and fails
    closed on execution-enabled or mutating contracts, but still performs no ACK
    eligibility mutation, frontend publication, rebuild traffic, or failback.
11. Add backup/restore and NVMe ANA parity after they can reuse the same action
   owner, evidence, and status model rather than creating another isolated
   control plane.

The internal release-train contract is
`internal/docs/ref/operation-layer-v0.5-release-train.md`. Phases 41-44 close
the operation-layer loop for a bounded `SwBlockVolume` protection finalizer.
Phase 46 reused this same fact -> judgment -> action -> evidence pattern for
returned-replica reintegration before any larger storage feature is enabled.
Phases 47-53 close the executor-admission, preflight, status-schema, ACK
evidence, executor-contract, and disabled executor-process bridge without
allowing mutation.

The practical rule is:

```text
Do not add a new data-plane feature if the product cannot yet explain who owns
the lifecycle action, what evidence authorizes it, how it is blocked, and where
the user sees the result.
```

## Future Read-Write Control Plane

The operation layer should evolve from status-only and bounded finalizer
mutation into broader read-write behavior in stages, not as one large
"operator does everything" jump.

Current proven boundary:

- CSI owns `SwBlockVolume` identity/spec creation.
- operator-status owns `.status` and Events.
- lifecycle-owner owns only the Seaweed Block protection finalizer.
- cleanup, repair, rebuild, failback, backup, and upgrade execution are not
  automatic operator actions.

The next read-write stages should be ordered by blast radius:

1. `safe_k8s`: repair only Seaweed Block-owned Kubernetes objects, with
   admission/RBAC confinement and terminal evidence.
2. `host_cleanup`: clean stale iSCSI/multipath/hostPath residue through a
   node-scoped executor, never through operator-status.
3. `authority_mutating`: productize returned-replica rebuild/reintegration and
   failback with fencing, frontier evidence, and multi-volume isolation.
4. `data_lifecycle`: snapshot, backup, and restore after the consistency and
   identity model is explicit.

The design reference is
[`wiki/deep-dives/read-write-control-plane-roadmap.md`](wiki/deep-dives/read-write-control-plane-roadmap.md).
The product rule remains: a mutating action needs live facts, preconditions,
an owner executor, an admission/RBAC boundary, user-visible status, and QA
evidence before it can be enabled.

## Future GPU Data Paths

GPU-oriented storage should be treated as a future design train, not as a
small flag on the current PVC path. There are three different tracks:

1. `cuFile` over a mounted Seaweed Block PVC: first prove API compatibility
   and byte-correct reads/writes on a supported Linux GPU node.
2. `cuObject` / object path: design a SeaweedFS object/S3-style GPU API with
   its own consistency, auth, and commit semantics.
3. Protocol-level GPU/RDMA/NVMe path: investigate only after the file/object
   claims are separated and the fencing/backpressure model is clear.

The first acceptable claim is narrow:

```text
On one supported Linux GPU node, a pod can use cuFile against a mounted
Seaweed Block PVC and verify byte-correct data transfer with documented
environment prerequisites.
```

It must not claim broad GPUDirect acceleration, zero-copy, multi-node failover,
or object API support until the evidence proves those separately. The design
reference is
[`wiki/deep-dives/gpudirect-cufile-cuobject.md`](wiki/deep-dives/gpudirect-cufile-cuobject.md).

## Future Non-Kubernetes Adapters

Kubernetes CSI remains the primary product surface. A Docker integration is
possible later, but it should be scoped as a Docker Volume Plugin, not a Docker
graph/storage driver.

Potential Docker Volume Plugin path:

- `docker volume create -d seaweed-block ...`
- local attach/mount/unmount through a small node agent or local daemon
- reuse ManagedVolume status, cleanup verification, support bundles, and
  negative-first evidence
- target single-node Docker, lab, edge, or developer workflows first

Explicit non-goals for the Docker path:

- no Docker graph/storage driver to replace `overlay2`
- no broad Docker Swarm HA claim before the K8s lifecycle model is stable
- no production failover claim without fencing, cleanup, and residue gates

Windows Docker Desktop support is an investigation item, not a current claim.
The likely path is a Linux Docker Desktop VM or WSL2 backend running the
Seaweed Block node component, with Windows accessing the mounted volume through
Docker. Native Windows block-device attachment is out of scope until the Linux
control-plane path is mature.

## Availability And Recovery Follow-Ups

1. Multi-node Kubernetes test.

   - at least two nodes
   - non-loopback frontend target
   - dynamic PVC attach from a pod on another node

2. Failover while mounted.

   - pod writes
   - primary dies
   - authority moves
   - frontend reconnect path is validated

3. Productized returned replica lifecycle.

   - observed replica returns after promotion
   - returned replica stays frontend-fenced until recovery evidence is current
   - candidate -> syncing/rebuilding -> ready is visible in CRD/report/dashboard
   - ready status gates placement/ACK eligibility
   - rebuild/reintegration/failback action owner is explicit and audited

4. Flow-control and pressure behavior.

   - pin too slow
   - WAL retention pressure
   - sync/full-ack unavailable policy

## Protocol And Backend Roadmap

1. iSCSI and NVMe-oF are the current release-gated frontends.

   iSCSI remains the default path for broad compatibility. NVMe-oF is now
   covered by ANA-aware multipath/failover and CSI protocol-selection gates.

   The runtime should be managed as a cluster of explicit mini-protocols:
   authority/epoch, replication recovery, iSCSI, NVMe, CSI lifecycle, and
   TestOps run-control. Each mini-protocol needs structured status and tests;
   avoid a single large protocol abstraction until repeated transition logic
   proves it is worth extracting. See `runtime-state-machines.md`.

2. CSI dispatch is protocol-aware.

   The CSI layer dispatches by frontend target protocol:

   ```text
   protocol=iscsi -> iscsiadm path
   protocol=nvme  -> nvme connect path
   ```

3. `walstore` remains the MVP backend.

   `smartwal` should be introduced behind an explicit gate with the same K8s
   scenarios, not silently switched into the MVP.

## Contributor-Friendly Work Items

Good first technical areas:

- improve Kubernetes manifests and docs
- add TestOps scenarios
- improve logs and diagnostic collection
- add protocol-neutral CSI target dispatch tests
- reduce `g7-debug` log noise behind a flag

Requires deeper storage context:

- recovery/failover semantics
- WAL retention and flow control
- replica reintegration
- multi-node authority/publisher changes
- broader V2 parity and long-running protocol regression coverage

## Definition Of Beta

A reasonable beta bar:

- K8s install path is a Helm chart with documented values and immutable images.
- Dynamic PVC create/delete works repeatedly.
- Volume data survives blockvolume pod restart.
- Multi-node attach works.
- Basic failover is tested under an attached workload.
- Read-only status and support evidence are available without SSH log spelunking.
- Negative and stale states are surfaced without false Ready claims.
- Operator lifecycle is either present behind a clear beta gate or explicitly
  listed as the next release boundary.
- TestOps can run the protocol release gate and produce stable artifacts.
- Non-claims are documented and visible to users.

Detailed post-alpha execution planning is kept in `internal/docs/` so the
public roadmap stays focused on user-visible behavior and non-claims.
