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
- no production or mutating operator yet

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
2. Complete the Operation Layer v0.5 release train before adding more storage
   features:
   - Phase 41: lifecycle-owner foundation. **Closed 2026-06-14, QA PASS**
     (`internal/docs/finished-plans/phase41_finishedplan_lifecycle_owner_foundation.md`).
     Observer/lifecycle-owner/executor roles defined; delete-safety preconditions
     and a dry-run finalizer-release action shipped; operator-status stays
     status/events-only; finalizer mutation deferred. Recorded carry-forward: a
     real live-apiserver/envtest RBAC/admission gate is still required before any
     finalizer mutation can ship.
   - Phase 42 (**active**): real API/admission proof. Show that a lifecycle owner
     can be granted main-object patch only for finalizer-shaped writes, with spec
     and unrelated metadata rejected by a real Kubernetes API/admission gate. This
     is the recorded Phase 41 carry-forward and the gate before any actual
     finalizer add/remove.
   - Phase 43: first bounded lifecycle mutation. The likely candidate is
     `SwBlockVolume` finalizer add/remove, with delete-safety preconditions and
     user-visible Events/status.
   - Phase 44: delete lifecycle close gate and release. Validate install,
     PVC, status, delete-request, blocked/releasable finalizer behavior,
     cleanup evidence, support bundle, and uninstall zero-residue as one user
     path.
3. Productize returned-replica rebuild/reintegration/failback after the
   operation layer can safely authorize, block, and audit lifecycle actions.
   The engine/transport already has rebuild and returned-replica safety pieces;
   the missing work is the Kubernetes/product control loop: facts, judgment,
   action ownership, status, Events, fencing, and multi-volume QA gates.
4. Add backup/restore and NVMe ANA parity after they can reuse the same action
   owner, evidence, and status model rather than creating another isolated
   control plane.

The internal release-train contract is
`internal/docs/ref/operation-layer-v0.5-release-train.md`; the Phase 42 entry
gate is drafted in
`internal/docs/ref/phase42-lifecycle-owner-api-admission-gate.md`.

The practical rule is:

```text
Do not add a new data-plane feature if the product cannot yet explain who owns
the lifecycle action, what evidence authorizes it, how it is blocked, and where
the user sees the result.
```

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

