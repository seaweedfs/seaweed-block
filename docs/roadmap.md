# Seaweed Block Roadmap

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
- iSCSI frontend only
- `walstore` backend only
- launcher-generated blockvolume state uses `emptyDir`
- harness applies generated blockvolume manifests
- generated blockvolume Deployments can use PVC owner references for cleanup
- no production operator yet

## Near-Term MVP Hardening

1. Package a clean Kubernetes install surface.

   - keep `deploy/k8s/alpha/` coherent
   - document image names and prerequisites
   - remove gate-specific naming from user paths where possible

2. Replace `emptyDir` for blockvolume state.

   - add durable root configuration
   - support hostPath/local-path style lab persistence
   - keep `emptyDir` only for explicit throwaway smoke tests

3. Add a minimal operator/controller loop.

   - watch generated workload intent or lifecycle state
   - apply/delete blockvolume Deployments
   - stop requiring the harness to apply generated workloads

4. Improve TestOps usability.

   - remote shell execution for K8s scenarios
   - stable result bundles
   - scenario registry index

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

3. Returned replica lifecycle.

   - observed replica returns
   - candidate -> syncing/rebuilding -> ready
   - ready status gates placement/ACK eligibility

4. Flow-control and pressure behavior.

   - pin too slow
   - WAL retention pressure
   - sync/full-ack unavailable policy

## Protocol And Backend Roadmap

1. iSCSI remains the MVP frontend.

   It is the currently tested Kubernetes path and should stay the default until
   the install and cleanup experience is stable.

2. NVMe-oF is a follow-up frontend.

   The CSI layer should dispatch by frontend target protocol:

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
- NVMe-oF frontend support

## Definition Of Beta

A reasonable beta bar:

- K8s install path is one command or one Helm chart.
- Dynamic PVC create/delete works repeatedly.
- Volume data survives blockvolume pod restart.
- Multi-node attach works.
- Basic failover is tested under an attached workload.
- TestOps can run the smoke suite and produce stable artifacts.
- Non-claims are documented and visible to users.

For the detailed post-alpha execution plan, see
[`production-readiness-plan.md`](production-readiness-plan.md).

