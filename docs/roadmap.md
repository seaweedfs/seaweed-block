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
- no production operator yet

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
   - then add CRDs/Conditions/Events for cluster, node, volume, and lifecycle
     state

4. Improve TestOps usability.

   - remote shell execution for K8s scenarios
   - stable result bundles
   - scenario registry index
   - stronger negative-path gates for blocked, stale, unreachable, corrupt
     evidence, and cleanup-residue cases

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

