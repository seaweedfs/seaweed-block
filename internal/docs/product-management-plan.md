# Product Management Plan

This document is the product-facing control plane for `seaweed-block`.

Its job is to keep product goals, prioritized engineering tasks, and evidence
links in one place so the team does not jump from every new finding directly
into code.

## Product Goal

Build a small, understandable Kubernetes block storage service for teams that
want something lighter than Ceph and more storage-engine-oriented than a simple
local-volume provisioner.

Target user:

- small teams running Kubernetes,
- users who need dynamic PVCs and simple replicated block volumes,
- contributors who want to understand the storage path without a large
  distributed-storage codebase upfront.

Near-term product promise:

```text
Install Seaweed Block in a lab Kubernetes cluster.
Create a PVC.
Run an app pod.
Write data.
Delete and recreate app pods.
See the data come from a Seaweed Block volume.
Clean up without dangling iSCSI sessions or Kubernetes resources.
```

Non-promise for the current alpha:

- no production HA claim,
- no seamless live failover claim,
- no performance claim,
- no multi-tenant security claim,
- no NVMe-oF production claim.

## Product Requirements

### Required For Alpha Stabilization

| Requirement | Product Meaning | Current Status |
|---|---|---|
| One-command-ish Kubernetes smoke | A newcomer can follow the README and see a PASS. | Done for single-node k3s. |
| Dynamic PVC create/delete | Users can create storage through standard Kubernetes PVCs. | Alpha path works. |
| App-facing demo | Users can see normal pods using a PVC, not only storage internals. | Done. |
| Clean teardown | Smoke leaves no iSCSI sessions and no visible K8s resources. | Mostly done; generated workload cleanup still harness-assisted. |
| Public docs | Users understand architecture, roadmap, and non-claims. | In progress. |
| OS initiator compatibility | Linux/Windows iSCSI should survive real mkfs/format-sized writes. | Closed for Linux + Windows single-host validation. |
| First-volume user loop | User can follow one runbook to launch, create a PVC, write/read, delete, and collect a failure bundle. | Closed for supported single-node alpha path. |

### Required For Beta

| Requirement | Product Meaning |
|---|---|
| Durable default state | Volume data survives blockvolume pod restart. |
| Operator/controller cleanup | Harness no longer acts as the operator for generated blockvolume workloads. |
| Multi-node attach | App pod and blockvolume can run on different K8s nodes. |
| Safe K8s recovery via CSI/pod recreate | RF=2 primary failure can recover through authority move plus CSI/node reattach on pod recreate, or fail closed with a precise blocker. |
| Replica lifecycle | Returned replicas go through candidate/syncing/rebuilding/ready, not heartbeat-to-ready. |
| Explicit ACK profile | Users know whether writes are best-effort, sync-quorum, or sync-all. |
| TestOps smoke suite | QA can run named scenarios and return result bundles. |

## Prioritized Backlog

Priority definitions:

- `P0`: blocks a credible alpha demo or corrupts user trust.
- `P1`: needed before beta.
- `P2`: important, but can follow beta if documented.

### P0: Alpha Demo Credibility

| Task | Why It Matters | Status | Evidence / Notes |
|---|---|---|---|
| README quick start fresh-user pass | First impression. | Done | Final close run: `20260504T000127Z`. |
| App PVC demo | Explains value to K8s users. | Done | `scripts/run-alpha-app-demo.sh`, `docs/kubernetes-app-demo.md`. |
| iSCSI large write / mkfs compatibility | Windows/Linux format failure kills demos. | Closed for current alpha claim | Linux runner-native gate passes with mkfs, mount, checksum, fio, cleanup, and clean dmesg delta. Windows 11 built-in Initiator validated NTFS format and 4 MiB checksum over SSH tunnel. |
| Remove misleading internal labels from public docs/comments | Open-source readers should not see internal phase jargon. | Active | Keep public docs free of internal gate names. |
| Branch/PR discipline after MVP merge | Product fixes need reviewable slices. | Active | New product fixes should use branch + PR. |

### P1: Beta Foundation

| Task | Why It Matters | Status | Evidence / Notes |
|---|---|---|---|
| Controller-owned generated workload cleanup | Current harness cleanup is not a product controller. | Closed for alpha path | Product-owned generated workload lifecycle is closed in phase 12. |
| Durable node-local volume state | `emptyDir` is not a production storage story. | Closed for RF=1 alpha path | Durable restart/reattach is closed in phase 13; RF=2 recovery still needs frontier evidence. |
| Multi-node K8s attach | Single-node loopback hides networking and placement problems. | Closed for same-node loopback alpha path | Phase 14 proves app/blockvolume co-location, normal CSI attach, inventory placement evidence, and unsupported remote-loopback attach as a non-claim. |
| Failover under mounted workload | Availability claim must be proven at app path. | Active | Current plan: RF=2 promotion-ready recovery via CSI/pod recreate. |
| Replica reintegration policy | Returned replicas must not become ready from heartbeat alone. | Partially designed | G9 lifecycle work informs this. |
| ACK profile rules | Avoid “best-effort” being mistaken for full durability. | Partially enforced | Fast guards reject beta RF=2 recovery on best-effort; sync-quorum/sync-all still need live D4 recovery evidence. |
| Observability/status surface | Users need diagnosis without reading debug logs. | Closed for read-only alpha inventory; expanding | Cluster inventory is closed; Stage 1.5 tracks product usability hardening. |

### P1.5: Product Usability Hardening

These items do not replace the Stage 1 recovery gate. They make the product
usable enough that a small-cluster operator can diagnose and trust the system
without internal context.

| Task | Why It Matters | Status | Evidence / Notes |
|---|---|---|---|
| Kubernetes-readable volume conditions | Users should see Ready/Degraded/Recovering/Blocked plus reason without parsing bundles. | Planned | Roadmap Track H. |
| One-command support bundle | First failure should produce attach/failover/cleanup evidence without component knowledge. | Planned | Builds on `sw-block ops status` and `ops inventory`. |
| Conservative timeout/retry defaults | iSCSI/NVMe/CSI behavior should be product defaults, not script folklore. | Planned | Must stay protocol-specific and documented. |
| Upgrade/rollback smoke | Version changes must not silently break existing PVC attach/read. | Planned | Start with a narrow smoke, not broad upgrade safety. |
| Capacity/replication preflight guards | Obvious impossible requests should fail before unsafe partial placement. | Planned | Especially RF and space constraints. |
| Delete/residue audit consistency | PVC delete must have stable target/session/artifact cleanup evidence. | Planned | Builds on prior cleanup attribution. |
| Multi-volume concurrency baseline | Two PVCs are proven; product confidence needs create/attach/delete at N volumes. | Planned | Start with N=10 if lab resources allow. |
| Productized fail-closed reasons | Blocker strings should map to documented operator next steps. | Planned | Keeps safe refusal actionable, not just correct. |

### P2: Expansion

| Task | Why It Matters | Status | Evidence / Notes |
|---|---|---|---|
| NVMe-oF frontend | Better Linux path/multipath story than iSCSI. | Follow-up | V2 has reference implementation. |
| Protocol-neutral CSI dispatch | CSI should support iSCSI now, NVMe later. | Planned | Do after iSCSI stabilizes. |
| `smartwal` backend option | More advanced storage backend. | Follow-up | Must not silently replace `walstore`. |
| Helm chart | Easier install. | Follow-up | After manifests stabilize. |
| Performance benchmarks | Users will ask about IOPS/latency. | Follow-up | Only after correctness and cleanup. |

## Done / Learned

These are completed product-shaping items that should not be reopened casually.

| Area | Decision / Learning |
|---|---|
| `targetLSN` as completion fact | Removed from recovery close semantics; completion needs witnessed progress, not target crossing. |
| Split WAL egress | Recovery and steady WAL senders must not race. Single feeder/egress ownership is the product rule. |
| Authority vs data facts | Assignment movement alone does not prove data continuity. |
| Placement vs authority | Placement intent is not authority minting. |
| Registration vs readiness | Heartbeat/registration is observation, not replica readiness. |
| Alpha protocol choice | iSCSI remains the default K8s path until the install and cleanup story is stable. |

## Decision Rules

Before starting code for a new problem, answer these:

1. Which product requirement does this serve?
2. Is it `P0`, `P1`, or `P2`?
3. What user-visible failure happens if we do not do it?
4. What test proves it?
5. Is this a protocol fix, control-plane fix, storage-engine fix, or docs fix?
6. Does this cross an authority/data truth boundary?
7. Should it be a branch + PR?

Default answer to #7 is yes for all product fixes after the MVP merge.

For P0/P1 product work, also fill or reference a product spec gate using
`internal/docs/ref/product-spec-gate-template.md` before broad implementation.
The spec must define the user-visible contract, non-negotiable semantics,
allowed simplifications, non-claims, and evidence contract. A green test does
not override the spec. If implementation weakens the spec, update the plan and
non-claims first or stop.

## Test And Evidence Index

| Evidence Type | Where |
|---|---|
| Fresh-user Kubernetes quick start | README, `scripts/run-k8s-alpha.sh`, final close artifact bundle. |
| App PVC demo | `docs/kubernetes-app-demo.md`, `scripts/run-alpha-app-demo.sh`. |
| Architecture overview | `docs/architecture.md`. |
| Developer architecture | `docs/developer-architecture.md`. |
| V2 frontend gap audit | `docs/v2-frontend-protocol-gap-audit.md`. |
| Runtime state machines | `docs/runtime-state-machines.md`. |
| Roadmap | `docs/roadmap.md`. |
| Production readiness detail | `internal/docs/ref/production-readiness-plan.md`. |
| Calibration evidence | `docs/calibration/`. |

## Current Immediate Recommendation

The light-use operations ladder is now closed through same-node placement,
attach, durable restart, and safe RF=2 refusal:

- first-volume runbook and failure bundle are closed,
- cluster inventory is closed,
- product-owned generated workload lifecycle is closed,
- durable RF=1 restart/reattach is closed,
- same-node alpha attach and placement visibility are closed,
- RF=2 mounted baseline and primary-failure safe refusal are closed.

The next concrete product step is:

```text
Build Stage 1 safe Kubernetes recovery: prove through the app/PVC path that an
RF=2 peer becomes promotion-ready, a controlled primary failure triggers safe
authority movement, CSI/node reattaches on pod recreate, and the reader verifies
the same data; otherwise fail closed with a precise blocker.
```

Current state:

- Protocol-level iSCSI/NVMe failover gates exist, but transparent host
  multipath is Stage 2, not the current claim.
- Current Stage 1 has candidate durable frontier evidence, but still lacks the
  mounted writer required frontier and positive CSI/pod-recreate recovery gate.
- The active plan is `RF=2 Promotion-Ready Recovery MVP`.
- Stage 1.5 usability hardening should be planned in parallel, but it must not
  dilute Stage 1's minimum HA product line.
