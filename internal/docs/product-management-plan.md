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
| OS initiator compatibility | Linux/Windows iSCSI should survive real mkfs/format-sized writes. | Active hardening. |

### Required For Beta

| Requirement | Product Meaning |
|---|---|
| Durable default state | Volume data survives blockvolume pod restart. |
| Operator/controller cleanup | Harness no longer acts as the operator for generated blockvolume workloads. |
| Multi-node attach | App pod and blockvolume can run on different K8s nodes. |
| Basic failover and reattach | Primary failure can be recovered by authority move and CSI reattach/pod restart. |
| Replica lifecycle | Returned replicas go through candidate/syncing/rebuilding/ready, not heartbeat-to-ready. |
| Explicit ACK profile | Users know whether writes are best-effort, quorum, or full-ack. |
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
| iSCSI large write / mkfs compatibility | Windows/Linux format failure kills demos. | Active | `frontend: chunk iSCSI R2T writes by MaxBurst`; needs OS retest. |
| Remove misleading internal labels from public docs/comments | Open-source readers should not see internal phase jargon. | Active | Keep public docs free of internal gate names. |
| Branch/PR discipline after MVP merge | Product fixes need reviewable slices. | Active | New product fixes should use branch + PR. |

### P1: Beta Foundation

| Task | Why It Matters | Status | Evidence / Notes |
|---|---|---|---|
| Controller-owned generated workload cleanup | Current harness cleanup is not a product controller. | Planned | Roadmap item. |
| Durable node-local volume state | `emptyDir` is not a production storage story. | Planned | Required before real data-retention claim. |
| Multi-node K8s attach | Single-node loopback hides networking and placement problems. | Planned | Needs lab scenario. |
| Failover under mounted workload | Availability claim must be proven at app path. | Planned | Build after multi-node attach. |
| Replica reintegration policy | Returned replicas must not become ready from heartbeat alone. | Partially designed | G9 lifecycle work informs this. |
| ACK profile rules | Avoid “best-effort” being mistaken for full durability. | Designed, not enforced | See production readiness plan. |
| Observability/status surface | Users need diagnosis without reading debug logs. | Planned | CLI or HTTP status. |

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

Do not start new feature work until the iSCSI OS-initiator compatibility issue
is verified.

The next concrete product step is:

```text
Run a real Linux/Windows initiator against a meaningful volume size.
mkfs/format it.
Write/read data.
Confirm no DID_BAD_TARGET / I/O error.
```

If it fails, keep the work in an iSCSI compatibility branch and port the missing
V2 execution pieces with tests:

- multi Data-In read splitting,
- CmdSN window and pending queue,
- Data-Out timeout,
- additional mkfs/fio regression scenarios.
