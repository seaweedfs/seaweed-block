# Phase 32 Negative-First Operator Status Plan

Date: 2026-05-25

Purpose: convert existing failure-taxonomy lessons and TestOps scenarios into a
read-only Kubernetes status plan for Phase 32.

This document is a planning contract. It does not add a product claim by
itself.

## Principle

The operator/status layer must not be another optimistic dashboard.

Required rule:

```text
participants publish observations
fact authorities publish authoritative facts
ManagedVolume / operator projection composes status
executors perform only explicitly allowed actions
evidence records why status or action is valid
TestOps audits from outside the product
```

For Phase 32, the executor set is read-only. Status publication and Kubernetes
Event emission are allowed. Promote, repair, rebuild, failback, delete, and
cleanup mutation are not allowed.

## Failure Taxonomy Mapping

Source: `C:/work/seaweedfs/sw-block/design/external-failure-taxonomy.md`.

| External lesson | Status rule | Condition / reason | Gate |
|---|---|---|---|
| Timer-based death without corroboration | A passive timeout may trigger investigation, but must not mint authority or Ready. | `EvidenceStale=True`, `reason=status_probe_required` | D7 |
| Stale epoch used for membership decision | Status must compare against authoritative epoch before publishing Ready after restart or promotion. | `Ready=False`, `reason=authority_epoch_stale` or `authority_persisted` when OK | D5 |
| Address reuse causes wrong target | Status identity must key by stable volume/replica identity, not frontend address alone. | `Blocked=True`, `reason=identity_ambiguous` | D6 |
| Health metadata survives rebuild/fault | Recovered/Ready must be derived from current durable/evidence facts, not stale previous health. | `Ready=False`, `reason=evidence_stale` | D4/D7 |
| Partial cleanup leaves split state | Cleanup status must include Kubernetes, iSCSI, multipath, dmsetup, process, and hostpath residue. | `CleanupRequired=True`, `reason=residue_detected` | D4 |
| Per-connection state interference | Per-volume and per-replica status must be isolated; one volume failure cannot rewrite another volume. | `Blocked=True` only on affected volume; untouched volume stays Ready. | D6 |
| Recovery starvation / thundering herd | If a future rebuild/repair queue exists, status must expose admission/backoff. For Phase 32, do not claim rebuild. | `Blocked=True`, `reason=rebuild_not_supported` | D8 non-claim |
| HA reaction under resource exhaustion | Status must distinguish resource pressure from semantic failure. | `Blocked=True`, `reason=resource_pressure` when evidence exists | future |

## Status Vocabulary

Phase 32 should use a small vocabulary first:

| Condition | Meaning |
|---|---|
| `Ready` | Volume/cluster is usable according to current evidence. |
| `Blocked` | User-visible progress is blocked and needs explanation. |
| `Recovering` | A recovery path is in progress and not terminal. |
| `Recovered` | A recovery path completed and data/authority evidence exists. |
| `CleanupRequired` | Live or host residue remains after an operation. |
| `EvidenceStale` | Passive evidence is old, missing, or contradictory. |

Reason codes should reuse existing report/timeline values where possible:

- `first_volume_verified`
- `reader_checksum_passed`
- `candidate_covers_required_frontier`
- `authority_persisted`
- `csi_node_image_pull_failed`
- `no_publish_target`
- `loopback_publish_target_rejected`
- `residue_detected`
- `evidence_stale`
- `read_only_operator`

## Existing Scenario Seeds

| Purpose | Existing scenario |
|---|---|
| First-volume happy path | `helm-first-volume-via-sw-block-cli-chain.yaml` |
| Multi-volume readiness | `helm-multi-volume-rf3-readiness-chain.yaml` |
| Support blocked bundle | `helm-support-bundle-diagnostics-chain.yaml` |
| Same-node / unsafe attach negative | `same-node-alpha-attach-negative-chain.yaml` |
| Durable restart failure negative | `csi-rf1-durable-restart-failure-chain.yaml` |
| First-volume breakage negative | `light-use-first-volume-breaks-chain.yaml` |
| RF3 promotion restart | `helm-rf3-promotion-restart-persistence-chain.yaml` |
| Multi-volume restart | `helm-multi-volume-rf3-restart-smoke-chain.yaml` |
| Multi-volume interleaved failover | `helm-multi-volume-rf3-interleaved-failover-chain.yaml` |
| Cleanup residue | `cleanup-residue-chain.yaml` |

## D1-D8 Acceptance Summary

| Gate | Acceptance |
|---|---|
| D1 negative matrix | This document exists, maps failure classes to status rules, and identifies scenario seeds. |
| D2 CRD/status contract | Alpha CRD or operator snapshot has stable Conditions, Events, RBAC, and read-only boundary. |
| D3 happy status | First-volume path publishes `Ready=True` and matching report/dashboard/bundle evidence. |
| D4 blocked status | At least one negative scenario publishes `Ready=False` with `Blocked` or `EvidenceStale` reason and evidence. |
| D5 restart status | Promoted authority survives restart in status, not just helper summary. |
| D6 multi-volume status | Three volumes remain independent in status under readiness/failover/restart. |
| D7 stale/probe policy | Stale evidence becomes `EvidenceStale` or `Unknown`; bounded probes are timed and evidenced. |
| D8 close | QA reruns happy, negative, restart, and multi-volume gates; PM reviews claims. |

## Explicit Non-Claims

- No mutating operator workflows.
- No automatic rebuild/failback.
- No backup/snapshot/restore.
- No NVMe ANA parity.
- No production SLO.
- No promise that every external taxonomy item is fully implemented; Phase 32
  only makes the selected status surface truthful for gated paths.

## QA Instruction Seed

QA should be asked to validate Phase 32 with a bias toward proving false-ready
bugs:

1. Run the first-volume happy path and confirm all surfaces say Ready with the
   same evidence.
2. Run a blocked/negative path and confirm no surface says Ready.
3. Run the RF3 promotion restart gate and confirm status preserves primary,
   epoch, and publish target.
4. Run a multi-volume path and confirm one volume's state does not contaminate
   another volume.
5. Probe dashboard/report/operator-snapshot for mutation affordances and fail
   if any unsafe action is exposed.

