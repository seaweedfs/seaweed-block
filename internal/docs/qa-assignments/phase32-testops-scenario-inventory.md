# Phase 32 TestOps Scenario Inventory And Classification

Date: 2026-05-25

Owner: QA. Source for D1a Workstream A.

53 scenario YAMLs under `testops/scenarios/`. This inventory maps each
scenario to the Phase 32 D3-D7 gate it best evidences. Scenarios not in scope
for Phase 32 are listed under "Out of Scope" with a note on what they
validate.

## Classification Table

### D3 Happy-Path Status Projection (`Ready=True reason=first_volume_verified`)

| Scenario | Coverage strength | Notes |
|---|---|---|
| `helm-first-volume-via-sw-block-cli-chain.yaml` | strong | Phase 28 close gate; emits `operator-snapshot.json` with read-only contract |
| `helm-first-volume-chain.yaml` | strong | Multi-node Helm install + first PVC writer/reader |
| `helm-single-node-first-volume-chain.yaml` | medium | Single-node loopback shape; supports D3 + D7 single-node |
| `activation-day1-first-volume-chain.yaml` | medium | Script-path first volume; less Helm-centric |
| `activation-day1-install-chain.yaml` | medium | Install-only smoke (no PVC) |
| `same-node-alpha-attach-chain.yaml` | medium | Same-node iSCSI attach happy path |
| `light-use-first-volume-chain.yaml` | weak | Older smoke, predates v0.3 Helm |
| `helm-multi-volume-day1-chain.yaml` | adjacent | More relevant for D6 |

**D3 verdict**: well covered. `helm-first-volume-via-sw-block-cli-chain.yaml`
is the canonical scenario.

### D4 Blocked / Negative Status Projection

| Scenario | Coverage strength | Blocker class proven | Reason code emitted |
|---|---|---|---|
| `helm-support-bundle-diagnostics-chain.yaml` | strong | CSI node image pull failure | `csi_node_image_pull_failed` |
| `same-node-alpha-attach-negative-chain.yaml` | strong | iSCSI attach refused | needs inspection (see B doc) |
| `csi-rf1-durable-restart-failure-chain.yaml` | medium | CSI durable restart failure | needs inspection |
| `light-use-first-volume-breaks-chain.yaml` | medium | first-volume break with bundle | needs inspection |
| `light-use-first-volume-failure-bundle-chain.yaml` | medium | controlled stop, ops-status during outage | n/a |
| `mounted-failover-rf2-primary-failure-safe-refusal-chain.yaml` | medium | RF2 primary kill + safe refusal | needs inspection |
| `mounted-failover-rf2-safe-refusal-chain.yaml` | medium | RF2 safe refusal | needs inspection |

**D4 verdict**: covered for image-pull blocker (the cleanest existing case).
Multiple other blocker classes have scenarios but need reason-code audits
under Workstream B. Gap: no scenario for `publish_target_loopback_cross_node`
beyond chart-config-time rejection.

### D5 Restart / Promotion Status Consistency

| Scenario | Coverage strength | Restart shape |
|---|---|---|
| `helm-rf3-promotion-restart-persistence-chain.yaml` | strong | RF3 + post-promotion + k3s restart; Phase 31 D4 strict PASS |
| `helm-single-node-restart-persistence-chain.yaml` | strong | RF1 single-node hostPath; Phase 31 D3 strict PASS |
| `helm-multi-volume-rf3-restart-smoke-chain.yaml` | strong | 3 PVCs at RF3 + restart; Phase 31 D5 strict PASS |
| `csi-rf1-durable-restart-chain.yaml` | medium | CSI durable restart |

**D5 verdict**: well covered. Phase 31 strict PASS evidence carries forward.

### D6 Multi-Volume Independence Status

| Scenario | Coverage strength | Independence dimension |
|---|---|---|
| `helm-multi-volume-rf3-readiness-chain.yaml` | strong | placement, no-fault readiness |
| `helm-multi-volume-rf3-reattach-recovery-chain.yaml` | strong | per-volume CSI reattach failover |
| `helm-multi-volume-rf3-mounted-failover-chain.yaml` | strong | per-volume mounted transparent failover |
| `helm-multi-volume-rf3-interleaved-failover-chain.yaml` | strong | concurrent overlapping faults |
| `helm-multi-volume-rf3-app-spread-failover-chain.yaml` | strong | writer pods spread across nodes |
| `helm-multi-volume-day1-chain.yaml` | medium | Day-1 multi-PVC smoke (RF=1) |

**D6 verdict**: well covered, Phase 27 cleared all sub-dimensions.

### D7 Stale Evidence And Bounded Probe

| Scenario | Coverage strength | Stale-evidence shape |
|---|---|---|
| `light-use-first-volume-failure-bundle-chain.yaml` | partial | controlled stop, ops-status during outage produces unhealthy summary |
| `helm-support-bundle-diagnostics-chain.yaml` | partial | blocked bundle replay (cold reader) |
| (gap) — no scenario explicitly forces `EvidenceStale=True` | — | needs new scenario |

**D7 verdict**: **partial gap**. Existing scenarios can demonstrate stale
collection (port-forward unreachable, partial bundle) as a side effect, but
no scenario deliberately drives `EvidenceStale=True` with a stable reason
code. Recommend new scenario `evidence-stale-bounded-probe-chain.yaml`
under D7 once D2 lands the field.

### Cleanup / Lifecycle (cross-cuts D3-D7)

| Scenario | Coverage strength |
|---|---|
| `cleanup-residue-chain.yaml` | strong; Phase 28/29 close gate |
| `helm-release-hygiene-chain.yaml` | strong; chart lint + uninstall residue |
| `helm-lifecycle-upgrade-rollback-chain.yaml` | medium; one upgrade/rollback smoke |

### Runner-Native Spike

| Scenario | Purpose |
|---|---|
| `experimental-runner-native-pvc-loop.yaml` | proves runner can drive PVC loop with named actions; documents missing primitives |

## Out Of Scope for Phase 32 D3-D7

The following scenarios validate other product layers and are not in the
Phase 32 D3-D7 target. They remain valuable for protocol/HA hardening but
should not be confused with Phase 32 status surface evidence.

| Scenario | What it validates |
|---|---|
| `iscsi-os-initiator-compat-chain.yaml` | OS initiator compatibility |
| `iscsi-p6-alua-failover-chain.yaml` | iSCSI ALUA primitives |
| `iscsi-p8-compat-soak-chain.yaml` | iSCSI compatibility soak |
| `iscsi-returned-replica-chain.yaml` | replica return + sync |
| `mounted-failover-rf2-*-chain.yaml` (6 scenarios) | RF=2 mounted failover variants (legacy/predecessor of RF3) |
| `mounted-failover-rf3-sync-quorum-recovery-chain.yaml` | RF3 sync-quorum recovery (single volume) |
| `node-loss-survival-rf3-reattach-chain.yaml` | single-volume node-loss recovery |
| `node-loss-topology-eligibility-chain.yaml` | placement eligibility under node loss |
| `nvme-p4-multipath-failover-chain.yaml` | NVMe multipath |
| `nvme-p5-csi-protocol-chain.yaml` | NVMe CSI protocol |
| `nvme-p5-protocol-component-gate.yaml` | NVMe protocol gate |
| `operations-status-diagnostics-chain.yaml` | ops CLI diagnostics |
| `operations-volume-status-cli-gate.yaml` | volume status CLI |
| `operations-volume-status-report-component-gate.yaml` | volume status report |
| `cluster-ops-inventory-chain.yaml` | cluster inventory |
| `csi-lifecycle-component-gate.yaml` | CSI lifecycle |
| `csi-rf1-durable-restart-component-gate.yaml` | CSI durable restart component |
| `light-use-first-volume-retry-chain.yaml` | retry behavior |
| `returned-replica-component-gate.yaml` | replica return gate |
| `stage2-iscsi-alua-multipath-baseline-chain.yaml` | stage 2 multipath baseline |
| `stage2-iscsi-alua-multipath-failover-chain.yaml` | stage 2 multipath failover |

## Gaps Explicitly Named

1. **D4 reason-code audit pending** — only `csi_node_image_pull_failed` is
   verified in current QA evidence. Other blocker classes have scenarios but
   their reason-code mapping needs explicit audit (Workstream B).
2. **D4 publish_target_loopback_cross_node** — chart-config-time rejection is
   visible via `compat.launcherRejectLoopbackFlag`, but no runtime blocker
   scenario validates the status surface emits this reason code at runtime.
3. **D7 stale-evidence gate missing** — no scenario deliberately triggers
   `EvidenceStale=True` or `Unknown` with a bounded probe and stable reason.
4. **Blockmaster-unreachable scenario** — Phase 27 D6 surfaced this incident
   (port-forward race), but no scenario gates it as a *status surface*
   blocker.
5. **Cleanup-residue blocking promotion** — D4 says "cleanup residue present"
   should produce a Blocked status, but `cleanup-residue-chain.yaml` proves
   cleanliness, not the *blocked* projection.

## Per-Gate Acceptance Summary

| Gate | Has candidate scenario? | New scenario needed? |
|---|---|---|
| D3 happy | yes (canonical: `helm-first-volume-via-sw-block-cli-chain.yaml`) | no |
| D4 image pull | yes (`helm-support-bundle-diagnostics-chain.yaml` blocked bundle) | no |
| D4 missing publish target | partial (Phase 27 D6 saw it incidentally) | possibly |
| D4 loopback rejection at runtime | no | **yes** |
| D4 writer pod mount failure | partial (legacy scenarios) | reason-code audit first |
| D4 cleanup residue blocking | no | **yes** |
| D4 blockmaster unreachable | no | **yes** |
| D5 restart promotion | yes (Phase 31 D4) | no |
| D6 multi-volume isolation | yes (Phase 27 D2-D8) | no |
| D7 evidence stale | no | **yes** |
| D7 bounded probe | no | **yes** |

Three of the five "yes new scenario needed" rows depend on D2 landing first
(operator snapshot needs to surface the new Condition/reason codes before
the scenarios can assert on them).
