# Phase 32 TestOps Product-Grade Validation Assignment

Date: 2026-05-25

Owner: QA, with dev support for missing runner primitives.

Purpose: make TestOps a product-grade validation layer for Phase 32 read-only
operator/status work. The goal is not just to run scenarios; the goal is to
prove the product does not publish false Ready status under failure.

## Scope

In:

- reuse existing TestOps scenarios as Phase 32 evidence seeds,
- identify which runner-native actions are missing for product-grade gates,
- add or propose scenario coverage for happy, blocked, restart, multi-volume,
  cleanup, and stale-evidence paths,
- collect artifacts that a cold reviewer can use without SSH,
- preserve strict cleanup/residue verification.

Out:

- no product mutating APIs,
- no operator repair/rebuild/failback actions,
- no full runner DSL rewrite,
- no agent-mode requirement for Phase 32 close.

## QA Workstream A: Scenario Inventory And Classification

Task:

Classify existing scenarios into this table:

| Class | Required examples |
|---|---|
| happy first-volume | Helm install, PVC, writer/reader, report/dashboard |
| blocked negative | image pull, no publish target, loopback rejection, mount failure |
| restart persistence | single-node restart, RF3 promoted restart, multi-volume restart |
| multi-volume isolation | readiness, reattach, mounted failover, interleaved failover |
| cleanup/lifecycle | cleanup residue, multipath/dmsetup/iSCSI/process/hostpath |
| runner-native spike | PVC loop, native waits, gaps |

Suggested scenario seeds:

- `helm-first-volume-via-sw-block-cli-chain.yaml`
- `helm-support-bundle-diagnostics-chain.yaml`
- `same-node-alpha-attach-negative-chain.yaml`
- `csi-rf1-durable-restart-failure-chain.yaml`
- `helm-single-node-restart-persistence-chain.yaml`
- `helm-rf3-promotion-restart-persistence-chain.yaml`
- `helm-multi-volume-rf3-restart-smoke-chain.yaml`
- `helm-multi-volume-rf3-interleaved-failover-chain.yaml`
- `cleanup-residue-chain.yaml`
- `experimental-runner-native-pvc-loop.yaml`

Deliverable:

- `internal/docs/qa-assignments/phase32-testops-scenario-inventory.md`

Acceptance:

- Every Phase 32 D3-D7 gate has at least one candidate scenario.
- Gaps are explicitly named, not hidden.

## QA Workstream B: Negative-First Evidence Contract

Task:

For at least one blocked scenario, produce a report that answers:

- Which user-visible status should be false?
- Which Condition should be true?
- Which reason code should appear?
- Which evidence file proves it?
- Which product surface must agree: report, dashboard, operator snapshot,
  support bundle, or future CRD?

Acceptance:

- A blocked path must never be reported as `Ready=True`.
- The report must include a stable reason, not only a timeout.
- Evidence must include Kubernetes events/logs or product status, not only a
  helper summary.

Deliverable:

- `internal/docs/qa-assignments/phase32-negative-status-evidence-review.md`

## QA Workstream C: Runner-Native Capability Spike

Task:

Rerun or update the runner-native PVC loop and identify which raw shell wrappers
remain.

Known missing primitives from prior QA:

- `kubectl_wait_jsonpath`
- `kubectl_wait_completed`
- `helm_install`
- `helm_uninstall`
- `sw_block_ops_report`
- `collect_k8s_snapshot`
- `assert_no_multipath_maps`
- `assert_alua_aas_transition`
- `iscsi_assert_io_rejected`

Acceptance:

- For each missing primitive, provide:
  - why product gates need it,
  - current shell workaround,
  - expected action parameters,
  - one pass/fail acceptance case.

Deliverable:

- update `internal/docs/ref/testops-runner-action-backlog.md`, or produce an
  addendum under `internal/docs/qa-assignments/`.

## QA Workstream D: Failure Snapshot Standard

Task:

Define the minimum failure bundle for K8s block tests.

Required snapshot contents:

- nodes,
- pods,
- deployments,
- events,
- PV/PVC/StorageClass,
- CSI controller/node logs,
- blockmaster logs,
- generated blockvolume logs,
- iSCSI sessions and node DB,
- multipath and dmsetup,
- product `ops cluster` or `ops report` output when reachable.

Acceptance:

- Any failed Phase 32 status scenario can be diagnosed from the bundle without
  manually SSHing into all nodes.
- Missing product endpoint must itself be captured as evidence.

Deliverable:

- `internal/docs/qa-assignments/phase32-failure-snapshot-standard.md`

## QA Workstream E: Close-Gate Dry Run

Task:

After dev lands D2 status/CRD/operator-snapshot changes, rerun:

1. Happy first-volume status gate.
2. At least one blocked negative status gate.
3. RF3 promotion restart status gate.
4. Multi-volume status isolation gate.
5. Cleanup residue gate.

Acceptance:

- All product surfaces agree on status and reason.
- No unsafe mutating action is exposed.
- Cleanup leaves no residue.

Deliverable:

- `internal/docs/qa-assignments/phase32-read-only-operator-status-qa-validation.md`

## PM-Visible Rule

QA should fail the phase if any surface makes this claim without evidence:

```text
Ready=True
```

The product is allowed to be blocked, degraded, stale, or unsupported. It is not
allowed to be confidently wrong.

