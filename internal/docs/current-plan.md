# Current Plan: Light-Use Install And Lifecycle Operations MVP

Status: active. Opened after closing
`finished-plans/phase9_finishedplan_light_use_operations_mvp.md`.

Current task: D1 first-volume runbook. The first slice is to make the user path
and boundary evidence explicit before adding more runner automation.

## Product Question

Can an early developer use Seaweed Block as a basic Kubernetes block product
without reading internal scripts, and can they diagnose the first failure with
one product-owned status bundle?

The previous plan proved the observe/report/coordinate loop. This plan must
prove the user-facing operational loop:

```text
install/launch -> create PVC -> attach app pod -> write/read -> delete ->
verify cleanup -> collect status bundle on failure
```

If this plan closes, the claim is still narrow:

```text
Single-node Kubernetes light-use path: a user can install/launch the alpha
stack, create one PVC, run app write/read, delete resources, and verify cleanup
with a documented command path and TestOps gate.
```

It is not a production HA, performance, upgrade, multi-node, or operator-grade
claim.

## Current Honest State

What already works in separate gates:

- CSI dynamic PVC create/delete works.
- iSCSI and NVMe protocol paths are release-gated.
- App write/read through PVC exists in the K8s demo path.
- Linux and Windows OS iSCSI initiator compatibility have evidence.
- `sw-block ops status` can produce a self-describing bundle for one volume.
- TestOps can coordinate shared-lab runs with active/history records and locks.

What is still weak for a light-use product:

- The user path is scattered across docs, alpha scripts, TestOps scenarios, and
  QA reports.
- Cleanup is partly product behavior and partly harness discipline.
- There is no single user-facing "first volume" claim with one fresh-run gate.
- Failure diagnosis is not yet stitched into the first-volume workflow.

## Research Baseline

See `ref/light-use-block-storage-ux-research.md`.

The useful comparison set is Longhorn, OpenEBS, Rook/Ceph, Piraeus/LINSTOR, and
EKS EBS CSI. The common light-user shape is:

```text
preflight -> install -> wait/verify components -> create StorageClass/PVC ->
create app -> verify bound/running/I/O -> inspect status -> teardown ->
collect support data on failure
```

Design rules for this plan:

- Pick one default path first: iSCSI + `walstore` + single-node k3s.
- Add preflight checks before install instead of discovering missing host
  dependencies late.
- Verify every boundary, not just final PASS: pods, StorageClass, PVC,
  generated `blockvolume`, app checksum, status bundle, cleanup.
- Treat `sw-block ops status` as the support-bundle step for failures after
  volume identity exists.
- Keep cleanup attribution explicit: product/Kubernetes cleanup vs TestOps
  guardrail cleanup.
- Do not start a UI/dashboard in this plan; stabilize CLI/runbook/status
  contracts first.

## Target User Experience

An early user should be able to:

1. Start from a fresh checkout or released artifact on the supported lab shape.
2. Run one documented launch/install path.
3. Apply or generate a StorageClass/PVC and an app pod.
4. Observe the PVC becoming usable.
5. Write data, replace the app pod, and read the data back.
6. Delete the app/PVC resources.
7. Verify no obvious product or host residue remains.
8. If the flow fails, run or receive a status bundle that explains what was
   checked, what was unchecked, and what artifacts to attach.

## Scope

In scope:

- Refresh the first-volume quickstart/runbook around the actual supported path.
- Make the claimed path executable by a TestOps scenario.
- Separate product-owned lifecycle behavior from TestOps-only cleanup.
- Add or tighten fast tests for any new parser, wrapper, or artifact contract.
- Keep the operations status bundle wired into the failure path.
- Add QA assignment for adversarial user-path validation.

Out of scope:

- Full Kubernetes operator/controller.
- Upgrade/uninstall story.
- Multi-node attach.
- HA failover claim for this user path.
- Performance/SLO claim.
- UI/dashboard.
- Mutating repair/admin controls.

## Top Blocking Issues

### P0: First-Volume Path Must Be One Coherent User Story

Today the pieces exist, but the user story is fragmented.

Close requirement: one runbook and one scenario prove the same flow.

### P0: Lifecycle Cleanup Must Be Attributed Correctly

The plan must not hide script-owned cleanup behind a product claim.

Close requirement: the final evidence explicitly marks which cleanup happened
because Kubernetes/product ownership worked and which cleanup was TestOps
guardrail cleanup.

### P0: Failure Bundle Must Be Attached To The User Path

The previous plan built `sw-block ops status`; this plan must show where it fits
when the first-volume path fails.

Close requirement: the TestOps scenario captures the relevant status/support
artifacts on failure or records why they are unavailable.

### P1: Keep Integration Time Under Control

The scenario may use M01/M02 or k3s, but most logic should be component-tested.

Close requirement: new code has fast tests; long integration is reserved for
the end-to-end user claim.

## Deliverables

### D1: First-Volume Runbook

Status: draft attached in `docs/quickstart-kubernetes.md`; dev-run validated
through `light-use-first-volume-chain` at run
`20260511-180107-1f37`. Still needs QA new-user review before final close.

Update or add a concise runbook that answers:

- preflight checks,
- prerequisites,
- exact launch/install command path,
- exact app/PVC path,
- boundary verification commands after install/PVC/app/cleanup,
- expected success line,
- expected cleanup result,
- how to collect `sw-block ops status` artifacts,
- non-claims.

Preferred source to refresh first: `docs/quickstart-kubernetes.md`.

### D2: Runner-Native First-Volume Scenario

Status: initial scenario attached as
`testops/scenarios/light-use-first-volume-chain.yaml`.

Dev validation:

```text
run_id: 20260511-180107-1f37
result: PASS
wall:   1m12s
actions: 35/35 passed
host: m02
```

The scenario proved preflight, alpha image build/import, documented
`run-k8s-demo.sh`, writer checksum, reader replacement checksum, generated
iSCSI blockvolume manifest, no active iSCSI session after delete, cleanup
attribution, and final process/session assertions.

Add a TestOps scenario for the same user path.

Expected scenario shape:

```text
pre_clean
pin_or_verify_build
install_or_launch
create_pvc_and_app
verify_write_read
delete_and_verify_cleanup
collect_ops_bundle_on_failure
collect_and_cleanup(always)
```

The scenario should emit normal result/status bundles and participate in the
shared control-data lock model.

### D3: Product-vs-Harness Cleanup Evidence

Status: initial attribution artifact attached in the runner-native scenario.
Dev run found the important boundary: active iSCSI sessions are removed by the
demo flow, but an iSCSI node database entry can remain until TestOps guardrail
cleanup deletes it. That is recorded as guardrail cleanup, not product-owned
cleanup.

The artifact bundle should include a short cleanup summary:

- Kubernetes resources deleted by normal owner references,
- iSCSI/NVMe host residue check,
- generated blockvolume deployment/state residue check,
- TestOps cleanup actions, if any,
- non-claims.

### D4: Failure Diagnosis Hook

When the first-volume flow fails after a volume identity is known, capture the
operations status bundle or a clear explanation:

```text
ops-status-unavailable: no volume id reached
ops-status-collected: <path>
```

### D5: Fast Gates And Review

Use TDD for new parsing/wrapper/status logic:

- component tests for artifact summary parsing,
- component tests for cleanup-attribution summary,
- validate scenario YAML,
- internal review agent before merge if code changes are non-trivial.

### D6: QA Close Assignment

Ask QA to validate as a new user, not just as an executor:

- run the documented path from a clean state,
- run the runner-native scenario,
- inspect the cleanup attribution,
- intentionally break one prerequisite and confirm the failure bundle is useful,
- report any confusing step or over-claim.

## Gates To Close

This plan closes only when:

1. The first-volume runbook is accurate from a fresh supported lab state.
2. A runner-native scenario proves the same flow.
3. The app write/read and pod replacement check pass.
4. Delete/cleanup evidence is explicit and attributed.
5. The operations status bundle is captured on failure, or the bundle explains
   why no volume identity existed yet.
6. Fast tests cover new logic.
7. QA validates the user experience independently and reports no blocking
   usability issue.

## Success Statement

After this plan, Seaweed Block can make a narrow light-use product claim:

```text
On the supported single-node Kubernetes lab shape, an early user can follow one
documented path to launch the alpha stack, create and use one block-backed PVC,
delete it, verify cleanup, and collect a useful support bundle if it fails.
```

That is the bridge from "our tests pass" to "a user can try the product."
