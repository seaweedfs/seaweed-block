# QA Sign-off - Phase 35 D4 Operator Status Events (Blocked-Condition Gate)

Verdict: **BLOCKED — status surface is correct, but the new event-emission
path aborts the reconcile on any blocked volume.** All six literal checklist
items pass in isolation (the SwBlockVolume status writes `blocked` with the
right conditions/reason, a `Warning` event lands, no `Ready=True` on any
surface, SA boundary intact). But driving the canonical blocked condition
(`csi_node_image_pull_failed`) surfaced a real defect: the volume's two
same-reason conditions (`Ready=False` + `Blocked=True`) generate **two events
with an identical name**, the second create returns `409 AlreadyExists`, and the
reconciler treats that as fatal — **aborting the whole iteration (`exit=2`)** and
preventing status publication for any volume ordered after the blocked one.

This is a gate red for a real reason: the headline feature of `e0330b7` (publish
operator-status events) fails on the most common shape it will ever encounter —
a blocked volume.

Date: 2026-06-03

Source commit: `e0330b7 phase35: publish operator status events`
(branch `phase33-testops-failure-hardening`)

## How the gate was driven

The `csi_node_image_pull_failed` blocked condition is only reachable through the
**`--from-bundle`** projection (`buildImagePullBlockedEvidence`); the live
`--master-api` path the helm chart wires cannot synthesize it. So, faithfully:

1. Built `e0330b7` images, `helm install … --set operatorStatus.create=true
   --set operatorStatus.dryRun=false` (exit=0, operator-status `1/1 Running`,
   `operator_status=write_status`).
2. Created `SwBlockCluster/sw-block` + `SwBlockVolume/unknown` stubs (the bundle
   blocked volume has `VolumeID=unknown`, so `SwBlockVolumeObjectName` → `unknown`).
3. **Inside the operator-status pod** (so it runs as the constrained
   operator-status SA via in-cluster config), injected a synthesized
   `kube-system-pods-deploys.txt` with a `sw-block-csi-node … ImagePullBackOff`
   line, then ran a one-shot write-mode reconcile:
   `sw-block ops operator-status --from-bundle /tmp/bb --namespace kube-system
   --cluster-name sw-block` (`--interval=0` → one iteration, no `--dry-run`).

This exercises the exact `OperatorStatusReconciler` + `KubernetesStatusClient`
write path the deployment uses, under the real SA RBAC.

## Checklist — all six pass in isolation

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | `SwBlockVolume.status.status=blocked` | **PASS** | CRD `.status.status="blocked"` |
| 2 | `Ready=False`, `Blocked=True` | **PASS** | conditions: `{type:Ready,status:False,reason:csi_node_image_pull_failed}`, `{type:Blocked,status:True,reason:csi_node_image_pull_failed}` |
| 3 | `reasonCode=csi_node_image_pull_failed` | **PASS** | CRD `.status.reasonCode` + both condition reasons |
| 4 | Warning Event, reason `csi_node_image_pull_failed` | **PASS (partial)** | `kubectl get events`: `Warning  csi_node_image_pull_failed  swblockvolume/unknown  "managed volume is blocked; …"` — **one** of the two events landed; see bug |
| 5 | No `Ready=True` in CRD/report/dashboard/operator-snapshot | **PASS** | CRD Ready=False; `grep -ic ready.*true` over report dir + dashboard = 0; operator-snapshot `status:blocked`; summary `status=blocked` |
| 6 | operator-status SA has no storage/workload mutation power | **PASS** | live `auth can-i` (below) |

### Surfaces (all show blocked, none show Ready=True)

operator-snapshot.json (from-bundle, the controller's own source):

```json
"status": "blocked", "reason_code": "csi_node_image_pull_failed",
conditions: [ {Ready, False, csi_node_image_pull_failed},
              {Blocked, True, csi_node_image_pull_failed} ],
events:     [ {Warning, csi_node_image_pull_failed},
              {Warning, csi_node_image_pull_failed} ]   <-- two same-reason events
```

summary.txt:

```text
status=blocked
managed_volume=unknown status=blocked reason=csi_node_image_pull_failed
managed_volume_condition=Ready   status=False reason=csi_node_image_pull_failed severity=warning
managed_volume_condition=Blocked status=True  reason=csi_node_image_pull_failed severity=warning
managed_volume_action=safe_k8s.import_csi_image mode=dry_run side_effect=safe_k8s
read_only=true
```

### SA boundary (live `auth can-i` as the operator-status SA)

```text
ALLOWED:  create events: yes   patch swblockvolumes --subresource=status: yes
          update swblockclusters --subresource=status: yes
DENIED:   patch/create/delete swblockvolumes (spec): no
          create/delete pods: no   patch/delete pvc: no
          create secrets: no   patch deployments: no   delete storageclass: no
```

Status publication + events only. No storage/workload mutation. (Events:create
was already in the D1 RBAC; this run confirms it is the *only* new write power.)

## The Bug (blocking): duplicate same-reason events collide and abort the reconcile

### Symptom

The one-shot write-mode reconcile failed:

```text
sw-block ops operator-status: create event csi_node_image_pull_failed failed:
  http 409 events "unknown-csi-node-image-pull-failed.1780554986804185681"
  already exists (reason: AlreadyExists)
command terminated with exit code 2
```

The SwBlockVolume `.status` was fully patched (it happens before the event
loop), and the **first** Warning event landed. The **second** event create
collided `409` and the reconcile aborted.

### Root cause

A blocked volume's negative-first projection emits **two conditions that share
one reason**: `Ready=False` and `Blocked=True`, both `csi_node_image_pull_failed`.
The reconciler emits **one event per condition**
(`operator_status_controller.go:166`) and stamps every event with the **same**
`observedAt` (`:173`). The event name
(`kubernetes_status_writer.go:173-178`) is:

```text
kubernetesName(InvolvedObject.Name + "-" + Reason) + "." + observedAt.UnixNano()
= "unknown-csi-node-image-pull-failed." + 1780554986804185681   (identical for both)
```

So both events resolve to the **same name** → event #1 creates, event #2 → `409
AlreadyExists`. `EmitEvent` returns that as an error, and the reconciler
**returns on the first EmitEvent error** (`operator_status_controller.go:174-176`),
aborting the whole `Reconcile` with `exit=2`.

### Why it is blocking, not cosmetic

- The negative-first contract emits `Ready=False` + `Blocked=True` with the same
  root reason for **every** blocked volume — so this fires for any blocked
  reason, including ones reachable on the **live `--master-api`** path
  (`wal_integrity_fault`, `writer_mount_failed`, loopback, …), not just the
  from-bundle image-pull case used to reproduce it.
- Because `Reconcile` returns on the error, **volumes ordered after the blocked
  one in the iteration never get their status written**, and the deployed
  controller logs perpetual `iteration failed exit=2; retrying` for that cluster.
- Net: one blocked volume poisons status publication for the rest of the
  iteration. The status path silently looks fine for healthy volumes but breaks
  the moment anything is blocked — exactly when operators most need it.

### Fix shape

Minimal and standard: in `EmitEvent`, treat `409 AlreadyExists` as idempotent
**success** (`return nil`). That alone makes the iteration pass; the two
same-name events collapse to one Warning event (acceptable — same object +
reason + time; the distinct Ready/Blocked detail is already on the CRD status
conditions).

If both events should land distinctly, also disambiguate the name per condition
(include `condition.Type` or a message hash in the suffix). And consider
deduping `volume.Events` by `(reason)` or `(reason,type)` at the contract layer
so the controller does not emit redundant same-reason events at all.

Either way, the reconcile must not abort on a duplicate event — event emission
is best-effort telemetry and must never block status publication.

### Why unit tests missed it

`kubernetes_status_writer_test.go` emits a **single** event and asserts the POST
body shape against a mock — it never emits two same-reason events in one
reconcile, and the mock does not enforce the API server's name-uniqueness, so
the 409 (and the reconcile-abort) only appear against a live API server with a
genuinely blocked, multi-condition volume. Recommend a reconciler test that
feeds a blocked volume (Ready=False + Blocked=True, same reason) through a fake
event sink that rejects duplicate names, asserting the reconcile still succeeds.

## Also Verified (the D3 carry-forward log polish)

`e0330b7` fixed the cosmetic I flagged in D3: the retry log now reads `iteration
failed` rather than `dry-run iteration failed`. (It was emitted on the pre-stub
404 path in D3; the wording is corrected.)

## Lab State

Clean — `SwBlockVolume`/`SwBlockCluster` stubs deleted, the Warning event
deleted, helm uninstalled, both CRDs deleted; 0 sw-block pods, 0 CRDs, 0 iSCSI
sessions.

## Bottom Line

- **D4 is BLOCKED.** The status surface is fully correct for a blocked volume
  (`status=blocked`, `Ready=False`, `Blocked=True`,
  `reasonCode=csi_node_image_pull_failed`, no `Ready=True` anywhere) and the SA
  still has zero storage/workload mutation power — all six literal checks pass.
- **But the new event-publishing path is not production-safe:** a blocked
  volume's two same-reason conditions mint duplicate event names; the second
  `409 AlreadyExists` aborts the entire reconcile (`exit=2`), halting status
  publication for the rest of the iteration. This fires for **any** blocked
  volume, including live `--master-api` blocked reasons.
- **Fix:** treat `409 AlreadyExists` as success in `EmitEvent` (and optionally
  disambiguate/dedupe per-condition events). Re-validate: drive a blocked volume
  in write mode and confirm the reconcile **succeeds** (`exit=0`), the Warning
  event(s) land, and subsequent volumes still get their status written.
- Do not close D4 until the reconcile no longer aborts on a blocked volume.
