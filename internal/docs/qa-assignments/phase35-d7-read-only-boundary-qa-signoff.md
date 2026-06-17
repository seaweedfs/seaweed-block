# QA Sign-off - Phase 35 D7 Read-Only Boundary

Verdict: **PASS.** The operator-status ServiceAccount is provably limited to CRD
read, CRD `/status` patch/update, and Event create — every forbidden mutation
(`swblockvolumes` spec, pods, PVC, PV, Secrets, Deployments, StorageClasses)
returns `no`, the live ClusterRole carries exactly those three rules and nothing
else, and a write-mode reconcile touches only `.status` + Events
(`mutation_allowed=false`, specs untouched, no PVC/PV/workload created).

One **gate-documentation** finding (not a product issue): the assignment's
`can-i` syntax for the status subresource (`resource.group/status`) returns a
**false `no`** on kubectl v1.34; the correct form is `--subresource=status`,
which returns `yes`. See below — fix the doc so the gate isn't failed falsely.

Date: 2026-06-04

Source commit: `a9f43e1 docs: prepare phase35 d7 boundary gate`
(branch `phase33-testops-failure-hardening`; docs-only — product binary is the
D6-validated `1b22ccc`, images reused, no rebuild)

Environment: k3s `v1.34.4+k3s1`, write-mode install
(`operatorStatus.create=true, dryRun=false`),
SA `system:serviceaccount:kube-system:sw-block-seaweed-block-operator-status`.

## Authoritative grant — live ClusterRole has exactly 3 rules

```text
resources=[swblockclusters, swblockvolumes]                 verbs=[get, list, watch]
resources=[swblockclusters/status, swblockvolumes/status]   verbs=[get, update, patch]
resources=[events]                                          verbs=[create]
```

No rule grants anything on pods, PVC, PV, Secrets, Deployments, StorageClasses,
or CRD spec. This is the source of truth; the `can-i` sweep below confirms it.

## Allowed verbs — all effectively `yes`

| Check | Result | Note |
|---|---|---|
| `get swblockvolumes…` | **yes** | |
| `list swblockvolumes…` | **yes** | |
| `watch swblockvolumes…` | **yes** | |
| `create events` | **yes** | |
| `patch swblockvolumes --subresource=status` | **yes** | doc form `…/status` falsely returns `no` |
| `update swblockvolumes --subresource=status` | **yes** | doc form `…/status` falsely returns `no` |
| `patch swblockclusters --subresource=status` | **yes** | doc form `…/status` falsely returns `no` |
| `update swblockclusters --subresource=status` | **yes** | doc form `…/status` falsely returns `no` |

All seven allowed capabilities are granted. The status ones are additionally
proven by the live runtime reconcile, which actually PATCHes `.status`
successfully (below).

## Forbidden verbs — all `no` (21/21)

```text
patch/update/delete swblockvolumes.block.seaweedfs.com   -> no / no / no   (spec & object)
create/patch/delete pods                                 -> no / no / no
create/patch/delete persistentvolumeclaims (default)     -> no / no / no
create/patch/delete persistentvolumes                    -> no / no / no
create/patch       secrets                               -> no / no
create/patch/delete deployments.apps                     -> no / no / no
create/patch/delete storageclasses.storage.k8s.io        -> no / no / no
```

Every storage / workload / config mutation is denied.

## Runtime boundary — write-mode reconcile touches only `.status` + Events

One write-mode reconcile (from a `status_endpoint_unreachable` bundle) against
`SwBlockCluster/sw-block` + `SwBlockVolume/pvc-ready` stubs:

```text
operator_status=write_status cluster=kube-system/sw-block volumes=1 events=2 mutation_allowed=false   EXIT=0
```

| Assertion | Result |
|---|---|
| `mutation_allowed=false` | PASS |
| `SwBlockCluster.spec` unchanged | PASS (`{}` before and after) |
| `SwBlockVolume.spec` unchanged | PASS (`{}` before and after) |
| `SwBlockCluster.status` updated | PASS (`volumeCount=1`, observedAt set) |
| `SwBlockVolume.status` updated | PASS (`status=unknown reason=status_endpoint_unreachable`) |
| Events created (allowed) | PASS (`pvc-ready-warning-status-endpoint-unreachable`, stable D6 name) |
| No PVC/PV created by operator-status | PASS (0 PVCs/PVs) |

The reconcile writes CRD `.status` and Events and nothing else — consistent with
the RBAC and the `KubernetesStatusClient` surface (`WriteClusterStatus`,
`WriteVolumeStatus`, `EmitEvent` only).

## Finding (gate doc, non-blocking): correct the status-subresource `can-i` syntax

The assignment (`phase35-d7-read-only-boundary-qa-assignment.md`, lines 58-60)
lists the allowed status checks as:

```text
kubectl auth can-i patch  swblockvolumes.block.seaweedfs.com/status  --as <SA> -n kube-system
kubectl auth can-i update swblockvolumes.block.seaweedfs.com/status  --as <SA> -n kube-system
kubectl auth can-i patch  swblockclusters.block.seaweedfs.com/status --as <SA> -n kube-system
```

On kubectl `v1.34.4+k3s1` these return **`no`** — a false negative. `can-i`
does not resolve the `<resource>.<group>/status` subresource spelling for these
CRDs, so anyone running the gate literally would see the *allowed* status checks
fail and could wrongly conclude the boundary is broken. (Same artifact I flagged
in the D3 sign-off.)

Correct, reliable form:

```text
kubectl auth can-i patch  swblockvolumes  --subresource=status --as <SA> -n kube-system   # yes
kubectl auth can-i update swblockvolumes  --subresource=status --as <SA> -n kube-system   # yes
kubectl auth can-i patch  swblockclusters --subresource=status --as <SA> -n kube-system   # yes
kubectl auth can-i update swblockclusters --subresource=status --as <SA> -n kube-system   # yes
```

Recommend updating the assignment doc to the `--subresource=status` form (and/or
noting that the ClusterRole rule dump + the live runtime status PATCH are the
authoritative confirmations). The RBAC itself is correct and needs no change.

## Lab State

Clean — `SwBlockVolume`/`SwBlockCluster` stubs deleted, Event deleted, helm
uninstalled, both CRDs deleted, operator-status ClusterRole gone; 0 sw-block
pods, 0 CRDs, 0 ClusterRoles, 0 iSCSI sessions.

## Bottom Line

- **D7 PASS.** The operator-status SA can read CRDs, patch/update CRD `/status`,
  and create Events — and nothing else. All 21 forbidden mutations are denied,
  the live ClusterRole carries exactly the three intended rules, and a write-mode
  reconcile mutates only `.status` + Events with `mutation_allowed=false` and no
  PVC/PV/workload/Secret/StorageClass side effects.
- **One non-blocking doc fix:** the assignment's status-subresource `can-i`
  commands use a spelling that false-negatives on kubectl v1.34; switch them to
  `--subresource=status`. The boundary is correct; only the documented
  verification command is wrong.
- **D7 can close.**
