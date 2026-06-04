# QA Sign-off - Phase 35 D3 Real CRD Status Writer

Verdict: **PASS (re-validated on `e3cf010`).** The original `0f76325` was
**BLOCKED** — the `SwBlockVolume` status writer emitted `allowedActions[]` in
snake_case (`mutation_allowed`) while the CRD schema requires camelCase
(`mutationAllowed`), so every per-volume patch was rejected `422` and
`SwBlockVolume.status` stayed empty. The fix `e3cf010` adds a separate camelCase
CRD-status DTO and maps into it before patching. Re-validated live: the per-volume
status now lands, includes `allowedActions[0].mutationAllowed`, agrees with
first-volume evidence, the 422 is gone, and the safety boundary is unchanged.

The original blocked write-up is preserved below as the record of the finding;
the **Re-Validation** section at the end is the current PASS.

Date: 2026-06-03 (blocked) → 2026-06-03 (re-validated PASS)

Source commit (blocked): `0f76325 phase35: add crd status writer`
Fix commit (PASS): `e3cf010 phase35: fix volume status action schema`
(branch `phase33-testops-failure-hardening`)

---

## ORIGINAL FINDING (blocked, `0f76325`) — preserved for the record

## Checks

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | Unit tests (`go test ./core/ops`) | PASS | ok — but see note: the writer unit test checks URL/method, not schema validity |
| 2 | Guard lifted: `dryRun=false` now installs | PASS | `--dry-run` arg conditional on `operatorStatus.dryRun`; install exit=0, pod `1/1 Running`, log `operator_status=write_status` (no longer `dry_run`) |
| 3 | RBAC: no storage/workload mutation power | **PASS** | live `auth can-i` as SA — all `no` (see below) |
| 4 | RBAC: can patch `/status`, not spec | **PASS** | `patch swblockvolumes --subresource=status: yes`; `patch swblockvolumes` (spec): `no`; same for swblockclusters |
| 5 | SwBlockCluster `.status` patched, agrees w/ evidence | **PASS** | `{readyVolumeCount:1, volumeCount:1, nodeCount:1, blockedVolumeCount:0, staleVolumeCount:0}` after first volume provisioned |
| 6 | **SwBlockVolume `.status` patched, agrees w/ evidence** | **FAIL** | `.status = []` (never written); writer rejected `422` every iteration (5×/40 log lines — deterministic) |
| 7 | Lab clean after teardown | PASS | 0 pods/pvc/pv/crd, 0 iSCSI sessions, 0 multipath |

## The Bug (blocking): SwBlockVolume status writer payload violates the CRD schema

### Live symptom

With the `SwBlockVolume` stub present (name `d3-first-vol`, ns `kube-system`,
matching the PVC the controller maps from), the controller's status PATCH is
rejected every iteration:

```text
sw-block ops operator-status: patch swblockvolumes/d3-first-vol status failed:
  http 422 ... SwBlockVolume "d3-first-vol" is invalid:
  status.allowedActions[0].mutationAllowed: Required value
  (reason: FieldValueRequired, field: status.allowedActions[0].mutationAllowed)
```

Result: `kubectl get swblockvolume d3-first-vol -o jsonpath='{.status}'` → empty.
The per-volume status is **never** published.

### Root cause: snake_case payload vs camelCase CRD schema

The CRD (D1, `8332225`) declares `status.allowedActions[]` with **required**
camelCase fields:

```text
status.allowedActions.items.required = ["type", "mode", "mutationAllowed"]
properties: type, mode(enum read_only|dry_run), mutationAllowed(bool),
            sideEffectClass, ownerExecutor, preconditions, invariantRefs, evidenceRefs
```

The writer (D3) builds the patch from `ManagedVolumeOperatorAction`
(`core/ops/managed_volume_operator_contract.go:28`), whose JSON tags are
**snake_case** — because it is the `operator-snapshot.json` contract type:

```go
type ManagedVolumeOperatorAction struct {
    Type            string `json:"type"`              // ✓ matches CRD
    Mode            string `json:"mode"`              // ✓ matches CRD
    SideEffectClass string `json:"side_effect_class"` // ✗ CRD: sideEffectClass
    OwnerExecutor   string `json:"owner_executor,omitempty"`
    MutationAllowed bool   `json:"mutation_allowed"`  // ✗ CRD: mutationAllowed (REQUIRED)
    ...
}
```

`operator_status_controller.go:146` copies `volume.AllowedActions` (this
snake-cased type) **straight into** the CRD `status.allowedActions` patch.
k3s structural-schema validation prunes the unknown `mutation_allowed` key, so
the required `mutationAllowed` is absent → `422 FieldValueRequired`.

### Why the cluster writer works but the volume writer does not

The cluster-status DTO (`operator_status_controller.go:41-45`,
`OperatorClusterStatus`) already uses **camelCase** tags
(`json:"mutationAllowed"`, `json:"allowedActionModes"`). That is why
`SwBlockCluster.status` patches cleanly. Only the per-volume `allowedActions[]`
array reuses the snake-cased `operator-snapshot` type without remapping.

### Fix shape (and a trap to avoid)

Do **not** simply rename the struct tag to `json:"mutationAllowed"`. That tag is
the live `operator-snapshot.json` contract — confirmed snake_case on the wire
(`mutation_allowed`, `side_effect_class`, `owner_executor` in shipped snapshots)
and asserted by `operator_snapshot_test.go` / `managed_volume_operator_contract_test.go`.
Flipping it would break the snapshot surface and those tests.

The surgical fix is a **separate camelCase CRD-status DTO** for
`allowedActions[]` (map `ManagedVolumeOperatorAction` → a CRD action type with
camelCase tags before the patch), mirroring what `OperatorClusterStatus` already
does for the cluster level. Then re-run this exact live check until
`SwBlockVolume.status` is non-empty and agrees with first-volume evidence.

### Why unit tests + dry-run missed it

- **D2 was dry-run** — zero writes, so no schema ever validated the payload.
- The writer unit test (`TestKubernetesStatusClientPatchesOnlyStatusSubresources`)
  asserts the request URL ends in `/status` and the method is `PATCH` against a
  *mock* server. A mock does not enforce the CRD OpenAPI schema, so a payload
  with the wrong field casing passes the unit test but fails the real API server.
- Only a **live** patch against an installed CRD (this check) exercises
  structural-schema validation. This is the value of the live gate.

## What PASSED (and is solid)

### Safety boundary — operator-status SA has no mutation power (live `auth can-i`)

```text
DENIED (must be no):
  create pods: no        delete pods: no         patch pvc: no        delete pvc: no
  create secrets: no     patch deployments: no   delete storageclass: no
  create swblockvolumes: no    delete swblockvolumes: no   (cannot even create/delete the CRs)
  patch swblockvolumes (spec): no   patch swblockclusters (spec): no

ALLOWED (status publication only):
  patch swblockvolumes  --subresource=status: yes
  patch swblockclusters --subresource=status: yes
  get/list swblockvolumes: yes      create events: yes
```

Even in write mode the controller logs `mutation_allowed=false` — a CRD status
write is not a storage mutation. The SA can publish `/status` and emit events
and **nothing else**. This is exactly the read-only-operator contract.

### Cluster status write — works and agrees with evidence

After provisioning a first volume (PVC `d3-first-vol`, Bound to
`pvc-08000df3-…`, `sw-block-dynamic` SC), `SwBlockCluster.status` was patched to:

```json
{"volumeCount":1,"readyVolumeCount":1,"nodeCount":1,
 "blockedVolumeCount":0,"staleVolumeCount":0,"observedAt":"2026-06-04T01:09:55Z"}
```

`readyVolumeCount` moved 0→1 as the volume came up — the cluster-level writer
correctly reflects first-volume-verified evidence. (Note: the consuming pod was
still `ContainerCreating` at teardown — iSCSI mount latency — but the volume was
provisioned and counted ready, which is the evidence under test.)

## Repro (for dev)

1. Build images, `helm install sw-block ... --set operatorStatus.create=true --set operatorStatus.dryRun=false`.
2. `kubectl -n kube-system apply` a `SwBlockCluster/sw-block` stub (`spec: {}`).
3. Create a PVC on `sw-block-dynamic` + consuming pod (first volume).
4. `kubectl -n kube-system apply` a `SwBlockVolume/<pvc-name>` stub (`spec: {}`).
5. Watch the controller: cluster status patches; volume status 422s on
   `allowedActions[0].mutationAllowed`. `get swblockvolume <name> -o jsonpath='{.status}'` → empty.

## Non-Blocking Carry-Forward (still open from prior slices)

- N1 (from D2): first-boot blockmaster `connection refused` transient still
  present in write-mode logs (`dial tcp …:9333 connect: connection refused`,
  self-heals on 30s retry). Cosmetic; tame so cold operators don't see an error.
- From D4: surface `reason=wal_integrity_fault` on the status surface (still
  generic) — independent of D3.

## Lab State

Clean — helm uninstalled, both CRDs deleted, stubs deleted, PVC/PV deleted,
0 sw-block pods, 0 iSCSI sessions, 0 multipath.

## Bottom Line

- **D3 is BLOCKED.** Safety boundary: PASS. Cluster status writer: PASS.
  **SwBlockVolume status writer: FAIL** — it never writes, because its
  `allowedActions[]` payload is snake_case while the CRD schema requires
  camelCase `mutationAllowed`. `422` on every iteration; `.status` stays empty.
- Single, precise fix: map `ManagedVolumeOperatorAction` → a camelCase CRD
  action DTO for the status patch (do **not** flip the snapshot struct tag —
  it would break `operator-snapshot.json`). Cluster status already does this
  right; mirror it for volumes.
- Add a regression that validates the volume status patch against the **real**
  CRD schema (envtest/server-side dry-run), not a mock — the current unit test
  only checks URL/method and so cannot catch field-casing drift.
- Re-validate live: `SwBlockVolume.status` non-empty and agreeing with
  first-volume evidence (`status=ready`, `reason=first_volume_verified`), with
  the safety boundary unchanged. Do not close D3 until the per-volume status
  actually lands.

---

## RE-VALIDATION (`e3cf010`) — PASS

Re-ran the exact live flow against `e3cf010 phase35: fix volume status action
schema` (fresh images built + imported to m01/m02/tp01; `helm install … --set
operatorStatus.create=true --set operatorStatus.dryRun=false`, exit=0).

### The fix

`operator_status_controller.go` now has a dedicated camelCase CRD-status DTO
`SwBlockVolumeCRDAction` (`json:"mutationAllowed"`, `sideEffectClass`,
`ownerExecutor`, `invariantRefs`, `evidenceRefs`) and maps each
`ManagedVolumeOperatorAction` into it via `swBlockVolumeCRDActions()` before
patching `SwBlockVolume.status`. The snake_case `ManagedVolumeOperatorAction` is
untouched, so `operator-snapshot.json` keeps its snake_case contract. Exactly
the surgical split recommended above; cluster status was already camelCase.

### Live results

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | SwBlockVolume `.status` patched (was empty) | **PASS** | `.status` now populated (full JSON below) |
| 2 | `allowedActions[0].mutationAllowed` lands | **PASS** | `"allowedActions":[{"mode":"read_only","mutationAllowed":false,"ownerExecutor":"ops","sideEffectClass":"observe","type":"observe.collect_bundle"}]` — camelCase |
| 3 | 422 gone | **PASS** | controller log: `422 occurrences: 0`; only prior failures were `http 404 NotFound` (pre-stub window, before the SwBlockVolume CR existed) |
| 4 | Status agrees with first-volume evidence | **PASS** | `status:ready`, `reasonCode:first_volume_verified`, Condition `Ready=True reason=first_volume_verified`, `volumeID:pvc-1d87b2ec-…` matches the bound PV, `pvcName:d3-first-vol` |
| 5 | Controller wrote **only** `/status`, not spec | **PASS** | `SwBlockVolume.spec = {}` after multiple write iterations |
| 6 | Cluster status still agrees | **PASS** | `SwBlockCluster.status.readyVolumeCount=1`, `volumes=1` in `write_status` log |
| 7 | Safety boundary unchanged | **PASS** | SA `auth can-i`: `patch swblockvolumes --subresource=status: yes`; `patch swblockvolumes (spec): no`; `create pods: no`; `patch pvc: no`; `delete swblockvolumes: no`. `mutation_allowed=false` in write mode |
| 8 | Steady-state clean | **PASS** | last 3 iterations all `operator_status=write_status … volumes=1 … mutation_allowed=false`, no failures |

Full written `SwBlockVolume.status`:

```json
{"allowedActions":[{"mode":"read_only","mutationAllowed":false,"ownerExecutor":"ops",
 "sideEffectClass":"observe","type":"observe.collect_bundle"}],
 "conditions":[{"type":"Ready","status":"True","reason":"first_volume_verified",
 "severity":"info","message":"managed volume is ready for the documented path"}],
 "status":"ready","reasonCode":"first_volume_verified",
 "pvcName":"d3-first-vol","volumeID":"pvc-1d87b2ec-7e56-471e-b7bd-c4b9e11c8dec",
 "observedAt":"2026-06-04T01:36:45Z"}
```

### Regression coverage (dev added)

`operator_status_controller_test.go` now asserts the marshaled volume status
JSON **contains** `"mutationAllowed":false` and **does not contain**
`mutation_allowed`. That directly guards this casing bug. Note it is a
marshaled-string assertion, not an envtest/server-side-dry-run against the live
CRD OpenAPI schema — so it catches field-casing drift but would not catch other
schema-shape drift (e.g. a newly-required field, or an out-of-enum `mode`
value). A server-side-dry-run check on the patch payload would close that gap
fully; non-blocking, filed as a follow-up.

### Carry-forward (non-blocking, unchanged)

- N1 (from D2): first-boot blockmaster connection-refused transient still
  present; self-heals.
- Cosmetic: the per-iteration failure log reads `dry-run iteration failed
  exit=2` even when running in **write** mode (the pre-stub 404 logged this).
  Mislabel — should say "iteration"/"write", not "dry-run". Tiny polish.
- From D4: surface `reason=wal_integrity_fault` on the status surface.

### Lab state

Clean — helm uninstalled, both CRDs deleted (cascades CR instances), PVC/PV/pod
deleted; 0 sw-block pods, 0 PVC, 0 iSCSI sessions, 0 multipath.

### Bottom line

- **D3 PASS on `e3cf010`.** `SwBlockVolume.status` is published with the
  camelCase `mutationAllowed` the schema requires; status agrees with
  first-volume evidence; the 422 is gone; the controller writes only `/status`
  (spec untouched); and the SA still has zero storage/workload mutation power.
- **D3 can close.** Recommend the server-side-dry-run regression and the
  log-label polish as tracked, non-blocking follow-ups.
