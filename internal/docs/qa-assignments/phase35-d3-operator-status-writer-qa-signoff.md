# QA Sign-off - Phase 35 D3 Real CRD Status Writer

Verdict: **BLOCKED — the core deliverable fails.** The safety boundary is
solid and the *cluster*-level status writer works, but the **`SwBlockVolume`
status writer never lands a single write**: every patch is rejected `422
Invalid` by the CRD's own schema because the writer emits the `allowedActions[]`
fields in **snake_case** (`mutation_allowed`) while the D1 CRD schema requires
**camelCase** (`mutationAllowed`). `SwBlockVolume.status` stays empty. The task's
central ask — "Verify `.status` is patched and agrees with first-volume
evidence" — passes for `SwBlockCluster` but **fails for `SwBlockVolume`**.

This is a gate red for a real reason: a writer-vs-schema contract gap that
dry-run (D2) and the unit test could not surface, because neither validates the
patch payload against the live CRD OpenAPI schema.

Date: 2026-06-03

Source commit: `0f76325 phase35: add crd status writer`
(branch `phase33-testops-failure-hardening`)

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
