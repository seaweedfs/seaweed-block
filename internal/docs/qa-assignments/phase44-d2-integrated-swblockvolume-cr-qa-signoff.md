# QA Sign-off — Phase 44 D2 Integrated SwBlockVolume Creation And Protection

Verdict: **PASS.** The normal Day-1 PVC path now automatically produces the
Kubernetes object the bounded delete lifecycle needs, end-to-end and with no
manual stub creation:

```text
CSI CreateVolume -> SwBlockVolume CR exists (CSI owns identity/spec)
  -> lifecycle-owner adds the protection finalizer
  -> operator-status writes Ready=True / first_volume_verified + Events
```

The three-way ownership split holds (CSI = metadata/spec identity, operator-status
= `.status`, lifecycle-owner = the protection finalizer), default installs stay
backward-compatible, and cleanup is zero-residue. Validated live on a real
VAP-capable cluster with fresh candidate images.

Date: 2026-06-16
Source: branch `phase41-lifecycle-owner-foundation` @ `e56b844 phase44: register
swblockvolume from csi create`
Images: fresh `sw-block:local` **and** `sw-block-csi:local` built from `e56b844`
(the CSI image now carries `--swblockvolume-cr-namespace`; older CSI images do
not), imported to m01+m02.
Environment: m02 k3s **v1.34.4+k3s1** (ValidatingAdmissionPolicy enforced).
Install: `--set operatorStatus.create=true --set operatorStatus.dryRun=false
--set lifecycleOwner.create=true --set lifecycleOwner.dryRun=false`.

## G1 — Local Contract — PASS

- `go test ./core/csi ./cmd/blockcsi ./core/ops ./cmd/sw-block` → `ok` (all).
- `helm lint` → 0 failed.
- Default render does **not** include `--swblockvolume-cr-namespace` (count 0).
- Enabled render includes `--swblockvolume-cr-namespace=kube-system`.
- Enabled render grants the CSI controller ClusterRole
  (`sw-block-seaweed-block-csi-controller`, bound to SA
  `sw-block-seaweed-block-csi`) `swblockvolumes [get,list,watch,create,update,patch]`
  — and **no** `swblockvolumes/status` or `swblockvolumes/finalizers`.

## G2 — Live Install And First Volume — PASS

Install succeeded; `csi-controller (3/3)`, `csi-node (2/2)`, `blockmaster (1/1)`,
`operator-status (1/1)`, `lifecycle-owner (1/1)` Running. First-volume:
`first_volume_status=ok`, `writer_verified=true`, `reader_verified=true` (PVC
`sw-block-example-pvc`, volume `pvc-9ea507bd-...`).

## G3 — SwBlockVolume CR Exists — PASS

```text
count: 1
NAME=sw-block-example-pvc  .spec.pvcName=sw-block-example-pvc
.metadata.finalizers=[block.seaweedfs.com/swblockvolume-protection]   (exactly one)
```

Exactly one `SwBlockVolume` for the PVC, named per the operator-status convention
(PVC name), `.spec.pvcName` matches, exactly one protection finalizer, no foreign
finalizer. CSI created the CR; the lifecycle-owner protected it within its
interval — no manual stub was needed (the gap that existed through Phase 43).

## G4 — Status And Event Agreement — PASS

```text
status=ready   reasonCode=first_volume_verified   Ready=True   deleteSafety=<null>
Event: Normal finalizer_added       swblockvolume/sw-block-example-pvc
Event: Normal first_volume_verified swblockvolume/sw-block-example-pvc
```

operator-status wrote status for the CSI-created CR; `deleteSafety` is absent for
the normal non-deleting volume; both bounded Normal Events are present.

## G5 — Boundary — PASS

`kubectl auth can-i` across the three service accounts:

```text
operator-status (sw-block-seaweed-block-operator-status):
  patch swblockvolumes/status=yes  create events=yes
  patch swblockvolumes(main)=no  /finalizers=no  pvc=no
lifecycle-owner (sw-block-seaweed-block-lifecycle-owner):
  patch swblockvolumes(main)=yes (VAP-confined to finalizers)  patch /status=no  pods=no
CSI controller (sw-block-seaweed-block-csi):
  create swblockvolumes(main)=yes  patch swblockvolumes(main)=yes
  patch /status=no  patch /finalizers=no  delete pvc=no
```

No overlap: CSI holds only spec-object verbs (identity/spec), operator-status only
`/status` + Events, lifecycle-owner only the VAP-admitted finalizer patch. (The
VAP forbidden-mutation matrix is unchanged from Phase 43 D1/D2 and remains in
force.)

## G6 — Cleanup — PASS

```text
helm uninstall: release uninstalled
verify-helm-cleanup.sh: cleanup_status=ok   cleanup_observed_at=2026-06-16T08:42:05Z
swblockvolumes=0  pvc=0  pods=0  helm=0  lifecycle-owner VAP=0
```

No stuck `SwBlockVolume`, PVC/PV, pods, helm release, VAP, iSCSI, multipath, or
dmsetup residue. (The CR's protection finalizer was cleared by admin for teardown,
since release-on-delete is D3/D4 scope, not D2.)

## Blocking Findings

None. The CR is created by the normal CSI path, protected by the lifecycle-owner,
and status-published by operator-status; no surface gained out-of-scope mutation
power; PVC/PV/workload/storage were not mutated by operator-status or
lifecycle-owner.

## Non-Blocking Findings

1. **CSI controller SA name ≠ ClusterRole name.** The pod runs as
   `sw-block-seaweed-block-csi`, while the swblockvolumes ClusterRole is
   `sw-block-seaweed-block-csi-controller` (bound to that SA). Correct, but a
   `can-i` against the `-controller` name misleads — note the SA for future
   boundary checks.
2. The new CSI CR-registration requires a candidate `seaweed-block-csi` image
   (the `--swblockvolume-cr-namespace` flag is new in `e56b844`); publish both
   images together before any release that enables the operator/lifecycle
   surfaces.
3. tp01 `NotReady` — unrelated to this single-node gate.

## Recommendation

**Phase 44 D2 passes.** The integrated Day-1 path is real-API-proven: a normal PVC
now yields a CSI-created `SwBlockVolume` CR, protected by the lifecycle-owner and
status-published by operator-status, with a clean three-way ownership boundary and
zero residue — and it is off by default. This closes the "CR must exist before the
delete lifecycle can act" gap. The natural next step is the end-to-end delete
close gate: PVC delete → CSI DeleteVolume → delete-safety projection → gated
finalizer release → uninstall zero-residue as one user path.
