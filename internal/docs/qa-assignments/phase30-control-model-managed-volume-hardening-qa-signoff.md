# QA Sign-off - Phase 30 Control Model / ManagedVolume Hardening

Verdict: **PASS**

Date: 2026-05-25

Validated source commit: `a9979cb docs: close phase30 control model hardening`

Scope: lightweight independent QA sign-off on top of the dev-side regression
in `phase30-control-model-managed-volume-hardening-close-report.md`. Does NOT
re-run the full Phase 27/29 multi-volume HA matrix.

## Method

Per the dev's proposed sign-off shape:

1. Read the close report.
2. Rerun the two cheapest gates from a clean lab.
3. Spot-check one report bundle for cleanup / operator-snapshot / ManagedVolume
   action field consistency.
4. Final residue audit.

## Close Report Review

`internal/docs/qa-assignments/phase30-control-model-managed-volume-hardening-close-report.md`:

| Claim | Result |
|---|---|
| D1 dependency review covers PVC/PV/launcher/CSI/authority/host-path/cleanup/report | OK (per `internal/docs/ref/phase30-control-state-dependency-review.md`) |
| D2 ManagedVolume contract separates fields from actions | OK (per `core/ops/managed_volume_contract.go`) |
| D3 cleanup projection has one owner (`CleanupEvidence`) | OK (per `core/ops/cleanup_evidence.go`) |
| D4 regression evidence cites 4 run IDs + unit tests | OK |
| Non-claims still strict (no mutating operator, no rebuild/failback, no NVMe ANA, no backup) | OK |

## QA Reruns

| Gate | QA run ID | Result |
|---|---:|---|
| `go test ./core/ops ./cmd/sw-block` on synced Phase 30 tree | local | PASS |
| `helm-first-volume-via-sw-block-cli-chain.yaml` | `20260525-085618-406e` | 34/34 PASS |
| `cleanup-residue-chain.yaml` | `20260525-085715-df88` | 13/13 PASS |

Lab pre-state confirmed clean before each run (no helm, no iSCSI, no
multipath, no dmsetup, no pods).

## Spot-Check: Field Consistency Across Surfaces

Ran `sw-block ops report --from-bundle <G1-run-bundle> --out /tmp/p30-spotcheck`
against the G1 run's full artifact tree (which includes both the running-state
report and the post-uninstall cleanup-summary.txt).

### Cleanup-summary.txt (Phase 29/30 vocabulary)

```text
cleanup_status=ok
helm_release=sw-block
helm_namespace=kube-system
iqn_substr=io.seaweedfs
k8s_residue_count=0
iscsi_residue_count=0          ← new in Phase 30 D5 hardening
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

Phase 29 non-blocking N1 (missing `iscsi_residue_count`) is **resolved** -
the field is now emitted directly by `verify-helm-cleanup.sh`.

### Report summary.txt

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
multipath_residue_count=0
process_residue_count=0
hostpath_residue_count=0
failure_count=0
cleanup_evidence=/mnt/smb/.../cleanup/verify/cleanup-summary.txt
```

All 7 cleanup fields + `cleanup_evidence` pointer present.

### Operator-snapshot.json

```json
"cleanup": {
  "status": "ok",
  "evidence_ref": "/mnt/smb/.../cleanup/verify/cleanup-summary.txt"
}
```

Read-only block under cluster status, consistent with the `CleanupEvidence`
projection owner.

### Dashboard HTML

Contains `<h2>Lifecycle Cleanup</h2>` section with status / k8s / iscsi /
multipath / processes / hostpath / failures / evidence columns - consistent
with the summary fields.

### ManagedVolume action boundary (D2 contract)

```text
managed_volume_action=observe.collect_bundle mode=read_only side_effect=observe executor=ops
```

Action carries the explicit `mode=read_only`, `side_effect=observe`,
`executor=ops` triple. All Phase 30 actions stay within the read-only / dry-run
boundary - matches the close report's claim that "all Phase 30 actions remain
read_only, dry_run, or disabled until future operator policy".

## Final Residue Audit

```text
helm release sw-block: none
iscsiadm sessions:     No active sessions
multipath -ll:         empty
dmsetup ls:            No devices found
kubectl pods | sw-block: no pods
```

## Hard-Gate Acceptance

| Requirement | Result |
|---|---|
| Close report claims have referenced source artifacts | PASS |
| Unit tests pass on synced Phase 30 tree | PASS |
| Sample TestOps regression repeats on clean lab | PASS (G1 34/34, cleanup-residue 13/13) |
| Phase 29 N1 resolution (`iscsi_residue_count` in summary) carries through | PASS |
| Report / dashboard / operator-snapshot / summary use same `CleanupEvidence` field names | PASS |
| ManagedVolume actions stay within `read_only` / `dry_run` mode boundary | PASS |
| Non-claims wording remains narrow (no mutating, no rebuild, no NVMe, no backup) | PASS |
| Cleanup residue audit clean | PASS |

## Blocking Findings

**None.**

## Non-Blocking Findings

**None.**

The Phase 29 N1 follow-up (`iscsi_residue_count` field on `cleanup-summary.txt`)
landed as part of Phase 30 D5 hardening and is now visible in the QA-reproduced
artifact. No new non-blocking observations from this sign-off.

## Verdict

Phase 30 sign-off **PASS**. Recommend marking Phase 30 as independent-QA-cleared.

This sign-off does not replace the full Phase 27/29 multi-volume HA QA cycle;
those remain the latest independent QA baselines for their respective scopes.
Phase 30 only hardens the control-model surface they depend on; the QA reruns
above confirm the hardening did not regress that surface.
