# QA Sign-off - Phase 31 D5 Multi-Volume RF3 Restart Smoke

Verdict: **PASS (strict)**

Date: 2026-05-25

Validated source commit: `75a5660 testops: harden RF3 restart persistence gate`
D5 scenario was added in earlier commit `f275e3b testops: add multi-volume
restart smoke gate` and runs cleanly under the same hardened pre-clean as D4.

## Scope

Independent QA replay of Phase 31 D5. Verifies that after a k3s restart on
hostPath persistence with **3 RF=3 PVCs**, every volume's primary identity
+ frontend + ManagedVolume status survives, no two distinct volumes end up
sharing a publish target, and no per-volume authority gets swapped across
volumes.

## Run Summary

| Scenario | QA run ID | Result |
|---|---:|---|
| `helm-multi-volume-rf3-restart-smoke-chain.yaml` | `20260525-123233-541b` | **36/36 PASS** |

Lab pre-state confirmed clean (serially-owned, no parallel run this time):
no helm release, no iSCSI sessions, no multipath, no dmsetup, no sw-block
pods.

## Hard-Claim Compliance

`restart/multi-volume-restart-summary.txt`:

```text
multi_volume_restart_status=ok
before_volume_count=3
after_volume_count=3
managed_volume_count=3
duplicate_publish_target_for_distinct_volume=false
cross_volume_authority_mixup=false
reader_verified_count=3
```

Cleanup-summary:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

`restart/report/summary.txt` post-restart confirms 3 ManagedVolumes Ready:

```text
managed_volume=pvc-3206582a-... status=ready reason=first_volume_verified
managed_volume=pvc-ad4a73d8-... status=ready reason=first_volume_verified
managed_volume=pvc-b84662ad-... status=ready reason=first_volume_verified

managed_volume_condition=Ready status=True reason=first_volume_verified severity=info  (x3)
managed_volume_action=observe.collect_bundle mode=read_only side_effect=observe executor=ops  (x3)
```

All ManagedVolume actions stay within the Phase 30 D2 read-only contract
boundary.

## Hard-Gate Acceptance

| Requirement | Result |
|---|---|
| Scenario strict PASS (36/36) | PASS |
| `multi_volume_restart_status=ok` | PASS |
| `before_volume_count=3` | PASS |
| `after_volume_count=3` | PASS (no volume lost across restart) |
| `managed_volume_count=3` | PASS |
| `reader_verified_count=3` | PASS |
| `duplicate_publish_target_for_distinct_volume=false` | PASS (no two volumes share a publish target) |
| `cross_volume_authority_mixup=false` | PASS (no per-volume primary swapped across volumes) |
| `cleanup_status=ok` | PASS |
| Final residue zero | PASS |

## Final Residue Audit (post-D5)

```text
helm release sw-block:                  none
iscsiadm sessions:                      No active sessions
multipath -ll:                          empty
dmsetup ls:                             No devices found
sw-block / blockvolume pods:            none
```

Lab fully clean, matching D4 strict r3 state.

## Blocking Findings

**None.**

## Non-Blocking Findings

**None.**

The `reader_verified_count=pending` -> `reader_verified_count=3` line
sequence in `multi-volume-restart-summary.txt` reflects the helper writing
the summary twice (placeholder + final value) as the reader-after-restart
phase completed. Final value is `3` and the strict assertion gate passed
on the final line. Cosmetic, not worth changing.

## Verdict

**Phase 31 D5 PASS (strict)** on independent QA replay.

After the new hardened pre-clean + port-forward selection landed in
`75a5660`, both D4 (single-PVC) and D5 (3-PVC RF=3) restart smokes pass
strict on a serially-owned clean lab.

Combined Phase 31 D3 + D4 + D5 status:

| Sub-gate | QA strict result | Sign-off doc |
|---|---|---|
| D3 single-node restart persistence | PASS 40/40 | `phase31-restart-persistence-d3-qa-signoff.md` |
| D4 RF3 promotion restart persistence | PASS 34/34 | `phase31-restart-persistence-d4-qa-signoff.md` |
| D5 multi-volume RF3 restart smoke | PASS 36/36 | this doc |

Phase 31 restart-persistence QA cleared end-to-end at the single-node,
RF=3-single-PVC, and 3-PVC-RF=3 scopes.
