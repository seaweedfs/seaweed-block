# QA Sign-off - Phase 36 D4 Cleanup Visibility (Not Automatic Cleanup)

Verdict: **PASS.** Cleanup verifier evidence becomes user-visible status and safe
next-step guidance, and the operator deletes nothing: clean state projects
`CleanupRequired=False/cleanup_verified` with no `verify_cleanup` step; residue
projects `CleanupRequired=True` with residue counts and a scripted
`observe.verify_cleanup` step; all surfaces agree; and the controller never runs
the verifier/uninstall and holds no mutation power. One minor non-blocking
observation (a clean state still emits a `collect_bundle` step).

Date: 2026-06-05

Source commit: `e742f64 phase36: project cleanup visibility status`
(branch `phase33-testops-failure-hardening`)

Environment: k3s `v1.34.4+k3s1`, write-mode operator-status, fresh `e742f64`
images. Both cleanup summaries were produced by the **real**
`scripts/verify-helm-cleanup.sh` (clean lab, and with a controlled stale
`io.seaweedfs` iSCSI node DB record), then fed through from-bundle reconciles
under dedicated cluster names so the live controller didn't race the gate.

## G1 — Clean State Projects CleanupRequired=False — PASS

Real verifier on the clean lab → `cleanup_status=ok`, all residue counts 0.
Fed to `--cluster-name d4-clean`:

```text
SwBlockCluster.status.cleanup = {"status":"ok","evidenceRef":"/tmp/g1bb/cleanup-summary.txt"}
condition CleanupRequired: status=False reason=cleanup_verified
safeNextSteps: observe.collect_bundle(read_only)   <- no observe.verify_cleanup
report summary.txt: cleanup_status=ok
operator-snapshot.json: cluster.cleanup.status = "ok"
index.html: "Lifecycle Cleanup" section present
```

`cleanup.status=ok`, `CleanupRequired=False reason=cleanup_verified`, and **no
`observe.verify_cleanup` step** in the clean state. PASS.

## G2 — Residue Projects CleanupRequired=True — PASS

Controlled residue: `iscsiadm -m node -o new -T iqn.2024-01.io.seaweedfs:d4-residue
-p 127.0.0.1:3260` (a stale node DB record, no session — the exact safe residue
the assignment suggests). Real verifier → `cleanup_status=failed,
iscsi_residue_count=1, failure_count=1`. Stale record removed afterward. Fed to
`--cluster-name d4-residue`:

```text
SwBlockCluster.status.cleanup = {"status":"failed","iscsiResidueCount":1,"failureCount":1,
                                 "evidenceRef":"/tmp/g2bb/cleanup-summary.txt"}
condition CleanupRequired: status=True reason=cleanup_required
safeNextSteps:
  observe.collect_bundle  mode=read_only  mutationAllowed=false
  observe.verify_cleanup  mode=scripted   mutationAllowed=false
                          command=bash scripts/verify-helm-cleanup.sh "$PWD"
```

`cleanup.status=failed`, `iscsiResidueCount=1 > 0`, `CleanupRequired=True`, and
the `observe.verify_cleanup` step has `mode=scripted`, `mutationAllowed=false`,
command mentioning `verify-helm-cleanup.sh`. PASS.

(Reason is `cleanup_required` — the verifier's `cleanup-summary.txt` does not
emit a `reason_codes` line, so the projection's documented fallback applies. The
assignment requires the verifier reason "when present"; none was present.)

## G3 — Surface Agreement — PASS

Residue evidence through `sw-block ops report --from-bundle`:

```text
summary.txt:
  cleanup_status=failed
  iscsi_residue_count=1
  safe_next_step=observe.verify_cleanup mode=scripted mutation_allowed=false
    command="bash scripts/verify-helm-cleanup.sh \"$PWD\"" reason=cleanup_required
index.html:           "Lifecycle Cleanup"   (present)
operator-snapshot.json: cluster.cleanup.status = "failed"   (iscsi_residue_count=1)
```

CRD `status.cleanup` counters == operator-snapshot `cluster.cleanup` ==
summary.txt == index.html; the `safe_next_step` line matches the CRD
`safeNextSteps[]`. Clean case agrees too (CRD/snapshot/summary all `ok`). PASS.

## G4 — Boundary — PASS

```text
create events: yes
create pods: no    delete pods: no    delete persistentvolumes: no
patch swblockclusters (spec): no
operator-status ran the verifier? cleanup dirs in operator-status pod: 0
```

operator-status patches CRD status and creates Events only; it does **not** run
`verify-helm-cleanup.sh` or `uninstall-k8s-alpha.sh` (no cleanup artifact dirs
appeared in its pod), and cannot delete pods/PVCs/PVs/deployments/storageclasses
or mutate CRD spec. The verifier/cleanup commands are *suggested as scripted
next steps*, never executed by the controller. PASS.

## Final Cleanup Verifier — cleanup_status=ok

After teardown, the real verifier reports `cleanup_status=ok, iscsi_residue_count=0,
k8s_residue_count=0, failure_count=0`. Pass criterion met.

## Minor Observation (non-blocking)

A **verified-clean** cleanup state still emits an `observe.collect_bundle`
(`read_only`) safe next step. `safeNextStepsFromCluster`
(`operator_status_controller.go:166`) adds the collect-bundle step whenever
`cluster.Cleanup != nil` — i.e. whenever any cleanup evidence is present, even
`status=ok` with zero residue. The D4 assertion (no `verify_cleanup` in clean) is
satisfied, but suggesting "collect a support bundle" when everything is verified
clean is slightly noisy. Consider gating the collect-bundle step on actual
non-OK status / real evidence refs rather than mere presence of a (passing)
cleanup summary. Not a blocker.

## Non-Claims Verified

No automatic cleanup, no finalizer/delete behavior, no host or iSCSI/multipath
mutation by operator-status, no support-bundle upload. The controller only
*shows* cleanup state and *points* to the scripted verifier/cleanup path.

## Lab State

Clean — both `SwBlockCluster` stubs deleted, helm uninstalled, both CRDs deleted,
controlled iSCSI residue removed; final verifier `cleanup_status=ok`; 0 sw-block
pods, 0 CRDs, 0 iSCSI residue.

## Bottom Line

- **D4 PASS.** Clean cleanup evidence → `CleanupRequired=False/cleanup_verified`,
  no `verify_cleanup` step; residue → `CleanupRequired=True`, `cleanup.status=failed`,
  `iscsiResidueCount=1`, and a scripted, non-mutating `observe.verify_cleanup`
  step pointing at `verify-helm-cleanup.sh`. CRD status, operator-snapshot,
  summary.txt and index.html all agree, and the controller performs no cleanup
  and holds no mutation power. Final lab verifier is `cleanup_status=ok`.
- **One non-blocking polish:** suppress the `collect_bundle` safe step when the
  cleanup evidence is `status=ok` with no residue.
- **D4 can close.**

## Post-QA Dev Polish

Follow-up commit suppresses the clean-state `observe.collect_bundle` next step
when the only evidence is a passing cleanup summary. Clean cleanup evidence now
projects `CleanupRequired=False/cleanup_verified` with no safe next steps; the
residue path remains unchanged.
