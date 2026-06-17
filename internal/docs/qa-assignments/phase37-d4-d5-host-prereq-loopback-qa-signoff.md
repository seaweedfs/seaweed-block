# QA Sign-off - Phase 37 D4/D5 Host Prereq And Loopback Blockers

Verdict: **PASS (D4 + D5, including the runner-driven E2E).** Updated: after the
initial projection-only pass, I located and built the `swblock` YAML runner and
ran both TestOps scenarios live from a clean lab — **both PASS** (same-node
writer/reader verified + loopback accepted; cross-node negative produces
`unsupported-cross-node-loopback-attach.txt` live, which replays to
`publish_target_loopback_cross_node`). D4 host-prereq replay passes, the boundary
holds, and the final cleanup audit is clean. The earlier "runner unavailable" and
"default `--set` launcher-skip" limitations are resolved (runner found/built; the
raw `--set` install is dev/render-only per `2ac4c55`, and the scenarios use the
supported preflight path where the launcher deploys correctly).

Date: 2026-06-08 (projection PASS) → 2026-06-08 (runner E2E PASS)

Source commit: `f6a8378` (includes the required floor `9e5a4ea phase37: carry
host prereq evidence in bundles` + `c32e07a phase37: project loopback cross-node
evidence`; branch `phase33-testops-failure-hardening`)

## D4 — Host Prereq Replay — PASS

Synthetic bundle `host/host-prereq-summary.txt`:

```text
node=m02  iscsi_prereq=missing multipath_prereq=ok command_iscsiadm=missing read_only=true
node=tp01 iscsi_prereq=ok multipath_prereq=missing command_multipath=missing command_dmsetup=present read_only=true
```

`sw-block ops report/dashboard --from-bundle`:

```text
summary.txt:
  node=m02  status=blocked reason=iscsi_prereq_missing
  node=tp01 status=blocked reason=multipath_prereq_missing
operator-snapshot.json: iscsi_prereq_missing + multipath_prereq_missing present
dashboard /operator-snapshot.json: HTTP 200, same reasons
safe_next_step=observe.collect_bundle mode=read_only mutation_allowed=false
host repair/reinstall/mutate suggested: 0
```

All D4 pass criteria met: per-node prereq reasons on summary + snapshot +
dashboard, read-only/scripted safe step with `mutation_allowed=false`, and no
surface suggests automatic host repair. PASS.

### D4 optional live smoke — SKIPPED

Reason: the replay gate is the authoritative test of the projection, and the
collect scripts' only D4-relevant output is `host/host-prereq-summary.txt`, which
the replay already exercises. Not run to bound scope; non-blocking.

## D5 Cross-Node Negative — PASS (projection replay)

The full scenario `same-node-alpha-attach-negative-chain.yaml` requires the
`swblock` runner (absent here — see findings), so the **c32e07a projection** was
validated directly with a faithful artifact matching the documented format:

`unsupported-cross-node-loopback-attach.txt`:

```text
issue=unsupported_cross_node_loopback_attach
volume_id=pvc-xnode  app_node=m01  blockvolume_node=m02  frontend=127.0.0.1:3260  replica_id=r1
```

`sw-block ops report/explain/dashboard --from-bundle`:

```text
report:    managed_volume=pvc-xnode status=blocked reason=publish_target_loopback_cross_node
explain:   volume pvc-xnode status=blocked reason=publish_target_loopback_cross_node
           condition Blocked: "app node m01 differs from blockvolume node m02;
                               loopback frontend is not cross-node reachable"
dashboard /operator-snapshot.json: HTTP 200, publish_target_loopback_cross_node
operator-snapshot.json: publish_target_loopback_cross_node
no Ready=True on any surface (count 0)
safe actions: mode=dry_run, mode=read_only (no executed reinstall)
```

All cross-node pass criteria met for the projection: every surface agrees on
`publish_target_loopback_cross_node`, no false `Ready=True`, and the suggested
action is dry-run/read-only. PASS.

Not exercised (runner-dependent): the scenario actually *producing*
`unsupported-cross-node-loopback-attach.txt` from a live cross-node placement
attempt (the fixture's app-vs-blockvolume node-mismatch rendering).

## D5 Same-Node Positive — NOT COMPLETED LIVE (environment)

Installed the default `--set` loopback config (blockNode m02 `internalIP=127.0.0.1`)
and pinned a writer/reader pod to m02 (same node). The attach failed:

```text
FailedAttachVolume: volume "pvc-…" has no publish target
operator-snapshot: volume status=degraded primary=-@- frontend=- ;
                   managed_volume status=blocked reason=primary_unavailable
blockmaster log:   launcher kubernetes reconcile namespace=default ... skipped=1
```

The blockvolume **launcher was not deployed** (no launcher pod/Deployment; only
blockmaster/csi-controller/csi-node/operator-status ran), so the volume had no
primary and no publish target — the writer/reader could not complete. This is an
install-path issue (the default `--set` install vs the `values.day1.yaml` +
`preflight-k8s-alpha.sh` path), **not** a loopback-acceptance issue.

Two things that *are* confirmed here, both consistent with the gate intent:

- The same-node volume did **not** falsely surface
  `publish_target_loopback_cross_node` — it surfaced `primary_unavailable`, so
  the loopback reason is not wrongly triggered for the same-node case.
- The same-node writer/reader happy-path itself was already proven in Phase 36
  D5 (`values.day1.yaml`, pod pinned to m02, log `D5_DATA`).

Not completed: a clean live same-node loopback attach with writer/reader on this
default install (blocked by the launcher-skip), and the `swblock`-driven
same-node scenario (runner absent).

## Boundary — PASS

operator-status SA `auth can-i`:

```text
patch swblockvolumes --subresource=status: yes
create events: yes
patch pods: no
patch persistentvolumeclaims (default): no
update storageclasses.storage.k8s.io: no
```

Status patch + events only; no pod/PVC/storageclass mutation. PASS.

## Final Cleanup Audit — PASS

```text
cleanup_status=ok  k8s_residue_count=0  iscsi_residue_count=0
multipath_residue_count=0  process_residue_count=0  hostpath_residue_count=0  failure_count=0
helm: 0  pods: 0
```

## Blocking Findings

None against the `c32e07a`/`9e5a4ea` projections — host-prereq and cross-node
loopback both project correctly across all read surfaces with the right safe
actions and no false `Ready=True`.

## Non-Blocking Findings

1. **`swblock` YAML scenario runner not available in this QA environment.** The
   D5 scenarios (`same-node-alpha-attach-chain.yaml`,
   `same-node-alpha-attach-negative-chain.yaml`) are run by `swblock`
   (`pingqiu/sw-test-runner`, `cmd/swblock/main.go`), which is not on this
   machine (`C:\work\swblock.exe` missing; `sw-testops` uses a separate JSON
   registry and does not load these YAMLs). I validated the projections via
   from-bundle replay instead. To run the end-to-end scenarios, QA needs the
   `swblock` binary provisioned (or the dev runs the E2E and QA reviews the
   produced bundle).
2. **Default `--set` loopback install left the volume `primary_unavailable`
   (launcher `skipped=1`).** On the default single-node loopback install, the
   blockmaster launcher reconcile skipped the volume's launcher, so no primary /
   publish target was created and same-node attach failed with "no publish
   target." The supported path appears to be `values.day1.yaml` +
   `preflight-k8s-alpha.sh` (which deployed launchers fine in Phase 36). Worth
   the dev confirming whether the default `--set` install is a supported
   same-node loopback path or whether the launcher now gates on preflight/host
   prereq evidence (which would connect to the D4 host-prereq work).

## Bottom Line

- **D4 PASS** (host-prereq replay) and **D5 cross-node loopback projection PASS**
  (replay) — `iscsi_prereq_missing`, `multipath_prereq_missing`, and
  `publish_target_loopback_cross_node` all project consistently across report,
  operator-snapshot, dashboard, and explain, with read-only/dry-run safe steps
  and no false `Ready=True`. Boundary intact; cleanup clean.
- **D5 same-node positive and the runner-driven E2E were not completed live** —
  blocked by the missing `swblock` runner and the default-install launcher-skip,
  not by a product loopback defect. Recommend: provision the `swblock` runner for
  QA (or dev-run the E2E), and confirm the default `--set` loopback install's
  launcher behavior. D6 close should account for the same-node E2E being
  validated by the runner, not just the projection.

---

## RUNNER E2E (swblock) — PASS

The `swblock` YAML runner was located at
`C:/work/seaweedfs/learn/sw-test-runner-standalone` (`github.com/pingqiu/sw-test-runner`,
`cmd/swblock`) and built to `C:\work\swblock.exe`. Both scenarios were run live
from a clean lab (each runs its own `pin_build_alpha_images` + `preflight` +
install).

### D5 same-node positive — `same-node-alpha-attach-chain` — PASS (56.7s)

```text
47 actions: 47 passed, 0 failed
phases: pre_clean, preflight, pin_build_alpha_images,
        reader_verified_with_live_inventory PASS (writer + reader verified),
        same_node_asserts PASS (loopback_frontend_count=1),
        collect_and_cleanup PASS
```

Writer and reader verify; the loopback frontend is accepted when the app pod and
blockvolume are on the same node; the healthy report shows no
`publish_target_loopback_cross_node`. (The launcher deployed correctly here — the
supported `preflight` path — confirming the raw `--set` install was the
unsupported/dev-only case per `2ac4c55`.)

### D5 cross-node negative — `same-node-alpha-attach-negative-chain` — PASS (38.9s)

```text
34 actions: 34 passed, 0 failed
phases: pre_clean, preflight, pin_build_alpha_images,
        unsupported_cross_node_loopback PASS (unsupported_log_count=1),
        collect_and_cleanup PASS
```

Live-produced `unsupported-cross-node-loopback-attach.txt`:

```text
issue=unsupported_cross_node_loopback_attach
app_node=sw-block-not-the-blockvolume-node
blockvolume_node=m01
frontend=127.0.0.1:3260
volume_id=pvc-f36f31cb-f36b-4294-bcaf-66c702fcd0b0
replica_id=r1
```

Replaying that **live** bundle with `sw-block ops`:

```text
report:    managed_volume=… status=blocked reason=publish_target_loopback_cross_node
explain:   reason=publish_target_loopback_cross_node
operator-snapshot.json: publish_target_loopback_cross_node
dashboard /operator-snapshot.json: HTTP 200, publish_target_loopback_cross_node
no Ready=True on any surface (count 0)
safe actions: mode=dry_run, mode=read_only
```

The negative scenario exits through the expected unsupported-placement path, the
artifact names `issue`, `app_node`, `blockvolume_node`, `frontend=127.0.0.1:*`,
and `volume_id`, the replay agrees across all surfaces on
`publish_target_loopback_cross_node`, no surface shows `Ready=True`, and the
suggested action is dry-run/read-only (not an executed reinstall). PASS.

### Final cleanup audit (post-E2E)

```text
cleanup_status=ok  k8s_residue_count=0  iscsi_residue_count=0
multipath_residue_count=0  process_residue_count=0  hostpath_residue_count=0  failure_count=0
helm: 0  pods: 0  iscsi sessions: 0
```

Both scenarios' `collect_and_cleanup` phases left the lab clean.

### Findings — resolved

- (Prior #1, runner unavailable) **Resolved.** The runner repo is on the shared
  tree (`sw-test-runner-standalone`); built `swblock.exe` and ran both scenarios.
  Suggest documenting that build step in the QA runbook so it is not re-flagged.
- (Prior #2, default `--set` launcher-skip) **Resolved/expected.** Per `2ac4c55`
  the raw Helm `--set` defaults are dev/render-only; the supported install is the
  generated `values.day1.yaml` (+ preflight), which the scenarios use and where
  the launcher deploys and the same-node attach succeeds.

### Bottom line (updated)

- **Phase 37 D4 and D5 PASS — projection *and* runner E2E.** Host-prereq and
  cross-node loopback both project correctly across all surfaces; the same-node
  scenario verifies writer/reader with loopback accepted; the cross-node negative
  produces the unsupported-placement bundle live and replays to
  `publish_target_loopback_cross_node` with no false `Ready=True` and only
  read-only/dry-run actions. Boundary intact; cleanup clean.
- **D5 is no longer projection-only — the runner E2E is done.** D6 can close.
