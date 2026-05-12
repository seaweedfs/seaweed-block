# QA Report — Light-Use Install And Lifecycle Operations MVP

This is the formal close report against
`internal/docs/qa-assignments/light-use-mvp-close-hard-gate.md`.

```text
Product commit:       9a49992 (docs: record first-volume qa consistency)
Runner commit:        sw-test-runner-standalone @ 6ec7abd (swblock build used: 15.9 MB Windows binary)
Host:                 m02 (192.168.1.184) — Ubuntu 24.04.3 LTS / k3s v1.34.4+k3s1 / kernel 6.17.0-19-generic
Runbook path:         docs/quickstart-kubernetes.md (First Volume In 10 Minutes section)
Scenario run_id:      20260511-225935-0ff4   (QA-owned light-use-first-volume-chain run on commit ec76385)
Manual run artifact:  /tmp/sw-block-app-demo-20260512T055732Z   (QA-owned cold runbook follow on m02)
Scenario artifact:    /mnt/smb/work/share/g15d-k8s/20260511-225935-0ff4-first-volume
```

## Verdict

```text
PASS
```

The plan's user-facing operational loop on the supported single-node Kubernetes
shape is exercised end to end, the runbook and the runner-native scenario are
observably consistent, every named non-claim is honestly disclaimed, and the
failure-bundle mechanism produces a useful triage artifact under a real
broken-attach scenario.

## HG clause table

```text
HG-0  single entry point:                  PASS
HG-1  runnable preflight:                  PASS
HG-2  one happy path:                      MOSTLY PASS (non-blocking note 1)
HG-3  image fallback honest:               PASS
HG-4  boundary verification:               PARTIAL (non-blocking note 2)
HG-5  failure bundle reachable:            PASS
HG-6  bundle self-explanatory:             PASS
HG-7  line-level cleanup attribution:      PASS
HG-8  scoped cleanup commands:             PASS
HG-9  idempotency / retry:                 PASS
HG-10 ≥3 break classes:                    PASS
HG-11 claims match what was tested:        PASS
HG-12 runbook vs scenario consistency:     PASS (with documented residue)
```

### HG-0 single entry point — PASS

`README.md` "Quick start" section is a single bullet pointing to
`docs/quickstart-kubernetes.md` with the line *"That guide is the single
supported alpha entry point for a new user."* No A/B/C fork before a paragraph.
The README's `Development` section still has `scripts/run-k8s-alpha.sh` for
contributors but it is labeled as such, not as a user entry point.

### HG-1 runnable preflight — PASS (live evidence, QA-owned)

`scripts/preflight-k8s-alpha.sh --local-k3s` emits structured lines and exits
non-zero on failure. QA-owned live capture from manual run on m02:

```text
[preflight] checked name=bash status=PASS detail="/usr/bin/bash"
[preflight] checked name=kubectl status=PASS detail="/usr/local/bin/kubectl"
[preflight] checked name=iscsiadm status=PASS detail="/usr/sbin/iscsiadm"
[preflight] checked name=kubectl_client status=PASS detail="Client Version: v1.34.4+k3s1"
[preflight] checked name=kubernetes_nodes status=PASS detail="..."
[preflight] checked name=docker status=PASS detail="/usr/bin/docker"
[preflight] checked name=sudo status=PASS detail="/usr/bin/sudo"
[preflight] checked name=k3s_ctr_images status=PASS detail="k3s containerd image list accessible"
[preflight] unchecked name=ghcr_pull reason="local-k3s path selected"
[preflight] summary status=PASS checked=8 failed=0 unchecked=1 mode=local-k3s
```

The break-classes scenario (HG-10) also exercises the FAIL path: the
`SW_BLOCK_PREFLIGHT_FORCE_MISSING=iscsiadm` fixture produces
`checked name=iscsiadm status=FAIL`, the `Install open-iscsi/iscsiadm`
remediation line, and exit code 2.

### HG-2 one happy path — MOSTLY PASS

The default demo path is one command (`bash scripts/run-k8s-demo.sh`). The
"Use Your Own App" section now numbers `bash scripts/apply-k8s-alpha-blockvolumes.sh`
as step 3 of a 4-step flow with an explicit *"Until the operator exists"*
justification — the side-quest is elevated, not footnoted. The gate's
strict-pass condition asked for a boundary check after that step; none is
present. Counted as MOSTLY PASS, non-blocking note 1 below.

### HG-3 image fallback honest — PASS

Default path is now local k3s build/import (no GHCR dependency). GHCR is
moved to "Alternate Image Paths" with the explicit failure mode named
(`ImagePullBackOff`) and `kubectl -n kube-system describe pod` remediation
commands. The break-classes scenario validates the GHCR-fails branch via
`SW_BLOCK_IMAGE=ghcr.io/seaweedfs/does-not-exist:no-such-tag`, captured in
run `20260511-210727-5cb9` under `image-failure/image-pull-evidence.txt`.

### HG-4 boundary verification — PARTIAL

After-run evidence ladder + artifact table + cleanup-attribution lines are
present. Each boundary maps to a specific artifact file with the expected
content (e.g., writer/reader checksum lines, generated-blockvolume.yaml,
iscsi-sessions.after-delete.txt). During-run `kubectl get sc,pv,pvc,pod`
commands per individual state transition are not enumerated. Acceptable
for the demo's batch-evidence model; non-blocking note 2 below.

### HG-5 failure bundle reachable — PASS

`docs/quickstart-kubernetes.md` "If The Demo Fails" section names
`sw-block ops status --volume <id> --master <addr> --status-addr <addr> --out <dir>`
with the exact flags, including the documented
`ops-status-unavailable: no volume id/status address reached` branch.
The HG-10 image-failure fixture exercises that branch live:

```text
phase=failure
volume_id=<unavailable>
status_addr=<unavailable>
ops-status-unavailable: no volume id reached
```

### HG-6 bundle self-explanatory — PASS

QA cold-read on the failure-bundle slice's `volume-status-summary.txt`
(run `20260511-203619-885f`) and on the HG-10 attach-failure bundle
(run `20260511-210727-5cb9` under `attach-failure/ops-status/`) — both
self-attribute enough state for a stranger to form a triage hypothesis
without follow-up. Each summary names: status, volume id, replica,
authority role/healthy/assigned/epoch, replication role, durable entry,
residue counts, residue_unchecked, explicit `issues:` list.

Example triage hypothesis a stranger can form from the HG-10
attach-failure bundle alone: status=unhealthy, authority assigned=false,
durable_entry latched=false → control-plane assignment did not complete
for this replica.

### HG-7 line-level cleanup attribution — PASS

QA-owned scenario run `20260511-225935-0ff4` produced
`cleanup-attribution.txt` with 10 line-level entries; sample:

```text
pvc:sw-block-demo-pvc state=deleted deleted_by=demo-script-kubectl-delete evidence=demo/delete-pvc.log
blockmaster-manifest:pvc-e8c3ec30-... state=removed waited_by=demo-script-after-DeleteVolume evidence=demo/poll.log
blockvolume-deploy:sw-blockvolume-pvc-e8c3ec30-...-r1 namespace=default state=deleted deleted_by=pvc-owner-ref-or-demo-guard evidence=demo/blockvolume-namespace-pods-deploys.after-delete.txt
iscsi-session:iqn.2026-05.io.seaweedfs:pvc-e8c3ec30-... state=absent released_by=csi-node-unstage evidence=demo/iscsi-sessions.after-delete.txt
iscsi-node-db:iqn.2026-05.io.seaweedfs:pvc-e8c3ec30-... state=present_before_guardrail cleaned_by=testops-guardrail evidence=iscsi-nodes.after-demo.txt
testops-guardrail:pre_clean state=enabled cleans=stale-processes,stale-sessions,stale-nvme evidence=runner-phase-pre_clean
testops-guardrail:collect_and_cleanup state=enabled cleans=stale-processes,stale-sessions,stale-iscsi-node-db evidence=runner-phase-collect_and_cleanup
non_claim:operator-grade-reconciliation state=not_claimed
non_claim:multi-node-or-HA-lifecycle state=not_claimed
non_claim:upgrade-or-uninstall state=not_claimed
```

Every resource class has `state=` and `by=` and `evidence=`. Non-claims are
explicit. Product cleanup vs guardrail cleanup is distinguishable line by
line.

### HG-8 scoped cleanup commands — PASS

`docs/quickstart-kubernetes.md` Cleanup section uses
`kubectl -n default delete pod sw-block-demo-writer sw-block-demo-reader ...`
and `kubectl -n default delete deploy -l app=sw-blockvolume ...`. The
previous `-A` global sweep is removed from user-facing cleanup, with an
explicit warning:

> *"Do not use global cleanup commands such as
> `kubectl delete deploy -A -l app=sw-blockvolume` in a shared cluster.
> Broad sweeps are TestOps guardrails, not user-facing cleanup."*

### HG-9 idempotency / retry — PASS

Dev's run `20260511-204348-ccc4` exercises the retry chain. Verified
artifacts:

- `partial/run.log` contains `[app-demo] keeping resources for retry validation`
  (proves the first attempt was kept in a partial state intentionally).
- `retry_cleanup/` shows `[alpha-uninstall] PASS: seaweed-block alpha stack uninstall requested`
  between attempts.
- `retry_demo/run.log` contains
  `[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete`
  — the retry attempt succeeds after the cleanup.

The gate's three retry cases were specified as (1) run install, kill it
mid-way, re-run; (2) run install successfully, run again immediately;
(3) run install with a stale PVC of the same name still present. The
scenario primarily exercises (1) and the cleanup-before-retry pathway
covers the stale-state-clears branch of (3). Case (2) "run again
immediately after successful run" is implicitly covered by the runner-native
chain pre_clean + happy-path flow, but is not a separate explicit fixture.
Not a blocker; non-blocking note 3 below.

Note: HG-9 evidence is dev-produced, not QA-rerun, per the user's
instruction to use existing evidence. The scenario YAML
(`testops/scenarios/light-use-first-volume-retry-chain.yaml`) was reviewed
by QA and its assertions match the gate property under test.

### HG-10 ≥3 break classes — PASS

Dev's run `20260511-210727-5cb9` exercises all three required fixtures
from the gate. Verified artifacts:

```text
fixture 1: host preflight failure
  scenario phase: break_preflight_missing_iscsiadm
  fixture:        SW_BLOCK_PREFLIGHT_FORCE_MISSING=iscsiadm
  evidence:       preflight-missing-iscsiadm/preflight.txt contains
                  "checked name=iscsiadm status=FAIL" and
                  "Install open-iscsi/iscsiadm"
  exit_code:      2 (asserted)

fixture 2: mid-install image failure
  scenario phase: break_mid_install_bad_image
  fixture:        SW_BLOCK_IMAGE=ghcr.io/seaweedfs/does-not-exist:no-such-tag
  evidence:       image-failure/controlled-stop.txt =
                    "ops-status-unavailable: no volume id reached"
                  image-failure/image-pull-evidence.txt has
                    "ImagePullBackOff" + the bad image tag
  exit_code:      non-zero (asserted)

fixture 3: mid-app attach failure
  scenario phase: break_mid_app_attach
  fixture:        SW_BLOCK_DEMO_BREAK_AFTER_BLOCKVOLUME_READY=delete-generated-blockvolume
                  (scoped: deletes only the generated blockvolume Deployment
                  by label, not a broad sweep)
  evidence:       attach-failure/controlled-stop.txt =
                    "ops-status-collected: ... exit_code=1"
                  attach-failure/ops-status/ops-status-bundle.json present:
                    {"schema_version":"1.0","command":"sw-block ops status",
                     "volume_id":"pvc-5257b247-...","exit_code":1,
                     "status":"unhealthy"}
  exit_code:      non-zero (asserted)
```

All three fixtures produce either an `sw-block ops status` bundle that a
stranger can triage, or the documented `ops-status-unavailable` branch.

Note: HG-10 evidence is dev-produced, not QA-rerun, per the user's
instruction to use existing evidence. The scenario YAML
(`testops/scenarios/light-use-first-volume-breaks-chain.yaml`) was reviewed
by QA and the fixture commands match the gate exactly. The
`delete-generated-blockvolume` break is scoped by label, not a broad
namespace sweep — addresses prior reviewer concern about cleanup blast
radius.

### HG-11 claims match what was tested — PASS

`docs/quickstart-kubernetes.md` "Current Alpha Limitations" lists all
seven gate-required non-claims explicitly:

```text
- pod-local non-durable state
- failover-while-mounted is not claimed
- single-node Kubernetes
- NVMe-oF is not part of this alpha path
- Operator-grade reconciliation is not claimed
- Upgrade and uninstall safety are not claimed
- Performance numbers from this demo are not a product SLO
```

The success line `[app-demo] PASS: app pod wrote data, replacement app pod
read it back through the same PVC, cleanup complete` is precise — it does
not imply durability, HA, multi-node, or operator semantics by accident.

### HG-12 runbook vs scenario consistency — PASS (with documented residue)

QA-owned back-to-back runs on identical source (`ec76385`):

```text
manual:   /tmp/sw-block-app-demo-20260512T055732Z  (PVC pvc-34895575-...)
scenario: 20260511-225935-0ff4                    (PVC pvc-e8c3ec30-...)
```

Inside the demo artifact directory, `diff` returned empty for the file
list (39 files match). Final PASS line is byte-identical. Writer/reader
`/data/demo.bin: OK` matches in both. `iscsi-sessions.after-delete.txt`
is byte-identical. Resource names (`sw-block-demo-pvc`,
`sw-block-demo-writer`, `sw-block-demo-reader`,
`sw-blockvolume-<volume-id>-r1` in `default`) are identical.

Documented residue difference: after running the runbook's documented
`bash scripts/uninstall-k8s-alpha.sh`, the iSCSI node-DB entry remains and
`/var/lib/sw-block/` keeps stale per-PVC paths. The runbook discloses both:
*"A non-active iSCSI node database entry may remain until guardrail
cleanup removes it"* and the cleanup note that some alpha/test paths can
leave persistent blockvolume state under `/var/lib/sw-block/`. Scenario's
`collect_and_cleanup` phase uses the TestOps
guardrail to clean them; that boundary is named in `cleanup-attribution.txt`
as `cleaned_by=testops-guardrail`. The runbook's prose and the scenario's
attribution describe the same boundary in different surfaces.

## Residue audit

After QA's HG-12 scenario run terminated:

```text
iSCSI sessions:                            none ("iscsiadm: No active sessions.")
iSCSI node DB:                             cleaned by TestOps guardrail in
                                           collect_and_cleanup; "iscsiadm: No records found"
                                           after final guardrail step.
NVMe subsystems:                           none (scenario nvme_disconnect=true
                                           on cleanup paths)
blockmaster/blockvolume/blockcsi processes: none (verified by pgrep)
Kubernetes sw-block resources:             none (verified by
                                           `kubectl get all -A | grep sw-block`)
kubectl port-forward leftovers:            none (scenario explicitly pkills
                                           in pre_clean and collect_and_cleanup)
/var/lib/sw-block storage paths:           not cleaned by the alpha uninstall
                                           script. Disclosed in the Cleanup
                                           section as clean-lab state, and in
                                           limitations as part of "upgrade
                                           and uninstall safety not claimed."
                                           Persists across runs; TestOps
                                           guardrail clears it between scenario
                                           runs but a runbook-only user must
                                           clean it manually if they need a
                                           clean slate.
```

## Blocking findings

None.

## Non-blocking findings

1. **HG-2 boundary check after `apply-k8s-alpha-blockvolumes.sh` is missing.**
   The "Use Your Own App" path now elevates the script to a numbered step,
   but the gate's strict pass condition also asked for a boundary
   verification command after that step. Suggest adding a one-liner like
   `kubectl get deploy -n default -l app=sw-blockvolume -o wide` and the
   expected `Ready` column to the runbook.

2. **HG-4 during-run kubectl boundary commands are not enumerated.**
   The runbook's "Boundary checks" section is post-run, against the artifact
   directory. A user who is stuck mid-run has no list of per-state
   `kubectl get ...` commands with expected output lines. Acceptable for
   the demo's batch-evidence model, but worth a future doc pass.

3. **HG-9 third retry case is implicit, not explicit.**
   The gate listed three retry fixtures: (1) Ctrl-C and retry, (2) double
   run, (3) stale PVC of same name. The retry chain explicitly covers (1)
   and the cleanup-before-retry pathway covers the stale-state-clears
   branch. Case (2) "successful run, run again immediately" is not a named
   phase but is implicitly covered by the normal happy-path scenario plus
   pre_clean idempotency. Not a blocker; could be one additional retry
   phase in a future pass.

4. **`/var/lib/sw-block` is not cleaned by `uninstall-k8s-alpha.sh`.**
   Stale per-PVC paths can accumulate across runs for a runbook-only user.
   This is now named in the Cleanup section as clean-lab state, not normal
   user cleanup.

5. **`iscsi-sessions.after-reader.txt` exists in both runs.**
   This is now listed in the First-Volume Evidence Ladder artifact table.

None of these are close blockers under the gate. Items 4 and 5 were resolved
before staging this close report; items 1-3 remain future doc/scope polish.

## Provenance of evidence

| Run id | Purpose | Owner |
|---|---|---|
| `sw-block-app-demo-20260512T055732Z` | HG-12 manual cold runbook follow | QA |
| `20260511-225935-0ff4` | HG-12 / HG-7 scenario back-to-back | QA |
| `20260511-203619-885f` | HG-6 failure-bundle slice verification | QA |
| `20260511-200544-dca5` | Happy-path live validation | Dev (scenario YAML reviewed by QA) |
| `20260511-204348-ccc4` | HG-9 retry chain | Dev (scenario YAML reviewed by QA) |
| `20260511-210727-5cb9` | HG-10 break classes | Dev (scenario YAML reviewed by QA) |

Per the user's instruction to use existing evidence, the dev-owned runs
above were spot-checked by QA against their scenario YAMLs and against the
gate fixtures; QA did not personally re-execute those scenarios. The
underlying YAMLs and the artifact contents both line up with the gate
properties.

## Close recommendation

PASS — the plan is clear to move from `current-plan.md` to
`finished-plans/`. The remaining non-blocking findings should be tracked as
follow-up doc polish but do not gate the close.

## Dev Strict-Gate Follow-Up

After this report, QA re-read
`internal/docs/qa-assignments/light-use-mvp-close-hard-gate.md` as a strict
binary gate and found three gaps:

```text
HG-2: apply-k8s-alpha-blockvolumes was elevated but lacked its own boundary check.
HG-4: the runbook lacked per-boundary commands with exact expected lines.
HG-9: "successful run, run again immediately" was implicit, not a named fixture.
```

Dev addressed those gaps after the initial report:

- `docs/quickstart-kubernetes.md` now includes a verified
  `apply-k8s-alpha-blockvolumes.sh` boundary check with expected `1/1` output.
- `docs/quickstart-kubernetes.md` now includes direct per-boundary commands and
  expected output lines for CSI controller, CSI node, StorageClass, PVC,
  generated Deployment, writer, reader, delete, and residue.
- `testops/scenarios/light-use-first-volume-retry-chain.yaml` now includes a
  named `rerun_after_success_no_cleanup` phase.

Strict close should be re-issued by QA after validating those three deltas.
