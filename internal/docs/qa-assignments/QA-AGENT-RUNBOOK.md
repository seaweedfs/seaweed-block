# QA Agent Runbook — seaweed_block phase gates

A self-contained runbook for an agent running a **live lab gate** on the m01/m02
k3s lab and delivering a verdict. You should be able to follow this end-to-end
without asking for more context. Read it top to bottom the first time; after that
use the [cheat sheet](#11-cheat-sheet).

> **Unified TestOps context:** before running block gates as a reusable test
> agent, read the platform docs in
> `C:\work\seaweedfs\learn\sw-test-runner-standalone\docs\`:
>
> 1. `unified-testops.md` — global platform overview and 9099 dashboard/submit model.
> 2. `wiki/submitting.md` — developer submit and scenario authoring workflow.
> 3. `control-plane-product-contract.md` — common envelope, suite, worker, and product rows.
> 4. `qa-bundle-assert.md` — `qa-assert.sh` acceptance contract.
>
> This file is the **block phase-gate specialization** of that platform. If the
> Unified TestOps docs and this file disagree, follow this file for current
> seaweed_block phase gates and report the doc drift.

> **Your job:** the dev pushes a phase's commits + a gate, you run the **live**
> gate on the lab and report **PASS / FAIL / PARTIAL** with the exact terminal
> evidence. You do **not** edit product code. The dev's `go test` / `helm lint` /
> `swblock validate` are *their* pre-checks; you run the thing they can't:
> the real cluster.

---

## 0. The loop (7 steps)

```
understand → pre-flight (SYNC product_root!) → deploy (only if needed)
   → run gate → collect evidence → clean up residue → deliver report
```

Most phase gates are **pure RBAC `kubectl auth can-i` gates** — no image, no
deploy, ~10s. A minority are **full-install gates** (build images → helm install →
PVC → assert → uninstall), ~10–20 min. Step 0 tells you which.

---

## 1. What you're running

| Thing | Where | Shape |
|---|---|---|
| **Scenario** | `testops/scenarios/<gate>-chain.yaml` | YAML the runner executes; ends in `assert_*` on terminal-evidence lines |
| **Gate script** | `scripts/run-phaseNN-<slug>-gate.sh` | runs on m02, writes `…-summary.txt` with `key=value` evidence lines |
| **Runner** | `C:\work\swblock.exe` | drives the scenario; SSHes to the lab nodes |
| **Evidence** | `/mnt/smb/work/share/g15d-k8s/<run_id>-<slug>/…-summary.txt` | the `key=value` lines the scenario asserts |
| **Sign-off** | `internal/docs/qa-assignments/phaseNN-dX-<slug>-qa-signoff.md` | your deliverable |

The contract is **terminal evidence**: the gate script writes a final
`…_status=ok` line *only* if every internal check matched its expected value, and
the scenario asserts that line (plus a few key boundary lines). Your verdict is
mechanical: did the scenario PASS and does the summary say `…_status=ok`? Report
the actual lines — never paraphrase or infer intent (that's how "semantic loops"
start).

---

## 2. Environment & access

| Host | IP | Role |
|---|---|---|
| **m02** | 192.168.1.184 | k3s **control-plane** (v1.34.4+k3s1, VAP-capable), Docker build host, SeaweedFS/block. Most gates run here. |
| **m01** | 192.168.1.181 | second k3s node + RDMA/kernel client. |

- **SSH:** user `testdev`, key `C:\work\dev_server\testdev_key`
  (Git Bash: `/c/work/dev_server/testdev_key`).
  `ssh -i <key> testdev@192.168.1.184`
- **kubectl/helm on m02:** work as `testdev` with **no sudo** (k3s.yaml is mode
  644). `ssh m02 'kubectl get nodes'` should show both nodes `Ready`.
- **SMB share:** `//192.168.1.34/Work` = `V:\share` (Windows) = `/mnt/smb/work/share`
  (Linux). Gate artifacts go under `/mnt/smb/work/share/g15d-k8s/`.
- **Repo:** `C:\work\seaweed_block` is the **Windows checkout** (shared tree — the
  dev edits it directly; do not `git fetch && checkout` to "sync", just read it).
  It is **not** on m02 by default.
- **Runner:** `C:\work\swblock.exe`.

---

## 3. Step 1 — Understand the gate (before touching the lab)

1. `git -C C:\work\seaweed_block log --oneline -6` — confirm the phase commits are
   present and you're on the right branch.
2. Read `testops/scenarios/<gate>-chain.yaml` — note the `node`, the `exec` that
   runs the gate script, and **every `assert_*` line** (those are your PASS
   criteria).
3. Read `scripts/run-phaseNN-<slug>-gate.sh` — note:
   - does it **build/deploy an image** or is it **kubectl-only** (`auth can-i`,
     `apply` CRDs/RBAC)? → tells you if Step 4 (deploy) applies.
   - what `key=value` lines it writes to the summary, and the **final
     `…_status=ok`** line.
   - its **cleanup trap** — what it removes on exit (and what it leaves; see the
     CRD gotcha in §8).

You should be able to state, before running: *"PASS = scenario all-green AND
summary contains `…_status=ok` plus these N boundary lines: …"*.

---

## 4. Step 2 — Pre-flight the lab

```bash
KEY=/c/work/dev_server/testdev_key; M2=testdev@192.168.1.184
# k3s health
ssh -i $KEY $M2 'kubectl get nodes'                       # both Ready
# disk (k3s GCs :local images above ~85%)
ssh -i $KEY $M2 'df -h / | tail -1'                       # want < 80%
# clean baseline (no leftover install)
ssh -i $KEY $M2 'helm list -A | grep -i sw-block; kubectl get crd | grep -i swblock; \
  kubectl get pods,pvc,pv,ns -A | grep -iE "sw-block|seaweed-block"'
```

If there's residue, see §8 (clean it first — a clean baseline avoids false
failures and CRD-schema skew).

### ⚠️ THE #1 GOTCHA: sync product_root to m02

Gate scripts run on m02 from **`product_root=/tmp/seaweed_block`**, but the repo
is only on Windows and `/tmp/seaweed_block` is **usually stale or missing**. Sync
the phase tree the gate needs **before** running:

```bash
KEY=/c/work/dev_server/testdev_key; M2=testdev@192.168.1.184; R=/c/work/seaweed_block
ssh -i $KEY $M2 'mkdir -p /tmp/seaweed_block/scripts /tmp/seaweed_block/charts/seaweed-block/crds'
scp -i $KEY $R/scripts/run-phaseNN-<slug>-gate.sh        $M2:/tmp/seaweed_block/scripts/
scp -i $KEY $R/charts/seaweed-block/crds/*.yaml          $M2:/tmp/seaweed_block/charts/seaweed-block/crds/
```

For a full-install gate, sync the whole `charts/` + `scripts/` (or rsync the
repo). Verify the script + CRDs landed before Step 4.

---

## 5. Step 3 — Deploy (full-install gates only)

Skip entirely for `auth can-i` / admission gates (they only `kubectl apply` CRDs
+ RBAC themselves). For gates that install the product:

1. **Images.** Published images often *predate* a new subcommand, so for a phase
   gate build fresh from the phase commit on m02:
   `bash scripts/build-alpha-images.sh` → `sw-block:local` + `sw-block-csi:local`.
   **Import gap:** confirm both images exist on the node that runs the pods
   (`ssh m02 'docker images | grep sw-block'`); a build that isn't imported to
   m02's k3s → `ErrImageNeverPull`.
2. **Helm values.** `bash scripts/generate-helm-values-day1.sh <product_root>`
   (env: `SW_BLOCK_ACTIVATION_IMAGE_MODE=local|published`, `SW_BLOCK_IMAGE`,
   `SW_BLOCK_CSI_IMAGE`) → `helm_values_status=ok` + a values file.
3. **Install.** `helm install sw-block charts/seaweed-block -n kube-system -f <values>`.

For the **v0.5 release smoke** specifically: use the **published v0.5 dual
images at the same commit** (`ghcr.io/seaweedfs/seaweed-block:<v0.5>` +
`…-csi:<same>`). **Do not** use `sha-dc2972d0059b` — that's a v0.4 image.

---

## 6. Step 4 — Run the gate

```powershell
C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results `
  C:\work\seaweed_block\testops\scenarios\<gate>-chain.yaml
```

- **Flag order matters:** all flags (`-results-dir`, `-env`) must come **before**
  the scenario path — Go's flag parser stops at the first non-flag arg.
- **Windows/MSYS:** if you pass `-env x=/tmp/…` from Git Bash it gets mangled to a
  `C:\…` path. Prefer running `swblock.exe` from **PowerShell**, or prefix
  `MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'`. Most gates need no `-env` (the
  scenario carries its own `env:` defaults).
- The runner SSHes to the node itself; you do not run the script by hand.
- Watch for `=== <scenario> === PASS (…)` and `N actions: N passed, 0 failed`.

---

## 7. Step 5 — Collect evidence

```bash
ssh -i $KEY $M2 'cat /mnt/smb/work/share/g15d-k8s/<run_id>-<slug>/<slug>-summary.txt'
```

Capture the **full summary** — every `key=value` line, especially the final
`…_status=ok` and the boundary lines the scenario asserted. The runner also wrote
a bundle under `-results-dir` (`manifest.json`, `result.html`, collected
`artifacts/`). Quote the actual lines in your report.

---

## 8. Step 6 — Clean up residue

The lab is shared — leave it as clean as you found it (and clean for the next
gate / the v0.5 smoke).

- **Known gap:** most gate cleanup traps remove the namespace/RBAC/CRs they
  created but **NOT the CRDs they `kubectl apply`** (helm has the same `crds/`
  behavior). After a gate, check and remove leftover **empty** CRDs:
  ```bash
  ssh -i $KEY $M2 'kubectl get crd | grep -i swblock; \
    kubectl get swblockvolumes,swblockreplicaeligibilities -A --no-headers 2>/dev/null | grep -vc "No resources"'
  # if 0 CRs under them, safe to delete:
  ssh -i $KEY $M2 'kubectl delete crd <name1> <name2>'
  ```
  `scripts/verify-helm-cleanup.sh` reports `cleanup_status=ok` but **does not
  check CRDs/CRs** — don't rely on it alone for CRD residue.
- For full-install gates: `helm uninstall` then
  `bash scripts/verify-helm-cleanup.sh` (want `cleanup_status=ok`, all
  `*_residue_count=0`). The `testops/scenarios/cleanup-residue-chain.yaml`
  scenario is the canonical residue sweep.
- **Scoped only.** Never bare `pkill -f weed` or blanket-delete — you'll kill the
  dev's work. Before deleting anything you didn't create, check it has **no
  finalizers and no CRs** and there's **no active helm release/pods** referencing
  it; if it looks like live dev state, **surface it, don't delete it**.

---

## 9. Step 7 — Deliver the report

Write `internal/docs/qa-assignments/phaseNN-dX-<slug>-qa-signoff.md`. Verdict is
one of:

| Verdict | Meaning |
|---|---|
| **PASS** | scenario all-green + summary `…_status=ok` + the asserted boundary lines match. The phase step is verified; dev can proceed. |
| **FAIL** | an assertion failed / `…_status` ≠ ok. Quote the failing line(s) + actual value. Blocks the phase. |
| **PARTIAL** | couldn't fully verify. You **must classify** the cause: **artifact** (bad/missing image, wrong tag), **lab** (k3s/disk/sync/env), or **product** (the code's behavior). Only "product" blocks; artifact/lab you (or the dev) fix and re-run. |

Include: verdict; the exact run command; the **full summary `key=value` lines**;
which scenario assertions passed; any residue you cleaned; gotchas hit (e.g. had
to sync product_root); the commit SHA + branch tested. Keep interpretation
minimal — lead with the evidence.

---

## 10. Gotchas (hard-won)

1. **product_root `/tmp/seaweed_block` on m02 is stale** → sync the phase
   scripts + CRDs before running (§4). The #1 cause of a confusing first failure.
2. **Gate cleanup traps leave CRDs** → clean empty CRDs post-gate (§8).
3. **`verify-helm-cleanup.sh` doesn't check CRDs/CRs** → check them separately.
4. **Published images predate new subcommands** → build `sw-block:local` from the
   phase commit for full-install gates; `:alpha`/`sha-dc2972d0059b` are v0.4 and
   only carry `blockmaster`/`blockvolume` (no `sw-block` CLI).
5. **`-env` before the scenario path**; **MSYS mangles `/tmp` values** (run from
   PowerShell or `MSYS_NO_PATHCONV=1`).
6. **m02 disk > ~85%** → k3s garbage-collects `:local` images mid-install. Keep it
   under 80%; a weekly janitor on m01/m02 prunes docker + stale `/tmp` residue.
7. **VAP enforcement lags policy creation by seconds** → admission gates must wait
   for propagation (the gate scripts already probe-until-denied; don't shortcut).
8. **Shared tree** — the dev edits `C:\work\seaweed_block` directly; never delete
   what looks like active dev state (finalizers, CRs, a live helm release).

---

## 11. Cheat sheet

```bash
KEY=/c/work/dev_server/testdev_key; M2=testdev@192.168.1.184; R=/c/work/seaweed_block

# 1. understand
git -C "$R" log --oneline -6
#   read: testops/scenarios/<gate>-chain.yaml  +  scripts/run-phaseNN-<slug>-gate.sh

# 2. pre-flight + SYNC product_root
ssh -i $KEY $M2 'kubectl get nodes; df -h / | tail -1; helm list -A | grep -i sw-block; kubectl get crd | grep -i swblock'
ssh -i $KEY $M2 'mkdir -p /tmp/seaweed_block/scripts /tmp/seaweed_block/charts/seaweed-block/crds'
scp -i $KEY $R/scripts/run-phaseNN-<slug>-gate.sh $M2:/tmp/seaweed_block/scripts/
scp -i $KEY $R/charts/seaweed-block/crds/*.yaml   $M2:/tmp/seaweed_block/charts/seaweed-block/crds/

# 3. run (PowerShell)
#   C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results C:\work\seaweed_block\testops\scenarios\<gate>-chain.yaml

# 4. evidence
ssh -i $KEY $M2 'cat /mnt/smb/work/share/g15d-k8s/<run_id>-<slug>/<slug>-summary.txt'

# 5. clean residue (empty CRDs the trap left)
ssh -i $KEY $M2 'kubectl get crd | grep -i swblock; kubectl get swblockvolumes,swblockreplicaeligibilities -A --no-headers 2>/dev/null'
ssh -i $KEY $M2 'kubectl delete crd <name1> <name2>'   # only if 0 CRs

# 6. write internal/docs/qa-assignments/phaseNN-dX-<slug>-qa-signoff.md  (PASS/FAIL/PARTIAL)
```

### Worked example — Phase 54 D3 (pure RBAC gate)

```
scenario: testops/scenarios/authority-executor-target-rbac-chain.yaml
script:   scripts/run-phase54-authority-executor-target-rbac-gate.sh
PASS criteria: summary has phase54_authority_executor_target_rbac_status=ok and:
  exec_patch_swblockreplicaeligibilities_status_allowed=yes   (executor CAN write the ACK target)
  exec_patch_swblockreplicaeligibilities_main_denied=no       (denied the main object)
  default_patch_swblockreplicaeligibilities_status_denied=no  (non-executor CANNOT write it)
result: PASS, 14/14 actions, ~10s. Synced product_root first; cleaned 2 leftover CRDs after.
```
