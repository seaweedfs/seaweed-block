# QA Runbook — Current Block TestOps Flow (v1)

Use this when QA needs to validate **seaweed-block** work on the M02 k3s lab.

This is the block-specific sibling of the RDMA `QA-RUNBOOK-rdma-current-v2.md`.
The *flow* is the same four ideas — standard gate, dirty-adhoc DEBUG-only, deep
runbook for specialty failures, post-run provenance assertion — but the scenario,
project, evidence rows, and failure classes are block's, not RDMA's.

> For the **mechanics** of running a phase gate (env access, syncing
> `product_root`, terminal-evidence discipline, PASS/FAIL/PARTIAL), see
> [`QA-AGENT-RUNBOOK.md`](QA-AGENT-RUNBOOK.md). This runbook is the higher-level
> *which path / accept criteria / provenance* SOP.

> **Block ≠ RDMA in one important way:** there is **no 9099 submit-queue/worker**
> for block (that is RDMA-specific). A block gate is run by the QA agent with
> `swblock.exe` directly: sync `product_root` → `swblock run` → read the dashboard
> bundle. Do not pretend a `POST /api/rdma/submit` runs block.

## 0. What To Run

Pick exactly one path.

| Path | Use When | Result Can Be Used For |
| --- | --- | --- |
| Standard gate | A reviewable branch / commit (pushed, no dirty source) | QA verdict / phase or release evidence |
| Adhoc dirty debug | Developer has a local uncommitted `sw-block` build | Debug only, **not** QA ACCEPT |
| Deep runbook | A standard gate fails in/near a specialty area (SmartWAL, iSCSI/NVMe OS, residue) | Specialty QA evidence |

## 1. Standard Gate — Reviewable Branch/Commit

This is the normal process.

### Inputs

- A **pushed** seaweed-block branch or commit SHA. No local dirty source, no
  hand-copied binary.
- The **scenario** the dev assigned: a phase gate
  (`testops/scenarios/<gate>-chain.yaml` driven by
  `scripts/run-phaseNN-<slug>-gate.sh`) **or** the v0.5 release smoke.

### Images

Most phase gates run product code, so build from the reviewable commit (published
images often predate a new subcommand):

```bash
# on m02, from the synced product_root at that commit:
bash scripts/build-alpha-images.sh        # -> sw-block:local + sw-block-csi:local
ssh testdev@192.168.1.184 'docker images | grep sw-block'   # confirm BOTH exist on the node that runs pods
```

Pure RBAC / admission gates need **no image** (they only `kubectl apply` CRDs +
RBAC and run `auth can-i`). The **v0.5 release smoke** uses the **published v0.5
dual images at the same commit** (`ghcr.io/seaweedfs/seaweed-block:<v0.5>` +
`…-csi:<same>`) — **never** `sha-dc2972d0059b` (that is a v0.4 image).

### Sync product_root (the #1 gotcha)

Gate scripts run on m02 from `/tmp/seaweed_block`, which is usually stale. Sync the
phase tree first (details in [`QA-AGENT-RUNBOOK.md` §4](QA-AGENT-RUNBOOK.md)):

```bash
KEY=/c/work/dev_server/testdev_key; M2=testdev@192.168.1.184; R=/c/work/seaweed_block
ssh -i $KEY $M2 'mkdir -p /tmp/seaweed_block/scripts /tmp/seaweed_block/charts/seaweed-block/crds'
scp -i $KEY $R/scripts/run-phaseNN-<slug>-gate.sh $M2:/tmp/seaweed_block/scripts/
scp -i $KEY $R/charts/seaweed-block/crds/*.yaml   $M2:/tmp/seaweed_block/charts/seaweed-block/crds/
```

### Run

Run `swblock.exe` from a clean checkout at the reviewable commit and tag the run
so the dashboard ties it to the commit:

```powershell
C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results `
  -env product_root=/tmp/seaweed_block `
  C:\work\seaweed_block\testops\scenarios\<gate>-chain.yaml
```

For a release-smoke / helm scenario, also pass metadata (mirrors the RDMA
`-meta`):

```text
-meta project=block-qa -meta team=block -meta run_by=<qa-agent>
-meta test_id=<gate> -meta branch=<branch> -meta commit=<sha>
```

> Flags must come **before** the scenario path (Go stdlib flag parser). On Git
> Bash, `MSYS_NO_PATHCONV=1` to keep `/tmp/...` values intact; PowerShell avoids it.

### What It Runs

The assigned gate. Block gates fall into two evidence shapes:

- **Correctness / boundary gates** (most phase gates): the gate script writes a
  `…-summary.txt` with `key=value` lines and a final `<gate>_status=ok`; the
  scenario `grep_log` + `save_as` + `assert_*` those into `result.json` `vars`.
- **Functional / release gates** (helm first-volume, e2e, HA, v0.5 smoke): PVC
  writer/reader PASS, CSI→CR, finalizer add/release, operator-status
  `Ready=True/first_volume_verified`, zero-residue uninstall.

### Where To Watch

```text
http://192.168.1.181:9099/?project=block-qa
```

The dashboard re-scans on each load; the bundle appears under
`/mnt/smb/work/share/testops/results/block-qa/<run-id>/`.

### Post-Run Provenance Assertion

Do this before writing ACCEPT. It proves the bundle tested the commit QA ran and
that the gate's own terminal evidence passed.

> **Where the evidence lives (validated):** `result.json` `vars` holds the
> scenario's **`save_as` outputs**, which for `grep_log` gates are **counts** —
> `"1"` means the asserted line was present. So `EXPECT_VARS` uses the **save_as
> names**, e.g. `phase54_rbac_ok=1`, **not** the literal `…_status=ok` line (that
> line lives in the gate's `…-summary.txt` on SMB, not in the bundle). Leave
> `EXPECT_VARS` empty for functional gates that assert entirely in-scenario.

```bash
ssh testdev@192.168.1.184   # or m01; the bundle is on shared SMB

BUNDLE=/mnt/smb/work/share/testops/results/block-qa/<run-id>
COMMIT='<reviewed-branch-or-sha>'    # the value passed as -meta commit=<sha>
EXPECT_VARS='phase54_rbac_ok=1,exec_target_status_allowed=1,exec_target_main_denied=1,default_target_status_denied=1'
export BUNDLE COMMIT EXPECT_VARS

python3 - <<'PY'
import json, os, pathlib
b = pathlib.Path(os.environ['BUNDLE']); requested = os.environ['COMMIT']
required = ['result.json','status.json','result.html','scenario.yaml','manifest.json']
missing = [n for n in required if not (b/n).is_file()]
if missing: raise SystemExit('missing bundle files: ' + ', '.join(missing))
result = json.loads((b/'result.json').read_text())
status = json.loads((b/'status.json').read_text())
manifest = json.loads((b/'manifest.json').read_text())
meta = manifest.get('metadata', {}) or {}
vars = result.get('vars', {})
gitsha = manifest.get('git_sha','')
commit_ok = requested in (meta.get('commit',''), meta.get('branch',''), gitsha) \
            or (gitsha and gitsha.startswith(requested)) or (requested and requested.startswith(gitsha) if gitsha else False)
checks = {
  'result.status': result.get('status') == 'PASS',
  'status.state':  status.get('state') == 'pass',
  'commit_match':  bool(commit_ok),
}
for ev in [e for e in os.environ.get('EXPECT_VARS','').split(',') if e]:
    k, _, want = ev.partition('=')
    checks['evidence:'+ev] = (str(vars.get(k)) == want)
failed = [n for n, ok in checks.items() if not ok]
if failed: raise SystemExit('failed checks: ' + ', '.join(failed))
print('BLOCK_QA_BUNDLE_ASSERT_OK')
print('scenario=' + manifest.get('scenario_name',''))
print('commit=' + (meta.get('commit') or gitsha))
print('git_sha=' + gitsha)
print('run_id=' + manifest.get('run_id',''))
PY
```

> **`-meta commit=<sha>` is REQUIRED (validated).** A block bundle's
> `manifest.git_sha` is the **runner-cwd** repo's HEAD (e.g. it was the
> seaweedfs/learn repo, not seaweed_block, on an ad-hoc run) and `metadata` is
> null unless you pass `-meta`. So `commit_match` is only meaningful if you pass
> `-meta commit=<seaweed_block sha>` (and run `swblock` from a clean seaweed_block
> checkout). For a branch, resolve it first: `git -C C:\work\seaweed_block
> rev-parse <branch>` and pass that SHA.

### ACCEPT Criteria

ACCEPT only if:

1. The dashboard run is `pass` (`status.state == pass`, `result.status == PASS`).
2. The bundle has `result.html` and the assertion prints
   `BLOCK_QA_BUNDLE_ASSERT_OK`.
3. The tested commit is the reviewed branch/SHA (`commit_match`).
4. The gate's terminal evidence is present (the `<gate>_status=ok` + boundary
   lines, or the functional asserts: PVC RW, CSI→CR, finalizer add/release,
   operator-status Ready, zero-residue).
5. Any perf floors in a perf scenario pass.
6. The lab is left clean (no leftover CRDs/release/PVC — see §4 / `QA-AGENT-RUNBOOK §8`).

Report:

```text
Verdict: ACCEPT or REJECT (or PARTIAL — classify artifact/lab/product)
scenario: <gate>-chain.yaml
run: block-qa/<run-id>
commit: <branch-or-sha>
bundle: /mnt/smb/work/share/testops/results/block-qa/<run-id>
dashboard: http://192.168.1.181:9099/?project=block-qa
terminal evidence:
  <gate>_status=ok
  <key boundary / functional lines>
perf rows (if perf gate):
  read_iops / write_iops / p99_ms / MiB/s ...
first failure, if any: ...
residue cleaned: <CRDs/none>
not tested: ...
```

## 2. Adhoc Dirty Debug — Local `sw-block` Build

Allowed for investigation, **not** PR/release evidence.

### Rules

- Must use `project=block-dev`, not `block-qa`.
- Must mark metadata `dirty=true` and name the source/binary/image tested
  (e.g. `sw-block:local` built from an uncommitted tree).
- Must **not** produce an ACCEPT verdict.
- If the result matters, ask the dev to commit it and rerun the standard gate.

```powershell
C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results\block-dev `
  -env product_root=/tmp/seaweed_block `
  -meta project=block-dev -meta team=block -meta run_by=<qa-agent> `
  -meta test_id=adhoc-block-debug -meta branch=dirty-local -meta commit=dirty-local -meta dirty=true `
  C:\work\seaweed_block\testops\scenarios\<gate>-chain.yaml
```

### Report Format

```text
Verdict: DEBUG ONLY, not QA ACCEPT
reason: dirty local source / uncommitted image
source tested: <path / image tag>
run: block-dev/<run-id>
result: pass/fail
finding: ...
required next step: commit branch/SHA and rerun standard gate
```

## 3. Deep Runbooks — Specialty Paths

Use a deep path only when a standard gate fails in/near it, or the dev asks for a
focused root-cause run. They are expensive and narrow; do not start here for
normal validation.

| Area | Deep path |
| --- | --- |
| SmartWAL corruption / recovery | `testops/scenarios/helm-smartwal-corrupt-restart-chain.yaml` + the phase34-d4 SmartWAL corrupt sign-offs |
| iSCSI OS-initiator / ALUA MPIO | `scripts/run-iscsi-alua-*`, `run-iscsi-os-smoke.sh`; iscsi-p6 ALUA sign-offs |
| NVMe OS / multipath | `scripts/run-nvme-*-smoke.sh`, `run-nvme-multipath-smoke.sh`; nvme-p4 sign-offs |
| Helm support bundle | `testops/scenarios/helm-support-bundle-diagnostics-chain.yaml`, `scripts/collect-helm-support-bundle.sh` |
| Cleanup / residue | `scripts/verify-helm-cleanup.sh` (`cleanup_status=ok`), `testops/scenarios/cleanup-residue-chain.yaml` |

## 4. Failure Handling

When a standard gate fails:

1. Open `result.html`; find the first failed phase/action.
2. Check the action output + `artifacts/` + the gate `…-summary.txt`.
3. Check `kubectl`/pod logs only if the bundle is missing/incomplete.
4. Do **not** delete the failed bundle; do **not** rerun-until-green without
   explaining the first failure.

Classify:

| Failure Type | Next Step |
| --- | --- |
| **Image / artifact** (e.g. published image missing a new flag — the Phase 40 D6 class) | Send back to releng; do **not** call it a product bug. Re-run on a fresh build. |
| Build/link | Send back to dev with compiler output |
| Scenario bug | Fix the TestOps scenario, rerun |
| Perf regression | Compare iops/latency/throughput rows against the last accepted run |
| Correctness (PVC RW, CSI→CR, finalizer, operator-status, SHA) | Use a deep runbook or add a focused scenario |
| **Lab / environment** | `product_root` stale, CRD residue, m02 disk > ~85% (k3s GCs `:local`), VAP propagation lag → mark infra-blocked with host/log evidence |

A **PARTIAL** verdict must say which of these it is (artifact / lab / product).
Only **product** blocks the phase; artifact/lab are fixed and re-run.

## 5. Non-Claims

A green standard gate does **not** prove:

- multi-node failover / replica-policy behavior beyond the scenario's scope;
- long soak / restart-persistence stability (use the HA / restart-persistence
  chains);
- every iSCSI/NVMe OS-initiator combination (deep runbooks cover those);
- production performance under real workloads (perf gates are fixed shapes);
- backup / NVMe / rebuild surfaces that are still deferred.

If one of those is the claim under review, ask for a focused scenario.

## 6. QA Feedback Requested

When reviewing this v1 runbook, please answer:

1. Is the standard-gate path clear enough to run without asking the dev which
   scenario/image to use?
2. Is the `block-qa` vs `block-dev` (accept vs debug) boundary strict enough?
3. Does the provenance assertion's `EXPECT_VARS` mechanism cover both the
   correctness/boundary gates and the functional/release gates, or should
   functional gates get their own assertion keys?
4. Should the block provenance assertion ship as a committed
   `scripts/block-qa-assert.sh <run-id> <commit> [EXPECT_VARS]` (so QA does not
   paste python)?
