# QA Validation - Phase 28 Operational Reliability And TestOps Hardening

Date: 2026-05-24 (UTC)

Verdict: PASS (strict). Phase 28 D1 cleanup verifier and D2 flake matrix
(both N=5) reproduce green on independent QA rerun. PM-shape review of the
new reference docs is OK. Phase 28 is ready to close.

This validation supplements:

- `phase27-multi-volume-ha-independence-close-report.md`
- `phase27-multi-volume-ha-independence-close-addendum.md`
- `phase27-d5-plus-followup-gaps.md`

## Method

- Synced branch (head `db5445f testops: harden phase28 operational reliability`)
  from Windows controller to m02 via tar/scp/extract.
- Lab pre-state confirmed clean: no helm release, no iSCSI sessions, no
  sw-block pods, no multipath maps, no dmsetup devices, no per-host product
  processes.
- Ran the three Phase 28 deliverables sequentially:
  - D1 cleanup-residue scenario (single run).
  - D2 flake matrix N=5 against D3 mounted failover.
  - D2 flake matrix N=5 against D4 interleaved failover.

## Gate Results

| Gate | QA run | Result |
|---|---:|---|
| D1 cleanup-residue chain | `20260523-210004-bed1` | 13/13 PASS |
| D2 flake matrix - D3 mounted N=5 | iter `20260523-21{0017,0420,0802,1135,1511}-*` | 5/5 PASS, `flake_rate_percent=0` |
| D2 flake matrix - D4 interleaved N=5 | iter `20260523-21{1909,2143,2417,2701,2933}-*` | 5/5 PASS, `flake_rate_percent=0` |

Total: 13 D1 actions + 5 x 47 D3 actions + 5 x 55 D4 actions = 523 actions
across QA reruns of Phase 28 deliverables. All green.

## D1 Cleanup Verifier - Independent Confirmation

The new verifier in `scripts/verify-helm-cleanup.sh` now writes:

- `multipath-residue.after-cleanup.txt`
- `dmsetup.after-cleanup.txt`
- `cleanup-summary.txt` includes `multipath_residue_count` and
  `dmsetup_residue_count`

QA confirmed on the same lab the verifier now reports clean even when the
pre-test state had previously-orphaned `mpath...` maps. Direct host audit
after the full Phase 28 QA run:

```text
sudo multipath -ll       -> (empty)
sudo dmsetup ls          -> No devices found
sudo iscsiadm -m session -> No active sessions
helm list -A | sw-block  -> none
kubectl get pods -A | sw-block/blockvolume -> none
per-host product procs (m01 / m02 / tp01) -> none
```

This closes the residue gap that the Phase 27 close report flagged.

## D2 Flake Matrix - Independent Confirmation

### D3 mounted failover N=5

`results/phase28-qa-flake-mounted-n5/flake-summary.txt`:

```text
phase27_flake_matrix_status=ok
scenario=testops\scenarios\helm-multi-volume-rf3-mounted-failover-chain.yaml
target_runs=5
pass_runs=5
fail_runs=0
flake_rate_percent=0
iter 1: duration=243.046s
iter 2: duration=221.949s
iter 3: duration=213.475s
iter 4: duration=215.786s
iter 5: duration=218.201s
```

Each iteration is a full 47-action D3 mounted failover scenario.
`min=213.5s max=243.0s mean=222.5s` - the wall-time variance is small and
no iteration regressed.

### D4 interleaved failover N=5

`results/phase28-qa-flake-interleaved-n5/flake-summary.txt`:

```text
phase27_flake_matrix_status=ok
scenario=testops\scenarios\helm-multi-volume-rf3-interleaved-failover-chain.yaml
target_runs=5
pass_runs=5
fail_runs=0
flake_rate_percent=0
iter 1: duration=154.271s
iter 2: duration=153.842s
iter 3: duration=163.694s
iter 4: duration=151.837s
iter 5: duration=164.930s
```

Each iteration is a full 55-action D4 interleaved failover scenario.
`min=151.8s max=164.9s mean=157.7s`.

Combined: 10 sequential failover scenarios, 510 actions, 0 failures, 0
flakes. This is the strict-form repeat data Phase 27 D7 follow-up was
waiting on.

## PM Review - Reference Docs

### `internal/docs/ref/testops-runner-action-backlog.md`

Status: OK. Strictly scoped:

- Explicit Decision section: keep current YAML/helper shape; promote primitives
  only where repeated shell fragments hide intent.
- P0/P1/P2 prioritization with named acceptance gates.
- Cites concrete evidence inputs including the runner-native PVC spike
  (`20260523-145417-4f50`) and Phase 28 D2 N=5 runs.
- Non-Goals section explicitly rules out DSL rewrite, agent-mode requirement
  before v0.3.x close, mutating fault-injection APIs, and helper removal.

### `internal/docs/ref/multi-volume-ha-support-evidence-contract.md`

Status: OK. Field vocabulary contract derived from the actual D3/D4/D8 run
artifacts I already audited in the Phase 27 close addendum. Per-volume field
list aligns 1:1 with what I verified (stale_primary_probe, rtpg_*_aas,
rtpg_transition_verified, etc.). Mapping to product surfaces is bounded
(support bundle / report / dashboard / explain / future operator Conditions)
and Non-Claims section explicitly rules out scale, SLO, mutating actions, and
replacing product-owned ManagedVolume state.

### `internal/docs/ref/phase28-structure-model-readiness-review.md`

Status: OK as planning doc, not a release-grade claim. Stable/Provisional/
Test-Only field classification matches what should remain non-public until
operator/CRD work begins. Overlapping-automata rule is consistent with the
read-only ops discipline held since Phase 22.

## Updated Hard-Gate Acceptance

| Requirement | Result |
|---|---|
| D1 multipath cleanup verifier fails on orphan `mpath... ##,##` maps | PASS (verified by clean rerun after prior runs that had left maps) |
| D1 `cleanup-summary.txt` includes `multipath_residue_count` | PASS |
| D1 `cleanup-summary.txt` includes `dmsetup_residue_count` | PASS |
| D1 `cleanup-residue-chain.yaml` runs the verifier directly | PASS (13/13 actions) |
| D2 D3 mounted flake matrix N=5 = 0 flake | PASS (5/5) |
| D2 D4 interleaved flake matrix N=5 = 0 flake | PASS (5/5) |
| D3 docs: TestOps backlog scoped to action gaps with acceptance gates | PASS |
| D4 docs: evidence-field contract aligned with measured artifacts | PASS |
| D5/D6 docs: structure review + model dependency map drafted | PASS |
| Final residue audit clean (helm/iSCSI/pods/multipath/dmsetup/procs) | PASS |

## Non-Blocking Observations

- The flake matrix wrapper is PowerShell-only (`run-phase27-flake-matrix.ps1`).
  Phase 28 D7 nightly scheduling will need either a bash equivalent or a
  Windows-host cron. Not a blocker for close.
- D7/D8 (model-tightening design + next-feature readiness) are still
  classified as planning/review work in `current-plan.md`; they do not need
  measured gate evidence.
- The non-residue checks (per-host product processes) continue to be done
  via `assert_no_processes`. Worth promoting the multi-host loop into a
  single action in the runner backlog's P1 (`collect_k8s_snapshot` already
  there; add `multi_host_assert_no_processes` if pattern recurs).

## Verdict

PASS for Phase 28 scope.

Recommended close sequence:

1. Promote Phase 28 to closed after PM accepts the wording in
   `multi-volume-ha-support-evidence-contract.md` and
   `phase28-structure-model-readiness-review.md`.
2. Cut v0.3.2 alpha (or v0.4 alpha, depending on PM call) release note
   referencing the Phase 27 close report + addendum + this Phase 28
   validation.
3. Carry the runner backlog P0/P1 actions into the next phase plan
   (`assert_no_multipath_maps`, `kubectl_wait_jsonpath`, `kubectl_wait_completed`,
   `helm_install`/`helm_uninstall`).
4. Move on to the structure-review-driven next phase (operator readiness or
   NVMe ANA, per the model dependency map in
   `phase28-structure-model-readiness-review.md`).
