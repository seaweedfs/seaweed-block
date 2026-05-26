# QA Sign-off - Minimal Release New-User Validation

Verdict: **PASS** for the user-facing happy path; **one blocking finding**
on the documented cleanup procedure (B1 below) — must be fixed before
release, OR docs must be updated to include the verifier step.

Date: 2026-05-26 (UTC)

Validated source commit: `08070fa docs: close phase32 status surface`
Pinned release image: `sha-6260e46fd3be` (per README + quickstart).

## Scope

Strict literal new-user validation: a user follows the documented
quickstart commands from a clean Kubernetes lab and ends with a clean
lab. No exploratory deviation.

## 8-Step Walkthrough

| # | Step | Result |
|---|---|---|
| 1 | Lab pre-state clean | PASS (no helm release, no iSCSI sessions, no multipath, no dmsetup, no sw-block pods) |
| 2 | Follow documented quickstart literally | PASS (every command from the user-facing docs) |
| 3 | `sw-block ops generate-helm-values` with pinned SHA | PASS (`helm_values_status=ok`) |
| 4 | `helm install` | PASS (4.5s) |
| 5 | First PVC + writer/reader via `run-basic-app-example.sh` | PASS (34.9s; `writer_verified=true, reader_verified=true, cleanup_status=ok`) |
| 6 | Report bundle produces all 5 required artifacts | PASS (all 5 present) |
| 7 | Status surface verification | PASS (all 7 sub-checks) |
| 8 | Uninstall + cleanup residue | **PARTIAL** (5 of 6 checks clean; iSCSI node DB record remains) |

## Step 6: Required Report Artifacts

All 5 artifacts present in
`/tmp/sw-block-basic-app-20260526T025122Z/status/report/`:

```text
PRESENT summary.txt
PRESENT index.html
PRESENT cluster-evidence.json
PRESENT operator-snapshot.json
PRESENT timeline.jsonl
```

## Step 7: Status Surface

`summary.txt`:

```text
managed_volume=pvc-6b853f31-0c1b-4ddd-9595-85e797688c04 status=ready reason=first_volume_verified
managed_volume_condition=Ready status=True reason=first_volume_verified severity=info
read_only=true
```

`operator-snapshot.json`:

```text
"read_only": true
"mutation_allowed": false
"ready_volume_count": 1
"reason_code": "first_volume_verified"
"type": "Ready", "status": "True"
"mode": "read_only"
"mutation_allowed": false   (per-action)
```

Dashboard `/operator-snapshot.json`:

```text
GET /operator-snapshot.json -> HTTP 200
  read_only=true, mutation_allowed=false, ready_volume_count=1
POST/PUT/PATCH/DELETE -> 405
```

| Check | Result |
|---|---|
| one ManagedVolume exists | PASS |
| `Ready=True` | PASS |
| reason is `first_volume_verified` | PASS |
| `read_only=true` | PASS |
| `mutation_allowed=false` | PASS |
| dashboard `/operator-snapshot.json` HTTP 200 | PASS |
| POST/PUT/PATCH/DELETE return 405 | PASS |

## Step 8: Uninstall + Cleanup

Documented user commands:

```bash
helm uninstall sw-block --namespace kube-system
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

Post-cleanup audit:

| Check | Result |
|---|---|
| no helm release | PASS (`NONE`) |
| no sw-block pods/deployments | PASS (`NONE`) |
| no iSCSI sessions | PASS (`No active sessions`) |
| **no iSCSI node DB records matching `io.seaweedfs`** | **FAIL** (1 record remained) |
| no multipath maps | PASS (empty) |
| no dmsetup devices | PASS (`No devices found`) |
| no per-host product processes (m01/m02/tp01) | PASS |

The single failing check: an iSCSI node DB record for the test PVC's IQN
persisted on m01 after the documented uninstall:

```text
192.168.1.181:3260,1 iqn.2026-05.io.seaweedfs:pvc-6b853f31-0c1b-4ddd-9595-85e797688c04
```

`scripts/verify-helm-cleanup.sh` correctly catches this:

```text
cleanup_status=failed
iscsi_residue_count=1
failure=iscsi_node_records_present
```

After manually scrubbing with
`iscsiadm -m node ... -o delete`, the verifier returns
`cleanup_status=ok` with all 5 residue counts = 0.

## Blocking Findings

### B1: Documented quickstart cleanup does NOT scrub iSCSI node DB records

**What the user does (per documented quickstart §Cleanup):**

```bash
helm uninstall sw-block --namespace kube-system
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

**What gets cleaned:**
- helm release
- k8s pods/deploys/SC/CSI
- multipath maps (none generated in this scenario)
- dmsetup devices
- iSCSI active sessions
- per-host product processes

**What does NOT get cleaned:**
- iSCSI node DB records under `/var/lib/iscsi/nodes/<portal>/<iqn>/...`

`scripts/uninstall-k8s-alpha.sh:92-94` only LOGS `iscsiadm -m session` and
does not call `iscsiadm -m node -o delete` for sw-block IQNs.

The cleanup verifier `scripts/verify-helm-cleanup.sh` correctly flags this
as `failure=iscsi_node_records_present`. The testops `cleanup-residue-chain.yaml`
scenario uses this verifier and would PASS only after the iSCSI node DB
scrub is performed (which the testops scenarios do as part of their
`pre_run_cleanup` action, separately from the user-facing path).

A fresh user following the documented quickstart literally ends with a
stale node DB record on m01. This is not a security or data-loss issue,
but it:

1. Violates the "no residue" claim in the Phase 28/29 cleanup contract.
2. Causes the next `helm install` to potentially reattach to a stale
   target reference.
3. Surfaces inconsistency: the documented cleanup says "clean", but the
   cleanup verifier built into the same repo says "failed".

**Fix shape (pick one)**:

1. Extend `scripts/uninstall-k8s-alpha.sh` to scrub matching iSCSI node DB
   records:

   ```bash
   if command -v iscsiadm >/dev/null 2>&1; then
     sudo iscsiadm -m node 2>/dev/null | awk '/io.seaweedfs/ {print $1, $2}' \
       | while read portal target; do
         sudo iscsiadm -m node -T "$target" -p "$portal" -o delete || true
       done
   fi
   ```

2. Document `bash scripts/verify-helm-cleanup.sh` as the canonical
   post-uninstall verification in `docs/quickstart-kubernetes.md` §Cleanup,
   and have it report `cleanup_status=ok` before the user considers the
   lab clean.

Option 1 is invisible-to-user (cleanup just works) and preferred.
Option 2 also adds the residue-counter clarity that operators want.
Both can land together.

**Severity**: blocking for the strict "minimal release new-user validation"
because the documented user-facing flow leaves residue that the
project's own verifier classifies as failure.

## Non-Blocking Findings

### N1: `sw-block --version` reports `revision=unknown modified=unknown`

After `go build -o sw-block ./cmd/sw-block`, the binary's `--version`
output is `sw-block revision=unknown modified=unknown`. This was flagged
in Phase 28 as a follow-up; it returned. Worth restoring the
`-ldflags "-X main.revision=$(git rev-parse --short HEAD)"` build flag in
the documented build step, OR adding the build flag to the user-facing
`Step 0 - Build The CLI` snippet so users get the right output when they
follow the docs literally.

Not blocking because the binary works correctly — only the revision label
is missing.

## Optional Extended Scenarios

Not run in this minimal release pass. Coverage already established in
prior phase sign-offs:

| Scenario | Coverage |
|---|---|
| Multi-volume RF3 smoke | Phase 27/31 strict PASS; Phase 32 D6 PASS |
| Restart persistence | Phase 31 D3/D4/D5 strict PASS; Phase 32 D5 PASS |
| Negative status (CSI image pull) | Phase 28 G3 + Phase 32 D4 PASS |
| Stale evidence replay | Phase 32 D7 PASS |

The release candidate's HA / restart / negative / stale-evidence claims
are independently QA-validated through those phase sign-offs. Only B1
above is unresolved.

## Verdict

**Conditional PASS** on the minimal release new-user validation.

Update: B1 fix is now implemented in `scripts/uninstall-k8s-alpha.sh` by
scrubbing `io.seaweedfs` iSCSI node DB records and recording before/after
evidence. QA recheck assignment:
`internal/docs/qa-assignments/release-minimal-new-user-cleanup-recheck-assignment.md`.
Final release verdict remains conditional until QA confirms Step 8 strict PASS.

The product itself works correctly: a new user can go from a clean
cluster to a verified writer/reader PVC and read-only operator-snapshot
following only documented commands.

The cleanup path had one documented-flow gap (B1) where
`uninstall-k8s-alpha.sh` doesn't scrub the iSCSI node DB records that
the project's own `verify-helm-cleanup.sh` rejects. This is a small
fix and has been addressed in the script. The minimal release test becomes
strict PASS after QA confirms Step 8 recheck.

### Recommended release sequence

1. Re-run only Step 8 (cleanup audit) to confirm zero residue from the
   documented flow.
2. Optionally land N1 (build with ldflags so `sw-block --version` is
   informative).
3. Mark release.

Lab final state after this sign-off (post manual scrub): fully clean.
