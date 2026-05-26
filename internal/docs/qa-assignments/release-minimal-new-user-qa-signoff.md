# QA Sign-off - Minimal Release New-User Validation

Verdict: **PASS (strict)** after B1 fix landed in `7dd99a6 scripts: scrub
iscsi node records on uninstall`. Original cycle was conditional PASS
with B1 blocking on cleanup; B1 RESOLVED on QA recheck.

Date: 2026-05-26 (UTC)

Validated source commits:
- Original cycle: `08070fa docs: close phase32 status surface`
- B1 recheck: `7dd99a6 scripts: scrub iscsi node records on uninstall`

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

### B1 RESOLUTION (2026-05-26 recheck)

Fixed in commit `7dd99a6 scripts: scrub iscsi node records on uninstall`.
QA recheck on synced tree:

**Step 8 reproduction**: helm install + first-volume run (sha-6260e46fd3be)
created an iSCSI session that left a node DB record at:

```text
192.168.1.181:3260,1 iqn.2026-05.io.seaweedfs:pvc-b628b7d9-b856-42a9-a1b0-a755bc0cc22e
```

**Documented user-facing cleanup** (literal commands, unchanged):

```bash
helm uninstall sw-block --namespace kube-system
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

The uninstall script now logs:

```text
[alpha-uninstall] delete stale Seaweed Block iSCSI node records
```

and produces three new evidence artifacts in `$SW_BLOCK_ARTIFACT_DIR`:

| Artifact | Content |
|---|---|
| `iscsi-nodes.before-scrub.txt` | `192.168.1.181:3260,1 iqn.2026-05.io.seaweedfs:pvc-b628b7d9-...` |
| `delete-iscsi-node-records.log` | `portal=192.168.1.181:3260,1 target=iqn.2026-05.io.seaweedfs:pvc-b628b7d9-...` (per-record delete) |
| `iscsi-nodes.after-scrub.txt` | `iscsiadm: No records found` |

`scripts/verify-helm-cleanup.sh` on the post-uninstall lab:

```text
cleanup_status=ok
helm_release=sw-block
helm_namespace=kube-system
iqn_substr=io.seaweedfs
k8s_residue_count=0
iscsi_residue_count=0          ← was 1 in original cycle
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

Negative iscsiadm check:

```text
sudo iscsiadm -m node | grep io.seaweedfs   →   no match (PASS)
```

Unit test coverage: `scripts/uninstall_k8s_alpha_test.go` added in the
same commit; `go test ./scripts ./core/ops ./cmd/sw-block` PASS.

**B1: RESOLVED.**

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

**All four optional scenarios run in this release pass** (initially I had
deferred them to prior phase sign-offs; user requested explicit release-cycle
runs).

| Scenario | QA run / probe | Result |
|---|---:|---|
| Multi-volume RF3 smoke (`helm-multi-volume-rf3-restart-smoke-chain.yaml`) | `20260525-195935-a213` | **36/36 PASS** |
| Restart persistence single-node (`helm-single-node-restart-persistence-chain.yaml`) | `20260525-200344-b193` | **40/40 PASS** |
| Negative status / CSI image pull (`helm-support-bundle-diagnostics-chain.yaml`) | `20260525-200605-2e76` | **38/38 PASS** |
| Stale evidence replay (D7 precedence fix) | `sw-block ops report --from-bundle` against the Phase 32 D5 RF3 promotion-restart bundle `20260525-172250-bf28-helm-rf3-promotion-restart` | **PASS** |

Total: 114/114 actions across the three scenarios + 1 manual replay
confirmation.

### Per-scenario hard-claim summary

- **Multi-volume RF3 smoke**: `managed_volume_count=3`,
  `reader_verified_count=3`, `duplicate_publish_target_for_distinct_volume=false`,
  `cross_volume_authority_mixup=false`, three distinct `volume_id`/`pvc_name`
  entries in operator-snapshot, cleanup_status=ok.

- **Restart persistence (RF=1 single-node, hostPath)**:
  `restart_persistence_status=ok`, same PVC + same volume_id pre/post
  restart, reader checksum after restart `/data/demo.bin: OK`,
  ManagedVolume Ready=True post-restart, per-run scoped hostPath cleaned
  on teardown.

- **Negative status (synthetic blocked bundle)**: `Ready=False
  reason=csi_node_image_pull_failed`, `Blocked=True
  reason=csi_node_image_pull_failed`, action
  `safe_k8s.import_csi_image mode=dry_run`, no `Ready=True` anywhere in
  the blocked path, operator-snapshot `blocked_volume_count=1
  ready_volume_count=0`.

- **Stale evidence replay (D7 precedence fix)**: regenerating the report
  against an RF3 promotion-restart bundle that contains BOTH pre-promotion
  `cluster-evidence.json` AND post-restart `cluster-after-restart.json`
  correctly picks the freshest snapshot.
  - restart-promotion truth: `before==after primary=r2`,
    `publish_target=192.168.1.184:3260`.
  - regenerated summary: `volume=pvc-9724a9c6-... primary=r2@m02
    frontend=192.168.1.184:3260` (post-restart, not stale).
  - status correctly shows `Ready=Unknown reason=unknown severity=info`
    because the post-restart snapshot caught a reconverging moment - the
    negative-first rule holds (no false `Ready=True` against transient
    evidence).

### Cross-cutting observation: testops scenarios scrub iSCSI node DB; user flow does not

The optional scenarios all finished with **zero iSCSI node DB residue**
because their `pre_run_cleanup` and `helm_uninstall_cleanup` phases
explicitly call `iscsiadm -m node -o delete` for `io.seaweedfs` IQNs.
This is the cleanup path the testops framework uses internally.

The documented user-facing cleanup (Step 8) does NOT call this. That is
the B1 finding above. The optional-scenario PASS results do NOT mitigate
B1 - they confirm the test infrastructure cleans properly, while the
user-facing flow does not.

## Verdict (final, post-B1-recheck)

**Minimal release new-user validation: PASS (strict).**

All three blockers / partial checks from the original cycle now PASS on
the synced post-fix tree:

- Step 8 cleanup audit: zero residue across all 6 dimensions.
- B1 RESOLVED on QA recheck (see B1 §RESOLUTION above).
- 4 optional extended scenarios PASS (114/114 actions + replay).

The release candidate's HA / restart / negative / stale-evidence claims
are now independently QA-validated **in this release cycle**, not only
through prior phase sign-offs. B1 is resolved.

The product itself works correctly: a new user can go from a clean
cluster to a verified writer/reader PVC and read-only operator-snapshot
following only documented commands.

### Recommended release sequence

1. Optionally land N1 (build with ldflags so `sw-block --version` is
   informative).
2. Mark release.

Lab final state after this sign-off: fully clean.
