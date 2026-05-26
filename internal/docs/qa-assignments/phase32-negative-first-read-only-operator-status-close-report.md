# Phase 32 Close Report - Negative-First Read-Only Operator Status Surface

Date: 2026-05-25

Verdict: **PASS (strict)**. Phase 32 is closed at 100%.

## Product Claim

Seaweed Block now has a read-only Kubernetes operations status surface that
projects ManagedVolume evidence into consistent report, dashboard,
operator-snapshot, Condition, Event, and support-bundle vocabulary.

The status surface is negative-first:

- Ready is only published when current evidence supports Ready.
- Blocked, Unknown, CleanupRequired, and EvidenceStale states are explicit.
- Report/dashboard/operator-snapshot surfaces use the same reason codes.
- Read-only and dry-run actions are visible; mutating storage actions are not
  enabled.

## Claim Boundary

This phase is an operations/status foundation. It does not ship a mutating
operator.

Non-claims:

- No promote, repair, rebuild, failback, delete, or live cleanup mutation.
- No backup/snapshot/restore.
- No NVMe ANA feature work.
- No production SLO or broad compatibility matrix.
- No controller-manager lifecycle ownership beyond the read-only alpha status
  contract.

## Hard-Gate Results

| Gate | Evidence | Result |
|---|---|---|
| D1 negative-first contract | `internal/docs/ref/phase32-negative-first-operator-status-plan.md` | PASS |
| D1a TestOps validation layer | scenario inventory, negative-status review, runner backlog, failure snapshot standard | PASS |
| D2 CRD / Condition / Event contract | `phase32-d2-crd-condition-event-qa-signoff.md`, `go test ./core/ops ./cmd/sw-block` | PASS |
| D3 happy-path status projection | QA `20260525-141234-8a89`, 34/34 PASS | PASS |
| D4 blocked / negative projection | QA `20260525-141341-9c7d`, 38/38 PASS | PASS |
| D5 restart / promotion status | QA `20260525-143121-3aaa`, 34/34 PASS | PASS |
| D6 multi-volume status isolation | QA `20260525-143400-e085`, 36/36 PASS; stronger interleaved `20260525-143747-f93a`, 56/56 PASS | PASS |
| D7 stale evidence replay | QA `20260525-172250-bf28`, replay precedence fixed | PASS |
| D8 close artifacts | this report + finished plan + roadmap update | PASS |

## Key Evidence

D3 happy path:

```text
managed_volume status=ready reason=first_volume_verified
Ready=True reason=first_volume_verified
read_only=true
dashboard /operator-snapshot.json HTTP 200
POST/PUT/PATCH/DELETE = 405
```

D4 blocked path:

```text
managed_volume status=blocked reason=csi_node_image_pull_failed
Ready=False
Blocked=True
safe_k8s.import_csi_image mode=dry_run
mutation_allowed=false
```

D5 restart/promotion:

```text
restart_promotion_status=ok
before_restart_primary=r2
after_restart_primary=r2
post_restart_primary_count=1
reader checksum after restart passes
```

D6 multi-volume:

```text
managed_volume_count=3
reader_verified_count=3
cross_volume_authority_mixup=false
duplicate_publish_target_for_distinct_volume=false
operator-snapshot has 3 distinct volumes
```

D7 stale evidence:

```text
old replay: r1@m01 frontend=192.168.1.181:3260
fixed replay: r2@m02 frontend=192.168.1.184:3260
Ready=Unknown while freshest post-restart snapshot reconverges
no false Ready=True
```

## What Shipped

- Alpha CRD / Condition / Event contract for read-only operator status:
  - `Ready`,
  - `Blocked`,
  - `Recovering`,
  - `Recovered`,
  - `CleanupRequired`,
  - `EvidenceStale`.
- `EvidenceStale` ManagedVolume projection:
  - `status=unknown`,
  - `reason_code=evidence_stale`,
  - `Ready=Unknown`,
  - `EvidenceStale=True`,
  - Kubernetes Event severity maps to Warning.
- Surface-agreement tests for:
  - happy first-volume path,
  - blocked CSI image-pull path,
  - restart/promotion path,
  - multi-volume status isolation.
- Bundle replay hardening:
  - `cluster-after-restart.json` is a first-class cluster-evidence candidate,
  - newest `captured_at` wins before path rank,
  - stale pre-promotion replay no longer masks newer post-restart evidence.
- QA/TestOps reference docs:
  - scenario inventory,
  - negative-status evidence review,
  - runner action backlog addendum,
  - failure snapshot standard.

## Blocking Findings

None.

## Non-Blocking Follow-Ups

- D7 can surface `Ready=Unknown` during the immediate post-restart
  reconvergence window. Future work can add a bounded refresh probe or a short
  scenario settle wait before post-restart report capture.
- Runner-native scenarios still need stronger primitives for JSONPath wait,
  completed pods, Helm install/uninstall, and product report capture.
- A future mutating operator must remain blocked until executor policy,
  invariants, and rollback/fencing evidence are explicitly gated.

## Recommendation

Close Phase 32. Next work should build on this read-only status foundation
rather than adding mutating operator behavior first.
