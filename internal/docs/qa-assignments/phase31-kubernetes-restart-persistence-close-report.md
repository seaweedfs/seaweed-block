# Phase 31 Close Report - Kubernetes Restart Persistence

Date: 2026-05-25

Verdict: **PASS (strict)**. Phase 31 is closed at 100%.

## Product Claim

Seaweed Block can run in a durable Helm hostPath mode where Kubernetes/product
restart does not forget block data, volume authority, promoted primary, publish
target, epoch, or per-volume ManagedVolume state.

Claim boundary:

- This is restart persistence for the same Kubernetes cluster and same host
  storage.
- It is not fresh-cluster restore, backup/snapshot/restore, returned-replica
  rebuild/failback, host disk loss survival, or broad production SLO.

## Hard-Gate Results

| Gate | Scenario / artifact | Result |
|---|---|---|
| D1 restart persistence contract | `phase31-kubernetes-restart-persistence-contract.md` | PASS |
| D2 durable Helm values / install contract | `go test ./cmd/sw-block ./core/launcher`, `helm lint charts/seaweed-block` | PASS |
| D3 single-node restart | `helm-single-node-restart-persistence-chain.yaml` QA `20260525-104016-f3d4` | 40/40 PASS |
| D4 RF3 promotion restart | `helm-rf3-promotion-restart-persistence-chain.yaml` QA `20260525-122723-f7ed` | 34/34 PASS |
| D5 multi-volume RF3 restart | `helm-multi-volume-rf3-restart-smoke-chain.yaml` QA `20260525-123233-541b` | 36/36 PASS |
| D6 close artifacts | this report + finished plan | PASS |

## Key Evidence

D3 single-node:

- Same PVC/PV survives k3s restart.
- Reader verifies `/data/demo.bin: OK` after restart.
- ManagedVolume remains `status=ready`.
- Cleanup residue counters are all zero.

D4 RF3 promoted authority:

```text
restart_promotion_status=ok
before_restart_primary=r2
after_restart_primary=r2
before_restart_publish_target=192.168.1.184:3260
after_restart_publish_target=192.168.1.184:3260
before_restart_epoch=2
after_restart_epoch=2
post_restart_primary_count=1
reason=authority_persisted
```

D5 multi-volume RF3:

```text
multi_volume_restart_status=ok
before_volume_count=3
after_volume_count=3
managed_volume_count=3
reader_verified_count=3
duplicate_publish_target_for_distinct_volume=false
cross_volume_authority_mixup=false
cleanup_status=ok
```

## Fixes Closed During The Phase

- Added explicit durable restart values generation through
  `sw-block ops generate-helm-values --restart-persistence hostpath`.
- Added Helm chart schema/values support for restart persistence and
  blockmaster state hostPath.
- Added a root `state-permissions` initContainer for blockmaster hostPath
  writes.
- Added single-node, RF3 promotion, and RF3 multi-volume restart gates.
- Hardened D4 against post-restart port-forward races by selecting a
  Running/Ready blockmaster pod and retrying the product `ops cluster` call.
- Hardened D4 pre-clean against dirty shared-lab starts by deleting and
  refusing leftover generated blockvolume Deployments.

## QA Sign-Offs

- `internal/docs/qa-assignments/phase31-restart-persistence-d3-qa-signoff.md`
- `internal/docs/qa-assignments/phase31-restart-persistence-d4-qa-signoff.md`
- `internal/docs/qa-assignments/phase31-restart-persistence-d5-qa-signoff.md`

## Blocking Findings

None.

## Non-Blocking Follow-Ups

- Document durable restart mode in user-facing quickstart/release notes before
  presenting it as a supported beta path.
- Preserve the narrow non-claim: restart persistence is not backup/restore or
  fresh-cluster disaster recovery.
- Keep Phase 27 measured stale-primary fencing as the stronger fencing evidence
  source; Phase 31 D4 proves the promoted authority does not roll back after
  restart.

## Recommendation

Close Phase 31 and move to Phase 32: read-only operator / Kubernetes status
surface.
