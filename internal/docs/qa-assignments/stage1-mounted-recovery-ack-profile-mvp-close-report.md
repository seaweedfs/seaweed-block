# QA Close — Stage 1 Mounted Recovery ACK Profile MVP

Formal close report against `internal/docs/current-plan.md`
("Stage 1 Mounted Recovery ACK Profile MVP", currently at 96% complete per
dev's status; this report covers QA validation of the substantive
deliverables D3 (RF=2 best-effort controlled recovery) and D4-extension
(RF=3 sync-quorum durable recovery)).

```text
Verdict:         PASS (strict) — both ACK profile claims delivered end to end with
                  reader-checksum recovery, distinct claim_profile labels, and
                  control-plane timeline evidence.

Product commit:  shared working tree at HEAD (post-merge plan) + dev's RF=3 quorum fix
                  (core/replication/peer.go SeedLiveShipCursor + volume.go barrier gate)
Runner commit:   sw-test-runner-standalone @ 6ec7abd (swblock 15.9 MB Windows)
Host/lab:        m02 (192.168.1.184) — Ubuntu 24.04.3 LTS / k3s v1.34.4+k3s1

RF=2 best-effort recovery run:  20260513-220439-8337  (PASS, 7/7 phases, 75/75 actions, with control-plane timeline)
RF=3 sync-quorum recovery run:  20260514-075221-da0d  (PASS, 7/7 phases, 75/75 actions, post-promotion errors absent)
```

## ACK profile claim split — both branches delivered

The plan's product split holds in the artifacts:

| ACK profile | Claim profile | What it claims | Run |
|---|---|---|---|
| `best-effort` | `controlled-best-effort-demo` | Controlled recovery demo, NOT quorum HA | `20260513-220439-8337` |
| `sync-quorum` | `beta-recovery` | Durable writable HA target with promotion-evidence gating | `20260514-075221-da0d` |

The product's primary-failure bundle and control-plane timeline both carry the
explicit `claim_profile=...` and `ack_profile=...` strings, so a reviewer can
tell at a glance which class of recovery happened.

## RF=2 best-effort controlled recovery — PASS

Already reported in detail at the prior turn. Key headline fields from
`primary-failure-recovery.txt`:

```text
failover_status: promotion_pending → promoted
ack_profile: best-effort
claim_profile=controlled-best-effort-demo
required_frontier_lsn=44 candidate_frontier_lsn=44 frontier_covered=true
data_check_after_failover=pending_reader → reader_checksum_passed
```

Control-plane timeline produced all six required events (`primary_observed`,
`candidate_evaluated`, `primary_failure_injected`, `authority_published`,
`csi_reattach_observed`, `data_check`).

## RF=3 sync-quorum durable recovery — PASS (the production-HA target)

This is the substantive new claim. Full evidence from
`primary-failure-recovery.txt` (run `20260514-075221-da0d`):

```text
failover_status: promotion_pending → promoted
ack_profile: sync-quorum                         ← production HA profile, not demo
claim_profile=beta-recovery                      ← production HA claim label
failure_class=primary-blockvolume-controlled-stop
before_primary_replica=r1
promotion_candidate_replica=r2
failed_replica=r1                                ← derived from live inventory, equals before_primary_replica
required_frontier=pvc-eb27904b-...=44
candidate_ready=true
candidate_promotion_evidence=promotion: ... replica=r2 candidate_ready=true reason=promotion_ready
                                claim_profile=beta-recovery ack_profile=sync-quorum
                                required_frontier_known=true required_frontier_lsn=44
                                candidate_frontier_known=true candidate_frontier_lsn=44
                                frontier_covered=true
target_deployment=deployment.apps/sw-blockvolume-pvc-eb27904b-...-r1
target_ready_replicas=0
post_failure_primary_count=1                     ← exactly one primary after the move, no dual-primary
frontend_primary_ready_issue_count=0             ← no contradictory "primary not ready" issues
after_primary_replica=r2
promoted_replica=r2
data_check_after_failover=reader_checksum_passed
reader_verified=true
```

### Writer + reader checksum

```text
writer.log "/data/demo.bin: OK"  : 1 match  (pre-failure write succeeded)
reader.log "/data/demo.bin: OK"  : 1 match  (post-promotion reader verified)
```

### CSI re-staged on promoted replica's frontend

From `blockcsi-node.log`:

```text
[14:53:19] NodeStageVolume: ... staged transport=iscsi portal=127.0.0.1:3260  (r1, pre-failure primary)
[14:53:56] NodeStageVolume: ... staged transport=iscsi portal=127.0.0.1:3261  (r2, post-promotion primary)
```

CSI observed the authority move and re-staged the volume against the new
primary's frontend address. That's the "method=pod-recreate" path the
timeline names.

### Control-plane timeline — all six events fire in order

```text
event=primary_observed              replica=r1 evidence=replica: ... role=primary epoch=1 ...
event=candidate_evaluated           replica=r2 candidate_ready=true reason=promotion_ready
                                    claim_profile=beta-recovery ack_profile=sync-quorum frontier_covered=true
event=primary_failure_injected      replica=r1 deployment=deployment.apps/...-r1 failure_class=primary-blockvolume-controlled-stop
event=authority_published           from=r1 to=r2 primary=r2 primary_count=1
                                    evidence=replica: ... replica=r2 role=primary epoch=2 endpoint_version=1
event=csi_reattach_observed         reader_pod=sw-block-demo-reader method=pod-recreate
event=data_check                    reader_verified=true result=reader_checksum_passed
```

Note the `authority_published` event explicitly carries `primary_count=1` — direct evidence that the
promotion did not produce a transient dual-primary state.

### Dev's RF=3 quorum-fix claim verified — post-promotion errors are absent

Searched all logs in the run's artifact tree for the three signatures dev called out
as previously appearing post-promotion:

```text
tail-emit cursor gap     : 0 occurrences
quorum not met           : 0 occurrences
SCSI WRITE backend.Write FAILED : 0 occurrences
```

The new gate in `core/replication/volume.go` (newly installed peers must prove
`AchievedLSN >= local frontier` via barrier before their live cursor is seeded)
is doing what it claims: r3 / catching-up peers can't cause quorum write failures
on the newly-promoted primary.

## Residue audit

```text
iSCSI sessions:                                  No active sessions
blockmaster/blockvolume/blockcsi/iscsi-target:   none
kubectl port-forward svc/blockmaster:            none
app=sw-blockvolume Deployments:                  none
run-scoped /var/lib/sw-block/testops-*:          (none, no leak)
```

Lab fully clean. The RF=3 scenario's cleanup correctly removes its run-scoped
hostPath (same pattern as the RF=2 chains use after their HG-8 fix).

## Comparison to prior gates

| Run | Plan | Substantive outcome | claim_profile |
|---|---|---|---|
| `20260513-160112-d3f9` (Phase 15) | mounted-failover-reattach | safe refusal (r2 not promotion-ready) | n/a |
| `20260513-194757-dd75` | Stage 1 D2 | safe refusal + ACK propagation | (none yet) |
| `20260513-201020-b902` | Stage 1 D3 step 1 | r2 replica_ready, durable not latched | (none yet) |
| `20260513-203715-e4d5` | Stage 1 D3 step 2 | r2 promotion-ready (no failure injected) | (none yet) |
| `20260513-220011-029b` | Stage 1 D3 step 3 | first recovery + reader checksum | (no explicit profile) |
| `20260513-220439-8337` | Stage 1 D3 + timeline | recovery + control-plane timeline | **controlled-best-effort-demo** |
| `20260514-075221-da0d` | **Stage 1 D4 RF=3** | **sync-quorum durable recovery** | **beta-recovery** |

Each slice closed one layer. The full chain from "safe refusal because r2 wasn't
ready" to "RF=3 sync-quorum recovery with reader checksum" is now end-to-end
demonstrated, with the dev's intentional split between demo-profile (RF=2
best-effort) and production-HA-profile (RF=3 sync-quorum) preserved in the
artifact labels.

## Blocking findings

None.

## Non-blocking observations

1. The RF=3 `claim_profile=beta-recovery` label is a stronger production claim
   than the RF=2 `controlled-best-effort-demo`. Worth ensuring the user-facing
   docs (`docs/operations-v1.md` "Scope And Non-Claims" + RF section, and
   `docs/quickstart-kubernetes.md`) reflect this distinction before any public
   communication: RF=2 still demo-only, RF=3 sync-quorum is the production
   recovery target. The plan's `internal/docs/ref/rf2-promotion-ready-recovery-contract.md`
   already documents this; the user-facing manuals should mirror it.

2. The control-plane timeline schema works identically for both ACK profiles —
   same six events, same evidence pointers. Good consistency between the
   demo branch and the production branch.

3. CSI re-staging happens via pod-recreate (the reader pod is freshly applied
   after the primary stop). This matches the plan's documented model. It is
   not transparent in-place I/O continuation, and the non-claims should
   continue to reflect that.

## Close recommendation

```text
PASS (strict) — the substantive D3 / D4 deliverables of the Stage 1
Mounted Recovery ACK Profile MVP are validated end to end.
```

The two ACK-profile recovery claims the plan promised — RF=2 best-effort
controlled recovery as a *demo*, RF=3 sync-quorum durable recovery as a
*beta production target* — both produce reader-checksum recovery with
honest claim_profile labels, frontier-covered evidence, single-primary
post-promotion state, and a self-explaining control-plane timeline.
Dev's RF=3 quorum fix (peer barrier before cursor seed) eliminates the
post-promotion failure signatures that previously appeared in the
backend logs.

When dev signals plan ready to close, the substantive product claim
behind this MVP is in place. Open user-facing doc work: mirror the
`controlled-best-effort-demo` vs `beta-recovery` distinction in
`docs/operations-v1.md` "RF=2 Mounted Failover Status" + a new RF=3
section before any external claim.
