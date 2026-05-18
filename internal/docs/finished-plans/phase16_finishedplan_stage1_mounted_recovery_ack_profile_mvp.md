# Finished Plan: Stage 1 Mounted Recovery ACK Profile MVP

Status: closed. QA close report passed strict validation in
`qa-assignments/stage1-mounted-recovery-ack-profile-mvp-close-report.md`.

Opened after closing
`finished-plans/phase15_finishedplan_basic_mounted_failover_and_reattach_mvp.md`,
and closed after QA-owned RF=3 sync-quorum recovery run
`20260514-075221-da0d` passed from clean state with 7/7 phases and 75/75
actions.

D1/D2 product audit and recovery contract were drafted. D3/D4 prove the RF=2
best-effort controlled recovery demo end-to-end through the
mounted Kubernetes PVC path: writer checksum, promotion-ready peer, controlled
primary stop, master-published new primary, CSI/pod recreate, and reader
checksum. That RF=2 result is explicitly labeled
`claim_profile=controlled-best-effort-demo`; it is not a quorum HA claim.

The final implementation slice produced a QA-owned RF=3 `sync-quorum` durable
recovery pass. The product rule is stricter than
heartbeat readiness: when an RF3 primary fails, master actively uses promotion
evidence that the surviving candidate covers the sync-ACK frontier before
allowing authority promotion. The code seam is in `core/host/master`: RF3
promotion is blocked without a fresh promotion probe. If the required sync ACK
LSN is known, only a `sync-quorum`/`sync-all` candidate whose durable LSN covers
that required LSN remains eligible. If the required LSN is not known yet, the
seam falls back to the highest probed durable LSN as a weak recovery choice;
that is useful for best-effort/control recovery and for shrinking loss, but it
is not enough for a beta HA claim until committed-frontier reporting exists.
The live wiring refreshes that prober from blockmaster's launcher workload plans
when `--launcher-status` is enabled: master derives each candidate's loopback
status endpoint from the generated workload port plan, calls `/status` and
`/status/durable` on demand, and feeds `replica_ready` plus durable frontier LSN
into the RF3 gate. The promoted primary also verifies surviving peers with a
barrier at its local frontier and seeds their live ship cursor before admitting
post-promotion writes, so sync-quorum writes continue after one primary is
stopped. QA verified the post-promotion error signatures were absent:
`tail-emit cursor gap`, `quorum not met`, and
`SCSI WRITE backend.Write FAILED`.

Stage: **Stage 1 - Safe K8s Recovery via CSI/Pod Recreate**.

The current plan deliberately targets a safe Kubernetes recovery loop through
CSI/pod recreate. Transparent in-place host-path failover through iSCSI ALUA or
NVMe ANA multipath is the next plan, not this one.

Minimum product line: if Seaweed Block wants to claim HA block behavior in
Kubernetes, Stage 1 is the floor. "Master changed primary" is not a product
claim. The minimum acceptable user experience is automatic detection, automatic
safe promotion, automatic CSI/node reattach on pod recreate, and post-failure
data verification without manual operator repair. Stage 2 raises that to
transparent or near-transparent multipath failover.

QA needed now: no for this plan. The next QA touchpoint belongs to the next
active plan.

Product decision: Stage 1 splits into two explicit profiles:

- **RF=2 best-effort controlled recovery demo**: prove master promotion,
  CSI/pod recreate, and post-failure checksum for the exact gated bytes. This
  is useful light-use evidence, but it is not a quorum durability claim.
- **RF=3 sync-quorum durable recovery**: prove the beta-facing writable HA
  recovery line, because losing one replica can still leave write quorum.

## Product Question

Can Seaweed Block recover a mounted Kubernetes app/PVC path after controlled
primary failure under an ACK profile that honestly matches the user claim:
RF=2 best-effort as a controlled demo, or RF=3 sync-quorum as the durable HA
line?

This plan serves the product positioning in
`ref/product-positioning-v1.md`: Seaweed Block should be a lightweight
Kubernetes block product with enterprise protocol discipline and fail-closed
recovery semantics. Recovery is not just another scenario. It is the
availability proof behind that positioning, and the ACK profile defines what
kind of availability can be claimed.

The engine design lesson from this plan is captured in
`ref/engine-automata-design-note.md`: identity, authority, durability frontier,
recovery, replication handoff, frontend publish, and host attach should be
treated as separate small automata with explicit facts, not one broad green-path
"engine" state.

The product impact question is:

```text
Can a small-cluster Kubernetes user trust Seaweed Block to recover a volume
through documented ACK semantics, or refuse promotion/write recovery with
evidence strong enough for an operator/support engineer to act on?
```

If this plan cannot improve that user trust, it should not be treated as done.

The last plan proved the honest negative:

```text
RF=2 app path works before failure -> r1 primary is stopped -> r2 is not
promotion-ready -> product refuses recovery safely and says why
```

This plan targets the next product step in two profiles:

```text
RF=2 best-effort demo:
app writes data -> r2 catches up for the gated frontier -> r1 fails ->
authority moves safely -> CSI/pod recreate reaches r2 -> reader verifies the
same bytes -> bundle says this is controlled-best-effort-demo, not quorum HA
```

```text
RF=3 sync-quorum durable HA:
app writes data with quorum ACK -> one primary fails -> authority promotes a
quorum-covered candidate -> CSI/pod recreate reaches the new target -> reader
verifies the same bytes and RW mount succeeds because quorum remains available
```

The close target is not "master promoted a replica" by itself. A promotion that
does not restore a Kubernetes host path is only an internal control-plane event.
The user-visible product result must be one of:

```text
master/authority promotes -> CSI/node reattaches on pod recreate -> mounted data is verified
```

or:

```text
master/authority refuses -> host path recovery is not claimed -> bundle says exactly why
```

The outcome must be either a real recovery proof or a sharper blocker that
states exactly which product semantic is missing. A weaker abstraction is not
acceptable.

## Stage Gates

### Stage 1 Hard Gate: Safe K8s Recovery Via CSI/Pod Recreate

This is the current plan. It is the minimum HA-like Kubernetes product line.

Pass requires every item below:

- The peer is promotion-ready before failure based on durable frontier and ACK
  evidence, not heartbeat or Deployment readiness.
- Master/authority promotes automatically after controlled primary failure.
- Promotion publishes a new active target generation with epoch and
  endpoint-version evidence.
- Old primary is fenced, degraded, or otherwise not presented as a valid writer.
- CSI/node reattach is proven through pod recreate: before/after publish target
  and staged target are captured.
- The reader pod uses the same PVC and verifies the same bytes after failure.
- No operator runs a manual promote, manual repair, direct filesystem read, or
  direct blockvolume read.
- If any item is missing, recovery is not claimed and the bundle emits a
  fail-closed blocker reason.

### Stage 2 Hard Gate: Transparent Multipath Host Failover

This is the next plan. Do not use it to close Stage 1.

Pass requires every item below:

- CSI publishes/configures multiple paths before failure.
- The protocol-specific host multipath stack is configured and visible:
  iSCSI ALUA + dm-multipath, or NVMe ANA + native NVMe multipath.
- Master promotion changes protocol path state consistently with authority
  epoch/endpoint generation.
- The mounted workload continues through the host path without pod recreate if
  transparent switching is claimed.
- Post-failure checksum verifies through the mounted workload path.
- If transparent switching is not proven, Stage 2 cannot close as recovered.

## Target Claim

If successful, the narrow Stage 1 claims are:

```text
RF=2 controlled demo: On the documented same-node alpha Kubernetes iSCSI path,
with two logical replicas and ack_profile=best-effort plus
claim_profile=controlled-best-effort-demo, an RF=2 PVC can write data, show a
promotion-ready non-primary peer, survive a controlled primary blockvolume
Deployment stop through master-published CSI target generation change,
documented pod recreate/reattach, and verify the exact same bytes after
recovery. Quorum durability and post-failure writable HA are not claimed.
```

```text
RF=3 durable HA: On the documented alpha Kubernetes iSCSI path, with three
logical replicas and ack_profile=sync-quorum, a PVC can lose one primary
blockvolume, promote a quorum-covered candidate, reattach through CSI/pod
recreate, mount RW, and verify the same bytes after recovery with inventory
evidence for old-primary fencing, new-primary authority, quorum frontier
coverage, peer eligibility, and CSI/node reattach.
```

If not successful, the plan should close only with a specific safe blocker:

```text
Mounted recovery remains a non-claim because <replication catch-up | durable
frontier | ACK profile | authority promotion | CSI reattach | fencing | quorum
availability> is not yet product-ready. The support bundle identifies the
missing condition.
```

## Product Semantics Guardrail

Do not make RF=2 recovery pass by weakening the block contract.

Positioning guardrail:

- The public story leads with product promise, not matrix rows.
- Any new claim must map to a gate, QA evidence, and non-claim boundary.
- If recovery semantics are weaker than the promise, publish safe refusal or a
  blocker instead of softening the promise silently.
- A green runner scenario is useful only if it advances one of the visible
  values in the positioning: lightweight Kubernetes experience, protocol
  discipline, or fail-closed recovery semantics.

Non-negotiable semantics:

- no promotion from heartbeat alone,
- no promotion without durable frontier / catch-up evidence,
- no promotion unless the candidate frontier covers the writer's
  acknowledged/flushed mounted-write boundary,
- no RF=2 recovery claim from master-side promotion alone,
- no RF=2 recovery claim in this plan unless the host path is proven by
  documented CSI/node reattach on pod recreate,
- no recovery claim without post-failure mounted app read/checksum,
- no stale primary left looking like a valid frontend writer,
- no hidden ACK profile; best-effort, sync-quorum, or sync-all must be named,
- no RF3 HA candidate selection from "largest LSN" alone when a required sync
  ACK frontier is known; the candidate must first cover the required frontier,
  then highest durable LSN can be used as a tie-break/preference among safe
  survivors,
- if the required sync ACK frontier is unknown, highest durable LSN is allowed
  only as a weak recovery heuristic and must be labeled as such,
- no beta-facing quorum durability wording on a best-effort-only run;
  best-effort can only close as a controlled recovery demonstration or safe
  blocker,
- no RF=2 sync-quorum writable recovery claim after one replica loss; RF=2
  sync-quorum must fail closed when quorum is unavailable,
- no RF=3 sync-quorum recovery claim unless the mounted app path proves RW
  reattach/checksum after one replica loss,
- no "replica observed" standing in for "replica promotion-ready",
- no test-only shortcut that bypasses CSI attach/reattach if the claim is
  Kubernetes mounted recovery.

Use `ref/product-spec-gate-template.md` before implementation. Tests validate
the product contract; tests are not the contract.

## Why This Is Next

The product now has a useful, honest safe-refusal path. The remaining user gap
is availability, not placement:

```text
Can Seaweed Block actually recover when the peer is ready?
```

QA repeatedly observed the same blocker in RF=2 gates:

```text
r1: primary, healthy, epoch=1
r2: observed, status=unhealthy, replication=not_ready, epoch=0
```

That state is correctly refused. The next plan must make `r2` eligible or prove
why it cannot be made eligible in the current architecture.

## Current Honest State

What works:

- RF=2 PVC can render two generated `blockvolume` Deployments in the
  two-logical-server alpha topology.
- RF=2 mounted writer/reader path works before failure through `r1`.
- Inventory exposes `rf=2 desired=2 observed=2`, per-replica status bundles,
  primary identity, and degraded peer state.
- Controlled primary failure is scoped and observable.
- Safe refusal is strict and QA-validated when `r2` is not promotion-ready.
- RF=2 best-effort controlled recovery is QA-validated: master promotes the
  promotion-ready supporting replica, the reader pod reattaches through CSI/pod
  recreate, and the post-failure checksum passes. The bundle labels this as
  `controlled-best-effort-demo`, not quorum HA.
- The alpha launcher path can render an explicit ACK profile into generated
  `blockvolume` Deployments; the current D4 safe-refusal gate uses
  `sync-quorum` instead of silently inheriting the blockvolume `best-effort`
  default.
- Fast guards prevent several unsafe failover-looking states from appearing
  healthy.
- Fast master-side RF3 guards now block promotion from stale heartbeat readiness
  alone. RF3 promotion requires a fresh promotion evidence provider result. With
  a known required sync ACK LSN, the candidate must be `sync-quorum`/`sync-all`
  and cover that LSN. With no known required LSN, the gate can select the
  highest durable survivor as a weak recovery path, including best-effort, but
  not as a quorum HA claim.
- Blockmaster launcher ticks now install a workload-plan promotion evidence
  provider when `--launcher-status` is enabled. The provider probes candidate
  blockvolume loopback status endpoints on demand and returns durable frontier
  evidence without adding heartbeat/protobuf fields.
- RF3 candidate choice now follows the product rule: threshold first, best
  survivor second. `durable_lsn >= required_lsn` decides whether a candidate can
  be promoted; highest durable LSN only decides which safe candidate should be
  preferred. If `required_lsn` is absent, highest durable LSN is the fallback
  choice and the RPO/commit-boundary remains explicitly weaker.
- RF3 sync-quorum Kubernetes recovery has a QA-owned close run:
  `20260514-075221-da0d` on m02. Evidence: writer `/data/demo.bin: OK`, r2
  `candidate_ready=true reason=promotion_ready` at required frontier 44,
  controlled stop of r1, master-published primary r2 at epoch 2, CSI/node
  restaged portal `127.0.0.1:3261`, reader `/data/demo.bin: OK`, and
  `data_check_after_failover=reader_checksum_passed`.
- Promoted-primary replication handoff now seeds covered surviving peers with a
  barrier proof at the local frontier. The RF3 pass showed
  `seeded live cursor lsn=44`, `ship ok peer=r3`, and zero `tail-emit cursor
  gap`, zero `quorum not met`, and zero `SCSI WRITE backend.Write FAILED`
  matches after promotion.

What does not work or is not proven:

- The required LSN source is still weak/minimal. The current seam can fall back
  to highest durable LSN when the required sync ACK LSN is unknown. A
  product-grade HA line still needs a committed-frontier source with a defined
  RPO window, such as primary-reported committed LSN on event/periodic
  boundaries plus failover probe cross-checks.
- Transparent protocol multipath failover is intentionally not part of this
  plan's close claim. The existing iSCSI ALUA and NVMe ANA protocol gates remain
  substrate evidence only until a Kubernetes multipath plan wires them into CSI.

## Scope

In scope:

- Audit the RF=2/RF3 replication/catch-up path from primary writes to peer
  durable readiness.
- Define the positive recovery contract: ACK profile, peer eligibility, failure
  injection, CSI/pod-recreate recovery method, and evidence fields.
- Add fast tests for peer promotion readiness and refusal when readiness is
  missing.
- Add a runner-native gate that waits for a promotion-ready peer, injects
  controlled primary failure, then either proves recovery with reader checksum
  or emits a sharper safe blocker.
- Add the RF3 sync-quorum gate as the durable HA product line once promotion
  evidence is backed by live replica probes.
- Update `docs/operations-v1.md` only when a user-facing claim boundary changes.

Out of scope:

- transparent in-place I/O continuation,
- Kubernetes CSI multipath setup and transparent host-path switching,
- arbitrary node loss,
- remote-node attach to loopback frontends,
- production HA,
- performance/soak,
- upgrade/uninstall safety,
- UI/operator-grade remediation.

## Top Blocking Issues

### P0: Peer Promotion Readiness Must Be Real

The product must distinguish:

```text
observed replica != caught-up replica != promotion-ready replica
```

Close requirement: status/inventory exposes a peer readiness reason that a QA
operator can read without logs, and authority refuses promotion when the peer is
not ready.

### P0: Durable Frontier / ACK Profile Must Be Explicit

Recovery means something only if the product says which acknowledged writes are
covered.

Close requirement: the gate names `ack_profile=<profile>` and records durable
frontier/catch-up evidence that justifies promoting the peer for the data being
verified.

### P0: Stale Primary Fencing Must Survive The Positive Case

After authority moves, the old primary must not look like a valid writer.

Close requirement: after-failure inventory/support bundle shows old primary
state, new primary state, epoch/endpoint-version transition, and an issue if
the old primary is still frontend-ready.

### P0: Recovery Must Use The App/PVC Path

The positive claim is not a component test.

Close requirement: writer checksum before failure and reader checksum after
failure use the Kubernetes PVC path and documented pod recreate/reattach method.

### P0: CSI/Pod-Recreate Host Path Correctness Is The User-Visible Recovery

Master-side promotion and target publication are necessary, but not sufficient.
The product only recovers for the user when the Kubernetes path reattaches
through the documented Stage 1 mechanism:

```text
CSI/node reattach on pod recreate
```

Close requirement: D4 must prove that after authority movement, CSI consumes the
new master-published target generation, the recreated reader pod stages through
that target, and the mounted data checksum matches. If CSI/node reattach is not
proven, the only valid outcome is safe refusal with
`reason=host_path_recovery_not_verified` or a more specific subclass.

The next plan will target Stage 2:

```text
protocol multipath switch via iSCSI ALUA/dm-multipath or NVMe ANA/native multipath
```

## Deliverables

### D1: RF=2 Replication Reality Audit

Status: drafted in `ref/rf2-promotion-readiness-audit.md`.

Read code, tests, logs, and current RF=2 artifacts to answer:

- how writes are replicated from `r1` to `r2`,
- why prior live runs left `r2` at `replication=not_ready epoch=0`,
- what status endpoint or authority field should indicate peer catch-up,
- how `--replication-ack` is wired in generated Kubernetes workloads,
- whether existing V2 or protocol-gated paths have useful behavior to port
  without weakening V3 semantics.

Output: `internal/docs/ref/rf2-promotion-readiness-audit.md`.

### D2: Positive Recovery Contract

Status: drafted in `ref/rf2-promotion-ready-recovery-contract.md`.

Create a cold product contract for RF=2 recovery:

- supported topology,
- ACK profile,
- peer eligibility fields,
- durable frontier evidence,
- controlled failure class,
- old-primary fencing evidence,
- host-path recovery method: CSI/node reattach on pod recreate,
- non-claims.

Output: `internal/docs/ref/rf2-promotion-ready-recovery-contract.md`.

### D3: Fast Promotion-Readiness Guards

Status: in progress. Added first guard set:

- heartbeat/live process alone does not set `ReadyForPrimary`,
- supporting replica-ready can be a promotion candidate while frontend-unhealthy,
- superseded replica is not promotion-ready,
- initial placement can bind a placeable replica before promotion readiness,
- failover/rebalance still require `ReadyForPrimary`,
- current-primary endpoint refresh can proceed when no promotion-ready peer
  exists.
- beta-facing recovery rejects `best-effort` ACK profile unless explicitly
  labeled as a controlled best-effort demonstration,
- promotion readiness requires known durable frontier evidence,
- a peer behind the mounted-write frontier is not promotion-ready,
- `sync-quorum` or `sync-all` with covered frontier can be promotion-ready.
- inventory/support summaries expose per-replica `candidate_ready`,
  `ack_profile`, frontier-known fields, and `candidate_not_promotion_ready=...`
  issue lines for RF>1 non-primary replicas.
- CSI `ControlStatusLookup` carries internal publish-target generation evidence
  (`epoch`, `endpoint_version`) copied from master status for before/after D4
  assertions, while `ControllerPublishVolume` still keeps authority-shaped
  fields out of CSI `publish_context`.
- durable status and nested ops-status bundles expose storage boundary evidence
  (`frontier_known`, `durable_lsn`, `retained_lsn`, `head_lsn`); Kubernetes
  inventory propagates the candidate's durable frontier but still refuses
  promotion with `required_frontier_missing` until D4 supplies the mounted-write
  required frontier.
- Kubernetes launcher rendering now has an explicit `launcher-replication-ack`
  product input, validates the profile, and emits `--replication-ack=<profile>`
  into every generated blockvolume Deployment. The D4 primary-failure
  safe-refusal gate opts into `sync-quorum` and asserts the generated manifests
  plus safe-refusal marker carry that profile.
- supporting replicas now consume their own master-minted peer descriptor from
  the active primary's `AssignmentFact`, seeding adapter lineage for the same
  epoch/endpoint-version while preserving frontend gating and avoiding
  replication fan-out installation on the non-primary.
- `scripts/run-alpha-app-demo.sh` has a `SW_BLOCK_DEMO_STOP_AFTER=promotion-ready`
  mode and the runner-native
  `mounted-failover-rf2-promotion-ready-chain.yaml` scenario verifies the
  non-primary candidate becomes promotion-ready before any failure is injected.
- supporting-replica durable storage opened before assignment can now latch the
  assigned epoch/endpoint-version through a ready-marker path without requiring
  frontend Healthy; QA verified that this closes the live
  `durable_frontier_missing` blocker and turns the RF=2 volume rollup to
  `status=ok` before failure.
- `scripts/run-alpha-app-demo.sh` now branches the primary-failure path: a
  not-ready candidate still emits the existing safe-refusal marker, while a
  promotion-ready candidate writes `primary-failure-recovery.txt`, injects the
  scoped primary stop, waits for the candidate to become the master-published
  primary, and then lets the reader pod recreate/reattach and verify data.
- `mounted-failover-rf2-promotion-recovery-chain.yaml` is the first
  runner-native positive recovery attempt. It asserts writer checksum,
  candidate readiness, promoted primary differs from failed primary, no
  safe-refusal marker, and reader checksum after failure.
- Dev manual run `20260514-042312-manual-recovery` exposed a same-replica epoch
  latch blocker: promoted r2 reached `epoch=2`, but durable storage still
  refused Normal I/O because it was latched to `epoch=1`.
- Dev manual run `20260514-043408-manual-recovery2` advanced past that blocker:
  master promoted r2, CSI/attach reached the promoted iSCSI target, and the old
  primary was not presented as valid. It then failed at RW filesystem mount
  because RF=2 `sync-quorum` cannot acknowledge writes after one replica is
  stopped.
- Dev manual run `20260514-045800-manual-rf2-best-effort-recovery` changed the
  positive RF=2 recovery gate to `ack_profile=best-effort` plus
  `claim_profile=controlled-best-effort-demo`. It completed with the
  reader-verified exit code, `failover_status: promoted`,
  `promoted_replica=r1`, `data_check_after_failover=reader_checksum_passed`,
  and `/data/demo.bin: OK`. This proves the RF=2 controlled recovery demo
  branch, not quorum HA.
- The RF=2 positive recovery script now emits
  `control-plane-timeline.txt` with stable event lines for
  `primary_observed`, `candidate_evaluated`, `primary_failure_injected`,
  `authority_published` or `safe_refusal`, `csi_reattach_observed`, and
  `data_check`. The runner gate asserts this timeline so operators can see what
  happened without reconstructing it from blockmaster/CSI logs.
- RF3 product-loop promotion now has a master-side promotion evidence seam. The
  authority controller may still use `ReadyForPrimary` for RF2 controlled demo
  behavior, but RF3 promotion is blocked unless master obtains fresh promotion
  evidence for surviving candidates. The guard rejects missing probes,
  `best-effort` profiles, unknown sync ACK LSN, and candidates whose durable
  LSN is behind the required sync ACK LSN.

Closed D4/D5 result: RF2 best-effort controlled recovery and RF3 sync-quorum
durable recovery are both QA-validated. The RF3 gate reached the product path:
`master promotes -> CSI/node reattaches on pod recreate -> reader checksum
passes`, and the promoted-primary replication handoff no longer breaks RW
remount because covered surviving peers are seeded from a barrier-proven
frontier. QA verified the user-facing claim boundary and the cleanup hygiene.

Remaining follow-up for a later plan:

- wire a stronger required/committed frontier source when available, instead of
  relying on the unknown-required-LSN fallback;
- keep `docs/operations-v1.md` aligned as the claim moves from Stage 1 beta
  recovery to later transparent/multipath recovery.

Add or tighten fast tests for:

- observed but not-ready peer is not eligible,
- peer with durable frontier/catch-up evidence can be eligible,
- primary handoff advances epoch/endpoint-version,
- stale primary frontend-ready is unhealthy after handoff,
- missing ACK/durable evidence causes safe refusal.
- RF3 does not promote from heartbeat `ReadyForPrimary` alone,
- RF3 rejects `best-effort` evidence for the HA claim,
- RF3 with a known required sync ACK LSN only promotes a candidate whose durable
  LSN covers that LSN.
- RF3 with an unknown required sync ACK LSN falls back to the highest durable
  survivor and labels the semantics as weaker than quorum HA.
- RF3 prefers the highest durable LSN among candidates that cover the required
  sync ACK LSN, or among all ready candidates when the required LSN is unknown.
- promoted primary seeds live ship cursors only after a barrier proves the peer
  covers the local frontier; a below-frontier survivor is degraded and cannot
  satisfy `sync-quorum`.

### D4: Runner-Native Promotion-Ready Gate

RF2 best-effort controlled recovery gate: QA passed. It proves a controlled
demo profile only.

RF3 sync-quorum durable recovery gate: QA passed. Scenario:

```text
pre_clean
preflight
build/install alpha RF=3 three-logical-server path
create RF=3 PVC with ack_profile=sync-quorum
write/check data through app writer and record sync ACK frontier
wait for inventory-derived non-primary candidates to cover the required frontier
or time out with blocker bundle
capture before-failure inventory
inject scoped primary failure
master probes surviving replicas before promotion
observe promotion only if a candidate covers the sync ACK frontier
prove CSI/node reattach target-generation change if recovery is claimed
reattach/recreate reader only if recovery is legitimately claimed
verify reader checksum if recovered
capture after-failure inventory/support bundle
collect_and_cleanup(always)
```

If no non-primary candidate can cover the sync ACK frontier, the gate should
pass only as a safe-blocker fixture with a specific issue class, not as
recovery.

Dev evidence: `20260514-012711-d50a` passed with 7/7 phases and 75/75 actions.
QA close evidence: `20260514-075221-da0d` passed from clean state with 7/7
phases and 75/75 actions.
The recovery marker shows `failover_status: promoted`, `promoted_replica=r2`,
`after_publish_target_evidence` on frontend `127.0.0.1:3261`,
`reader_verified=true`, and
`data_check_after_failover=reader_checksum_passed`.

### D5: Operations Manual Update

Updated `docs/operations-v1.md` after QA confirmed the RF3 D4 claim:

- RF2 remains a controlled best-effort demo profile, not quorum HA.
- RF3 sync-quorum is described as the Stage 1 durable mounted recovery path
  with the same recovery marker and cleanup evidence.

### D6: QA Close Gate

QA close report validated:

- runbook claim boundary,
- ACK profile and durable frontier evidence,
- pre-failure peer readiness,
- scoped primary failure,
- old-primary fencing,
- new-primary authority if recovered,
- host-path recovery method if recovered: CSI/node reattach on pod recreate,
- post-failure reader checksum if recovered,
- explicit non-claim if not recovered,
- cleanup hygiene.

## Gates Closed

This plan closed because:

1. The RF2 best-effort demo boundary and RF3 sync-quorum HA boundary are
   documented without ambiguity.
2. Fast tests cover eligibility/refusal/fencing semantics, including RF3
   rejection of heartbeat-only and best-effort promotion evidence.
3. A runner-native RF3 sync-quorum gate proves host-path recovery plus reader
   checksum, or a sharper product blocker.
4. Inventory/support bundles explain the outcome without implementation logs.
5. User-facing docs do not over-claim.
6. QA validates independently and reports no blocking issue.

## Next Plan Candidate: Transparent Multipath Host Failover

Do not fold this into the current plan.

The next plan should target the mature host-path experience:

```text
master promotion -> protocol path state changes -> Linux multipath switches path -> mounted workload continues
```

Candidate scopes:

- NVMe ANA + native multipath through Kubernetes CSI,
- iSCSI ALUA + dm-multipath through Kubernetes CSI,
- CSI publishes/configures multiple paths up front,
- master-published epoch/endpoint generation stays consistent with protocol
  path state,
- mounted workload verifies data after primary failure without pod recreate if
  the protocol path claims transparent switching.
