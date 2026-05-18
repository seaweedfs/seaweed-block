# RF=2 Promotion Readiness Audit

Date: 2026-05-13
Plan: `RF=2 Promotion-Ready Recovery MVP`
Positioning input: `internal/docs/ref/product-positioning-v1.md`

## Question

Can the current product-owned Kubernetes RF=2 path make a peer promotion-ready
with enough evidence to recover a mounted PVC after primary failure, or must the
product keep publishing safe refusal?

Short answer: the replication/recovery substrate exists, but the Kubernetes
alpha RF=2 path does not yet wire promotion readiness as a product claim. The
current honest state is still:

```text
r1 primary serves mounted I/O
r2 is placed and observed
r2 is not proven caught-up / promotion-ready
controlled r1 failure must refuse recovery
```

This is not a small test gap. It is the availability proof behind the product
positioning. A green scenario is acceptable only if it proves durable catch-up,
authority movement, fencing, and post-failure mounted read, or else emits a
sharper blocker.

## Current Product Facts

### What Phase 15 Closed

The closed `Basic Mounted Failover And Reattach MVP` proved:

- RF=2 PVC can create two generated `blockvolume` Deployments in the
  two-logical-server alpha topology.
- The mounted app path works before failure: writer checksum and reader checksum
  pass through the PVC.
- Inventory exposes `rf=2 desired=2 observed=2`, primary identity, per-replica
  status bundles, and degraded replica reasons.
- A controlled primary failure is scoped to the current primary Deployment.
- If the peer is not ready, the product publishes strict safe refusal:

```text
failover_status: refused
candidate_ready=false
data_check_after_failover=not_claimed
reason=candidate_not_ready_for_primary
```

That is a real product claim: fail-closed recovery semantics. It is not yet a
recovery claim.

### What The RF=2 Baselines Show

QA repeatedly observed this live shape:

```text
r1: status=ok role=primary healthy=true epoch=1 endpoint_version=1
r2: status=unhealthy role=unknown replication=not_ready healthy=false epoch=0 endpoint_version=0
```

The writer/reader path succeeds because `r1` serves the frontend. The `r2` row
is valuable inventory evidence, but it is not promotion evidence.

## Existing Reusable Machinery

### Replication And Recovery Substrate

`cmd/blockvolume/main.go` already wires a real replication stack when a durable
provider is configured:

- `replication.NewReplicationVolume` owns primary-side peer fan-out.
- The durable backend installs the replication volume as write observer.
- `--replication-ack` supports `best-effort`, `sync-quorum`, and `sync-all`.
- `--recovery-mode=dual-lane` installs dual-lane recovery transport.
- The replica listener binds on `--data-addr`.
- `--degraded-probe-interval > 0` starts the degraded-peer probe loop.
- `--status-recovery` can expose `/status/recovery` with engine projection
  details.

Component and hardware scripts exercise these pieces. For example,
`scripts/iterate-m01-replicated-write.sh` starts primary and replica with
`--status-recovery`, `--t1-readiness`, and primary-side
`--degraded-probe-interval=5s`. That is the kind of behavior this plan should
port into the product-owned Kubernetes path.

### Authority Substrate

`core/authority/controller.go` can reassign authority:

- if no authority line exists, bind an acceptable candidate,
- if current authority remains acceptable, keep it,
- if current authority is no longer acceptable, choose another acceptable
  candidate,
- if no candidate is acceptable, emit no reassignment.

The controller's acceptance predicate requires:

```text
slot.Reachable &&
slot.ReadyForPrimary &&
slot.Eligible &&
!slot.Withdrawn &&
server.Reachable &&
server.Eligible
```

This shape is correct only if `ReadyForPrimary` is a real durable/catch-up fact.

### Fencing Substrate

`core/host/volume/projection_bridge.go` fails closed when a replica is
superseded or acting only as a supporting replica. A stale primary should stop
looking frontend-ready after authority moves. The positive recovery gate must
assert this through inventory/status, not just trust the mechanism.

### Inventory Substrate

`sw-block ops inventory` already collects per-replica status bundles via
Kubernetes port-forward. It can report:

- PVC/PV/volume/replica mapping,
- desired vs observed replica count,
- primary identity,
- frontend/status endpoints,
- durable status,
- peer status,
- degraded/unreachable/missing-placement issue lines.

This is the right operator surface for promotion-ready and recovery evidence.

## Product Gaps

### P0: Kubernetes Launcher Does Not Enable Recovery Readiness

`core/launcher/k8s_renderer.go` currently renders the generated blockvolume
with durable storage, dual-lane recovery mode, status endpoint, and iSCSI
frontend args. It does not render the knobs that make peer catch-up observable
and active:

```text
--degraded-probe-interval
--degraded-probe-cooldown-base
--degraded-probe-cooldown-cap
--status-recovery
--replication-ack
--wal-retention-lsns
--t1-readiness
```

Impact: the product places `r2`, but the alpha Kubernetes path does not yet
drive or expose the catch-up/readiness loop needed for a recovery claim.

### P0: Heartbeat ReadyForPrimary Was Too Weak

The D1 audit found that `core/host/volume/host.go` built the heartbeat slot
with:

```text
ReadyForPrimary: true
Eligible: true
Reachable: true
```

That was unsafe as a promotion signal by itself. A running process is not the
same as a caught-up replica. The first D3 guard changed the volume heartbeat to
derive `ReadyForPrimary` from local engine Healthy state and supersede status,
and changed the controller so initial placement can still use reachable/eligible
slots without treating them as failover-ready candidates.

Close requirement: `ReadyForPrimary` must either be derived from real
promotion-readiness evidence, or the failover path must ignore/refuse it until
that evidence exists. The remaining product gap is durable-frontier coverage:
`ReadyForPrimary=true` still must be paired with writer-bound frontier evidence
before the runner gate can claim recovered data.

### P0: ACK Profile Is Not Product-Owned In K8s

`--replication-ack` defaults to `best-effort`. Best-effort can support a narrow
controlled demonstration:

```text
the bytes explicitly written, flushed, caught up, and later verified survived
this controlled failure
```

It cannot support:

```text
all acknowledged writes are quorum durable
```

The Kubernetes launcher does not yet expose or record the ACK profile in the
generated workloads. The recovery gate must name the profile in artifacts and
inventory. A beta-facing RF=2 recovery claim should use `sync-quorum` and prove
that unavailable quorum fails writes closed. A `best-effort` run may only be
called a controlled recovery demonstration, not RF=2 acknowledged-write
recovery.

### P0: Durable Frontier Evidence Is Not Yet A User Contract

The engine and recovery packages track useful facts such as local durable
frontier, catch-up/rebuild decisions, and post-close durable acknowledgement.
But the K8s RF=2 support bundle does not yet expose a cold operator line like:

```text
candidate_ready=true reason=caught_up durable_frontier=<N> peer_epoch=<E> peer_endpoint_version=<EV>
```

Without that line, promotion readiness would require implementation knowledge.

The required frontier must be tied to the mounted writer, not just to an
arbitrary local WAL position. The bundle must record:

```text
required_frontier_lsn=<writer acknowledged/flushed boundary>
candidate_frontier_lsn=<candidate durable/catch-up boundary>
frontier_covered=true|false
```

Promotion is valid only when:

```text
candidate_frontier_lsn >= required_frontier_lsn
```

### P0: Product Loop Uses RF>1 Placement As Input, Not A Full Recovery Policy

`core/host/master/product_loop.go` submits RF>1 placement snapshots to the
topology controller, and only direct-bind asks for single-slot placements. This
is enough for observed RF=2 placement and safe refusal. Positive recovery still
needs a product policy that feeds acceptable candidates only when readiness is
real.

### P1: CSI Reattach Must Be Explicit

`core/csi/master_backend.go` looks up current frontend target; it does not
claim transparent in-place I/O continuation. The MVP recovery path should use a
documented pod recreate/reattach method and verify bytes after that reattach.

## Existing Paths To Reuse

Use these as implementation references:

- `scripts/iterate-m01-replicated-write.sh`: recovery/probe/status-recovery
  wiring outside Kubernetes.
- `scripts/run-iscsi-alua-mounted-failover-smoke.sh`: mounted failover and
  status polling vocabulary outside the product-owned K8s lifecycle.
- `core/replication/component/g9a_best_effort_recovery_test.go`: degraded peer
  recovery mechanics with probe loop.
- `core/host/volume/status_server.go`: `/status`, `/status/peers`, and
  `/status/recovery` surfaces.
- `core/ops/k8s_inventory.go`: nested per-replica bundle collection.

Do not port shortcuts that weaken V3:

- no heartbeat-as-ready,
- no authority move without publisher epoch/endpoint-version,
- no returned-replica promotion without catch-up evidence,
- no component-only data check as Kubernetes mounted recovery.

## Readiness Contract Candidate

A peer should be considered promotion-ready only when the support bundle can
show all of the following:

```text
replica observed: true
replica process reachable: true
replication role: replica_ready OR equivalent caught-up state
durable entry: latched=true operational=true
lineage: epoch/endpoint_version aligned with current authority basis or explicit recovery target
primary peer view: peer state=healthy, probe_in_flight=false, closed=false
recovery evidence: decision=none OR caught_up, durable_frontier >= required frontier
frontend: not serving as primary until authority moves
```

If any of those are missing, the correct product output is safe refusal:

```text
failover_status: refused
reason=<missing condition>
candidate_ready=false
data_check_after_failover=not_claimed
```

## Recommended Next Slice

1. Write the contract in
   `internal/docs/ref/rf2-promotion-ready-recovery-contract.md`.
2. Add fast guard that heartbeat `ReadyForPrimary=true` is not sufficient for
   promotion readiness in the mounted failover path.
3. Add launcher knobs for recovery observability:
   `--status-recovery`, `--degraded-probe-interval`, ACK profile, and retention
   configuration.
4. Add inventory wording for promotion readiness and missing durable frontier.
5. Build the runner gate to wait for `r2` readiness. If readiness never appears,
   close the slice as a specific blocker, not as recovery.

## Audit Verdict

The product has strong enough internals to pursue RF=2 mounted recovery without
an architecture reset. It is not yet ready to claim recovery in Kubernetes.

The first implementation risk is not "make the test pass." The risk is
accidentally treating an observed process as a promotion-ready replica. That
would contradict the product positioning and turn the block product into a toy.
