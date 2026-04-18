# P14 S2 — Bounded Authority Path (Design Note)

This is the design note for P14 S2. Scope is deliberately narrow:
publish `assignment` / `epoch` / `endpointVersion` through a
system-owned route, consumed by the existing adapter ingress. Nothing
more.

The audit note is `p14-s2-audit.md`. Read it first for the factual
baseline. This note proceeds on that baseline and commits to a shape.

## Load-bearing boundaries (do not blur)

These three statements are the contract of S2. Every piece of code in
this phase must be reducible to them.

1. **S2 owns publication of authority truth.** The package introduced
   here is *the* place where `assignment` / `epoch` / `endpointVersion`
   become visible to the rest of the system.
2. **S2 does NOT own failover policy.** *Who* becomes primary, *when*
   to promote, *where* to place replicas, and *how* to rebalance are
   explicitly NOT in S2. They belong to P14 S3.
3. **Engine and adapter remain consumers, not producers, of
   assignment truth.** Nothing in this phase adds a new engine fact,
   a new policy ownership inside the engine, or a new mutation ingress
   inside the adapter. The adapter's `OnAssignment(...)` stays the
   one and only consumer entry point.

If a change seems to need more than these three statements, it is not
S2.

## Closure target

> After S2, sparrow must reach Healthy through a genuine
> `authority.Publisher -> adapter.OnAssignment(...)` route, not only
> through harness injection.

That sentence is the acceptance test. Everything else serves it.

## Package layout

New package: `core/authority/`.

Rationale:

- Outside `cmd/sparrow` and outside `core/adapter` — it is *not* the
  volume body.
- Outside `core/engine` — authority is not a reducer fact.
- Sibling to `core/transport`, `core/ops`, `core/storage`. Adapter is
  the sole connection point.

Files:

- `authority.go` — `Publisher` type, subscriber bookkeeping, publish
  semantics.
- `directive.go` — `Directive` input interface + a trivial
  `StaticDirective` implementation (bounded producer policy source,
  see below).
- `authority_test.go` — unit tests for publication and consumption.

## Input type — narrower than `AssignmentInfo`

A `Directive` does NOT hand the publisher an `AssignmentInfo`. It
hands the publisher an `AssignmentAsk` — a request for the publisher
to perform authorship. Epoch and EndpointVersion are deliberately
absent; they are not input, they are output.

```
type AssignmentAsk struct {
    VolumeID    string   // required
    ReplicaID   string   // required
    DataAddr    string   // required (may change to request an endpoint bump)
    CtrlAddr    string   // required
    Intent      AskIntent // Bind | RefreshEndpoint | Reassign
}

type AskIntent int  // see "Authoring rules" below
```

S2 ships exactly three intents: `Bind`, `RefreshEndpoint`, `Reassign`.
A fourth obvious intent — `Remove` / retirement — is intentionally
omitted. Retirement would need a second delivery shape (either a
channel close that the subscriber must interpret, or a new
`adapter.OnRemoval`-style mutation ingress), and both options expand
the S2 boundary: the first makes the subscription contract carry
semantics beyond "the next fact", the second opens a second
authority-to-adapter mutation path in violation of boundary #3
(the adapter's `OnAssignment(...)` is the one and only consumer
entry point). The S2 closure target — sparrow reaches Healthy via
the real publication route — does not require retirement, so S2
ships without it. Retirement is a later-phase concern and will be
designed alongside the S3 policy seam.

`Intent` is the only control knob the directive has. The publisher
then decides which authoritative field to advance and by how much.
This is the seam by which S3 will later ask the publisher to bump
epoch without S3 ever constructing the epoch itself.

## Authoring rules — the publisher is the minter

The publisher owns a monotonic counter keyed by `(VolumeID,
ReplicaID)`. Given an `AssignmentAsk` it decides as follows:

| `Intent` | Precondition | Publisher action | Emitted `AssignmentInfo` |
|---|---|---|---|
| `Bind` | No prior publish for this `(vid, rid)` | mint `Epoch=1`, `EndpointVersion=1` | fresh |
| `Bind` | Prior publish exists | reject (directive bug) | none |
| `RefreshEndpoint` | Prior publish exists; addrs differ | keep `Epoch`, bump `EndpointVersion` | same epoch, new EV |
| `RefreshEndpoint` | Prior publish exists; addrs identical | no-op (idempotent) | none |
| `Reassign` | Prior publish exists | bump `Epoch`, reset `EndpointVersion=1` | new epoch |

Rejected or no-op directives do not mutate state and do not fan out.

The publisher NEVER accepts a preformed epoch or endpointVersion from
any caller. The only knob is `Intent`. This is what makes the publisher
a real author rather than a relay.

## Who can construct an `AssignmentInfo`

Only `core/authority/` may construct an `AssignmentInfo` with
`Epoch > 0` in a production path. Enforcement is twofold:

1. `AssignmentInfo` stays in `core/adapter` as it is today (it is
   the adapter's ingress shape). The authority package is the only
   production package that constructs one.
2. A package-level test in `core/authority/` audits production
   imports with a simple "forbidden constructor" grep: any
   non-test `.go` file outside `core/authority/` that literal-typed
   `adapter.AssignmentInfo{` with a non-zero `Epoch` field fails the
   test. Test helpers (`_test.go` files) are exempt — calibration
   and conformance retain their right to forge test facts.

## Publication API — no public write verb

The publisher exposes three verbs, none of which accept an
`AssignmentInfo`:

- `Subscribe(volumeID, replicaID string) <-chan AssignmentInfo` — a
  consumer registers interest in authoritative assignments for a
  specific `(VolumeID, ReplicaID)`. Returns a receive channel.
  Unsubscribe via `Unsubscribe(volumeID, replicaID)` or ctx cancel.
- `LastPublished(volumeID, replicaID string) (AssignmentInfo, bool)`
  — read-only inspection of the last emitted fact for a key. Used
  by late subscribers to catch up without waiting for the next
  directive.
- `Run(ctx)` — drive the publisher: consume `AssignmentAsk` values
  from the directive, apply the authoring rules above, fan out to
  subscribers. One goroutine. Stops on ctx cancel.

What the publisher will NOT expose:

- No `PublishAssignment(AssignmentInfo)`. Removed. Its earlier
  presence let callers forge authoritative truth — that was the
  relay-not-author bug the architect caught.
- No `SetEpoch(...)`, no `ForcePromote(...)`, no per-replica
  health reads, no placement hints, no rebalance verbs.

## Directive interface

```
type Directive interface {
    Next(ctx context.Context) (AssignmentAsk, error)
}
```

The trivial S2 implementation is `StaticDirective` — a pre-wired
sequence of `AssignmentAsk` values set by process configuration,
typically one `Bind` per replica at startup for the closure target.

What is explicitly NOT in scope for any `Directive` implementation
in S2:

- No eligibility rules ("is replica X allowed to be primary").
- No promotion triggers ("promote when primary fails").
- No failover orchestration ("advance everyone's epoch on quorum
  loss").
- No placement or rebalance ("pick the least-loaded node").

All four belong to S3. The `Directive` + `AssignmentAsk.Intent`
pair is the seam where S3 will plug in real policy. S2 ships only
`StaticDirective`.

## Subscriber model

Adapters subscribe by `(VolumeID, ReplicaID)` at construction. The
sparrow main loop wires the adapter to the publisher:

```
pub := authority.NewPublisher(dir)
a := adapter.NewVolumeReplicaAdapter(exec)
go authority.Bridge(ctx, pub, a, "vol1", "r1")
    // receives on Subscribe("vol1", "r1"),
    // calls a.OnAssignment(info)
```

`Bridge` is a thin for-loop: receive, forward, repeat. It has no
decision logic. Its only state is "am I still the live subscriber".

Identity is uniformly `(VolumeID, ReplicaID)` across subscription,
authoring, and fanout — fixing the narrow-key bug where same-named
replicas across different volumes would have been multiplexed.

Stale rejection continues to live where it already lives — in
`engine.applyAssignment`. The publisher does NOT replicate that logic;
it only enforces its own output monotonicity (per authoring rules
above) and lets the engine's existing guards handle the rest.

## V2 port plan (what this commits to)

From the audit, the V2 portable-mechanism items are:

- `BlockVolumeAssignment` — useful as a wire *shape* reference. V3
  `AssignmentInfo` already exists; we may adopt field names from V2
  for consistency but do not import.
- `BlockVolumeInfoMessage` — same. Shape only.
- `BlockVolumeHeartbeatCollector` — NOT imported in S2. Its role
  (periodic status collection) is orthogonal to publication. It may
  become a subscriber in a later phase.
- `ProcessBlockVolumeAssignments` — NOT imported. It is the *volume
  side* consumer in V2; in V3 that job is the adapter's.

Policy-tangled items (`HandleAssignment`, `promote`, `demote`) are
NOT ported in S2 and will NOT be ported in any subsequent V3 phase as
a copy — their behavior, if still needed, will be re-derived from
engine commands.

## What S2 closes vs what stays open

| Question | S2 answers | S2 does NOT answer |
|---|---|---|
| Who publishes assignment truth? | `authority.Publisher` | — |
| How does the adapter receive it? | via `Subscribe(vid,rid)` → `Bridge` → `OnAssignment` | — |
| Who *authors* epoch / endpointVersion? | `Publisher` (mints from `Intent`; never accepts preformed values) | — |
| When should epoch bump? | — | S3 |
| Who is eligible to be primary? | — | S3 |
| What triggers a failover? | — | S3 |
| How should replicas be placed or rebalanced? | — | S3 |
| What is the external control API? | — | P15 |

## Test plan (sketch — full tests land in code step)

- **Publisher is the minter**: a directive issues only `AssignmentAsk`
  values (no epoch/endpointVersion). After `Run(ctx)` processes them,
  subscribers observe `AssignmentInfo` with monotonically increasing
  `Epoch` / `EndpointVersion` authored by the publisher.
- **Non-forgeability**: a structural test grep-audits production
  `.go` files under `core/` (excluding `_test.go` and
  `core/authority/`) for literal `adapter.AssignmentInfo{ Epoch:` or
  `.Epoch = ` assignments. Fails if anything outside authority mints
  a non-zero epoch in production code.
- **Fan-out keyed by (VolumeID, ReplicaID)**: two subscribers on the
  same `(vol1, r1)` both receive. A subscriber on `(vol2, r1)` does
  NOT receive `vol1`-keyed assignments (regression for the
  narrow-key bug).
- **Intent semantics**: `Bind` on fresh key mints `Epoch=1, EV=1`;
  `RefreshEndpoint` with changed addrs keeps epoch, bumps EV;
  `RefreshEndpoint` with unchanged addrs is a no-op; `Reassign`
  bumps epoch and resets EV to 1; `Bind` on an existing key is
  rejected.
- **Bridge → adapter → engine**: end-to-end assertion that publisher
  output reaches `ReplicaState.Identity` via the existing ingress,
  with no additional event type added to the engine.
- **Stale rejection unchanged**: engine rejects an older-epoch fact
  via its existing guard. The publisher does NOT duplicate the
  check (regression: no silent second rejector).
- **Closure target**: a sparrow integration test boots with a
  `StaticDirective` and reaches `Mode == ModeHealthy` without a
  single `harness.assign(...)` call.

## What we are NOT building

- No admin or CLI mutation surface for assignments.
- No V2 heartbeat or V2 HandleAssignment port.
- No new engine events, commands, or truth domains.
- No promotion / demotion state machine.
- No eligibility or placement computation.
- No retirement / `Remove` intent. Adding retirement requires a
  second delivery shape that would either overload subscriptions
  with lifecycle semantics or open a second mutation ingress on
  the adapter. Neither fits the S2 boundary. Deferred.

## Acceptance

S2 is architect-clean when:

1. The three load-bearing boundary statements are enforced by the
   code (no backdoor mutation, no engine/adapter-side production of
   authority, Directive is the only policy seam).
2. The publisher is the minter, not a relay. `Directive.Next`
   returns `AssignmentAsk` (no epoch / no endpointVersion). No
   public verb accepts a preformed `AssignmentInfo`. The
   non-forgeability structural test passes.
3. Identity is `(VolumeID, ReplicaID)` uniformly — subscription,
   authoring state, and fan-out all key by the same unit. The
   multi-volume fan-out test passes.
4. The closure target integration test passes.
5. The V2 policy-tangled items listed in the audit remain
   unimported.
