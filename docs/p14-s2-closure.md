# P14 S2 — Closure (what S2 closed, what belongs to S3)

This note records exactly what landed in S2, exactly what stayed out,
and where the boundary with S3 sits. It is intended to be stable:
future phases must not quietly widen S2's scope after the fact.

The audit note is `p14-s2-audit.md`. The design note is
`p14-s2-design.md`. They are unchanged by this closure.

## What S2 closed

1. **A system-owned publication route for assignment truth.** The
   new package `core/authority/` hosts a `Publisher` that authors
   `adapter.AssignmentInfo` values and fans them out to subscribers
   keyed by `(VolumeID, ReplicaID)`. The publisher is the only
   production code in V3 that constructs an `AssignmentInfo` with a
   non-zero `Epoch`.

2. **Ack-gated authorship of `Epoch` and `EndpointVersion`.** The
   only input to the publisher is a narrow `AssignmentAsk`
   (`VolumeID`, `ReplicaID`, `DataAddr`, `CtrlAddr`, `Intent`).
   Epoch and EndpointVersion are *output*, not input. Directives
   cannot hand the publisher a preformed epoch.

3. **Three explicit authoring intents.** `Bind`, `RefreshEndpoint`,
   `Reassign`. Each has a pinned, table-specified publisher
   action. Rejected and no-op cases are explicit. Retirement
   (`Remove`) is deliberately deferred.

4. **A Directive seam for S3.** `Directive.Next(ctx) AssignmentAsk`
   is the single upstream interface. The trivial `StaticDirective`
   ships in S2. S3's failover policy will plug in behind the same
   interface when it arrives — it will not need to reach into the
   publisher.

5. **A Bridge that forwards publisher output to the adapter.** The
   `Bridge` helper subscribes to a `(VolumeID, ReplicaID)` stream
   and forwards each `AssignmentInfo` to the adapter's existing
   `OnAssignment` ingress. The adapter remains the one and only
   consumer entry point; no second mutation ingress was added.

6. **Closure target met.** The integration test
   `TestClosureTarget_SparrowReachesHealthyViaAuthorityRoute` in
   `core/authority/authority_test.go` boots a `Publisher` with a
   `StaticDirective`, wires a `Bridge` into a real
   `VolumeReplicaAdapter`, and reaches `Mode == ModeHealthy`
   without any `harness.assign(...)` call.

7. **Non-forgeability is enforced by structural test.**
   `TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority`
   walks every `.go` file under `core/` (excluding `_test.go`
   files and the allowlisted test-infrastructure packages
   `authority`, `calibration`, `conformance`, `schema`) and fails
   if any production file constructs an `AssignmentInfo` with a
   non-zero `Epoch`. Adding a new package to the allowlist is an
   explicit, reviewable change.

## What S2 did NOT close

Explicit non-goals, to avoid future scope drift:

- **No failover policy.** S2 does not decide who becomes primary,
  when to promote, when to reassign, or under what conditions.
  Those are S3.
- **No eligibility rules.** S2 does not reason about which replicas
  are allowed to serve at an epoch.
- **No placement or rebalance.** S2 does not choose where replicas
  go or move them.
- **No retirement intent.** `Remove` is deferred. Neither of the
  possible retirement delivery shapes (channel close vs. a new
  adapter mutation ingress) fit the S2 boundary.
- **No external control API.** There is no HTTP or CLI endpoint
  that accepts an `AssignmentInfo` or an `AssignmentAsk`. That
  surface belongs to P15.
- **No V2 port of policy-tangled units.** `HandleAssignment()`,
  `promote()`, `demote()` are NOT imported and will not be ported
  as copies in any future V3 phase.
- **No new engine fact, no new engine command, no new truth
  domain.** The engine is untouched by S2.
- **No new mutation ingress on the adapter.** `OnAssignment` stays
  the sole consumer entry point.

## S2 / S3 boundary (table)

| Question | Owned by | Notes |
|---|---|---|
| Who publishes assignment truth? | S2 (`authority.Publisher`) | |
| How does the adapter receive it? | S2 (`Bridge` → `OnAssignment`) | |
| Who *authors* `Epoch` / `EndpointVersion`? | S2 (`Publisher` mints from `Intent`) | Never accepts preformed values. |
| When should epoch bump? | S3 | S2's `Directive` is the seam. |
| Who is eligible to be primary? | S3 | |
| What triggers a failover? | S3 | |
| How should replicas be placed or rebalanced? | S3 | |
| What retirement looks like on the wire? | S3 / P15 | Deferred. |
| What is the external control API? | P15 | |

## Files added / changed in S2

Added:

- `core/authority/directive.go` — `AskIntent`, `AssignmentAsk`,
  `Directive`, `StaticDirective`, validation errors.
- `core/authority/authority.go` — `Publisher`, `Subscribe`,
  `Unsubscribe`, `LastPublished`, `Run`, `Bridge`,
  `AssignmentConsumer`.
- `core/authority/authority_test.go` — 17 tests covering intent
  semantics, fan-out keyed by `(VolumeID, ReplicaID)`,
  late-subscriber catch-up, `Run` loop, `Bridge`, closure target,
  non-forgeability.
- `docs/p14-s2-audit.md`, `docs/p14-s2-design.md`, this file.

Changed: none. S2 did not modify the engine, adapter, transport,
ops, storage, or sparrow wiring. The authority route stands as an
additive, opt-in producer.

## How to verify S2 is still closed in a future session

Run these tests. All must pass. All must continue to pass:

```
go test ./core/authority/ -count=1
```

Specifically:

- `TestPublisher_Bind_MintsEpochAndEndpointVersion`
- `TestPublisher_Bind_RejectsDoubleBind`
- `TestPublisher_RefreshEndpoint_BumpsOnlyEndpointVersion`
- `TestPublisher_RefreshEndpoint_IdempotentOnSameAddrs`
- `TestPublisher_RefreshEndpoint_RejectsUnboundKey`
- `TestPublisher_Reassign_BumpsEpochAndResetsEndpointVersion`
- `TestPublisher_Reassign_RejectsUnboundKey`
- `TestPublisher_Validate_RejectsMissingFields` (six sub-cases)
- `TestPublisher_FanOut_TwoSubscribersOnSameKey`
- `TestPublisher_FanOut_SubscriberOnOtherVolumeDoesNotReceive`
- `TestPublisher_LateSubscriber_ReceivesLastPublished`
- `TestPublisher_Unsubscribe_ClosesChannel`
- `TestPublisher_Run_DrivesDirectiveUntilCtxCancel`
- `TestPublisher_Run_LogsAndContinuesOnRejectedAsk`
- `TestBridge_ForwardsToConsumer`
- `TestClosureTarget_SparrowReachesHealthyViaAuthorityRoute`
- `TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority`

If the non-forgeability test starts failing because some new
production package wants to construct `AssignmentInfo{Epoch: ...}`,
the correct response is to route that new site through the
authority publisher, not to add a path to the allowlist by reflex.
