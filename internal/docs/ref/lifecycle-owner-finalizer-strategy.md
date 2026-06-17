# Lifecycle Owner Finalizer Strategy

Status: Phase 41 D4 strategy decision.

## Decision

Phase 41 does **not** ship `SwBlockVolume` finalizer add/remove.

The selected strategy is:

```text
defer finalizer mutation until a lifecycle-owner component has real
Kubernetes API/RBAC/admission proof.
```

Phase 41 may continue building the lifecycle-owner decision model and dry-run
status path, but no component gains production `patch swblockvolumes` on the
main CRD object in this slice.

The dry-run status path is represented as:

```text
action=safe_k8s.release_swblockvolume_finalizer
mode=dry_run
executor=lifecycle_owner
mutation_allowed=false
decision=allowed|rejected|unknown
```

This is an executable contract shape for future lifecycle-owner work. It is not
finalizer execution.

## Why

Phase 39 live QA proved two Kubernetes facts:

1. CRDs do not expose a usable HTTP `/finalizers` endpoint.
2. Changing `metadata.finalizers` on a CRD requires main-object
   `patch swblockvolumes`.

That permission is too broad for the released `operator-status` component. The
whole v0.4 beta claim is that `operator-status` is status/events-only. Giving it
main-object patch would invalidate that boundary.

Phase 41 D2 adds a schema-aware boundary gate showing what a future lifecycle
owner must prove:

- observer/status writer cannot patch the main object,
- lifecycle owner can only issue a finalizer-shaped patch,
- spec and unrelated metadata patches are rejected,
- fake `/finalizers` endpoint is rejected.

However, D2 is not yet a full live-apiserver/admission gate. Until that exists,
shipping actual finalizer mutation would be a code-enforced promise, not a
Kubernetes-enforced product boundary.

## User Impact

Current user-facing behavior remains:

- `SwBlockVolume.status.deleteSafety` says whether finalizer release would be
  allowed, rejected, or unknown.
- Cleanup residue and stale/missing evidence are visible with stable reasons.
- `operator-status` creates Events and writes status only.
- Seaweed Block does not yet hold Kubernetes deletion with its own finalizer.

This is safer than a weak finalizer controller. Users get accurate visibility
without a controller that can patch the whole CR object.

## Release Non-Claim

Until the future lifecycle-owner gate passes, releases must keep this visible
non-claim:

```text
No SwBlockVolume finalizer ownership or finalizer add/remove.
Delete-safety is status-only guidance, not Kubernetes deletion protection.
```

## Future Path To Enable Finalizers

A later phase may choose Path A from
`internal/docs/ref/lifecycle-owner-control-contract.md` if it can prove all of:

```text
1. a separate lifecycle-owner component or mode exists,
2. it has minimal main-object patch permission,
3. admission or equivalent enforcement admits only metadata.finalizers patches,
4. spec and unrelated metadata patches fail against a real API server,
5. clean delete-safety permits finalizer release,
6. blocked, missing, or stale delete-safety prevents release,
7. multi-volume isolation holds,
8. cleanup verifier ends clean after the gate.
```

If those proofs are not available, the product should keep finalizer mutation
deferred.
