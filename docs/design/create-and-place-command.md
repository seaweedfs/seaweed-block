# Online create and placement command (v1.0.1 S1)

Draft design and red test. This branch must not merge before v1.0.0 is cut.
S1 is the single-replica operations item: create a volume and record its
placement online, without restarting blockmaster. It composes existing RPCs;
it does not add a state, grant, promotion, host registration or kernel transition.

Proposed operator interface:

```sh
sw-block ops volume create --master MASTER --size-bytes 268435456 \
  --protocol nvme --transport tcp --server-id registered-host --replica-id r1 \
  --data-addr 192.0.2.10:9201 --ctrl-addr 192.0.2.10:9202 \
  --timeout 10s new-volume
```

The node must already be registered. Online host registration is outside S1.
The command validates all local arguments before any RPC, then invokes
LifecycleService.CreateVolume with RF1, waits for its store-ready answer,
registers the exact per-volume WorkspaceBinding, and calls PlaceWorkspace.
It uses the existing authenticated connection options. A successful answer
means placement recorded, not a lease issued or a target serving. Starting the
unit, assignment and guest connection remain separate operator steps.

CreateVolume owns the volume record and the store-ready gate. The existing
binding receiver owns endpoint reservations and registered-node checks.
PlaceWorkspace owns placement admission under workspaceMu and its publisher
guard. The command never writes these stores directly, mints an epoch, or
uses time to decide authority. Its one context deadline only bounds waiting.

| Last completed step | Refused next step | Unknown next outcome |
| --- | --- | --- |
| none | report refusal; do not call binding/placement | report unknown create; retry same name and geometry |
| store-ready create | leave volume created; do not place | report binding unknown; retry identical intent |
| binding | leave binding prepared; no success claim | report placement unknown; retry identical intent |
| placement | report placement recorded, never serving | no success without an acknowledged placement result |

There is no client journal, rollback or atomic three-record claim. Identical
retries use existing receiver idempotence. Changed geometry/identity, conflicting
bindings and an existing assignment must remain subject to receiver refusal;
the CLI must validate the returned create identity/geometry before proceeding.
No ordinary-create request ID is sent: that existing field denotes restore.

The first red test, TestOpsVolumeCreateAndPlaceWithoutMasterRestart, invokes
the real CLI dispatcher against public gRPC receivers. It requires exactly
create -> bind -> place with the stated RF1 intent while one endpoint remains
running. It fails on main because only volume resolve exists. It proves the
missing command seam, not real store creation or guest I/O. Subsequent vertical
slices must cover refusal/unknown outcomes, argument admission, matching reply
identity, repeat intent and a real-master integration run before readiness.

Existing contracts relied on: M2 authority/assignment in invariants.md;
INV-AUTH-HOLDER-IDENTITY-001 and INV-VOL-ID-NO-REUSE-001;
core/host/master/services.go CreateVolume;
core/host/master/workspace_binding.go RegisterWorkspaceBinding/PlaceWorkspace;
core/rpc/proto/control.proto and workspace.proto. No guard is changed here.
