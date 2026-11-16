# Chunk garbage collection (W5)

`NewCollector(catalog, blobDeleter, config).Run(ctx)` traces every volume returned by the catalog, every HEAD, every snapshot (including unnamed attempt entries), and every live upload/copy intent. Use a deadline-bound context. Supply `IssuedEpoch` from the authoritative blockmaster reader, not lease expiry or a host-local projection. The CLI below binds that reader to blockmaster; there is no automatic scheduler.

A capture intent can expire only when its age strictly exceeds IntentTTL, the blockmaster has issued a higher epoch, and the actual remote HEAD has advanced to a higher epoch no greater than the issued one. Unknown authority retains it. Snapshot/fork copy intents never expire by epoch. `SweepCopies(ctx)` may settle an attempt past its recorded deadline only by claiming its final immutable publication target through Catalog.AbortSnapshot/AbortFork. Snapshot targets are `snaps/<name>/<attempt>`; fork targets are HEAD, never VOLUME. A confirmed permanent aborted target or a validated published target settles the intent. Unknown claim/read retains it. The deadline admits a claim but never by itself releases protection. Old copy intents without deadline metadata remain protected.

An immutable binding becomes a candidate only when its creation time is strictly older than Grace and no root names it. Reclamation requires another successful mark at least Grace after its first observation with the same binding bytes. A rooted candidate is forgotten; failed marks discard coverage. A bare Collector restart discards coverage; ExecuteGC restores only a completed, configuration-matching filer mark. Grace must exceed the catalog operation deadline plus the measured mark duration. Backward clock movement refuses the pass.

Before physical deletion the binding receives a conditional `swb.gc.deleted=1` tombstone; after every resolved replica deletion succeeds it becomes `done`. Bindings are never removed or rebound. Failed/uncertain blob deletion returns an error and retains the fid for a conservative retry after fresh marks. The SeaweedFS adapter deletes the exact fid, accepts only HTTP 200/202/204/404 (SeaweedFS returns 202 after synchronous replicated deletion), and propagates transport/other HTTP failures. Authenticated deployments must supply a BlobDeleter with their write authorization; this patch does not add an auth provisioning path.

Unbound needles (upload succeeded but key binding never persisted) are RETAINED. The existing interfaces cannot enumerate/count them, so `UnknownUnbound=true` reports that limitation instead of a fabricated zero. No sweep is attempted.

## Scope and follow-ups

This first Draft implements the GC core and the real Capture/intent/HEAD caller regression. Retention (newest N plus logarithmic age buckets), workspace tombstoning, automatic scheduling remain follow-up W5 slices. The CLI and blockmaster epoch binding are available below. Unnamed deleting snapshot attempts root their keys until their deleting timestamp is older than grace; normal two-mark/upload-age admission still applies afterward. The immutable attempt record is never removed, even after reclaim. Active unnamed attempts remain roots. This is not complete workspace lifecycle acceptance.

## Lock order

The new Collector mutex serializes its own passes and candidate state; it never enters WAL, publication, shard or chunk-layer locks. Remote calls execute only while holding that collector-local mutex. Producer Capture already uploads after releasing its capture/publication barrier, so GC adds no edge to the storage lock graph. The epoch-reader and BlobDeleter must not re-enter this Collector. Multiple collectors may repeat idempotent physical deletes, but each must independently satisfy the two-mark grace rule.

## Evidence boundary

The assertion RED is an initial GC candidate that already implemented HEAD/snapshot tracing and two marks but omitted intent roots; the base has no GC API to test. It is NOT claimed as a GC bug reproduced on unmodified main. The same test holds the shipping Capture before CAS after real immutable uploads, runs two GC marks, resumes CAS, and reads exact bytes through ChunkRemote. Filer/needle services are deterministic adapters; no SeaweedFS process, hardware, lab or cloud acceptance is claimed.

## Copy-sweep integration

The follow-up branch merges W2 #119 b51713e with #117 5f010ca (the #121 parent). The sole capture conflict keeps #117's pre-upload intent list and cleanup semantics, and retains #119's typed `ErrChunkRemoteUnavailable` wrapper on upload failure. No capture intent is swept through the copy protocol. Name reuse creates a new snapshot attempt; permanent old attempt tombstones continue refusing late old inserts. Volume IDs cannot be reused.

The first copy-sweep RED uses the prior retain-forever behavior (a no-op SweepCopies seam), not a claimed bug on an unmodified main. The same real Catalog caller turns GREEN with permanent-target claims. Tests cover a pending snapshot across name reuse, a fork with VOLUME already present and HEAD held, published unnamed/deleting attempts, unknown claim recovery, and the retained stalled-Capture case. Deterministic remote services only; no lab/cloud actions.

## Operational CLI

Use the existing `sw-block ops chunk` surface rather than a background job: the
operator explicitly chooses the catalog namespace and when deletion is allowed.
Before applied GC, configure durable filer writes for the namespace using the
SeaweedFS shell, e.g. `fs.configure -locationPrefix=/swblock/ -collection=swblock -fsync -apply`,
and retain that configuration. The catalog path must fall under the configured
prefix. A different namespace needs its matching rule. The default is a read-only JSON preview:

```sh
sw-block ops chunk gc --chunk-filer 127.0.0.1:18888 \
  --chunk-path /swblock/chunks --master 127.0.0.1:9090
# First applied mark records candidates; it does not reclaim newly seen keys.
sw-block ops chunk gc --chunk-filer 127.0.0.1:18888 \
  --chunk-path /swblock/chunks --master 127.0.0.1:9090 --apply
# Repeat --apply after at least --grace (default 1h).
```

`--master` is the blockmaster gRPC endpoint, not the SeaweedFS master. The
`EvidenceService.QueryVolumeStatus` reply must identify the requested volume and
an assigned, nonzero epoch. Unknown authority retains capture intents. Expiry
still requires both the higher issued epoch and a higher actual remote HEAD,
plus `--intent-ttl` (default 1h). No lease-expiry inference is used.

The JSON `roots` array identifies HEADs, snapshots, snapshot attempts, capture
intents and `retained_copy_intent` roots with their keys. `would_delete` lists
mature unrooted key/fid pairs; `would_expire_intents` names eligible capture
intents. `Reclaimed`, `ExpiredIntents`, `settled_copy_intents` and `mark_saved`
describe actual effects. A preview does not settle copy intents, expire capture
intents, write marks or delete blobs. Consequently a copy that an apply can
settle may still appear as a retained root in the preceding preview.

Independent apply calls persist two-mark coverage in `_GC_MARK` under the
catalog namespace. A conditional in-progress mark invalidates old coverage
before effects; only a completed pass saves fresh coverage. A failed scan after an acknowledged in-progress write does not save completed
coverage. Configuration changes cannot reuse prior coverage. A mark write can
commit and then lose its response: an error does **not** prove that remote
coverage is absent or unchanged. Do not edit or copy this state between namespaces. All filer/master/
blob calls share `--timeout` (default 1m). Grace must exceed the catalog operation
deadline (5m) plus the measured mark duration. Increase timeout only if the pass
still fits that safety margin. Permanent attempt, HEAD and key tombstones are
never removed by this command. Pre-binding needle enumeration remains unknown.

The CLI adds no storage lock edge: the collector retains its local mutex and
uses the existing remote APIs. The persisted mark uses filer conditional writes,
not a new WAL/publication/shard lock. Concurrent apply calls can conflict and
report an error. Callers must keep the failure status, but must not infer that
all effects were rolled back or that no completed mark exists remotely.

Errors after GC execution begins return `{code,message,retryable,gc}` with the
known partial counters/root report preserved in `gc`. `mark_saved=true` means
the completed-write response was acknowledged, not merely attempted.
`mark_outcome` describes this invocation: `not_attempted`, `in_progress`
(acknowledged), `complete` (acknowledged), or `unknown` (write response lost or
failed; remote outcome not established). `unknown_outcomes` names incomplete
stages (`in_progress_mark_write`, `copy_intent_settlement`, `reclamation`, or
`completed_mark_write`). A stage may have partially taken effect even though
its acknowledged counters are lower; for example a blob delete may succeed
then lose its reply. Do not interpret the counters as an upper bound or assume
that retries start from a pristine store. The next invocation reads the actual
conditional mark and authoritative roots as usual.

The opt-in end-to-end test invokes the actual CLI entrypoint against a real
blockmaster runtime and real SeaweedFS master/volume/filer processes:

```sh
CHUNKSTORE_WEED=/path/to/weed go test -tags chunkstore_integration \
  ./cmd/sw-block -run '^TestIntegration_OpsChunkGC_RealMasterAndStore$' \
  -v -count=1 -timeout=180s
```

This test requires the binary and fails rather than skips when it is absent. It configures and reads back the filer rule `fs.configure -locationPrefix=/swblock/ -collection=swblock -fsync -apply` before creating catalog data.
It advances only the GC clock to exercise grace admission deterministically;
network services, conditional metadata writes, authority RPC and blob deletion
are real. It is not an elapsed-time soak or a lab qualification.
