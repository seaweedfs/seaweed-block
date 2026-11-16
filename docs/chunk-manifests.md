# Chunk upload and manifest format

`blockvolume --durable-impl=walstore-chunk` uploads dirty chunks in parallel.
`--chunk-upload-concurrency` defaults to 8 and accepts 1 through 64; 1 selects
serial upload. The setting applies to both capture and pre-copy. Segment size
is a format constant: each segment covers one logical GiB.

HEAD format 1 remains readable. A volume's next cut upgrades it to format 2 in
one ordinary fenced root CAS (including an otherwise no-op cut); generation
advances once, with the ordinary epoch/holder/LSN checks. Old binaries cannot
read format 2: deploy format-aware readers and GC before publishing new cuts.
Downgrading a volume after its first format-2 cut is unsupported.

A format-2 HEAD contains lineage, chunk geometry/count, and immutable segment
references, instead of a flat chunk map. Each segment contains a sparse map of
chunk indices and SHA-256 references; missing indices mean ZERO. Unchanged
ranges retain their exact segment keys. Data and segment keys are protected by
cut intents before upload. A root is published only after its objects exist;
an unknown root CAS retains the intents until reconciliation.

Segment keys are owner-qualified (`@<volume>/seg/<nonce>`); a segment's chunk
references retain their source namespace. Snapshot/fork copies only the root.
The fork's root alone keeps inherited segments/data live after deleting its
parent snapshot. There is no separate segment claim or segment publication
tombstone. Snapshot attempt tombstones and never-reused volume ids are
unchanged. Reclaimed immutable key bindings keep the existing GC marker that
prevents rebinding an old key.

GC traces HEADs, snapshot attempts and copy intents through their verified
segments to data. It also marks segment objects themselves. A published
segment that cannot be read or verified refuses the entire mark, in dry-run
and apply; no chunks are deleted after an incomplete mark. Capture intents
protect prospective keys directly, including not-yet-uploaded segments. The
existing higher-epoch plus TTL rule, copy-target tombstone settlement, two
marks separated by grace, upload age floor and unbound-needle retention remain.
A collector adapter without blob reads can collect flat roots but refuses a
segmented root rather than omitting its children.

## Locks and cancellation

The existing cut/publication serialization is unchanged. Capture buffers and
staged versions are selected as before; upload workers acquire no WAL,
publication, shard or capture-state lock. Only the caller updates maps and
counters after joining workers. The worker queue mutex guards admission and
first error only, with no I/O under it. Failure cancels new admission and joins
all owned calls before clean-abort intent cleanup. A remote ignoring
cancellation can delay that join; it is never reported settled early.

## Reproducible proxy measurements

`go test -tags=manifest_perf ./core/frontend/chunkstore -run '^TestManifestPerf_' -count=1 -timeout=600s -v`

This materializes and fsyncs immutable files, including all 10 GiB of the large
volume. Metadata uses the deterministic filer model. It compares 64 dirty
4 MiB chunks at concurrency 1/8 in alternating order (three samples each), and
flat versus segmented snapshot/fork operations (three samples each). Reported
root bytes exclude segment bytes, which are reported separately. It excludes
network latency, the block frontend, guest fsync and power-loss behavior.
