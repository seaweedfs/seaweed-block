# Chunk volumes need their own collection

**Behaviour change.** A `walstore-chunk` blockvolume and `sw-block ops chunk gc`
now refuse to run without an explicit `--chunk-collection`. Chunk needles are
referenced only by sw-block's own records, so SeaweedFS maintenance that does
not know about them (`volume.fsck` with a delete flag, a TTL, deleting a
collection) can remove live data. A collection used only by sw-block is what
keeps that maintenance away from them. A filer default or a filer storage rule
does not count: only the flag does.

- blockvolume without the flag exits 78 (the shipped units do not restart it)
  before it creates anything, with `blockvolume: chunk volumes need a
  collection used only by sw-block: set --chunk-collection <name>
  (SWB_CHUNK_COLLECTION in the unit's env file); nothing was created`.
- With the flag, blockvolume logs `blockvolume: chunk collection "<name>"`
  at start.
- `sw-block ops chunk gc` without the flag fails with exit 2 and
  `--chunk-collection is required` before it dials the filer.

## Upgrading

Existing deployments must add the flag before they upgrade blockvolume:

1. Set `SWB_CHUNK_COLLECTION=<name>` in each host's blockvolume env file. Use
   the same name on every host and in the `sw-block ops chunk gc` command.
2. Volumes created before this change wrote their needles to the filer's
   default collection (or the collection a storage rule chose). Those needles
   stay where they are and are still read and reclaimed by GC; only new
   needles go to the named collection. Keep the same cluster maintenance rules
   for the old collection until the volumes that use it are deleted.
3. Add `--chunk-collection <name>` to every scripted `sw-block ops chunk gc`.
