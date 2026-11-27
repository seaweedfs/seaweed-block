# Replicated volumes in v1.0: a preview

Replicated volumes (`replication_factor` above 1, standalone and Kubernetes RF3) ship in v1.0 as a preview. The
single-replica volume is the supported configuration. In the acknowledging modes (`sync-quorum`, `sync-all`),
acknowledged writes read back intact in every run behind this note; the default `best-effort` can lose the writes
acknowledged last in a failover. v1.0 is two designs: the single-replica volume with the object store (`walstore-chunk`: snapshots, restore,
moves, object-store copy), and replicated volumes on local stores, which have none of those (`walstore-chunk`
refuses replication at start and on any assignment with replica slots). The limits are about availability under a
writing guest:

1. **A replica that misses writes is not caught up by itself.** Under steady writes the primary retains only a few
   log entries beyond its checkpoint; a replica that falls behind (restarted, or fenced by a failover) needs a
   rebuild, and a rebuild onto its retained data is refused in v1. It stays degraded until the operator wipes its
   data directory and re-adds it (not a tested procedure in this preview; an interrupted re-add needs another wipe).
   `--wal-retention-lsns` does not help in this release. On Kubernetes there is no remedy in this preview.
2. **A durability shortfall reaches the guest as an immediate I/O error.** When the required replicas cannot
   acknowledge (`sync-quorum`: a quorum, `sync-all`: all), the write fails at once (`EIO`). With three replicas and
   `sync-quorum`, one left-out replica plus one more fault loses the quorum; one failover can be enough.
3. **`sync-all` refuses writes while any replica is down.**
4. **No path failover.** A target that is no longer the primary answers with a status the guest does not retry, so
   there is no multipath failover in v1.0; the guest is started again on the new primary.
5. **Under `best-effort`, a replica can fall behind while it shows `healthy`.** A `best-effort` write waits for no
   replica, so a replica whose connection dropped can keep showing `healthy` while it misses writes. Use `sync-quorum`
   when the replicas must hold the data.

**Planned for v1.0.1.** Supported: RF3 `sync-quorum` installed on hosts (writes continue through one replica's
fault, and that replica returns by itself), and RF2 `sync-quorum` with its limit (no acknowledged write is lost and
failover is automatic, but writes stop while either replica is out). `sync-all` and Kubernetes RF3 stay a preview. In
every replicated configuration, an old primary cut off from blockmaster by a partition may keep serving reads of
older data until its lease ends.

**Carrying over.** No change to the store format is planned for the release that lifts the preview, so a replicated
volume created under it is expected to carry over without re-creating it. That release's design is still under
review, so this is a plan, not a guarantee.

What to do: see [replicated volumes (preview)](../operator-guide.md#replicated-volumes-preview) in the operator guide.
