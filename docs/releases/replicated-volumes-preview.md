# Replicated volumes in v1.0: a preview

Replicated volumes (`replication_factor` above 1, standalone and Kubernetes RF3) ship in v1.0 as a preview. The
single-replica volume is the supported configuration. In every run behind this note, acknowledged writes read back
intact; the limits are about availability under a writing guest:

1. **A replica that misses writes is not caught up by itself.** Under steady writes the primary retains only a few
   log entries beyond its checkpoint; a replica that falls behind (restarted, or fenced by a failover) needs a
   rebuild, and a rebuild onto its retained data is refused in v1. It stays degraded until the operator wipes its
   data directory and re-adds it.
2. **A durability shortfall reaches the guest as an immediate I/O error.** When the required replicas cannot
   acknowledge (`sync-quorum`: a quorum, `sync-all`: all), the write fails at once (`EIO`). With three replicas and
   `sync-quorum`, one left-out replica plus one more fault loses the quorum; one failover can be enough.
3. **`sync-all` refuses writes while any replica is down.**
4. **No path failover.** A target that is no longer the primary answers with a status the guest does not retry, so
   there is no multipath failover in v1.0; the guest is started again on the new primary.

What to do: see [replicated volumes (preview)](../operator-guide.md#replicated-volumes-preview) in the operator guide.
