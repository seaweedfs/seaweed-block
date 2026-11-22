# One live process per volume per host

A blockvolume with a durable root takes an exclusive, non-blocking lock on
`/run/sw-block/locks/<volume id>.lock` before it opens the volume's store, and
holds it until the volume closes. A second blockvolume for the same volume on
the same host is refused before it opens anything, whatever its
`--durable-root`. The lock is released when the process exits, including on
a crash or kill.

The directory is not created by blockvolume. Install the tmpfiles.d entry
shipped in [`deploy/systemd/sw-block.tmpfiles.conf`](../../deploy/systemd/sw-block.tmpfiles.conf):

```bash
sudo install -m 0644 deploy/systemd/sw-block.tmpfiles.conf /etc/tmpfiles.d/sw-block.conf
sudo systemd-tmpfiles --create /etc/tmpfiles.d/sw-block.conf
```

It creates `/run/sw-block/locks` as `0700`, owned by `sw-block`, at every
boot. Without it blockvolume exits at start with `host volume lock directory
missing ... install the tmpfiles.d entry or create the directory`. Both
shipped units allow writes to it (`ReadWritePaths=-/run/sw-block/locks`)
under `ProtectSystem=strict`. If you run blockvolume as another user, give
that user the directory instead.

## A rolled-back copy of a root is refused

An existing local root attaches only if its local durable LSN reaches the
`remote_lsn` of the HEAD it attaches under. A copy of a root taken before
a later cut (a backup, a snapshot of the disk, an older replica of the
directory) is below that HEAD: it is refused with `ErrChunkStaleLocalRoot`,
never serves, and its process exits 78 (no automatic restart) with the root
kept. Remove or quarantine the local root to attach fresh from HEAD.

## Do not

- Do not start a second blockvolume for a volume on any host while one runs.
  The lock covers one host only. Two hosts are kept apart by the master's
  assignment and the workspace lease, not by this lock.
- Do not restore a local root from a copy. A copy taken after the root's last
  cut has the same LSN as HEAD, attaches, and loses the writes the original
  acknowledged after the copy was taken (in its local WAL, not yet cut). No
  local check can see that. Restore from the remote image (the sealed or
  latest HEAD).
- Do not copy or move a local root to another host or another
  `--durable-root` and start it there while the original may still run.
- Do not delete `/run/sw-block/locks` or its lock files while blockvolume
  runs: a new process would then get a different lock file and not see the
  running one.
- Do not run blockvolume in containers that do not share the host's
  `/run/sw-block/locks`. In a container the lock is per host only if that
  directory is the host's (a hostPath mount). The Kubernetes renderer
  (`core/launcher/k8s_renderer.go`) and `deploy/k8s` manifests do not mount
  it yet: there, the lock is per pod.
