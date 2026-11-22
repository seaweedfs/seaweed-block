# Local root identity

Each walstore-chunk volume's local root has an identity: a nonce created when
the root is created (a fresh root, or one recreated after quarantine), stored
next to the volume as `<volume>.root`, and written into every HEAD the root
publishes (`holder_root`). Before each of its HEAD updates the root records
durably the generation it is about to publish (its high-water).

An existing root attaches only under a HEAD of its own lineage: the same
holder (server and replica), the same root when HEAD names one, and a
generation not above the root's high-water. Another root of the same holder,
a rolled-back copy of this root, and a copy of a root taken before its first
start on this release are refused (`ErrChunkStaleLocalRoot`); remove or
quarantine such a root to attach fresh from HEAD.

The high-water detects a rolled-back root only when HEAD has moved past it. A
copy of a root taken after its last cut carries the same high-water as HEAD,
attaches, and loses the writes the original acknowledged after the copy was
taken (in its local WAL, not yet cut). No local check can see that. Never
restore a local root from a copy: restore from the remote image (the sealed
or latest HEAD).

Upgrades: a HEAD written before this release has no `holder_root`; the same
holder's existing root adopts it once on its first start (its claim then
writes the root). A binary from before this release cannot read a HEAD that
carries `holder_root`: upgrade the master and every volume of a volume
together, with binaries from the same release. walstore-chunk volumes now
require their holder (`<server-id>/<replica-id>`); a replica id alone is
refused.
