# Holder identity and pre-release upgrades

Assignments and workspace lease requests carry the holder's server id as well
as its replica id. Replica ids and loopback addresses may be reused on another
host, so they do not identify the holder by themselves. A volume refuses an
assignment with a missing or different server id, or different carried data or
control endpoints. The master refuses a workspace lease request whose server
id is missing or does not match the assigned holder.

Upgrade the master first, then the volumes, using binaries from the same
release. A new volume connected to an older pre-release master stays not
serving until the master is upgraded. An old volume does not gain the new
assignment guard merely because its master has been upgraded. There is no
compatibility flag or endpoint-based fallback.

This affects pre-release version mixing; it does not change the local volume
or remote HEAD format.
