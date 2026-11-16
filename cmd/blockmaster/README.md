# Cluster imports and desired volumes

With `--lifecycle-store`, authority publication requires a lifecycle VolumeRecord.
A placement intent or a remote chunk catalog `VOLUME` alone is not that record.

`--cluster-spec` registers each volume with positive `size_bytes` and its
`replication_factor` before importing placement and starting publication. The
registration is durable and idempotent. A different size/RF from an existing
record refuses startup; existing protocol/transport and Kubernetes metadata are
preserved. Volume registration records desired state, not authority or an epoch.

For a legacy cluster spec without `size_bytes`, call the LifecycleService
`CreateVolume` RPC (the CSI create path also does this) before expecting the
imported placement to publish. `--lifecycle-placement-seed` is placement-only:
it has no capacity field, so the same CreateVolume prerequisite applies.
The master refuses missing records with the volume ID and this instruction;
it does not infer capacity or bypass the publication guard.

Legacy `--topology` without a lifecycle store keeps its existing behaviour.
