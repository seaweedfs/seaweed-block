# Stage 2 Transparent Multipath Host Failover Spec

Status: D1 cold product spec for
`current-plan.md` Stage 2 Transparent Multipath Host Failover MVP.

## Product Question

Can a Kubernetes user keep a mounted Seaweed Block PVC usable through controlled
primary failure without pod recreate, using standard Linux multipath behavior,
and understand the result from product evidence rather than internal logs?

## First Protocol Choice

Stage 2 closes iSCSI first:

```text
iSCSI ALUA + Linux dm-multipath
```

Reason:

- iSCSI is the current alpha default frontend.
- The user-facing quickstart and operations manual already center on iSCSI.
- Prior protocol work already produced ALUA/MPIO lab evidence in
  `iscsi-p6-alua-mpio-design.md`.
- It avoids mixing two hard changes: protocol selection UX and multipath
  recovery.

NVMe ANA remains the next protocol path. It has strong lab evidence in
`nvme-p4-multipath-failover-design.md`, but Kubernetes CSI protocol selection
and NVMe native multipath should be a separate close gate.

## User-Visible Contract

Given:

```text
topology=alpha Kubernetes, same node allowed for first Stage 2 gate
protocol=iscsi
host_multipath=dm-multipath configured and detected
replication=RF3 sync-quorum
ack_profile=sync-quorum
claim_profile=stage2-iscsi-alua-multipath
```

When:

```text
an app pod has a mounted PVC
the writer has verified /data/demo.bin
the current primary blockvolume is stopped in a scoped way
master promotes a candidate that covers the required frontier
```

Then:

```text
Linux host path changes through dm-multipath
old primary path cannot acknowledge stale data I/O
the mounted workload verifies /data/demo.bin without pod recreate
support bundle names authority, ALUA path state, multipath state, and data check
all attach/mount/path-switch/I/O waits are bounded and produce blocker reasons
```

If dm-multipath is unavailable, ALUA is not visible, paths do not merge into one
device, or the workload only recovers after pod recreate, Stage 2 must fail
closed. It may produce a useful blocker bundle, but not a Stage 2 recovery
claim.

## Required User Evidence

Minimum evidence in the artifact bundle:

```text
multipath-prereq.txt
iscsi-discovery.txt
sg-inq.txt
sg-vpd83.txt
sg-rtpg.before.txt
multipath-before.txt
writer.log
pre-failure-inventory/
primary-failure.txt
authority-after.txt
sg-rtpg.after.txt
multipath-after.txt
workload-after-failover.log
post-failure-inventory/
support-bundle/
bounded-waits.txt
cleanup-audit.txt
```

Required stable lines:

```text
protocol=iscsi
multipath_enabled=true
host_device=<dm-* or /dev/mapper/...>
path_count_before>=2
path_count_after>=1
active_path_before=<portal/replica>
active_path_after=<portal/replica>
before_primary_replica=<rN>
failed_replica=<same rN>
promoted_replica=<rM>
post_failure_primary_count=1
old_primary_stale_io_success_count=0
data_check_after_failover=mounted_workload_checksum_passed
pod_recreate_used=false
bounded_waits=pass
blocked_reason=none
```

## Non-Negotiable Semantics

- **Authority boundary**: master chooses/promotes primary; CSI, iSCSI, and
  multipath do not elect authority.
- **Durability boundary**: promotion candidate must cover the required
  sync-quorum frontier, same as Stage 1 RF3.
- **Protocol boundary**: ALUA state reflects frontend/authority facts; it does
  not make unsafe replicas writable.
- **Host boundary**: Linux must see one multipath device with multiple paths
  for the same volume identity. Two independent `/dev/sdX` devices do not
  satisfy Stage 2.
- **Mounted I/O boundary**: the same mounted workload path must verify data
  after failure. Recreating the pod is Stage 1, not Stage 2.
- **Fencing boundary**: old primary path must not return successful stale data
  I/O after authority moves.
- **Reliability/observability boundary**: Kubernetes attach, mount, ALUA/RTPG
  read, multipath map creation, authority movement, stale-primary fencing, and
  post-failure workload I/O must all have explicit timeouts. A hung wait is a
  product failure and must surface as a blocker reason in the bundle.

## Bounded Failure Classes

Stage 2 must name these classes in support bundles instead of timing out
silently:

```text
attach_timeout
iscsi_login_timeout
multipath_map_timeout
alua_state_unavailable
authority_promotion_timeout
path_switch_timeout
stale_primary_fence_timeout
post_failure_io_timeout
cleanup_timeout
```

Each class must include:

```text
resource=<pvc|pod|iqn|portal|multipath-device|replica>
timeout=<duration>
last_observed_state=<short stable string>
next_action=<operator-readable hint>
```

## Current Code Gaps

### CSI Publishes One Target

Current `core/csi/controller.go` and `core/csi/master_backend.go` return one
`PublishTarget`. `core/csi/node.go` stages one iSCSI portal/IQN and formats the
device returned by `GetDeviceByIQN`.

Stage 2 needs a multi-path publish shape:

```text
volume -> same IQN / stable device identity
paths  -> multiple portals with path role/state
node   -> login all eligible paths before mounting the multipath device
```

Partial implementation status: NodeStage now understands a deliberate
`stage2_multipath=true` publish context with multiple `iscsiAddrs`. It refuses
fewer than two portals, logs in all portals, and mounts the multipath device
reported by the iSCSI utility. CSI control lookup can convert multiple iSCSI
frontend facts with the same IQN into that publish context, but only through an
explicit Stage 2 opt-in (`--stage2-multipath` / multipath lookup constructor).
The default CSI path remains single-target to avoid silently changing Stage 1
attach behavior before ALUA evidence is complete. Master status can now expose
all observed replica frontend facts for a volume as read-only evidence. Alpha
install scripts render the opt-in only when `SW_BLOCK_STAGE2_MULTIPATH=1`, and
the CSI node image/DaemonSet now carries the first-pass multipath tooling and
kernel-module prerequisites (`multipath-tools`, `sg3-utils`, `dm_multipath`,
`scsi_dh_alua`, `/run/udev` read-only mount).

Runner status: `testops/scenarios/stage2-iscsi-alua-multipath-baseline-chain.yaml`
now exists as the first Stage 2 baseline. It validates opt-in wiring, loaded
host multipath prerequisites, same-IQN RF=3 paths, `sg_rtpg` path-state
evidence, and targeted multipath-map cleanup. It records
`transparent_failover_claimed=false`; the mounted primary-failure/no-pod-recreate
proof remains open and should use a separate close scenario.

QA finding 2026-05-15: the first baseline runs showed the opt-in reached
NodeStage but the node staged only `127.0.0.1:3260`, leaving no multipath map.
The node path now waits for refreshed multi-portal publish evidence and emits
`multipath publish target ready portals=...` before logging into all paths. This
keeps Stage 2 fail-closed if master never exposes multiple portals.

QA/internal review timing finding 2026-05-15: host-path evidence must be captured
while a pod still holds the staged device. The baseline therefore uses the
writer-hold manifest and stops at `writer-verified`, before writer deletion or
CSI unstage, then captures `iscsi-sessions.writer-mounted.txt`,
`multipath.writer-mounted.txt`, and `sg-rtpg.writer-mounted.txt`. A post-reader
capture can be empty for the wrong reason and is not acceptable evidence of
Stage 2 wiring.

QA finding 2026-05-15: the writer-held Stage 2 path is the first baseline to run
the app as UID/GID 1000 with `fsGroup: 1000`. A permission-denied write to
`/data/demo.bin` is a CSI/Kubernetes filesystem ownership failure, not an app
failure. The alpha CSIDriver must declare `fsGroupPolicy: File`, and the
scenario must preserve mounted host-path diagnostics even if writer verification
fails.

QA evidence 2026-05-15 run `20260515-091810-6d74`: the baseline reached the
intended host boundary. It showed three iSCSI sessions to one IQN, one
dm-multipath map with `hwhandler='1 alua'`, and `sg_rtpg` states with one
active/optimized path and two standby paths. Scenario assertions should match
the always-emitted `staged transport=iscsi ... portals=... multipath=true` line;
the `multipath publish target ready` line is only present when NodeStage had to
wait for refreshed publish context.

QA evidence 2026-05-15 run `20260515-092817-61f9`: the Stage 2 baseline passed
strictly. The next scenario is
`testops/scenarios/stage2-iscsi-alua-multipath-failover-chain.yaml`, which keeps
the writer pod mounted, derives and stops the primary from inventory, and
requires the same writer pod to verify data after failure without reader pod
recreate.

### Node Stage Mounts The Raw Path Device

Current NodeStage formats/mounts the portal-specific device. Stage 2 needs to
wait for the dm-multipath device, verify it represents the volume identity, and
mount that device instead.

Partial implementation status: with `stage2_multipath=true`, NodeStage mounts
the dm-multipath device path returned by `GetMultipathDeviceByIQN`. The real
Linux utility now refreshes multipath maps with bounded subprocess calls,
resolves the IQN to raw `/dev/disk/by-path` devices, and matches those devices
to a `/dev/mapper/<map>` entry in `multipath -ll`. It still needs lab
validation against M02 artifact output.

### ALUA Must Be Wired To Product Facts

The iSCSI ALUA protocol substrate exists as prior work, but the Kubernetes
path must prove:

- current primary reports active optimized,
- valid non-primary path reports standby or active non-optimized according to
  the selected policy,
- old primary becomes unavailable/transitioning after promotion,
- data I/O on non-writable paths fails closed.

### Inventory Must Explain Host Path

Current inventory explains PVC, replicas, frontend endpoints, status bundles,
and promotion evidence. Stage 2 additionally needs host-path evidence:

- multipath device,
- path count,
- active/standby/unavailable path states,
- selected active path before and after failure,
- whether pod recreate was used.
- bounded wait outcome and blocker reason, if any.

### K8s Alpha Uses Loopback Frontends

The first gate may still be same-node alpha, but it must not confuse that with
node-loss HA. Same-node is allowed only if Linux sees multiple real iSCSI
sessions/paths and dm-multipath owns the mounted device.

## Fast Test Targets

Before runner validation:

- CSI publish target model can carry multiple iSCSI paths for one volume.
- NodeStage rejects `stage2_multipath=true` when fewer than two paths exist.
- NodeStage mounts the multipath device, not the raw by-path device.
- iSCSI ALUA provider maps active/standby/unavailable from frontend facts.
- Old-primary path rejects data I/O after authority movement.
- Inventory summary prints multipath device, path count, active path, and
  `pod_recreate_used=false`.
- Bounded wait helper returns a stable blocker class and support-bundle line for
  attach timeout, multipath map timeout, path switch timeout, and
  post-failure I/O timeout.

## QA Gate Summary

The close gate should fail if any of these are true:

- recovery requires pod delete/recreate,
- Linux sees two independent devices instead of one multipath device,
- ALUA/RTPG state is missing or inconsistent,
- old primary can still return successful stale data I/O,
- reader checksum is obtained only after CSI re-stage,
- support bundle requires blockmaster/blockvolume logs to understand the
  outcome.
- any Kubernetes attach/mount/session/path-switch/I/O step can hang until the
  outer runner kills the scenario instead of producing a product blocker.

## Product Non-Claims Until Close

- No Stage 2 transparent failover claim.
- No NVMe ANA Kubernetes claim.
- No node-loss claim.
- No production HA/RTO claim.
- No Windows MPIO claim.
- No multi-distro compatibility claim.
