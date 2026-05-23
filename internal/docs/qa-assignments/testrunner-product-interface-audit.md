# TestRunner ↔ Product Interface Audit

Date: 2026-05-23

This audit answers: "for creating various installation conditions and
operations / accidents, what does the testrunner offer today, what do we
compose by hand, and where are the gaps?"

## TL;DR

The runner has a much richer action vocabulary than current scenarios use.
Every existing scenario in `testops/scenarios/` composes faults and K8s ops
through raw `exec: bash + kubectl + sudo` strings. None of them use the
runner's chaos tier (`corrupt_wal`, `inject_netem`, `inject_partition`,
`fill_disk`, `clear_fault`), K8s tier (`kubectl_apply`, `kubectl_delete_pod`,
`kubectl_wait_condition` etc.), or the block-engine tier (`v3_status`,
`v3_wait_primary`, `v3_assert_no_authority_leak`, `poll_shipper_state`,
`measure_recovery`).

On the product side, the only test hook is `-t0-print-ready` on all three
daemons (blockmaster / blockvolume / blockcsi). There is no
fault-injection endpoint, debug RPC, or test-only verb on `sw-block ops`.
Read-only ops is the boundary the product has held since Phase 22.

## Runner Action Inventory (45 actions across 5 tiers)

| Tier | Actions |
|---|---|
| **core** | exec, sleep, print, assert_equal, assert_contains, assert_greater, assert_status, collect_path, collect_results, ctr_load, docker_build, go_build, grep_log, image_digest, pre_run_cleanup, validate_replication, bench_compare, bench_stats, benchmark_postcheck, benchmark_preflight, benchmark_report, fio_parse |
| **block** | iscsi_discover, iscsi_login, iscsi_login_direct, iscsi_logout, iscsi_cleanup, fsck_ext4, fsck_xfs, fio_json, nvme_connect, nvme_connect_direct, nvme_disconnect, nvme_disconnect_all, nvme_cleanup, nvme_id_ctrl, nvme_id_ns, nvme_get_device, nvme_read_ana_log, nvme_assert_subsystem, poll_shipper_state, measure_rebuild, measure_recovery, validate_recovery_regression, v3_status, v3_wait_primary, v3_assert_no_authority_leak, assert_no_active_iscsi_sessions, assert_no_processes |
| **devops** | v3_apply_cluster_spec, v3_start_blockcsi, v3_start_blockmaster, v3_start_blockvolume |
| **chaos** | corrupt_wal, fill_disk, inject_netem, inject_partition, clear_fault |
| **k8s** | kubectl_apply, kubectl_assert_exists, kubectl_assert_not_exists, kubectl_delete, kubectl_delete_pod, kubectl_exec, kubectl_get_condition, kubectl_get_field, kubectl_label, kubectl_logs, kubectl_pod_ready_count, kubectl_rollout_status, kubectl_set_image, kubectl_wait_condition |

**Actually used in `testops/scenarios/`:** `exec`, `grep_log`, `assert_greater`,
`assert_no_active_iscsi_sessions`, `assert_no_processes`, `pre_run_cleanup`,
`collect_path`. **7 of 45.**

## Coverage Matrix - What We Can Express

| Need | How today | Quality |
|---|---|---|
| **Install conditions** | | |
| RF=1/2/3 | helm values knob | direct |
| Single-node vs multi-node | helm values + chart compat | direct |
| Loopback vs external iSCSI | helm values | direct |
| CHAP on/off | helm values | direct |
| Stage 2 multipath | helm values | direct |
| Image SHA pin | helm values + scenario env | direct |
| Compat flag toggles | helm values | direct |
| Custom node count | chart blockNodes array | direct |
| **Operations** | | |
| Install / uninstall | `exec: helm install/uninstall` | composed (could be `helm_install` action) |
| PVC create / delete | `exec: kubectl apply/delete` | composed (could be `kubectl_apply/delete`) |
| Pod scheduling | `exec: kubectl apply` + nodeSelector | composed |
| Wait for ready | `exec: kubectl rollout status` | composed (could be `kubectl_rollout_status`) |
| Port-forward to blockmaster | `exec: kubectl port-forward` | composed |
| Read-only inspection | `exec: sw-block ops cluster/volumes` | composed (could be `sw_block_ops_cluster`) |
| Replica health check | `exec: sw-block ops status` | composed |
| **Faults / accidents** | | |
| Kill primary blockvolume | `exec: kubectl scale --replicas=0` | composed |
| Pod delete | `exec: kubectl delete pod` | composed (have `kubectl_delete_pod`!) |
| Node cordon / drain | `exec: kubectl cordon` | composed |
| Process kill on host | `exec: ssh ... pkill` | composed |
| iSCSI session drop | `exec: iscsiadm logout` | composed (have `iscsi_logout`!) |
| Network partition | NOT USED | **chaos tier has `inject_partition`** |
| Network latency / loss | NOT USED | **chaos tier has `inject_netem`** |
| WAL corruption | NOT USED | **chaos tier has `corrupt_wal`** |
| Disk full | NOT USED | **chaos tier has `fill_disk`** |
| Fault cleanup / restore | NOT USED | **chaos tier has `clear_fault`** |
| Stale-I/O write probe | NOT POSSIBLE | needs new action |
| ALUA AAS parse + assert | bash via sg_rtpg | composed (D6) |
| SCSI sense-key assert | bash via sg_raw | composed |
| **Cleanup verification** | | |
| iSCSI session residue | `assert_no_active_iscsi_sessions` | direct |
| Process residue | `assert_no_processes` | direct |
| K8s residue | `exec: kubectl get` + bash | composed |
| Multipath residue | NOT CHECKED | open gap (Phase 27 follow-up) |
| Host-path residue | bash via cleanup-summary | composed |

## What's Working Well

1. **Install knobs are first-class** through helm values. Chart accepts every
   distinct install condition we need; scenarios pass them via
   `generate-helm-values` flags or explicit values file overrides.

2. **Read-only ops surface composes well** in `exec` strings - we get rich
   per-volume JSON from `sw-block ops cluster -o json`, primary identity from
   `sw-block ops volumes`, ManagedVolume conditions from `sw-block ops
   explain`, etc.

3. **Cleanup primitives** (`assert_no_active_iscsi_sessions`,
   `assert_no_processes`) are named, semantic, and used everywhere.

4. **Chart-vs-image compat machinery** is solid - the
   `compat.launcherRejectLoopbackFlag` /
   `compat.launcherReplicationAckFlag` pattern lets us run old images with
   new charts.

## Gaps / Friction Points

### G1. Chaos tier exists but is dormant

The runner ships with `corrupt_wal`, `fill_disk`, `inject_netem`,
`inject_partition`, `clear_fault`. **No scenario uses them.** Every fault
today is a bash-composed `kubectl scale --replicas=0` or `pkill`. We are
leaving real fault models on the table:

- **Network partition** between blockmaster and a blockvolume replica - is
  promotion still safe? Does the partitioned replica refuse to serve I/O?
- **Network latency injection** on the WAL shipper path - does the primary
  ack-quorum behavior tolerate slow replicas?
- **WAL corruption** - does the engine refuse to replay corrupted WAL and
  emit a clean recovery failure?
- **Disk full on a replica** - is the failure surfaced through the
  ManagedVolume model as a blocker condition?

These are real product claims that have never been tested as scenarios.

### G2. K8s tier unused; raw `exec: kubectl ...` everywhere

Every scenario has multi-line bash strings like:

```yaml
cmd: "for i in 1 2 3; do kubectl -n default delete pod sw-block-multi-writer-$i sw-block-multi-reader-$i --ignore-not-found=true --wait=true --timeout=120s; ..."
```

We have `kubectl_delete_pod`, `kubectl_wait_condition`, `kubectl_apply`,
`kubectl_rollout_status` available. Using them would shrink scenarios,
improve readability, and let the runner expose richer diagnostics on
failure (current bash failures often surface as `exec: code=1 stderr=` with
no detail).

### G3. v3 engine actions unused

The block tier has `v3_status`, `v3_wait_primary`,
`v3_assert_no_authority_leak`, `poll_shipper_state`, `measure_rebuild`,
`measure_recovery`, `validate_recovery_regression`. The Phase 27 D2/D3/D4
scenarios manually poll `sw-block ops volumes -o json` in bash to detect
promotion - `v3_wait_primary` likely does this natively. `v3_assert_no_authority_leak`
is exactly what we want for cross-volume non-interference assertions.

### G4. No actions for the Phase 27 D5/D6 verification claims

- **D5 stale-I/O fence**: need `iscsi_assert_write_rejected` or
  `sg_write_assert_fail` - login to old primary path, attempt SCSI WRITE,
  assert sense key `ABORTED_COMMAND` or `NOT READY`.
- **D6 ALUA RTPG transition**: need `assert_alua_aas_transition` -
  pre/post sg_rtpg parse + transition check.
  Currently both are composed in `scripts/run-multi-volume-mounted-failover.sh`
  and exposed only as text fields in the per-volume summary.

### G5. Multipath residue not in any assertion

`assert_no_active_iscsi_sessions` and `assert_no_processes` exist; no
`assert_no_multipath_maps`. The Phase 27 close report flagged 2 stale
mpath maps; the only way to assert that is bash.

### G6. No product-side fault injection

The product binaries have only one test hook (`-t0-print-ready`). For some
faults (clock skew, lease expiration, write-stall on a specific replica)
the only way to inject them externally is approximate. Examples that would
benefit from a controlled product knob:

- Force a replica into "degraded" state without killing its process
  (today: kill the process, but that's a different fault model).
- Force the master to delay promotion to test workload tolerance.
- Inject WAL ack timeout on a specific replica.

These all violate the read-only-CLI discipline if added to user-facing
binaries. The cleaner shape would be a separate `sw-block-test-agent` /
gRPC sidecar built only into test images, controlled by a runner action
`sw_block_test_inject_fault`. That's a bigger commitment than scenario
authoring.

### G7. Multi-node coordinator mode untested

The runner exposes `sw-test-runner coordinator` + `agent` for multi-node
test execution, but every current scenario runs everything from m02
(single SSH target). For real multi-volume cases with app pods spread
across nodes (Phase 27 D8) and per-node fault injection, the coordinator
mode would be the right tool. Untested today.

## Recommendations

Ordered by ROI:

### R1. Adopt the K8s and chaos tiers in Phase 27 D5+ scenarios

When D5/D6 scenarios are authored, prefer named actions over bash `exec`:

- `kubectl_delete_pod` for forced primary kill (with `--grace-period=0
  --force` knobs exposed).
- `kubectl_wait_condition` for promotion-readiness polling instead of bash
  retry loops.
- `inject_netem` for slow-replica timing tests.
- `inject_partition` for partition-tolerance verification (independent of
  Phase 27 - this is a new D9-class scenario).
- `corrupt_wal` for engine-level recovery refusal tests.

### R2. Add three new actions in the block tier

- `iscsi_assert_write_rejected` (covers D5).
- `assert_alua_aas_transition` (covers D6).
- `assert_no_multipath_maps` (covers Phase 27 cleanup follow-up).

These are bash one-liners today; promoting them to named actions makes the
assertion intent explicit in the YAML and lets the runner give better
diagnostics on failure.

### R3. Wire `v3_wait_primary` + `v3_assert_no_authority_leak` into existing scenarios

Replace the manual `sw-block ops volumes -o json | jq` polling. Same
behavior, half the bash. Especially valuable for the D4 interleaved case
where two volumes need parallel promotion-readiness waiting.

### R4. Spike the chaos tier with a single scenario each

Write four small scenarios:

- `chaos-wal-corruption-refuses-replay.yaml` (uses `corrupt_wal` +
  `assert_status`)
- `chaos-disk-full-emits-blocked-condition.yaml` (uses `fill_disk` +
  ManagedVolume condition assert)
- `chaos-network-partition-rejects-stale-writes.yaml` (uses
  `inject_partition`)
- `chaos-network-latency-tolerated-by-quorum.yaml` (uses `inject_netem`)

Each gives us one concrete data point on whether the chaos primitive
actually works against the product and what observable evidence the
ManagedVolume model surfaces. Cheap to write, high signal.

### R5. Do NOT add a product-side fault injection RPC for now

The cost (compromising the read-only boundary, even behind a build flag)
outweighs the benefit. Most fault models can be expressed with the chaos
tier + K8s tier + iscsi/nvme primitives. Revisit only if a specific
claim cannot be tested any other way.

### R6. Park the coordinator-mode adoption

The single-SSH-target shape (everything from m02) works for the current
gates. Coordinator mode is the right tool when we get to N-node
geographically distributed faults; not needed for Phase 27 D5-D8.

## Concrete Phase 27 D5/D6 Implementation Hint

If/when dev implements D5 (stale-I/O probe), the cleanest shape is:

```yaml
- action: iscsi_login_direct
  node: m02
  portal: "{{ old_primary_portal }}"
  iqn: "{{ old_primary_iqn }}"
  save_as: old_path_dev
- action: iscsi_assert_write_rejected   # NEW action
  node: m02
  device: "{{ old_path_dev }}"
  expected_sense_key: "ABORTED_COMMAND"
  save_as: stale_write_blocked
- action: assert_equal
  actual: "{{ stale_write_blocked }}"
  expected: "1"
```

vs the current shape:

```yaml
- action: exec
  node: m02
  cmd: "<150-line bash> ... echo old_primary_stale_io_success_count=0"
- action: grep_log
  pattern: "^old_primary_stale_io_success_count=0$"
```

The first one self-documents the claim. The second only documents the
script's behavior.

## Summary Table

| Category | Today | Improvement |
|---|---|---|
| Install conditions | well-covered via helm values | no change needed |
| Read-only observability | well-covered via `sw-block ops` | no change needed |
| Common K8s ops | bash via `exec` | adopt k8s tier (R1) |
| Common faults | bash via `exec: kubectl scale/delete` | adopt k8s tier + chaos tier (R1, R4) |
| Stale-I/O verification | hardcoded `=0` | new `iscsi_assert_write_rejected` (R2) |
| ALUA AAS transition | text grep only | new `assert_alua_aas_transition` (R2) |
| Multipath residue | not asserted | new `assert_no_multipath_maps` (R2) |
| Engine-level recovery assertions | unused | adopt `v3_*` actions (R3) |
| Network / WAL / disk faults | NOT POSSIBLE | chaos tier exists, adopt it (R1, R4) |
| Product-side fault injection | none | keep none, use chaos tier instead (R5) |
