# Current Plan: Phase 27 - Multi-Volume HA Independence

Status: complete, 100% complete. Started on 2026-05-22 after Phase 26 Helm
lifecycle hardening closed.

## Product Goal

Prove that Seaweed Block can host multiple Kubernetes PVC-backed RF=3 volumes
and eventually recover each volume independently under faults.

This phase is intentionally separate from v0.3.1. Phase 26 proved Helm lifecycle
and multi-PVC Day-1 behavior. Phase 27 raises the bar to RF=3 multi-volume
readiness and, later, multi-volume failover independence.

## Scope Contract

| In | Out |
|---|---|
| RF=3 multi-volume readiness and fault gates | v0.3.1 release claim |
| per-volume primary/replica/frontend independence | operator/CRD implementation |
| CSI reattach recovery per volume | backup/snapshot/restore |
| transparent mounted failover per volume if multipath is enabled | broad production HA |
| cross-volume non-interference evidence | performance/SLO claims |
| support-bundle/timeline evidence per volume | NVMe ANA parity |

## Claim Boundary

Allowed after Phase 27:

```text
Three RF=3 PVC-backed volumes can coexist on the 3-node lab, bind, mount,
write/read, appear as three independent ManagedVolume rows, recover
independently through CSI/pod recreate, and recover independently through
Stage 2 iSCSI ALUA/dm-multipath without pod recreate when the mounted host
path is enabled.
```

Still not allowed:

```text
Broad production HA.
Backup/snapshot/restore.
Operator-managed lifecycle.
NVMe ANA parity.
Performance or scale SLOs beyond the gated 3-volume lab.
```

## D1: RF=3 Multi-Volume Readiness

Goal: prove the placement / port-assignment / observation scheme can host
`N=3` concurrent RF=3 volumes before fault injection is attempted.

Status: PASS on 2026-05-22.

Evidence:

- Scenario: `testops/scenarios/helm-multi-volume-rf3-readiness-chain.yaml`
- Run: `20260522-170901-70c5`
- Result: PASS, 6/6 phases, 35/35 actions
- Flow:
  - Helm install completed with generated RF=3 values
  - three PVCs bound
  - three writers verified `/data/demo.bin`
  - three readers verified persisted bytes
  - report showed `volumes=3`
  - report included `rf=3` for all three volumes
  - cleanup removed all generated blockvolume Deployments, iSCSI sessions, and
    product processes
- Summary fields:
  - `multi_volume_status=ok`
  - `requested_volume_count=3`
  - `replication_factor=3`
  - `writer_verified_count=3`
  - `reader_verified_count=3`
  - `managed_volume_count=3`
  - `cleanup_status=ok`

Fix included:

- `scripts/run-multi-volume-example.sh` now supports
  `SW_BLOCK_MULTI_VOLUME_RF`.
- Multi-volume helper waits for generated blockvolume Deployments to disappear
  before declaring cleanup success, avoiding RF=3 async launcher cleanup races.

## D2: Multi-Volume CSI Reattach Recovery

Goal: for each volume in the set, kill that volume's current primary and prove
only that volume recovers through the CSI/pod-recreate path while the other
volumes remain stable.

Acceptance:

```text
N=3 RF=3 PVCs ready
for each target volume:
  primary deployment stopped
  promoted primary changes only for target volume
  post_failure_primary_count=1 for target volume
  reader checksum passes after reattach
  non-target volumes keep primary/frontend stable
  cross_interference_observed=false
cleanup clean
```

Status: PASS on 2026-05-22.

Evidence:

- Scenario: `testops/scenarios/helm-multi-volume-rf3-reattach-recovery-chain.yaml`
- Run: `20260522-223126-21fd`
- Result: PASS, 6/6 phases, 29/29 actions
- Flow:
  - Helm installed RF=3 sync-quorum stack
  - three PVCs bound and passed writer/reader setup
  - each volume's current primary Deployment was stopped in turn
  - each target volume promoted to a surviving replica
  - replacement reader verified `/data/demo.bin` after each promotion
  - non-target volumes kept primary/frontend stable during each target
    recovery
  - cleanup removed Helm resources, iSCSI sessions, and product processes
- Summary fields:
  - `multi_volume_reattach_status=ok`
  - `requested_volume_count=3`
  - `replication_factor=3`
  - `recovered_volume_count=3`
  - `cross_interference_observed=false`
  - `cleanup_status=external_to_script`
- Per-volume evidence:
  - volume 1: `before_primary=r1`, `promoted_replica=r2`,
    `post_failure_primary_count=1`, `reader_verified=true`
  - volume 2: `before_primary=r2`, `promoted_replica=r1`,
    `post_failure_primary_count=1`, `reader_verified=true`
  - volume 3: `before_primary=r3`, `promoted_replica=r1`,
    `post_failure_primary_count=1`, `reader_verified=true`

Fix included:

- Product loop now probes promotion candidates for known placement volumes that
  are excluded from the supported authority snapshot only because of
  recoverable inventory gaps such as `PartialInventory`.
- If the current primary cannot be proven healthy and a survivor passes the
  promotion probe, master emits a direct `IntentReassign` for that volume
  without relaxing unsafe evidence classes such as conflicting primary claims.
- New TDD coverage:
  `TestMountedFailover_ProductLoopRF3PromotesWhenCurrentSlotMissingButSurvivorProbeReady`.

## D3: Multi-Volume Mounted Transparent Failover

Goal: with Stage 2 iSCSI ALUA/dm-multipath enabled, keep workloads mounted and
prove each target volume can fail over without pod recreate while other volumes
continue serving.

Acceptance:

```text
N=3 RF=3 mounted workloads
pod UID unchanged for target workload
old data readable after primary stop
new data writable after failover
primary_count=1 per target volume
old_primary_stale_io_success_count=0
non-target workloads continue without interruption
cleanup clean
```

Status: PASS on 2026-05-23.

Evidence:

- Scenario: `testops/scenarios/helm-multi-volume-rf3-mounted-failover-chain.yaml`
- Run: `20260523-090534-24e4`
- Result: PASS, 8/8 phases, 47/47 actions
- Flow:
  - Helm installed RF=3 sync-quorum stack with Stage 2 multipath enabled
  - three RF=3 PVCs bound
  - three long-running writer pods mounted the PVCs on `m02`
  - each volume's current primary Deployment was stopped in turn
  - each target volume promoted to a surviving replica
  - the same writer pod UID verified old data and wrote new data after
    failover
  - non-target mounted workloads remained healthy during each target failover
  - cleanup removed Helm resources, iSCSI sessions, and product processes
- Summary fields:
  - `multi_volume_mounted_failover_status=ok`
  - `requested_volume_count=3`
  - `replication_factor=3`
  - `recovered_volume_count=3`
  - `mounted_workload_checksum_passed_count=3`
  - `pod_recreate_used=false`
  - `cross_interference_observed=false`
  - `transparent_failover_claimed=true`
- Per-volume evidence:
  - volume 1: `before_primary=r1`, `promoted_replica=r2`,
    `post_failure_primary_count=1`, `pod_recreate_used=false`
  - volume 2: `before_primary=r2`, `promoted_replica=r1`,
    `post_failure_primary_count=1`, `pod_recreate_used=false`
  - volume 3: `before_primary=r3`, `promoted_replica=r1`,
    `post_failure_primary_count=1`, `pod_recreate_used=false`

Fix included:

- New helper:
  `scripts/run-multi-volume-mounted-failover.sh`.
- New TestOps gate:
  `testops/scenarios/helm-multi-volume-rf3-mounted-failover-chain.yaml`.
- The gate uses bounded mounted-I/O checks and split cleanup actions so a host
  path stall or cleanup race becomes diagnosable evidence instead of a hung
  scenario.

## D4: Concurrent / Interleaved Multi-Volume Faults

Goal: inject overlapping primary failures on multiple volumes and prove
promotion and recovery remain per-volume isolated.

Acceptance:

```text
two target volumes fail within a bounded window
both promote independently
untouched volume primary/frontend unchanged
no dual-primary per volume
no timeline side-effect on untouched volume
cleanup clean
```

Status: PASS on 2026-05-23.

Evidence:

- Scenario: `testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml`
- Run: `20260523-093348-6b02`
- Result: PASS, 8/8 phases, 55/55 actions
- Flow:
  - Helm installed RF=3 sync-quorum stack with Stage 2 multipath enabled
  - three RF=3 PVCs were mounted by long-running writer pods on `m02`
  - two target volumes had their current primary Deployments stopped in a
    bounded interleaved window
  - both target volumes promoted independently
  - both target writer pods kept the same pod UID and verified old/new data
    without pod recreate
  - untouched volume kept primary/frontend stable and its mounted workload
    verified old/new data
  - cleanup removed Helm resources, iSCSI sessions, and product processes
- Summary fields:
  - `multi_volume_interleaved_failover_status=ok`
  - `failover_mode=interleaved`
  - `target_volume_count=2`
  - `recovered_volume_count=2`
  - `mounted_workload_checksum_passed_count=2`
  - `pod_recreate_used=false`
  - `cross_interference_observed=false`
  - `interleaved_fault_window_seconds=0.464`
  - `untouched_volume_stable=true`
  - `untouched_workload_ok=true`
- Per-volume evidence:
  - volume 1: `before_primary=r1`, `interleaved_fault=true`,
    `promoted_replica=r2`, `post_failure_primary_count=1`,
    `pod_recreate_used=false`
  - volume 2: `before_primary=r2`, `interleaved_fault=true`,
    `promoted_replica=r1`, `post_failure_primary_count=1`,
    `pod_recreate_used=false`
  - volume 3: untouched workload wrote and verified
    `/data/non-target-interleaved.bin`

Fix included:

- `scripts/run-multi-volume-mounted-failover.sh` now supports
  `SW_BLOCK_MULTI_VOLUME_FAILOVER_MODE=interleaved`.
- New TestOps gate:
  `testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml`.

## D5: Stale Primary Fencing Evidence Hardening

Goal: make stale-primary fencing evidence measured, not a scripted constant.

Status: PASS on 2026-05-23.

Evidence:

- Scenario: `testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml`
- Run: `20260523-114708-46bc`
- Result: PASS, 8/8 phases, 55/55 actions
- The mounted failover helper now probes the old primary's exact iSCSI by-path
  device with a bounded direct read. The path is scoped by both old frontend
  (`ip-<host>:<port>`) and `volume_id`, so it does not accidentally probe the
  promoted path.
- Per-target artifacts:
  - `failover/volume-1/stale-primary-probe.log`
  - `failover/volume-2/stale-primary-probe.log`
- Both target volumes produced:
  - `stale_primary_probe=direct_read`
  - `candidate_result=expected_failure`
  - `old_primary_stale_io_success_count=0`
- The scenario now asserts the probe log exists and carries the measured
  `old_primary_stale_io_success_count=0`; the summary alone is no longer
  accepted as fencing evidence.

## D6: RTPG AAS Transition Evidence Hardening

Goal: make the iSCSI ALUA evidence assert concrete RTPG asymmetric access state
values before and after failover, not just the presence of RTPG text.

Status: PASS on 2026-05-23.

Evidence:

- Scenarios:
  - `testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml`
  - `testops/scenarios/helm-multi-volume-rf3-mounted-failover-chain.yaml`
- Runs:
  - D4 interleaved: `20260523-123229-9dd4`, PASS, 8/8 phases, 55/55 actions
  - D3 sequential: `20260523-123647-2fc4`, PASS, 8/8 phases, 47/47 actions
- The mounted failover helper now records per-volume RTPG state files:
  - `failover/volume-N/rtpg-before-states.txt`
  - `failover/volume-N/rtpg-after-states.txt`
- Both interleaved target volumes produced:
  - `rtpg_before_old_primary_aas=0x00`
  - `rtpg_before_promoted_aas=0x02`
  - `rtpg_after_old_primary_aas=missing`
  - `rtpg_after_promoted_aas=0x00`
  - `rtpg_transition_verified=true`
- The scenario now asserts the per-volume state files exist and requires
  `rtpg_transition_verified=true` for each target volume. This covers all
  three sequential D3 targets and both interleaved D4 targets.

## D7: Flake Matrix Wrapper

Goal: make the Phase 27 reliability gates repeatable and measurable instead of
single-run-only.

Status: dev implementation complete on 2026-05-23; QA/nightly N>=5 pending.

Implementation:

- Added `scripts/run-phase27-flake-matrix.ps1`.
- The wrapper runs a selected TestOps scenario `N` times, stores each run under
  `iterations/iteration-NN/`, and emits:
  - `flake-summary.txt`
  - `flake-summary.json`
- Summary fields:
  - `target_runs=<N>`
  - `pass_runs=<P>`
  - `fail_runs=<F>`
  - `flake_rate_percent=<percent>`
  - one line per iteration with result, exit code, run id, and duration

Evidence:

- Smoke run: `results/phase27-d7-flake-smoke`
- Scenario: `testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml`
- Iterations: 1
- Result: PASS, `pass_runs=1`, `fail_runs=0`, `flake_rate_percent=0`
- Dev stability run: `results/phase27-d7-flake-interleaved-n3`
- Scenario: `testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml`
- Iterations: 3
- Result: PASS, `pass_runs=3`, `fail_runs=0`, `flake_rate_percent=0`
- Iteration run IDs:
  - `20260523-124555-691c`
  - `20260523-124832-7d5c`
  - `20260523-125104-532c`

Release-grade target:

- Run D3 and D4 at least `N=5` each in nightly or QA-owned validation.
- Required result for release-grade wording: `flake_rate_percent=0`.

## D8: App Pods Spread Across Nodes

Goal: prove the multi-volume mounted failover path is not limited to the old
single app-node shape. Three mounted writer pods must run on three distinct
Kubernetes nodes, and each PVC must still recover transparently through its own
host path.

Status: PASS on 2026-05-23.

Evidence:

- Scenario:
  `testops/scenarios/helm-multi-volume-rf3-app-spread-failover-chain.yaml`
- Run: `20260523-151100-e241`
- Result: PASS, 8/8 phases, 32/32 actions
- Independent QA rerun: `20260523-160348-6cc2`, PASS, 32/32 actions
- Flow:
  - Helm installed RF=3 sync-quorum stack with Stage 2 multipath enabled
  - three RF=3 PVCs bound and launched long-running writer pods distributed
    across `m01`, `m02`, and `tp01`
  - helper waited for all 9 generated BlockVolume Deployments to become
    available before mounting writers
  - each volume's current primary Deployment was stopped in turn
  - each writer pod kept the same UID and verified old/new data after failover
  - RTPG and stale-primary probes ran on the writer pod's actual Kubernetes
    node, not a fixed controller node
  - cleanup removed Helm resources, iSCSI sessions, and product processes
- Summary fields:
  - `multi_volume_mounted_failover_status=ok`
  - `requested_volume_count=3`
  - `replication_factor=3`
  - `app_node_selector=m01,m02,tp01`
  - `app_node_distribution_count=3`
  - `recovered_volume_count=3`
  - `mounted_workload_checksum_passed_count=3`
  - `pod_recreate_used=false`
  - `cross_interference_observed=false`
  - `transparent_failover_claimed=true`
- Per-volume evidence:
  - volume 1: writer on `m01`, `before_primary=r1`, `promoted_replica=r2`,
    `post_failure_primary_count=1`, `rtpg_transition_verified=true`
  - volume 2: writer on `m02`, `before_primary=r2`, `promoted_replica=r1`,
    `post_failure_primary_count=1`, `rtpg_transition_verified=true`
  - volume 3: writer on `tp01`, `before_primary=r3`, `promoted_replica=r1`,
    `post_failure_primary_count=1`, `rtpg_transition_verified=true`
  - all three volumes measured `old_primary_stale_io_success_count=0`

Fix included:

- `scripts/run-multi-volume-mounted-failover.sh` accepts comma-separated app
  node selectors and distributes writer pods round-robin.
- RTPG and stale-primary direct-read probes now execute on each PVC's mounted
  writer node, which is required once app pods are spread across nodes.
- The helper waits for the expected `volume_count * replication_factor`
  BlockVolume Deployments to be available before writer pods are created.
- The D8 Helm scenario captures applied Helm values, rendered manifest, and
  kube-system rollout state after install so image/flag drift is self-evident.

## Risks

| Risk | Mitigation |
|---|---|
| Cleanup race hides real residue | helper waits for generated Deployments before `cleanup_status=ok` |
| Per-node port reuse collides across volumes | D1 requires RF=3 report and writer/reader success for all volumes |
| One volume's event stream contaminates another | D2-D4 require per-volume timeline and non-target stability checks |
| Multipath evidence becomes too noisy | D3 must reuse Stage 2 host evidence shape and keep assertions narrow |
| Host-path evidence sampled on the wrong node | D8 runs RTPG/stale probes on each writer pod's mounted node |

## Progress

- D1: PASS - RF=3 multi-volume readiness `20260522-170901-70c5`
- D2: PASS - RF=3 per-volume CSI reattach recovery `20260522-223126-21fd`
- D3: PASS - RF=3 multi-volume mounted transparent failover `20260523-090534-24e4`
- D4: PASS - RF=3 interleaved multi-volume mounted failover `20260523-093348-6b02`
- D5: PASS - measured stale primary direct-read probe `20260523-114708-46bc`
- D6: PASS - measured RTPG AAS transition evidence `20260523-123229-9dd4`, `20260523-123647-2fc4`
- D7: DEV PASS - flake matrix wrapper, D4 N=3 pass, N>=5 QA/nightly pending
- D8: PASS - app pods spread across m01/m02/tp01 with mounted transparent failover `20260523-151100-e241`; QA rerun `20260523-160348-6cc2`
