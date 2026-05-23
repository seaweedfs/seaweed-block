# QA Close Addendum - Phase 27 D5-D8 Independent Rerun

Date: 2026-05-23

Verdict: PASS (strict). D5, D6, and D8 hardening all confirmed on independent
QA rerun. D7 (flake matrix N>=5) remains pending nightly schedule.

This addendum supplements
`phase27-multi-volume-ha-independence-close-report.md` and
`phase27-d5-plus-followup-gaps.md`.

## Method

- Synced branch (head `e698d61 testops: add app-spread multi-volume failover
  gate`) from Windows controller to m02 via tar/scp/extract.
- Lab pre-state confirmed clean: no helm release, no iSCSI sessions, no
  sw-block pods on any host.
- Ran the three scenarios that carry the D5/D6/D8 instrumentation
  sequentially via `swblock.exe run` from Windows.

## Gate Results

| Gate | QA rerun | Actions |
|---|---:|---|
| D3 mounted failover (with D5+D6 instrumentation) | `20260523-155700-9a63` | 47/47 PASS |
| D4 interleaved failover (with D5+D6 instrumentation) | `20260523-160109-cd3d` | 55/55 PASS |
| D8 app-spread multi-volume failover (new) | `20260523-160348-6cc2` | 32/32 PASS |

Total: 134/134 actions across QA reruns of the hardened gates.

## D5 Independent Confirmation - Real Stale-Primary Fence Probe

Per-volume artifacts now carry a measured probe field, not a hardcoded `=0`:

```text
stale_primary_probe=direct_read
stale_primary_fence_evidence=target_ready_replicas=0,stale_path_direct_read_success_count=0
old_primary_stale_io_success_count=0
```

This is the assertion the original D5 gap flagged as missing
(`old_primary_stale_io_success_count=0` was script-literal). The summary
now carries a `stale_primary_probe=direct_read` marker indicating a probe
ran AND a `stale_path_direct_read_success_count=0` field carrying the
measured count from that probe. All 3 volumes in D3, both target volumes in
D4, and all 3 volumes in D8 show the same measured-0 result.

## D6 Independent Confirmation - Real ALUA RTPG Transition

Per-volume artifacts now carry pre/post RTPG AAS values per path:

```text
rtpg_before_old_primary_aas=0x00
rtpg_before_promoted_aas=0x02
rtpg_after_old_primary_aas=missing
rtpg_after_promoted_aas=0x00
rtpg_transition_verified=true
```

Interpretation:

- old primary path was Active/Optimized (`0x00`) before the fault.
- promoted-replica path was Standby (`0x02`) before the fault.
- old primary path is no longer reachable (`missing`) after the fault -
  fence + path teardown happened.
- promoted-replica path is now Active/Optimized (`0x00`) - real ALUA-mediated
  path switch confirmed.

Same transition pattern observed across all 3 volumes in D3, both target
volumes in D4, all 3 volumes in D8. No volume passed `rtpg_transition_verified=true`
without a real AAS change.

## D8 Independent Confirmation - App Pod Spread Across Nodes

Per-volume `writer_node` field carries the actual node where the long-running
writer pod was scheduled:

| Vol | writer_node | before_primary_node | failover promoted | mounted checksum |
|---|---|---|---|---|
| 1 | m01 | m01 | r1 -> r2 | passed |
| 2 | m02 | m02 | r2 -> r1 | passed |
| 3 | tp01 | tp01 | r3 -> r1 | passed |

Run-level summary
`/v/share/g15d-k8s/20260523-160348-6cc2-helm-multi-volume-rf3-app-spread-failover/recovery/multi-volume-mounted-failover-summary.txt`:

```text
multi_volume_mounted_failover_status=ok
requested_volume_count=3
replication_factor=3
target_volume_count=3
app_node_selector=m01,m02,tp01
app_node_distribution_count=3
recovered_volume_count=3
mounted_workload_checksum_passed_count=3
pod_recreate_used=false
cross_interference_observed=false
transparent_failover_claimed=true
```

The distribution count is the assignment's hard-gate field; QA confirms
`app_node_distribution_count=3` matches `target_volume_count=3` - one
writer per node, one initiator stack per node, three independent failovers.

## Final Residue Audit

Required checks (all clean):

```text
helm release sw-block:                  none
iSCSI active sessions:                  none
iSCSI nodes DB (io.seaweedfs):          none
generated app=sw-blockvolume Deployments: none
sw-block / blockvolume pods:            none
per-host product processes (m01/m02/tp01): none
```

Per-scenario `cleanup-summary.txt` all show `cleanup_status=ok`,
`k8s_residue_count=0`, `process_residue_count=0`, `hostpath_residue_count=0`,
`failure_count=0`.

### Stale multipath maps remain

After the three reruns, `sudo multipath -ll` on m02 still shows one stale
`mpath` map (different identifier per run cycle: `mpathbi` pre-run,
`mpathbp` post-run). Underlying iSCSI sessions are gone, so these are
orphaned dm-multipath entries that survive the helm uninstall + verify-cleanup
path.

This is the same finding as the original Phase 27 close report; addressing it
remains a v0.3.2+ follow-up (extend `scripts/verify-helm-cleanup.sh` to assert
no leftover maps match the sw-block PVC IQN substring).

## D7 Status

D7 (flake-rate matrix, N>=5 sequential D3 and D4 runs) is not in this
addendum. Dev shipped the wrapper script (`scripts/run-phase27-flake-matrix.ps1`)
and a 3-iteration smoke. Full N>=5 runs are a nightly-schedule QA task, not a
single-session validation. Assignment carries forward unchanged.

## Updated Hard-Gate Acceptance

All previously-validated criteria still hold (per the original close report).
New criteria added by D5/D6/D8:

| Requirement | Result |
|---|---|
| D5 stale-primary probe runs and measures 0 successes per volume | PASS (D3 3/3, D4 2/2, D8 3/3) |
| D5 summary carries `stale_primary_probe=direct_read` marker | PASS |
| D6 RTPG AAS pre/post values measured (not just text grep) | PASS |
| D6 `rtpg_transition_verified=true` per target volume | PASS |
| D6 old-primary path verified unreachable after fault | PASS |
| D6 promoted path AAS transitions to Active/Optimized | PASS |
| D8 writer pods distributed across distinct nodes | PASS (m01, m02, tp01) |
| D8 `app_node_distribution_count=3` matches `target_volume_count=3` | PASS |
| D8 transparent failover preserved across distributed initiator hosts | PASS (3/3) |
| Run-level cleanup remains hygienic | PASS (modulo known multipath-residue gap) |

## Verdict

PASS for the D5/D6/D8 hardening scope on top of Phase 27.

### Recommended close sequence

1. Treat this addendum + the original Phase 27 close report as the joint
   evidence for v0.3.2 alpha release.
2. The v0.3.2 release note CAN now claim:
   - "stale primary I/O rejection verified by direct-read probe"
   - "ALUA RTPG path transition verified per volume"
   - "transparent mounted failover validated with writer pods distributed
     across all alpha lab nodes"
3. Two open follow-ups:
   - D7 nightly flake matrix (dev wrapper shipped; QA owns scheduling +
     window).
   - Multipath residue cleanup verifier extension.
4. Three untracked files in my working tree (`testrunner-product-interface-audit.md`,
   `experimental-runner-native-pvc-loop.yaml`, `tmp/`) are QA reference
   material from the audit work this session - dev can pull them into the
   Phase 27 commit set or carry as standalone, QA's recommendation is to
   keep them as reference docs (the audit doc) and example scenario (the
   experimental runner-native loop is useful as a runner-action discovery
   reference, not as a gate).
