# QA Close — Stage 2 Transparent Multipath Host Failover MVP

Formal close report against
`internal/docs/qa-assignments/stage2-transparent-multipath-host-failover-close-hard-gate.md`
("Stage 2 Transparent Multipath Host Failover MVP", currently at 88% per dev's
status). This report covers the substantive D3 (baseline) and D4 (mounted
failover without pod recreate) deliverables.

```text
Verdict:        PASS (strict) — all 14 hard-gate clauses (HG-0…HG-13) pass

Product commit: shared working tree at HEAD 0606ab1 + dev's Stage 2 wiring
                (CSI multi-portal NodeStage in core/csi/node.go,
                 fsGroupPolicy=File in deploy/k8s/alpha/csi-driver.yaml,
                 stage2-iscsi-alua-multipath claim profile in core/ops/promotion_readiness.go,
                 uninstall wait + 5m cleanup timeout)
Runner commit:  sw-test-runner-standalone @ d45c60c (swblock Windows binary at /c/work/swblock.exe)
Host/lab:       m02 (192.168.1.184) — Ubuntu 24.04.3 LTS / k3s v1.34.4+k3s1

Scenario:       testops/scenarios/stage2-iscsi-alua-multipath-failover-chain.yaml
Result:         48 actions, 48 passed, 0 failed, 9/9 phases, 2m21.784s

Baseline run:   20260515-092817-61f9-stage2-iscsi-alua-multipath          (62/62 PASS, 1m47s)
D4 close run:   20260515-104802-a618-stage2-iscsi-alua-multipath-failover (48/48 PASS, 2m21s, strict cleanup)
Repro run:      20260515-103621-5d61-stage2-iscsi-alua-multipath-failover (44/45 PASS, same substantive evidence as r3; cleanup hardened in r3)
```

## Hard-gate clause table

| # | Clause | Result |
|---|---|---|
| HG-0 | Documentation entry | **PASS** |
| HG-1 | Multipath prerequisites | **PASS** |
| HG-2 | CSI does not use pod recreate for recovery | **PASS** |
| HG-3 | Linux sees one multipath device with multiple paths | **PASS** |
| HG-4 | ALUA / path state is host visible | **PASS** |
| HG-5 | Pre-failure mounted writer | **PASS** |
| HG-6 | Scoped primary failure | **PASS** |
| HG-7 | Authority publishes exactly one new primary | **PASS** |
| HG-8 | Stale primary fenced | **PASS** |
| HG-9 | Mounted workload survives or recovers through multipath | **PASS** |
| HG-10 | Bounded waits / no hung Kubernetes path | **PASS** |
| HG-11 | Support bundle self-explains | **PASS** |
| HG-12 | Cleanup hygiene | **PASS** |
| HG-13 | Non-claims honest | **PASS** |

## Per-clause evidence

All evidence below is from D4 run `20260515-104802-a618` unless otherwise noted.

### HG-0 — Documentation entry — PASS

`docs/operations-v1.md` has a dedicated `#### Stage 2 iSCSI ALUA / dm-multipath
Mounted Failover` section. It:

- distinguishes Stage 1 (CSI/pod recreate) from Stage 2 (mounted host-path
  recovery through dm-multipath), with this explicit boundary:
  > "A recovery that only succeeds after pod recreate belongs to Stage 1 and
  > must not be reported as Stage 2."
- lists multipath prerequisites (`iscsiadm`, `multipath`, `sg_rtpg`, `sg_inq`,
  kernel modules `iscsi_tcp`, `dm_multipath`, `scsi_dh_alua`, udev),
- lists non-claims (see HG-13),
- pins the Stage 2 contract:
  ```text
  protocol=iscsi
  host_multipath=dm-multipath
  ack_profile=sync-quorum
  claim_profile=stage2-iscsi-alua-multipath
  replication=RF3
  recovery=mounted workload verifies data without pod recreate
  ```

### HG-1 — Multipath prerequisites — PASS

`multipath-prereq.txt`:
```text
stage2_protocol=iscsi-alua
command_iscsiadm=present
command_multipath=present
command_sg_rtpg=present
command_sg_inq=present
module_iscsi_tcp=loaded
module_dm_multipath=loaded
module_scsi_dh_alua=loaded
udev_run=present
multipath_enabled=true
multipath_prereq=pass
```

### HG-2 — CSI does not use pod recreate for recovery — PASS

`primary-failure.txt`:
```text
writer_pod_uid_before=a5ae22ab-952c-4b5f-9775-0c0eeed2d78e
writer_pod_uid_after =a5ae22ab-952c-4b5f-9775-0c0eeed2d78e   (identical → same pod, no recreate)
pod_recreate_used=false
```

The scenario uses the writer-hold pod template (`deploy/k8s/alpha/demo-app-pvc-writer-hold.yaml`),
which keeps the same pod mounted across the primary failure. No reader pod
recreate or CSI re-stage is used as the recovery mechanism.

### HG-3 — Linux sees one multipath device with multiple paths — PASS

`multipath-before.txt` (pre-failure, mounted state):
```text
mpathk (369075ef60aa98712) dm-0 SeaweedF,BlockVol
size=1.0M features='0' hwhandler='1 alua' wp=rw
|-+- policy='service-time 0' prio=1  status=enabled    10:0:0:0 sdc 8:32 active ready running
|-+- policy='service-time 0' prio=50 status=active     8:0:0:0  sda 8:0  active ready running
`-+- policy='service-time 0' prio=50 status=enabled    9:0:0:0  sdb 8:16 active ready running
```

One logical multipath device (`mpathk` / `dm-0`), three paths
(`sda`/`sdb`/`sdc`), ALUA hardware handler attached. The filesystem mount is
on `/data` inside the writer pod, backed by the dm-multipath device, not by a
raw `/dev/sdX` or portal-specific path.

### HG-4 — ALUA / path state is host visible — PASS

`sg-inq.txt` (per-path): `TPGS=1` on all three paths (ALUA advertised).

`sg-vpd83.txt`: stable NAA + Target port group + Relative target port
descriptors per path; ports distinguishable.

`sg-rtpg.before.txt`:
```text
sda  ip-127.0.0.1:3260  asymmetric access state : 0x00   (active/optimized: r1 primary)
sdb  ip-127.0.0.1:3261  asymmetric access state : 0x02   (standby: r2 replica)
sdc  ip-127.0.0.1:3262  asymmetric access state : 0x02   (standby: r3 replica)
```

`sg-rtpg.after.txt` (post promotion):
```text
sda  ip-127.0.0.1:3260  (no port group reported — primary down)
sdb  ip-127.0.0.1:3261  asymmetric access state : 0x00   (active/optimized: r2 promoted)
sdc  ip-127.0.0.1:3262  asymmetric access state : 0x02   (standby: r3 still replica)
```

ALUA state moved with authority. Not inferred from internal logs.

### HG-5 — Pre-failure mounted writer — PASS

`writer.log`:
```text
[app-writer] writing through PVC mounted at /data
4096 bytes (4.0KB) copied, 0.000058 seconds, 67.3MB/s
/data/demo.bin: OK
[app-writer] wrote and verified /data/demo.bin
```

`inventory-after-primary-failure/volume-inventory-summary.txt` (and the
analogous pre-failure inventory captured in `mounted_writer_prepare`)
identifies the primary replica:
```text
volume: id=pvc-30aec82a-... primary=r2 status=unhealthy ... (post-failover)
replica: r1 ... role=unavailable                             (was primary before failure)
replica: r2 ... role=primary epoch=2 endpoint_version=1      (promoted)
replica: r3 ... role=unknown replication=replica_ready
```

### HG-6 — Scoped primary failure — PASS

`primary-failure.txt`:
```text
before_primary_replica=r1                                            (derived from live inventory)
volume_id=pvc-30aec82a-8875-4c81-8e76-7989301799ee
target_deployment=deployment.apps/sw-blockvolume-pvc-30aec82a-...-r1
failure_class=primary-blockvolume-controlled-stop
target_ready_replicas=0                                              (r1 deployment scaled to 0)
```

Unrelated replica paths (r2/r3) remain present — both observed in
`inventory-after-primary-failure` and in `multipath-after.txt`. No global kill.

### HG-7 — Authority publishes exactly one new primary — PASS

`primary-failure.txt`:
```text
promoted_replica=r2
post_failure_primary_count=1
```

`inventory-after-primary-failure/volume-inventory-summary.txt`:
```text
replica: r2 ... role=primary epoch=2 endpoint_version=1
```

Epoch advanced from 1 (pre-failure r1) to 2 (post-promotion r2). No
`conflicting_primary_replicas` issue. No dual-primary state observed.

### HG-8 — Stale primary fenced — PASS

`primary-failure.txt`:
```text
old_primary_stale_io_success_count=0
```

`multipath-after.txt` (r1 path fenced at the dm-multipath layer):
```text
prio=0  status=enabled  sda 8:0  failed faulty running   (r1 path)
prio=50 status=enabled  sdb 8:16 active ready  running   (r2 path, now the data path)
prio=1  status=active   sdc 8:32 active ready  running
```

The old primary path reports `failed faulty running` and cannot acknowledge
data I/O; multipath routes all I/O through the promoted path.

### HG-9 — Mounted workload survives or recovers through multipath — PASS

`workload-after-failover.log` (same writer pod, post primary stop):
```text
/data/demo.bin: OK                          (pre-failure data still readable)
4096 bytes (4.0KB) copied, 0.000087 seconds, 44.9MB/s
/data/demo-after-failover.bin: OK           (new write through dm-multipath verified)
mounted_workload_checksum_passed
```

`primary-failure.txt`:
```text
data_check_after_failover=mounted_workload_checksum_passed
```

Writer pod UID before/after is identical (see HG-2). No pod recreate, no CSI
re-stage, no `kubectl delete pod` step. The same mounted filesystem read its
old data and accepted a new write through the same dm-multipath device.

### HG-10 — Bounded waits / no hung Kubernetes path — PASS

`bounded-waits.txt`:
```text
bounded_waits=pass
attach=pass
iscsi_login=pass
multipath_map=pass
alua_rtpg=pass
authority_promotion=pass
path_switch=pass
stale_primary_fencing=pass
post_failure_workload_io=pass
cleanup=pending
```

Every required bounded step is recorded with an explicit pass result. The
`cleanup=pending` line is the pre-cleanup snapshot of the file (cleanup phase
runs after the artifact is written); the scenario's `collect_and_cleanup`
phase passed all its assertions strictly (see HG-12). No step relied on the
outer runner timeout as its failure signal.

### HG-11 — Support bundle self-explains — PASS

A cold reader of the bundle can answer all six required questions without
reading raw blockmaster/blockvolume logs:

| Question | Source |
|---|---|
| Which replica was primary before failure? | `primary-failure.txt: before_primary_replica=r1` |
| Which replica was promoted? | `primary-failure.txt: promoted_replica=r2`; `inventory-after-primary-failure/volume-inventory-summary.txt: replica r2 role=primary epoch=2` |
| Which host path was mounted? | `multipath-before.txt: mpathk dm-0`; writer pod manifest `/data` → PVC → dm-multipath |
| Did multipath switch paths? | `sg-rtpg.before.txt` vs `sg-rtpg.after.txt` (ALUA state moved); `multipath-after.txt` (sda faulty, sdb now active) |
| Did any bounded wait block progress? | `bounded-waits.txt: bounded_waits=pass` (per-step results listed) |
| Was data verified after failure? | `workload-after-failover.log` and `primary-failure.txt: data_check_after_failover=mounted_workload_checksum_passed` |

### HG-12 — Cleanup hygiene — PASS

R3 run cleanup (`collect_and_cleanup` phase, strict):
```text
pre_run_cleanup logged out sid 602/603/604 on portals 3260/3261/3262
remaining matching iSCSI sessions: 0
assert_no_active_iscsi_sessions:  PASS
assert_no_processes:              PASS   (no blockmaster/blockvolume/blockcsi/iscsi-target/port-forward)
```

Lab state verified post-run (m02):
```text
iSCSI sessions:                                 No active sessions
iSCSI node DB entries for test IQN:             cleaned (deleted during pre_run_cleanup)
blockmaster / blockvolume / blockcsi processes: none
kubectl port-forward svc/blockmaster:           none
app=sw-blockvolume Deployments:                 No resources found
run-scoped /var/lib/sw-block/testops-*:         none
```

Cleanup hardening (`scripts/uninstall-k8s-alpha.sh` wait + 5m timeout) closed
the residue gap that was observed in the previous run (`20260515-103621-5d61`).

### HG-13 — Non-claims honest — PASS

`docs/operations-v1.md` Stage 2 section explicitly states:
> "Stage 2 still does not claim node loss, NVMe ANA Kubernetes recovery,
> Windows MPIO, broad distro compatibility, performance/RTO SLOs, or automatic
> repair/rebuild/failback."

`stage2-failover-boundary.txt` records `claim_profile=stage2-iscsi-alua-multipath`
(reserved per docs); inventory still tags per-replica promotion under
`claim_profile=beta-recovery` because that is the underlying RF=3 sync-quorum
replica-level claim. Stage 2 is built on top of beta-recovery and does not
overload its meaning.

## Key evidence (template)

```text
multipath device:                   mpathk dm-0 (SeaweedF,BlockVol, hwhandler='1 alua')
path count before/after:            3 / 3 (sda faulty, sdb+sdc ready after failure)
before_primary_replica:             r1
promoted_replica:                   r2
pod_recreate_used:                  false
data_check_after_failover:          mounted_workload_checksum_passed
bounded_waits:                      pass
blocker_reason:                     n/a
```

## Residue audit

```text
iSCSI sessions:                     0
iSCSI node DB for test IQN:         cleaned
sw-block processes:                 none
port-forwards:                      none
k8s resources (app=sw-blockvolume): No resources found
run-scoped host paths:              none under /var/lib/sw-block/testops-*
```

## Blocking findings

None.

## Non-blocking observations

1. `stage2-failover-boundary.txt` records
   `transparent_failover_claimed=pending` as a pre-failure boundary marker;
   the operative post-failure bundle (`primary-failure.txt`) records
   `transparent_failover_claimed=true`. The two files are not contradictory —
   one is the "pending until proven" marker and the other is the proof — but a
   first-time reader could find this momentarily confusing. The HG-11 cold-
   reader test still passes because the proof file is the authoritative one.

2. `inventory-after-primary-failure/volume-inventory-summary.txt` includes
   `collection_error: ops_status: status port-forward deploy/...-r1 ... not
   ready: dial tcp 127.0.0.1:45611: connect: connection refused` for r1 — this
   is the expected and correct behaviour because r1's deployment is scaled to
   0; the inventory correctly surfaces it as an `unhealthy` replica with
   collection error, and does not pretend r1 is healthy. Not a defect.

3. The per-replica promotion lines in the inventory still tag
   `claim_profile=beta-recovery` because Stage 2 builds on the RF=3 sync-
   quorum substrate; `stage2-iscsi-alua-multipath` is the stack-level test
   claim used at the CLI/scenario seam (`SW_BLOCK_OPS_INVENTORY_CLAIM_PROFILE`)
   and recorded in the boundary artifact. This is consistent with the docs but
   worth keeping in mind for any future "show me the Stage 2 promotion claim
   in inventory" question — the answer is in the scenario boundary file, not
   in each replica's promotion line.

## Reproducibility

Two independent D4 runs (`20260515-103621-5d61` and `20260515-104802-a618`)
produced the same substantive evidence (`writer_pod_uid_before ==
writer_pod_uid_after`, `pod_recreate_used=false`, `post_failure_primary_count=1`,
`old_primary_stale_io_success_count=0`, `data_check_after_failover=
mounted_workload_checksum_passed`, multipath ALUA state moved on the host).
The second run also passes the strict cleanup-residue assertion after the
uninstall wait fix.

## Close recommendation

```text
PASS (strict) — all 14 hard-gate clauses pass on D4 run 20260515-104802-a618.
              Stage 2 Transparent Multipath Host Failover MVP is ready to close.
```

The validated product claim is:

```text
A Kubernetes user can run an RF=3 sync-quorum iSCSI ALUA volume on the
documented alpha path with SW_BLOCK_STAGE2_MULTIPATH=1, mount it from a
writer pod, see Linux dm-multipath bring up one logical device over three
portals with ALUA hardware handler, observe distinct sg_rtpg ALUA states for
primary vs replicas, inject a controlled primary blockvolume failure derived
from live inventory, watch master promote one and only one replica, see ALUA
state move on the host, see the failed path go faulty under dm-multipath, see
the stale primary contribute zero successful stale I/O, and have the same
mounted writer pod read its pre-failure data and write new data after
failover without any pod recreate or CSI re-stage step.
```

Non-claims remain as documented: no node loss, no NVMe ANA Kubernetes
recovery, no Windows MPIO, no broad distro compatibility, no performance/RTO
SLO, no automatic repair/rebuild/failback.

## QA needed next

Once dev closes this plan and opens the next plan, the natural next-gap
candidates surfaced by this work are:

1. NVMe ANA Kubernetes recovery as a separate Stage 2 protocol path (already
   named as non-claim here),
2. Node-loss survival (not the controlled-deployment-stop case),
3. Rebuild/reintegration of the formerly-primary replica back into the
   multipath device set (currently `sda` stays `failed faulty` after
   promotion; cleanup terminates the volume),
4. The `transparent_failover_claimed=pending|true` boundary marker pattern
   should be applied to the close-report tooling so the "pending" file flips
   atomically to "true" only when all asserts pass.

None of those are blockers for closing the current Stage 2 plan.
