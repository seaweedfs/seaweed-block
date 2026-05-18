# QA Assignment: Stage 2 Transparent Multipath Host Failover Close Hard Gate

Status: draft gate for the active
`Stage 2 Transparent Multipath Host Failover MVP` plan.

This gate is intentionally strict. Stage 1 already proved recovery through
CSI/pod recreate. Stage 2 must prove host multipath behavior without pod
recreate.

## Product Contract Under Test

```text
protocol=iscsi
host_multipath=dm-multipath
ack_profile=sync-quorum
claim_profile=stage2-iscsi-alua-multipath
failure=controlled primary blockvolume stop
recovery=mounted workload verifies data through multipath without pod recreate
```

## Required Runner Scenario

Expected scenario name:

```text
testops/scenarios/stage2-iscsi-alua-multipath-failover-chain.yaml
```

If the scenario name changes, the report must state the replacement name and
why.

## Hard-Gate Clauses

Any single `FAIL` blocks close.

### HG-0 Documentation Entry

Pass:

- `docs/operations-v1.md` has a Stage 2 iSCSI ALUA/dm-multipath section,
- it distinguishes Stage 1 CSI/pod-recreate recovery from Stage 2 multipath
  recovery,
- it names non-claims: no node loss, no NVMe ANA Kubernetes claim, no Windows
  MPIO, no RTO/performance SLO.

Fail:

- docs imply Stage 1 recovery is transparent failover,
- docs omit multipath prerequisites.

### HG-1 Multipath Prerequisites

Pass:

- artifact `multipath-prereq.txt` exists,
- it shows `multipath_enabled=true`,
- it records `multipathd` availability/status,
- it records iSCSI tools availability: `iscsiadm`, `sg_inq`, and an RTPG tool
  path or equivalent.

Fail:

- missing prerequisite artifact,
- multipath unavailable but scenario continues as success.

### HG-2 CSI Does Not Use Pod Recreate For Recovery

Pass:

- artifact explicitly shows `pod_recreate_used=false`,
- writer pod identity remains the mounted workload identity, or the scenario
  uses a long-running workload pod whose mount survives the injected failure,
- no post-failure `kubectl delete pod` / reader recreate step is used as the
  recovery mechanism.

Fail:

- recovery path is `pod recreated -> CSI restaged -> checksum passed`.

### HG-3 Linux Sees One Multipath Device With Multiple Paths

Pass:

- `multipath-before.txt` shows one logical multipath device for the volume,
- path count before failure is `>=2`,
- the mounted filesystem uses the multipath device, not a raw `/dev/sdX` or
  portal-specific `/dev/disk/by-path` device.

Fail:

- two independent devices are present but not merged,
- mounted source is a raw path device.

### HG-4 ALUA / Path State Is Host Visible

Pass:

- `sg-inq.txt` shows TPGS/ALUA advertised for the target,
- `sg-vpd83.txt` shows stable volume identity and path-distinguishing
  descriptors,
- `sg-rtpg.before.txt` shows at least one active/optimized path and one
  non-primary path state,
- `sg-rtpg.after.txt` shows path state changed after promotion.

Fail:

- ALUA metadata is absent,
- all paths report identical writable active state without authority evidence,
- path state is only inferred from internal logs.

### HG-5 Pre-Failure Mounted Writer

Pass:

- writer writes and verifies `/data/demo.bin`,
- checksum evidence is in `writer.log`,
- pre-failure inventory identifies the primary replica and candidate path.

Fail:

- no real mounted filesystem write/read,
- data was written outside the mounted PVC path.

### HG-6 Scoped Primary Failure

Pass:

- failed replica is derived from live inventory,
- `failed_replica == before_primary_replica`,
- target Deployment/process is stopped in a scoped way,
- unrelated replica paths remain present.

Fail:

- failed replica is hard-coded without inventory proof,
- cleanup/failure injection globally kills all blockvolumes.

### HG-7 Authority Publishes Exactly One New Primary

Pass:

- post-failure control-plane evidence shows promoted replica,
- `post_failure_primary_count=1`,
- promoted replica epoch increases,
- no `conflicting_primary_replicas` issue.

Fail:

- dual-primary observed,
- authority movement not visible.

### HG-8 Stale Primary Fenced

Pass:

- old primary path cannot return successful data WRITE or SYNC after authority
  moved,
- issue/evidence line records `old_primary_stale_io_success_count=0`,
- stale path reports unavailable/transitioning or another documented
  non-writable ALUA state.

Fail:

- old primary returns GOOD for stale data I/O,
- stale path is not tested.

### HG-9 Mounted Workload Survives Or Recovers Through Multipath

Pass:

- the mounted workload verifies pre-failure data after failure,
- post-failure write/read/checksum succeeds if the selected policy claims
  writable continuation,
- evidence says `data_check_after_failover=mounted_workload_checksum_passed`,
- no pod recreate or CSI re-stage is required for that checksum.

Fail:

- checksum only passes after pod recreate,
- workload path hangs without a bounded failure/success result.

### HG-10 Bounded Waits / No Hung Kubernetes Path

Pass:

- artifact `bounded-waits.txt` exists,
- it records each bounded step and result:
  - attach / iSCSI login,
  - multipath map creation,
  - ALUA/RTPG state read,
  - authority promotion,
  - path switch,
  - stale-primary fencing,
  - post-failure workload I/O,
  - cleanup,
- successful run shows `bounded_waits=pass`,
- failing run, if any, uses one stable blocker class such as
  `attach_timeout`, `multipath_map_timeout`, `path_switch_timeout`, or
  `post_failure_io_timeout`,
- no step relies on the outer TestOps runner timeout as its only failure
  signal.

Fail:

- a PVC, pod, iSCSI session, multipath map, or workload I/O hangs until the
  scenario-level timeout,
- failure bundle lacks a stable blocker reason and last observed state.

### HG-11 Support Bundle Self-Explains

Pass:

- bundle contains authority evidence, ALUA state evidence, multipath evidence,
  stale-primary fencing evidence, bounded-wait evidence, and data-check result,
- a cold reader can answer:
  - which replica was primary before failure,
  - which replica was promoted,
  - which host path was mounted,
  - whether multipath switched paths,
  - whether any bounded wait blocked progress,
  - whether data was verified after failure.

Fail:

- reader must inspect raw blockmaster/blockvolume logs to understand the
  result.

### HG-12 Cleanup Hygiene

Pass:

- no active iSCSI sessions for the test IQN,
- no stale iSCSI node DB entries for the test IQN unless explicitly attributed
  and removed by TestOps guardrail,
- no `blockmaster`/`blockvolume`/`blockcsi`/`iscsi-target` processes,
- no `kubectl port-forward svc/blockmaster`,
- no `app=sw-blockvolume` Deployments,
- no run-scoped `/var/lib/sw-block/testops-*` paths.

Fail:

- any unexplained residue remains.

### HG-13 Non-Claims Honest

Pass:

- docs and bundle explicitly do not claim node loss, NVMe ANA Kubernetes
  recovery, Windows MPIO, broad distro compatibility, performance/RTO SLO, or
  repair/rebuild/failback.

Fail:

- Stage 2 evidence is used to imply broader production HA.

## Report Template

QA report must include:

```text
Verdict: PASS|FAIL (strict)
Product commit:
Runner commit:
Run id:
Scenario:
Result:

HG table:
HG-0 ...
...
HG-13 ...

Key evidence:
- multipath device:
- path count before/after:
- before_primary_replica:
- promoted_replica:
- pod_recreate_used:
- data_check_after_failover:
- bounded_waits:
- blocker_reason:

Residue audit:
- iSCSI sessions:
- iSCSI node DB:
- sw-block processes:
- port-forwards:
- k8s resources:
- run-scoped host paths:

Blocking findings:
Non-blocking findings:
QA needed next:
```

## Close Recommendation Rule

Only recommend close if all 14 clauses pass.

If the product recovers only through Stage 1 pod recreate, report `FAIL` even
if the checksum passes.
