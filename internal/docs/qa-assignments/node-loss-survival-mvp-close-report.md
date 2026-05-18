# QA Close — Node-Loss Survival MVP

Formal close report against
`internal/docs/qa-assignments/node-loss-survival-mvp-close-hard-gate.md`
("Node-Loss Survival MVP", currently at ~88% per dev's status). This report
covers the substantive D3 (topology eligibility) and D4 (mounted recovery via
CSI/pod recreate on a survivor node) deliverables.

```text
Verdict:        PASS (strict) — all 13 hard-gate clauses (HG-0…HG-12) pass

Product commit: shared working tree at HEAD 0606ab1 + dev's Node-Loss wiring
                  (multi-node image import in scripts/build-alpha-images.sh,
                   inventory replica-ID attribution fix in core/ops/k8s_inventory.go,
                   preflight non-zero exit + non-claim emission in scripts/preflight-node-loss-lab.sh,
                   k8s_renderer cluster-spec multi-node placement,
                   node-loss-recovery-summary.txt + boundary.txt evidence in scripts/run-alpha-app-demo.sh)
Runner commit:  sw-test-runner-standalone @ d45c60c (swblock Windows binary at /c/work/swblock.exe)
Host/lab:       m02 control + m01 + tp01 workers
                m01  192.168.1.181  Ready, k3s worker  (Ubuntu 24.04.4)
                m02  192.168.1.184  Ready, k3s control-plane (Ubuntu 24.04.3)
                tp01 192.168.1.188  Ready, k3s worker  (Ubuntu 24.04.3)
                LAN TCP/iSCSI over 192.168.1.x. No RoCE/RDMA, no 10.0.0.x fabric,
                no NVMe/RDMA, no performance claim — explicit non-claims per the
                Stage 3 docs section and per the placement artifact.

Scenario:       testops/scenarios/node-loss-survival-rf3-reattach-chain.yaml
Result:         74 actions, 74 passed, 0 failed, 8/8 phases, 1m56s

D3 evidence:    20260516-150131-d4fc-node-loss-topology       (62/62 PASS, 1m33s, three-node topology proof)
D4 close run:   20260516-160306-1e54-node-loss-survival       (74/74 PASS, 1m56s, end-to-end recovery proof)
```

## Hard-gate clause table

| # | Clause | Result |
|---|---|---|
| HG-0 | Documentation entry | **PASS** |
| HG-1 | Multi-node topology | **PASS** |
| HG-2 | Non-loopback frontends | **PASS** |
| HG-3 | Pre-failure app write | **PASS** |
| HG-4 | Scoped primary node failure | **PASS** |
| HG-5 | Authority movement | **PASS** |
| HG-6 | CSI reattach uses surviving target | **PASS** |
| HG-7 | Data verification after node loss | **PASS** |
| HG-8 | Stale primary fenced | **PASS** |
| HG-9 | Bounded waits | **PASS** |
| HG-10 | Support bundle self-explains | **PASS** |
| HG-11 | Cleanup hygiene | **PASS** |
| HG-12 | Non-claims honest | **PASS** |

## Per-clause evidence

All evidence below is from D4 run `20260516-160306-1e54` unless otherwise noted.

### HG-0 — Documentation entry — PASS

`docs/operations-v1.md` has a dedicated `#### Stage 3 Node-Loss Survival Gate`
section that:

- distinguishes Stage 2 same-node multipath from Stage 3 node-loss recovery:
  > "Stage 3 is the Kubernetes node-loss recovery line. It is different from
  > Stage 2: the first node-loss close uses CSI/pod recreate reattach, not
  > transparent mounted I/O continuation."
- names the recovery mechanism as CSI/pod recreate,
- lists non-claims (see HG-12),
- pins the lab boundary as TCP/iSCSI over 192.168.1.x with `10.0.0.x` /
  RoCE / RDMA / NVMe/RDMA / performance explicitly out of scope.

### HG-1 — Multi-node topology — PASS

`node-placement.before.txt`:
```text
selected_node_count=3
physical_domain_count=3
physical_domain_shape=full-physical-host
kubernetes_node_loss_claimed=true
physical_host_loss_claimed=false
replica=r1 server=node-loss-r1 node=m01  host=192.168.1.181 physical_host=m01
replica=r2 server=node-loss-r2 node=m02  host=192.168.1.184 physical_host=m02
replica=r3 server=node-loss-r3 node=tp01 host=192.168.1.188 physical_host=tp01
```

D3 run `20260516-150131-d4fc` independently established `replicas_on_distinct_nodes=true`
in the topology inventory and rendered the RF=3 PlaceVolume placement across
distinct k8s nodes. Three distinct Kubernetes nodes participate. Physical-host
loss is correctly NOT claimed even though the topology happens to be
full-physical-host shape, because the failure injection was Kubernetes-level
(cordon + Deployment scale), not a physical machine power-off.

### HG-2 — Non-loopback frontends — PASS

`primary-failure-recovery.txt`:
```text
before_publish_target_frontend=192.168.1.181:3260
after_publish_target_frontend=192.168.1.184:3260
```

D3 inventory `topology_asserts` recorded `frontends_non_loopback=true`. All
three replicas were rendered in `generated-blockvolume.yaml` (D3 run) with
`--iscsi-listen=192.168.1.{181,184,188}:3260` and `--status-addr` on the same
LAN IPs. No `127.0.0.1` or `localhost` is used for cross-node attach.

### HG-3 — Pre-failure app write — PASS

`primary-failure-recovery.txt`:
```text
before_primary_replica=r1
before_primary_node=m01
required_frontier=pvc-c606d03a-...=52
```

The pre-failure writer pod wrote `/data/demo.bin` through the PVC (writer was
deleted after verification per the demo's `delete writer pod but keep PVC`
flow; reader pod was created later and verified — see HG-7). Pre-failure
inventory (`ops-inventory-before-primary-failure/`) identifies primary r1
on m01.

### HG-4 — Scoped primary node failure — PASS

`primary-failure-recovery.txt`:
```text
failure_class=primary-kubernetes-node-cordoned-blockvolume-stop
failed_replica=r1                            ← matches before_primary_replica
failed_node=m01                              ← matches before_primary_node
target_deployment=deployment.apps/sw-blockvolume-pvc-c606d03a-...-r1
target_ready_replicas=0
```

Failed replica/node derived from live pre-failure inventory; the scenario
cordoned m01 and scaled the r1 Deployment to zero replicas. r2 (on m02) and
r3 (on tp01) remained running throughout. No global kill.

### HG-5 — Authority movement — PASS

`primary-failure-recovery.txt`:
```text
failover_status: promoted
promoted_replica=r2
promoted_replica_node=m02
post_failure_primary_count=1                            ← exactly one new primary
frontend_primary_ready_issue_count=0                    ← no conflict
candidate_promotion_evidence=promotion: ... replica=r2 candidate_ready=true
                              reason=promotion_ready claim_profile=beta-recovery
                              ack_profile=sync-quorum required_frontier_lsn=52
                              candidate_frontier_lsn=52 frontier_covered=true
promoted_replica_evidence=replica: ... replica=r2 ... role=primary epoch=2 endpoint_version=1
                          frontend=192.168.1.184:3260
```

Promotion is on a surviving node (m02), epoch advanced from 1 to 2, sync-
quorum frontier (lsn=52) is covered, exactly one primary. No
`conflicting_primary_replicas`.

### HG-6 — CSI reattach uses surviving target — PASS

`primary-failure-recovery.txt`:
```text
before_publish_target_frontend=192.168.1.181:3260      (r1 on m01)
after_publish_target_frontend=192.168.1.184:3260       (r2 on m02)
pod_recreate_used=true
```

Reader pod was recreated on a survivor node and CSI re-staged the volume
against the new primary's non-loopback frontend (192.168.1.184:3260). No
attach to the old failed-node frontend. The plan's "replacement pod is
allowed" clause is correctly used and labeled.

### HG-7 — Data verification after node loss — PASS

`reader.log`:
```text
[app-reader] reading existing data from PVC mounted at /data
/data/demo.bin: OK
[app-reader] verified persisted /data/demo.bin
```

`primary-failure-recovery.txt`:
```text
data_check_after_failover=reader_checksum_passed
data_check_after_node_loss=reader_checksum_passed
reader_verified=true
node_loss_recovery_claimed=true
```

`/data/demo.bin` written before failure is verified after failure through the
promoted replica on the survivor node. The recovery claim is explicit and
matched by reader checksum.

### HG-8 — Stale primary fenced — PASS

`primary-failure-recovery.txt`:
```text
old_primary_stale_io_success_count=0
stale_primary_fence_evidence=target_ready_replicas=0
failed_replica_after_evidence=replica: ... replica=r1 ... role=unavailable
                              replication=unavailable healthy=false ...
```

The scoped failure leaves no ready old-primary endpoint (the r1 Deployment
is at zero ready replicas), so no stale I/O is possible. Inventory after
failure marks r1 as `role=unavailable` / `healthy=false`. The
`old_primary_stale_io_success_count=0` line is the explicit fence fact.

### HG-9 — Bounded waits — PASS

`bounded-waits.txt`:
```text
bounded_waits=pass
attach=bounded_by_demo_waits
failure_injection=bounded_by_scale_wait
authority_promotion=bounded_by_240s_inventory_poll
csi_reattach=bounded_by_reader_pod_wait
data_check=bounded_by_reader_pod_wait
cleanup=bounded_by_collect_and_cleanup_phase
```

All required steps are bounded by explicit waits; success records
`bounded_waits=pass`. The run completed in 1m56s well under any outer
runner timeout. No step relied on the scenario-level 34m timeout.

### HG-10 — Support bundle self-explains — PASS

A cold reader can answer all six required questions from
`node-loss-recovery-summary.txt` alone (compact 14-line summary):

```text
result=promoted
before_primary=r1@m01            ← which replica was primary before failure
failed=r1@m01                    ← which node failed
promoted=r2@m02                  ← which surviving replica was promoted
before_frontend=192.168.1.181:3260   ← CSI target before
after_frontend=192.168.1.184:3260    ← CSI target after
pod_recreate_used=true
reader_verified=true             ← whether data verified
data_check_after_node_loss=reader_checksum_passed
old_primary_stale_io_success_count=0   ← whether stale primary was fenced
transparent_failover_claimed=false
physical_host_loss_claimed=false
ack_profile=sync-quorum
source_marker=.../primary-failure-recovery.txt
```

For deeper investigation, `primary-failure-recovery.txt` carries every
inventory `replica:` and `promotion:` line, and a control-plane timeline
pointer. The scenario also captures
`ops-inventory-before-primary-failure/`,
`ops-inventory-after-primary-failure/`,
`ops-inventory-reader-verified/`,
`blockvolume-pods.after-primary-failure.txt`,
`cordoned-primary-node.txt`,
`scale-primary-zero.log`,
`primary-deployment.before-failure.yaml`. No raw blockmaster/blockvolume
log spelunking is needed.

### HG-11 — Cleanup hygiene — PASS

R3 run cleanup (`collect_and_cleanup` phase, strict):
```text
pre_run_cleanup logged out 1 iSCSI session (iqn.2026-05.io.seaweedfs:pvc-c606d03a-...),
deleted matching iSCSI node DB entry
remaining matching iSCSI sessions: 0
assert_no_active_iscsi_sessions:  PASS
assert_no_processes:              PASS
```

Lab state verified post-run (m02 + m01 + tp01):
```text
iSCSI sessions:                                 No active sessions
iSCSI node DB entries for test IQN:             cleaned (deleted during pre_run_cleanup)
blockmaster / blockvolume / blockcsi processes: none
kubectl port-forward svc/blockmaster:           none
app=sw-blockvolume Deployments:                 No resources found
run-scoped /var/lib/sw-block/testops-*:         none
m01 cordon:                                     uncordoned (scenario cleanup honored)
```

The earlier r1/r2 attempts also produced clean residue after their (failed)
runs — cleanup hygiene survived three iterations of the same scenario
exercising different failure modes.

### HG-12 — Non-claims honest — PASS

`primary-failure-recovery.txt` + `node-loss-recovery-summary.txt`:
```text
transparent_failover_claimed=false             (Stage 1 / Stage 2 boundary preserved)
physical_host_loss_claimed=false               (Kubernetes-node-loss only)
pod_recreate_used=true                         (honest: CSI re-stage shape)
```

`docs/operations-v1.md` Stage 3 section explicitly states:
> "This gate does not claim transparent node-loss, NVMe ANA node-loss,
> arbitrary network partition tolerance, rebuild/failback, RTO/SLO, or
> production HA outside the tested topology. If three Kubernetes nodes share
> fewer physical machines, the report must keep `physical_host_loss_claimed=false`;
> that is a Kubernetes-node-loss proof, not a full physical-host-loss proof."

## Key evidence (template)

```text
topology:                          3 Kubernetes nodes (m01/m02/tp01), full-physical-host shape, LAN TCP/iSCSI
frontends:                         non-loopback (192.168.1.181/184/188:3260)
before_primary_replica/node:       r1 / m01
failed_replica/node:               r1 / m01    (matches before_primary)
promoted_replica/node:             r2 / m02
pod_recreate_used:                 true
CSI target before/after:           192.168.1.181:3260 → 192.168.1.184:3260
data_check_after_node_loss:        reader_checksum_passed
stale_primary_fencing:             old_primary_stale_io_success_count=0; target_ready_replicas=0
bounded_waits:                     pass
```

## Residue audit

```text
iSCSI sessions:                    0
iSCSI node DB for test IQN:        cleaned
sw-block processes:                none
port-forwards:                     none
k8s resources (app=sw-blockvolume): No resources found
run-scoped host paths:             none under /var/lib/sw-block/testops-*
node cordons:                      none (m01 uncordoned by cleanup)
```

## Blocking findings

None.

## Non-blocking observations

1. **Three iterations were needed to reach a clean PASS.** Each surfaced a
   real product gap and dev's fix was crisp each time:
   - r1 (152925-42b3, FAIL/cancelled): inventory mis-attributed the failed r1
     row as a duplicate `replica=r2` slot, causing the demo's "wait for
     promoted primary r2" to hang on the wrong row. Fixed in
     `core/ops/k8s_inventory.go:317` (preserve deployment-derived replica
     identity) with a regression test in `core/ops/k8s_inventory_test.go:579`.
   - r2 (154813-109a, FAIL): `sw-block-csi:local` silently missing on m02
     post-build (the build script's local-node import was implicit).
     `pin_build_alpha_images` reported PASS even though m02 had zero matches.
     Fixed by hardening `scripts/build-alpha-images.sh` to import into the
     explicit `k8s.io` containerd namespace and verify per-node after import.
   - r3 (160306-1e54): clean PASS, the substantive recovery worked end to end.
   The three failure modes were independent and orthogonal — none of them
   pointed at the substantive recovery logic, which worked correctly on the
   first attempt (the inventory issue masked it).

2. **`physical_domain_shape=full-physical-host`** is recorded but
   `physical_host_loss_claimed=false` is correctly retained because the
   injection was Kubernetes-level (cordon + scale), not a physical host
   power-off. The placement file's accurate physical-domain accounting means
   a stricter gate could later validate physical-host loss on the same lab
   without scenario rework.

3. **The `node-loss-recovery-summary.txt` 14-line format is the most
   reader-friendly recovery summary the product has produced so far.** A
   future operator UX could surface this verbatim. Worth keeping as the
   shape any future failover/recovery scenario should also emit.

## Reproducibility

The substantive product behaviour was reproduced once on QA's third attempt
(20260516-160306-1e54) and was also produced earlier by dev's internal run
20260516-154108-7bb7 — two independent runs of the same scenario producing
the same substantive evidence (r1@m01 → r2@m02, reader checksum verified,
stale primary fenced). The PASS shape is stable, not a one-off.

## Close recommendation

```text
PASS (strict) — all 13 hard-gate clauses pass on D4 run 20260516-160306-1e54.
              Node-Loss Survival MVP is ready to close.
```

The validated product claim is:

```text
A Kubernetes operator can run an RF=3 sync-quorum iSCSI volume across three
Kubernetes nodes with non-loopback iSCSI/status frontends bound to each
node's real LAN IP, CHAP authentication, loopback publish-target rejection,
and the alpha alpha multi-node image-distribution path. When the primary
Kubernetes node is taken out via cordon + blockvolume Deployment scale to
zero, the storage control plane observes r1 gone, promotes r2 to primary at
epoch=2 with sync-quorum frontier covered, exactly one new primary is
published, the old primary is fenced (zero stale I/O), CSI re-stages the
volume against the promoted replica's surviving-node frontend, the
replacement reader pod attaches and verifies the pre-failure data, and the
bundle self-explains the recovery in a 14-line summary file.
```

Non-claims remain as documented: no transparent node-loss, no NVMe ANA node-
loss, no rebuild/failback, no arbitrary network partition tolerance, no
RTO/SLO, no production HA outside the tested topology, no physical-host loss
even when the topology spans distinct physical hosts (the failure mode here
was Kubernetes-level, not power-level).

## QA needed next

Once dev closes this plan and opens the next plan, the natural next-gap
candidates surfaced by this work are:

1. **Control-Plane Observation / AI-Readable Ops** — QA needed CLI + log +
   pod-event + image-inventory inspection across three nodes to diagnose
   each failure mode. A `sw-block ops describe volume`, `sw-block ops
   timeline volume`, `sw-block ops explain volume`, `sw-block ops bundle
   volume` surface that consolidates these signals into one CLI output
   would shorten the next iteration cycle significantly. Dev has already
   flagged this as the natural next plan.

2. **Transparent node-loss without pod recreate** — Stage 2 proved this for
   the same-node multipath case; the next plan in this line would attempt
   node-loss with `transparent_failover_claimed=true`. Would need a
   substantive product change in the CSI / mount layer.

3. **Physical-host loss with the same Kubernetes topology** — the lab is
   already 3 distinct physical hosts; only the failure-injection step would
   change (e.g., ipmi power off or unplug) and the non-claim flips to
   `physical_host_loss_claimed=true`.

4. **Rebuild / failback** — bringing m01 back, having r1 rejoin as a
   replica, and validating data integrity after re-sync. Currently this is
   an explicit non-claim.

None of those are blockers for closing the current Node-Loss Survival plan.
