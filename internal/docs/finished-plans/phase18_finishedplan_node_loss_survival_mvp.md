# Current Plan: Node-Loss Survival MVP

Status: active, opened after closing
`finished-plans/phase17_finishedplan_stage2_transparent_multipath_host_failover_mvp.md`,
96% complete. D1 cold product spec and D2 strict QA hard gate are drafted. D3
topology eligibility and D4 conservative node-loss recovery scenarios are
authored. The lab has been expanded to three Ready Kubernetes nodes (`m02`
control-plane plus `m01` and `tp01` workers), the node-loss preflight passes,
and QA has now passed the D4 RF3 sync-quorum node-loss recovery gate through
CSI/pod recreate after the inventory attribution and image-import fixes:

- `ref/node-loss-survival-mvp-spec.md`
- `ref/node-loss-lab-setup.md`
- `qa-assignments/node-loss-survival-mvp-close-hard-gate.md`

QA needed now: formal close report against
`qa-assignments/node-loss-survival-mvp-close-hard-gate.md`.

## Product Question

Can a Kubernetes user lose the node that hosts the active Seaweed Block primary
and still recover the same PVC data through a documented path, with evidence
that authority moved safely, stale paths were fenced, and the data check used a
surviving replica on another node?

Stage 2 proved:

```text
same physical node
-> multiple logical replicas
-> iSCSI ALUA + dm-multipath
-> primary blockvolume pod stop
-> same mounted writer pod survives without recreate
```

This plan must move to the user-visible HA question:

```text
primary Kubernetes node becomes unavailable
-> surviving replica on another Kubernetes node is promoted
-> workload recovers through the declared mechanism
-> same PVC data verifies
```

## Product Position

This plan serves `ref/product-positioning-v1.md`: mature block storage cannot
stop at same-node process failover. A credible Kubernetes block product must
name what happens when a node fails.

The value is not "the master selected a different replica." The value is:

```text
an operator can run a documented RF3 sync-quorum Kubernetes path, inject primary
node loss, and see the PVC recover with support-bundle evidence for placement,
authority, CSI target movement, stale-primary fencing, and data integrity.
```

## Scope Decision

First close uses the conservative recovery mechanism:

```text
CSI / pod recreate reattach
```

Reason:

- It isolates the new problem: real multi-node placement and non-loopback
  target selection.
- Stage 2 already proved transparent multipath on one physical node; combining
  real node loss and transparent multipath immediately would mix too many
  failure axes.
- Users still get a valuable claim: PVC data recovers after primary node loss,
  with no manual promote/repair.

Transparent multipath node-loss is a later gate or follow-up plan:

```text
same mounted pod -> primary node loss -> host multipath switches to surviving node
```

Lab topology precision:

- The D3/D4 MVP may run on three Kubernetes nodes spread across two physical
  machines if the artifacts disclose the physical-host shape.
- That is a Kubernetes-node-loss proof, not a full physical-host-loss proof.
- The stronger physical-host-loss claim requires the failed primary and all
  surviving promotion candidates to be on distinct physical fault domains, and
  must be gated separately before external positioning uses that wording.

## Target Claim

Narrow beta-facing claim after close:

```text
For the documented RF3 sync-quorum Kubernetes topology with non-loopback
frontends, Seaweed Block can recover a PVC after controlled primary-node loss
through CSI/pod recreate reattach, and the support bundle proves authority
movement, surviving-node target selection, stale-primary fencing, and data
integrity.
```

## Explicit Non-Claims

- No transparent mounted I/O continuation under node loss in the first close.
- No NVMe ANA node-loss claim.
- No Windows MPIO claim.
- No broad multi-distro compatibility claim.
- No RTO/RPO/SLO claim beyond bounded gate timings.
- No automatic rebuild, reintegration, or failback claim.
- No arbitrary multi-failure or network-partition tolerance unless a later gate
  explicitly proves it.
- No production HA claim outside the documented lab topology.
- No full physical-host-loss claim when multiple Kubernetes nodes share one
  physical machine; that topology is an intermediate Kubernetes-node-loss lab.

## D1: Topology And Placement Audit

Audit before code:

- Does the active TestOps/k3s lab have at least two schedulable Kubernetes
  nodes?
- How does `blockmaster` cluster-spec name physical nodes and server IDs?
- Can launcher place RF3 replicas across distinct Kubernetes nodes today?
- Can generated `blockvolume` frontends use non-loopback addresses reachable
  from CSI nodes?
- Does CSI publish lookup select a frontend reachable from the consuming node?
- Can inventory prove replica -> server -> Kubernetes node -> frontend mapping?
- What should count as controlled node failure in the lab: cordon/drain,
  kubelet stop, node network isolation, or deleting the node's blockvolume pods?

Exit criteria:

- update this plan with the honest current state,
- identify the smallest implementation slice,
- do not write recovery code until the topology gap is clear.

### D1 Audit Result

Current local Kubernetes context is a single Rancher Desktop/k3s node
(`ping-r13`, internal IP `192.168.127.2`). That context cannot run the
node-loss gate. Prior QA runs used the m02 lab, but this branch does not yet
have a runner-native proof that the active QA lab has multiple schedulable
Kubernetes nodes for this plan.

Code audit findings:

- `scripts/install-k8s-alpha.sh` renders every logical server onto one
  Kubernetes node (`SW_BLOCK_ALPHA_NODE_NAME`, defaulting to the first node
  from `kubectl get nodes`) and emits `127.0.0.1` data/control addresses.
- `core/lifecycle` and `core/host/master` already have a useful base for
  distinct physical nodes: `NodeRegistration` carries
  `kubernetes.io/hostname`, `DataAddr`, and `CtrlAddr`, and
  `TestMountedFailover_WorkloadPlanAllocatesPortsByPhysicalNodeAcrossRF2Volumes`
  proves per-node port allocation across `k8s-a`/`k8s-b`.
- `core/launcher/k8s_renderer.go` still renders generated blockvolume
  frontends as loopback-only (`--iscsi-listen=127.0.0.1:<port>`,
  `--status-addr=127.0.0.1:<port>`, and NVMe similarly). That was correct for
  Stage 2 same-node ALUA, but it is invalid for node-loss attach from another
  Kubernetes node.
- `cmd/blockvolume` enforces loopback-only frontend binds for iSCSI/NVMe.
  The code already has `--iscsi-portal-addr` and CHAP support, but an external
  bind is deliberately rejected. Node-loss needs an explicit opt-in external
  frontend mode, not an accidental bypass.
- Promotion/status probing is also loopback-shaped today. The generated
  `--status-addr=127.0.0.1:<port>` is reachable only through local/pod
  context, and master promotion evidence must not depend on a failed node's
  loopback. D3 must include a routable status/probe path or an explicitly
  documented Kubernetes port-forward/service model.
- Replication data/control addresses must be routable across physical nodes.
  If only iSCSI is external but `DataAddr`/`CtrlAddr` remain loopback, RF3
  sync-quorum can render three frontends but still fail the actual replication
  and promotion-readiness contract.
- CSI already passes `nodeID` into `LookupPublishTarget`, but
  `core/csi/master_backend.go` currently ignores it and selects published
  frontends without validating reachability from the consuming node.
- Inventory can already print replica `server`, Kubernetes `node`, `frontend`,
  `status_addr`, and promotion evidence. The node-loss gate should add stable
  derived lines such as `replicas_on_distinct_nodes=true` and
  `frontends_non_loopback=true` so QA does not infer eligibility from ad-hoc
  greps.

Conclusion: the next slice is not recovery logic. The next slice is a topology
eligibility implementation that makes cross-node targets real and auditable.

Smallest implementation slice:

```text
alpha cluster-spec can describe multiple Kubernetes nodes with routable data/control addrs
-> generated blockvolume Deployments bind/listen through explicit external iSCSI mode
-> status/probe endpoints are routable or explicitly service/port-forwarded
-> iSCSI advertises a routable portal address for each node
-> CSI rejects loopback targets for the node-loss profile and selects the current primary target
-> inventory emits distinct-node and non-loopback eligibility markers
-> runner gate proves RF3 placement and attach eligibility without failure injection
```

Security boundary for the external iSCSI mode:

- external bind must be opt-in and refused by default,
- the first node-loss gate should require CHAP for external iSCSI unless a
  later security review approves a narrower lab-only exception,
- CHAP must be plumbed on both sides: generated blockvolume target secret and
  CSI `NodeStage` secret delivery,
- NVMe external bind remains non-claimed for this plan.

D3a implementation started:

- `cmd/blockvolume` now has an explicit `--allow-external-iscsi-bind` flag.
  External iSCSI remains rejected by default, and opt-in external iSCSI requires
  target-side CHAP.
- `core/launcher` can render external iSCSI listens from the placement node
  address only when `K8sRenderConfig.ExternalISCSI` is enabled and a CHAP Secret
  is configured.
- `cmd/blockmaster` exposes `--launcher-external-iscsi` to pass that renderer
  mode.
- `scripts/install-k8s-alpha.sh` and `scripts/run-alpha-app-demo.sh` now accept
  explicit multi-node cluster-spec input through `SW_BLOCK_ALPHA_NODE_SPECS`
  entries shaped as `server_id|kubernetes_node|host_or_ip|pool_id`. This keeps
  the old single-node default intact while giving the node-loss runner a way to
  render routable `data_addr` / `ctrl_addr` values.
- The same scripts can inject `--launcher-external-iscsi` and
  `--launcher-iscsi-chap-secret-name=<name>` via
  `SW_BLOCK_LAUNCHER_EXTERNAL_ISCSI=1`.
- `scripts/run-alpha-app-demo.sh` now wires external iSCSI CHAP end-to-end for
  the app path: requires username/secret, creates the Secret in app and
  blockvolume namespaces, injects `csi.storage.k8s.io/node-stage-secret-*` into
  the rendered StorageClass, and cleans the Secret up.
- `scripts/install-k8s-alpha.sh` now refuses external iSCSI without CHAP
  credentials and creates the configured CHAP Secret for standalone install
  flows.
- CSI now has an explicit `--reject-loopback-publish-targets` guard. When
  enabled, master-backed publish lookup skips loopback or malformed frontend
  addresses and fails closed if no routable target remains.
- The alpha install/demo scripts can inject
  `--reject-loopback-publish-targets` through
  `SW_BLOCK_REJECT_LOOPBACK_PUBLISH_TARGETS=1`.
- `cmd/blockvolume` now has an explicit `--allow-external-status-bind` flag.
  The unauthenticated status API remains loopback-only by default; node-loss
  gates can opt into a concrete, non-loopback node-address bind so blockmaster
  can probe surviving replicas across nodes.
- `core/launcher` can render external status endpoints from the same placement
  node address when `ExternalStatus` is enabled alongside external iSCSI. The
  generated workload receives both `--status-addr=<node-ip>:<port>` and
  `--allow-external-status-bind`.
- `cmd/blockmaster` exposes `--launcher-external-status` and passes that mode
  to both the renderer and the workload-plan promotion evidence provider, so
  promotion probes use node-address status endpoints instead of
  `127.0.0.1:<port>`.
- The alpha install/demo scripts can inject `--launcher-external-status` via
  `SW_BLOCK_LAUNCHER_EXTERNAL_STATUS=1`.
- External node-loss mode now rejects loopback/unspecified node-spec hosts at
  the script, renderer, blockvolume bind, status bind, and promotion-probe
  boundaries. A bad `SW_BLOCK_ALPHA_NODE_SPECS` entry must fail early instead
  of producing fake cross-node evidence.
- `scripts/uninstall-k8s-alpha.sh` now deletes the configured iSCSI CHAP Secret
  from app and `kube-system` namespaces so standalone external-iSCSI installs
  do not leave credentials behind.
- Master status now publishes observed frontends for all assigned replicas, with
  the current primary replica first. This keeps multipath consumers able to see
  every path while making the conservative CSI/pod-recreate reattach path select
  the promoted primary instead of a stale topology-first replica.
- CSI's single-target control-status lookup now has a node-loss guard test that
  consumes the first routable frontend as the current-primary target when
  loopback rejection is enabled.
- Inventory now emits stable topology eligibility evidence per volume:
  `replicas_on_distinct_nodes=<bool>` and `frontends_non_loopback=<bool>` in
  both JSON and the summary. These are evidence markers, not health classifiers,
  so older single-node/non-node-loss paths are not made unhealthy by the marker.
- `scripts/preflight-node-loss-lab.sh` now provides a reusable lab readiness
  check. It emits `NODE_SPECS`, selected writer app node, selected survivor
  reader node, Kubernetes-node placement, physical-domain shape, and
  `physical_host_loss_claimed=false`. The D3/D4 scenarios call this helper
  instead of carrying private discovery logic.
- `scripts/run-alpha-app-demo.sh` now supports
  `SW_BLOCK_DEMO_READER_NODE_NAME` so conservative pod-recreate recovery can
  schedule the replacement reader onto a surviving node after the primary node
  is cordoned.
- `scripts/run-alpha-app-demo.sh` now supports
  `SW_BLOCK_DEMO_FAIL_PRIMARY_BEFORE_READER=cordon-node-scale-primary-to-zero`.
  The mode derives the primary node from live inventory, cordons that
  Kubernetes node, scales the primary blockvolume Deployment to zero, records
  `failure_class=primary-kubernetes-node-cordoned-blockvolume-stop`, and
  uncordons the node during cleanup.

Still open before QA can produce live validation:

- QA validation of the runner-native multi-node eligibility scenario:
  `testops/scenarios/node-loss-topology-eligibility-chain.yaml`. The scenario
  supplies real `SW_BLOCK_ALPHA_NODE_SPECS`, enables external iSCSI/status plus
  loopback publish-target rejection, and asserts RF3 placement with
  `replicas_on_distinct_nodes=true` and `frontends_non_loopback=true`.
- QA validation of the runner-native conservative recovery scenario:
  `testops/scenarios/node-loss-survival-rf3-reattach-chain.yaml`. It uses the
  same topology, cordons the live primary node, stops the primary blockvolume
  workload, recreates the reader on a survivor node, and requires
  `data_check_after_node_loss=reader_checksum_passed`.
- optional stronger publish-target identity. Current-primary-first ordering
  closes the immediate conservative reattach path, but `FrontendTarget` still
  does not encode server/node identity; add that only if D3 evidence needs more
  than stable ordering plus inventory markers.

## D2: QA Hard Gate

Hard gate is drafted at
`qa-assignments/node-loss-survival-mvp-close-hard-gate.md`.

It fails if:

- all replicas run on one physical node,
- any cross-node target is loopback,
- the proof uses only a pod stop instead of node failure/isolation,
- reader attaches to the old failed-node frontend,
- authority movement is not visible,
- stale primary is untested,
- cleanup leaves unexplained residue.

## D3: Multi-Node Topology Eligibility Gate

Add the first runner-native gate only after D1 audit:

```text
RF3 PVC
-> replicas placed on distinct Kubernetes nodes
-> non-loopback frontends
-> inventory shows mapping
-> no node failure injection yet
```

This gate proves the topology is eligible for node-loss recovery.

D3 scenario authored:

```text
testops/scenarios/node-loss-topology-eligibility-chain.yaml
```

It fails fast unless the lab has three Ready schedulable Kubernetes nodes with
non-loopback InternalIPs. Those nodes may share two physical machines for this
MVP gate, but the scenario records `physical_domain_count`,
`physical_domain_shape`, and `physical_host_loss_claimed=false` so the evidence
cannot be misread as full physical-host-loss proof. It then runs RF3
sync-quorum with external iSCSI, external status, CHAP, and loopback
publish-target rejection. It stops after writer verification and asserts the
inventory eligibility markers before cleanup.

D3 QA run 20260515-204300-c892:

```text
pre_clean                    PASS
preflight                    PASS
discover_multinode_topology  FAIL
collect_and_cleanup          PASS
```

Reason:

```text
node-loss topology gate requires at least 3 Ready schedulable nodes with
non-loopback InternalIP; found 1
```

This is an intentional fail-closed lab-capacity result. It did not exercise the
RF3 external iSCSI/status, CHAP, loopback-rejection, writer, or inventory
eligibility assertions. It must not be treated as product validation.

Lab update 2026-05-16:

```text
m02  Ready  control-plane  192.168.1.184  physical-host=m02
m01  Ready  worker         192.168.1.181  physical-host=m01
tp01 Ready  worker         192.168.1.188  physical-host=tp01
```

Fabric boundary: this is a LAN TCP/iSCSI topology using `192.168.1.x`.
`tp01` does not have RoCE and must not be used for any `10.0.0.x` RDMA fabric,
NVMe/RDMA, or performance claim in this plan.

`scripts/preflight-node-loss-lab.sh --min-k8s-nodes 3` now passes and emits:

```text
node_loss_topology_eligible=true
selected_node_count=3
physical_domain_count=3
physical_domain_shape=full-physical-host
replica=r1 server=node-loss-r1 node=m01 host=192.168.1.181 physical_host=m01
replica=r2 server=node-loss-r2 node=m02 host=192.168.1.184 physical_host=m02
replica=r3 server=node-loss-r3 node=tp01 host=192.168.1.188 physical_host=tp01
```

Next unblocker:

```text
rerun node-loss-topology-eligibility-chain.yaml
-> only then proceed to D4 node-loss recovery
```

D3 QA run 20260516-134043-becf on the expanded lab advanced past topology
discovery and failed in `rf3_external_topology` because `sw-blockmaster`
scheduled on `m01`, where `sw-block:local` had not been imported. This was a
real multi-node alpha pipeline gap: `scripts/build-alpha-images.sh` imported
local images only into m02's k3s containerd.

Fix implemented and smoke-validated:

```text
preflight emits K3S_IMPORT_NODES=<selected node IPs>
-> D3/D4 pin_build sources node-specs.env
-> build-alpha-images imports sw-block and sw-block-csi into each selected k3s node over SSH
```

This keeps the scheduler free to place blockmaster and blockvolume workloads on
any eligible node while avoiding a registry dependency for the lab.

Focused dev smoke on m02:

```text
K3S_IMPORT_NODES=192.168.1.181,192.168.1.184,192.168.1.188
build-alpha-images exit=0
m01: sw-block:local and sw-block-csi:local present in k3s containerd
m02: sw-block:local and sw-block-csi:local present in k3s containerd
tp01: sw-block:local and sw-block-csi:local present in k3s containerd
```

Dev rerun of D4 also exercises the same image-import path successfully, so D3's
prior image-locality blocker is considered fixed pending QA rerun.

An acceptable intermediate lab can be:

```text
m02 control-plane + m01 worker + one VM/containerized worker
```

as long as `kubectl get nodes` shows three Ready schedulable nodes with
non-loopback InternalIPs. The close report must disclose if two Kubernetes nodes
share one physical host.

Before asking QA for another D3 run, dev can run:

```bash
bash scripts/preflight-node-loss-lab.sh \
  --min-k8s-nodes 3 \
  --env-out /tmp/node-loss.env \
  --placement-out /tmp/node-placement.before.txt
```

If this exits non-zero, the full D3 scenario will fail closed for the same lab
reason.

Preflight contract after QA's m02 check:

- eligible lab: exit `0`, writes `node-loss.env` and
  `node-placement.before.txt`;
- ineligible lab: exit `3`, writes a negative placement file with
  `node_loss_lab_eligible=false`, removes/skips the env file, and must block
  D3/D4 reruns.

## D4: Conservative Node-Loss Recovery Gate

Runner-native recovery gate:

```text
writer writes /data/demo.bin
inventory identifies primary replica and primary node
controlled failure isolates/stops the primary node path
master promotes a surviving replica
replacement pod reattaches through CSI to surviving frontend
reader verifies /data/demo.bin
bundle records node_loss_recovery_claimed=true and transparent_failover_claimed=false
cleanup proves no residue
```

D4 scenario authored:

```text
testops/scenarios/node-loss-survival-rf3-reattach-chain.yaml
```

It is intentionally a Kubernetes-node-loss gate, not a physical-host-loss gate.
It records `physical_host_loss_claimed=false`, `pod_recreate_used=true`, and
`transparent_failover_claimed=false`. The replacement reader uses
`SW_BLOCK_DEMO_READER_NODE_NAME=$SURVIVOR_APP_NODE`, so success requires CSI to
reattach on a survivor node rather than scheduling back onto the cordoned
primary node.

D4 QA run 20260516-152925-42b3 was canceled after the recovery phase appeared
stuck. The important diagnosis from that run: master had already promoted the
volume to r2, but inventory relabeled the failed r1 deployment row as
`replica=r2` because the nested status report reflected the current primary.
The scenario then grepped the first `replica=r2` row, saw
`role=unavailable`, and waited until timeout. This was an inventory row
identity bug, not an authority-promotion failure.

Fix:

```text
core/ops/k8s_inventory.go
-> keep deployment-derived replica identity (`--replica-id`) stable
-> do not overwrite `replica.ReplicaID` from a status report's current-primary field
```

Regression guard:

```text
TestKubernetesInventoryCollector_PreservesDeploymentReplicaIdentityWhenStatusReportsCurrentPrimary
```

Validation:

```text
go test ./core/ops -count=1                                      PASS
remote m02 targeted regression                                  PASS
D4 dev rerun 20260516-154108-7bb7                               PASS
state/phases/actions                                             pass / 8/8 / 74/74
before primary                                                   r1@m01
failed node                                                      m01
promoted replica                                                 r2@m02
post_failure_primary_count                                       1
before frontend                                                  192.168.1.181:3260
after frontend                                                   192.168.1.184:3260
CSI recovery method                                              pod recreate
reader data check                                                /data/demo.bin: OK
data_check_after_node_loss                                       reader_checksum_passed
transparent_failover_claimed                                     false
physical_host_loss_claimed                                       false
cleanup                                                          clean
```

After-failure inventory now preserves all three rows correctly:

```text
r1 node=m01 role=unavailable
r2 node=m02 role=primary
r3 node=tp01 role=unknown replication=replica_ready
```

D4 QA rerun 20260516-154813-109a failed before the recovery path because the
fresh CSI node DaemonSet hit `ImagePullBackOff` on m02:

```text
sw-block-csi-node on m02: ImagePullBackOff for sw-block-csi:local
m01: sw-block:local and sw-block-csi:local present
m02: product images missing from k8s.io containerd namespace
tp01: sw-block:local and sw-block-csi:local present
```

This did not exercise the inventory fix; the writer never attached and the
scenario never reached cordon/scale-down.

Image pipeline hardening:

```text
scripts/build-alpha-images.sh
-> import explicitly into k3s containerd namespace k8s.io
-> verify sw-block:local and sw-block-csi:local after import
-> when SW_BLOCK_IMPORT_K3S_NODES is set, also import/verify the local k3s node if available
```

Dev validation 2026-05-16:

```text
build/import artifact: /mnt/smb/work/share/g15d-k8s/dev-image-import-verify-20260516-160041
m01: docker.io/library/sw-block:local and docker.io/library/sw-block-csi:local present
m02: docker.io/library/sw-block:local and docker.io/library/sw-block-csi:local present
tp01: docker.io/library/sw-block:local and docker.io/library/sw-block-csi:local present
```

The observation API/support-bundle spec now treats this class as first-class
operator evidence: `image_missing_on_node`, `pod_image_pull_failed`, CSI pod
events/logs, and per-node image inventory should be available through
`sw-block ops explain` / bundle rather than requiring manual SSH checks.

D4 QA rerun 20260516-160306-1e54 passed strictly:

```text
state/phases/actions                                             pass / 8/8 / 74/74
lab                                                               m01 + m02 + tp01 3-node k3s
transport                                                         LAN TCP/iSCSI, CHAP, external iSCSI/status
ack_profile                                                       sync-quorum
failure_class                                                     primary-kubernetes-node-cordoned-blockvolume-stop
before_primary_replica/node                                       r1@m01
failed_replica/node                                               r1@m01
promoted_replica/node                                             r2@m02
before_publish_target_frontend                                    192.168.1.181:3260
after_publish_target_frontend                                     192.168.1.184:3260
post_failure_primary_count                                        1
old_primary_stale_io_success_count                                0
target_ready_replicas                                             0
pod_recreate_used                                                 true
node_loss_recovery_claimed                                        true
data_check_after_node_loss                                        reader_checksum_passed
transparent_failover_claimed                                      false
physical_host_loss_claimed                                        false
cleanup                                                           strict clean
```

This run is the first end-to-end Kubernetes-node-loss recovery proof for this
plan: primary node cordoned, primary blockvolume stopped, master promoted a
surviving replica, CSI reattached to the survivor frontend, and the replacement
reader verified the pre-failure data.

## D5: Transparent Multipath Node-Loss Decision

After D4 passes, decide whether to extend this plan or open the next one for:

```text
same mounted pod
primary node loss
host multipath path switch to another node
no pod recreate
post-failure data check
```

Do not merge this into D4 unless the lab/product path is already stable.

## Gates To Close

This plan closes only when:

1. The lab topology has distinct Kubernetes nodes and non-loopback frontends.
2. A strict QA hard gate exists before implementation is treated as complete.
3. Fast tests protect node-aware placement, target selection, and non-loopback
   rejection.
4. A runner-native gate proves recovery after controlled primary-node loss.
5. Support bundles explain node placement, authority movement, CSI target
   movement, stale-primary fencing, bounded waits, and data verification.
6. User-facing docs distinguish Stage 2 same-node transparent failover from
   node-loss recovery.
7. QA validates independently and reports no blocking issue.
