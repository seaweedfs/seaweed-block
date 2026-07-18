# Product Roadmap

This is the short internal roadmap. Keep it current and readable.

## Product Goal

- Build a small Kubernetes block storage service that is easier to try and
  reason about than a large distributed-storage stack.
- Target early users running lab or small Kubernetes clusters.
- Keep alpha/beta claims narrow: dynamic PVC, iSCSI/NVMe protocol-gated paths,
  app write/read, clean teardown, and explicit non-claims.
- CHAP, iSCSI ALUA, NVMe ANA, CSI protocol selection, and mounted failover are
  release-gated in the lab. Do not claim production HA, broad distro
  compatibility, performance, upgrade safety, or multi-node readiness until
  separately tested.

## Product Phases

### Release Ladder

- `v0.1-alpha`: Kubernetes block foundation. CSI/PVC path, product-owned
  blockvolume lifecycle, inventory, recovery gates, iSCSI ALUA/dm-multipath,
  node-loss recovery, and read-only control-plane observation are proven in
  lab gates. This is an engineering alpha, not a polished install product.
- `v0.2-alpha`: Day-1 activation. A supported user can run the script-based
  activation path, create a first PVC, verify writer/reader data, generate a
  local read-only report, and clean up. This remains the script fallback
  boundary.
- `v0.3-alpha`: Helm activation. Package the same supported Day-1 path as a
  normal Kubernetes add-on: chart values, immutable images, CHAP/external
  iSCSI configuration, StorageClass, readiness output, first-volume smoke,
  local read-only report/dashboard, and uninstall hygiene. Phase 25 closed this
  release target on 2026-05-22.
- `v0.3.4-alpha`: Helm lifecycle, restart persistence, read-only operations
  surface, deterministic cleanup, and negative-first status vocabulary merged
  through PR #50 (`8102cf3`) on 2026-05-27.
- `v0.3.5-alpha` candidate: TestOps failure hardening. Expand release proof
  from happy-path gates to negative paths: blocked/stale/unreachable status
  surfaces, support-bundle replay under corrupt/partial evidence, failed-run
  cleanup, and multi-volume interference checks. Phase 33 closed this scope;
  release packaging remains a separate decision.
- `v0.3.6-alpha`: Phase 34 test realism and anti self-proving gates. Selected
  replay or summary-grep checks were upgraded into independent live/dirty
  evidence: live status endpoint unreachable, restart convergence, SmartWAL
  corruption refusal, and targeted cross-validation between helper summaries
  and product or Kubernetes facts. Phase 34 closed on 2026-06-02.
- `v0.4-beta-candidate`: Kubernetes-native read-only operator foundation. Phase
  35 closed this scope on 2026-06-04 with CRDs, Conditions, Events, a
  status-only controller, stable Event identity, stale/blocked status gates,
  and read-only RBAC proof. This is the first release boundary that can
  credibly feel like a normal Kubernetes product loop rather than an install
  script. Mutating workflows such as repair, rebuild, failback, delete safety,
  automatic cleanup, and CR object ownership come after the read-only status
  contract is stable.
- Closed on 2026-06-06: Phase 36 Productized Operations Actionability. It uses
  the Phase 35 CRD/status/Event foundation to publish node readiness, support
  evidence refs, cleanup visibility, safe next-step hints, and cross-surface
  agreement. This remains read-only and does not add mutating operator
  lifecycle.
- Recently closed development slice: NVMe Kubernetes CSI multipath attach. The
  returned-replica rebuild/reintegration/failback/frontend-publication operation
  loop reached a coherent close point at Phase 98, and Phase 99 pinned the
  existing NVMe ANA/CSI baseline. Phase 100 closed the next storage feature for
  the supported lab path: multiple NVMe frontend paths for one NQN/NSID now
  survive dynamic PVC provisioning, master status, CSI publish context,
  NodeStage, app writer/reader I/O, and delete cleanup.
  Phases 46-67 made
  returned replicas visible, fenced, action-gated, rebuild/catch-up-capable,
  terminal-evidence-driven, and ACK-eligibility-published. Phases 68-98 then
  connected disabled frontend-publication preflight, failback contracts,
  opt-in failback execution, frontend-publication targets/executors, and a
  live post-publication writer/reader close gate. Default automatic failback
  remains off. Do not claim backup/restore, broad NVMe compatibility, RoCE,
  performance, or production HA yet.
- Model hardening gate before the next large release: complete the
  ManagedVolume Operations Model under `internal/docs/protocol/` before
  expanding operator or broader HA claims. The goal is to prevent Kubernetes,
  CSI, authority, host-path, recovery, and future NVMe logic from becoming
  scattered scripts or unrelated small automata.
- Current backend optimization slice: Phases 148-152 took multi-block WAL
  records from local prototype to disabled-by-default runtime opt-in, mounted
  NVMe/TCP profile evidence, and mounted restart/recovery compatibility. Phase
  152 recovered `LSN=14545` after a force-deleted `blockvolume` restart with
  hostPath persistence. This is still default-off and not a performance/SLO,
  RoCE, or NVMe/RDMA claim. Phase 153 closed the release-boundary documentation
  gate for this source-gated opt-in. Phase 154 fixed the local diagnostic
  durable-status `HeadLSN` display by separating WAL byte-position metadata
  from LSN boundaries; Phase 155 confirmed the same boundary in the mounted K8s
  path with `DurableLSN == HeadLSN == recovered_lsn_after_restart=13511`.
  Phase 156 keeps the opt-in source-gated until a future matching-image release
  smoke validates the explicit opt-in recovery/status path on published
  artifacts.
- Phase 157 keeps RoCE/NVMe-RDMA as a product non-claim until the product owns a
  real RDMA transport path and passes standalone live I/O, Kubernetes
  publish/attach, status-surface, fallback/refusal, and cleanup gates. Host RDMA
  capability and external VFS/object RDMA evidence are inputs, not Block
  NVMe/RDMA product evidence.
- Phase 158 adds the first read-only product-owned capability probe:
  `/status/frontend-capabilities` reports NVMe/TCP supported and NVMe/RDMA
  unsupported with a stable reason. It does not start an RDMA listener or claim
  RoCE/NVMe-RDMA attach.
- Phase 159 defines the standalone NVMe/RDMA listener design gate. It rejects a
  fake TCP-over-RDMA implementation shape and selects a transport adapter seam
  before any Kubernetes publish/attach or performance claim.
- Phase 160 adds the transport adapter seam in code while preserving TCP tests
  and the RDMA unsupported capability surface.
- Phase 161 adds read-only RDMA preflight/refusal facts for module, device, and
  bind-address evidence while preserving the unsupported status.
- Phase 162 adds a disabled-by-default listener start decision skeleton. It
  reports start refusal reasons but still does not start an RDMA listener or
  claim live I/O.
- Phase 163 closes the first real standalone NVMe/RDMA data path. A standard
  Linux initiator connected over the 10.0.0.x RoCE network, wrote, flushed,
  and read back data through kernel `nvmet-rdma`, the product-owned NBD bridge,
  and the Seaweed backend. This source-gated claim excludes Kubernetes
  publication, failover, broad compatibility, and performance/SLOs.
- Phase 164 closes standalone hardening across rollback, flush/FUA, durable
  restart/reconnect, multi-target isolation, bounded churn, refusal evidence,
  and zero-residue cleanup.
- Phase 165 closes an opt-in, explicitly typed Kubernetes RDMA publish/attach
  path with mounted workload I/O. A dynamic PVC uses the RoCE target address,
  CSI and CRD status retain `rdma`, the Linux initiator confirms the active
  RDMA controller, and exact cleanup returns to baseline. NVMe/TCP remains the
  default; RDMA reconnect/failover and performance remain non-claims.

Do not skip from scripts directly to mutating operator lifecycle. Helm has
stabilized the installation contract, and Phase 35 added read-only CRD status,
Conditions, and Events. Any in-cluster controller ownership of upgrades, repair,
rebuild, delete safety, or cleanup must start as a separate gated phase.

### Alpha Preview

- Status: protocol-gated; moving toward beta hardening.
- Single-node k3s quick start works.
- Dynamic PVC create/delete works.
- App pod writes and reads through a PVC.
- Cleanup leaves no active iSCSI sessions and no visible Kubernetes residue.
- Default frontend: iSCSI.
- Optional frontend: NVMe-oF, behind explicit protocol selection and release
  gates.
- Default backend: `walstore`.

### Alpha Stabilization

- Make cleanup product-owned instead of script-owned.
- Make PVC owner-reference cleanup the default path.
- Stabilize iSCSI with real OS initiators and larger filesystem writes.
- Make repeated create/write/read/delete stable.
- Keep artifact bundles useful for QA and issue reports.
- Keep docs modest and accurate.

### Beta Candidate

- Multi-node Kubernetes attach.
- Durable volume state across `blockvolume` restart. (Closed for supported
  single-node RF=1 alpha path in phase 13.)
- Same-node multi-node-capable attach and placement visibility. (Closed for
  the supported RF=1 loopback alpha path in phase 14.)
- Basic failover with an attached workload.
- Returned replica lifecycle: observed -> candidate -> syncing/rebuilding ->
  ready.
- Explicit ACK profile: best-effort, sync-quorum, or sync-all
  (quorum/full-ack aliases may appear in older docs or discussions).
- TestOps can run named smoke scenarios and return result bundles.

### Production Candidate

- Soak and fault testing.
- Upgrade and uninstall story.
- Security and resource hardening.
- Operator-visible status and diagnostics.
- Reproducible release images.
- Documented operational limits.

### Iterative Release Rule

- Do not wait for the full enterprise block vision before publishing useful
  slices.
- Every hardening phase should have a user-visible preview with clear
  non-claims.
- TestOps evidence is release evidence, but user installs and issues are the
  feedback loop.
- Keep the basic block product and basic runner open enough to build trust;
  reserve advanced fleet automation, private scenario corpus, hosted
  validation, and enterprise operations as possible enterprise layers.

## Priority Tracks

### Track A: Kubernetes Install And Cleanup

- Closed: v0.2 alpha closes the script-based Day-1 loop for the supported
  Kubernetes path: activate, verify node readiness, create a first PVC, run
  writer/reader verification, inspect status/report evidence, and clean up.
  Product-owned generated `blockvolume` workload lifecycle is also closed for
  the supported alpha path.
- Closed on 2026-05-22: v0.3 Helm activation. Package the same Day-1 path as a chart while
  keeping preflight and host cleanup explicit:
  - charted blockmaster, CSI controller/node, RBAC, CSI driver, StorageClass,
    CHAP Secret, and cluster spec,
  - values for image tags/digests, ACK profile, external iSCSI/status,
    Stage 2 multipath, namespace, and StorageClass,
  - generated Day-1 values file for multi-node labs,
  - `helm install` + first PVC smoke + `sw-block ops report`,
  - local read-only `sw-block ops dashboard` over the same evidence,
  - `helm uninstall` plus explicit host cleanup verification.
- Next major milestone: Phase 35 read-only operator foundation. Introduce
  CRDs/Conditions/Events and status-only reconciliation after the Helm contract
  is stable. Keep mutating cleanup, delete finalizers, upgrade execution, and
  repair/rebuild outside this first operator slice.

### Track B: iSCSI Frontend Stability

- Current: Linux/open-iscsi and Windows 11 built-in Initiator correctness are
  validated for the current alpha claim. Evidence includes mkfs/NTFS format,
  checksum write/read, cleanup, and Linux dmesg-delta checks.
- Next: move remaining iSCSI work to component-first session/backend pressure
  tests instead of broad V2 porting.
- Later: larger compatibility coverage across host distros and initiator
  versions.

### Track C: Durable State

- Current: durable hostPath restart/reattach is closed for the supported
  single-node RF=1 alpha path. Users can configure a run-scoped durable path,
  restart the generated `blockvolume`, reattach through the PVC, and verify
  durable status/inventory evidence.
- Next: keep durable evidence wired into multi-node attach and later
  availability gates.
- Later: integrate returned-replica rebuild and storage-engine compaction.

### Track D: Availability And Recovery

- Current: Stage 1 mounted recovery and Stage 2 transparent host-path failover
  are closed for the documented Kubernetes alpha path. Stage 1 proved RF=3
  `sync-quorum` recovery through CSI/node reattach on pod recreate. Stage 2
  proved RF=3 `sync-quorum` iSCSI ALUA plus Linux dm-multipath failover with
  the same mounted writer pod, no pod recreate, no CSI re-stage, stale-primary
  fencing, and post-failure checksum verification.
- Next: Node-loss survival is the highest-impact availability gap. Move from
  same-node logical replica failure to real Kubernetes node failure semantics:
  distinct nodes, non-loopback frontends, node-aware publish target selection,
  authority promotion to a surviving replica, host-path recovery through the
  documented mechanism, and support-bundle proof of fencing and data integrity.
- Current returned-replica status: observed returned replicas are productized
  through the ManagedVolume surface and executor/action model. Phase 60 proved
  the rebuild/catch-up datapath can converge durable content. Phase 61 added
  the authority-executor runtime call-site. Phase 62 added HTTP runtime
  transport. Phase 63 schema-locked runtime target facts. Phase 64 added an
  opt-in blockvolume runtime endpoint that starts local rebuild/catch-up only
  after primary and lineage/session validation. Phase 65 adds terminal runtime
  evidence: `started -> running -> durable frontier -> caught_up`. Phase 66
  consumes caught-up as a disabled publication preflight. Phase 67 publishes
  ACK eligibility after caught-up. Phase 68 makes frontend publication
  explicitly disabled on that eligibility. Phase 69 creates the next typed
  frontend-publication target object without executing publication. Phase 70
  adds the status-only frontend publication executor boundary. Phase 71 adds
  the live API/RBAC boundary for that executor. Phase 72 adds the typed runtime
  contract for frontend publication, but no real endpoint yet. Phases 74-86 add
  the returned-replica failback contract, failback target CR, failback executor,
  master-owned authority runtime, disabled-by-default blockmaster RPC, executor
  gRPC transport, chart wiring, real-master local smoke, policy safety gate, and
  gRPC/HTTP endpoint decoupling. This is an opt-in/source-gated failback
  runtime path. Phase 88 packages the disabled-by-default failback target owner,
  failback executor, and blockmaster gRPC runtime as one explicitly enabled
  Helm suite with schema coverage and bounded RBAC. Phases 89-95 add current
  authority facts, activation, handoff isolation, deployed render, real
  blockmaster gRPC smoke, and a live k3s deployed-suite gate. Phase 96 plans a
  disabled frontend-publication target from terminal `failed_back` evidence.
  Phase 97 wires the explicit-policy frontend-publication executor call-site.
  Phase 98 closes the deployed workload-visible path: product-owned frontend
  publication after failback is followed by writer/reader verification and
  zero-residue cleanup in live k3s.
- Phase 101 NVMe Hardening And Soak is closed. Phase 100 proved the Kubernetes
  CSI NVMe multipath attach path; Phase 101 hardened it with path
  identity/health status, one-path-failure honesty, repeated stage/unstage
  residue checks, and a bounded writer/reader soak. Backup/restore, stronger
  committed-frontier reporting, broad distro/host compatibility, RoCE, and
  performance remain later work.
- Phase 102 NVMe Release Artifact Smoke is blocked only on matching
  published images. It validates that the published `seaweed-block` /
  `seaweed-block-csi` image pair can run the Phase 100 Kubernetes NVMe
  multipath attach gate and that binaries extracted from the published
  `seaweed-block` image pass the Phase 101 standalone hardening gates.
- Phase 103 NVMe Multi-Host / RoCE Preflight is closed. It adds a read-only host
  capability gate before any RoCE, multi-host, or performance claim: NVMe/TCP
  capability must be inspectable, RDMA devices and `nvme-rdma` capability can
  make a host a candidate, and missing RoCE hardware must surface as a
  non-claim rather than a failed product gate. A candidate host is still not a
  RoCE product claim until a live RoCE I/O gate passes.
- Phase 104 RoCE Live-I/O Feasibility Boundary is complete. Because the current
  NVMe target is explicitly TCP-only, this phase adds a machine-checkable
  refusal: `--nvme-transport=rdma` must fail clearly and release wording must
  keep RoCE as a non-claim. This is not a RoCE implementation.
- Phase 105 Multi-Host NVMe/TCP Topology Boundary is closed. It blocks
  cross-node loopback NVMe/TCP evidence with the existing protocol-neutral
  `publish_target_loopback_cross_node` reason, keeps `Ready=True` false, and
  surfaces `observe.inspect_publish_target_topology` instead of an iSCSI-only
  remediation.
- Phase 106 NVMe/TCP Cross-Node Non-Loopback Live Attach is closed for the
  supported lab path. External NVMe/TCP is opt-in at `blockvolume`, rendered by
  blockmaster/launcher/Helm, generated by
  `sw-block ops generate-helm-values --protocol nvme`, and does not leak
  iSCSI/CHAP remediation. The positive live gate proved a workload on a
  different Kubernetes node can write/read through a routable NVMe/TCP target
  (`192.168.1.181:4420`) with managed-volume status
  `ready/first_volume_verified`; a separate strict cleanup audit returned zero
  residue.
- Phase 107 NVMe/TCP Multi-Volume Cross-Node Isolation is closed for the
  supported lab path. Two PVCs using `protocol=nvme` can be mounted by
  writer/reader pods pinned to another Kubernetes node; both volumes report
  `ready/first_volume_verified`, expose distinct volume IDs and distinct NVMe
  NQNs, avoid loopback publish targets, and avoid cross-volume identity mix-up.
  A separate strict cleanup audit returned zero residue.
- Phase 108 NVMe/TCP Multi-Volume Lifecycle Soak is closed for the supported
  lab path. Two consecutive two-PVC NVMe/TCP cross-node lifecycle cycles
  verified writer/reader data and then drained generated blockvolume pods,
  matching PVs, and SeaweedFS NVMe subsystems before each cycle could claim
  helper cleanup success. Final strict cleanup returned zero residue.
- Phase 109 NVMe/TCP Status Surface Evidence is closed for the supported lab
  path. Two NVMe/TCP PVCs now expose matching protocol, NQN, namespace ID,
  address, path count, ready status, and `first_volume_verified` reason across
  `SwBlockVolume.status.nvme`, report summary, report operator snapshot,
  dashboard operator snapshot, and `ops explain`.
- Phase 110 NVMe/TCP Path-Loss Status Surface Honesty is closed. The real
  standalone mounted one-path-loss evidence from Phase 101 now replays through
  report, operator-snapshot, dashboard, and explain as
  `blocked/nvme_multipath_path_missing`, with `path_count=1`,
  `multipath_observed=false`, read-only actions, and no false `Ready=True`.
  This deliberately remains a support-surface replay gate, not a live
  Kubernetes CRD negative-path-loss claim.
- Phase 111 NVMe/TCP K8s Path-Loss CRD Honesty is closed. The supported-lab
  Kubernetes path now proves the same negative behavior through the authoritative
  CRD: one RF=2 NVMe/TCP PVC starts with two observed paths and
  `Ready=True/first_volume_verified`; scaling one generated blockvolume
  deployment to zero reduces the path count to one, and `SwBlockVolume.status`,
  report, operator-snapshot, dashboard, and explain all report
  `blocked/nvme_multipath_path_missing` with no false volume `Ready=True`.
- Phase 112 NVMe/TCP K8s Mounted Path-Loss I/O is closed. The supported-lab
  Kubernetes path now keeps a mounted pod alive through one observed NVMe path
  loss: the pod UID is unchanged, the same pod writes and reads after path loss,
  and the CRD/report/operator-snapshot/dashboard/explain surfaces still report
  `blocked/nvme_multipath_path_missing` with no false volume `Ready=True` and
  zero cleanup residue.
- Phase 113 NVMe/TCP K8s Mounted Path Restore is closed. The supported-lab path
  now restores the removed blockvolume deployment after one-path loss, keeps the
  same mounted pod alive, proves post-restore write/read I/O, and converges the
  CRD/report/operator-snapshot/explain surfaces back to two observed NVMe paths
  and `Ready=True/first_volume_verified`.
- Phase 114 NVMe/TCP K8s Multi-Volume Mounted Path Isolation is closed. The
  supported-lab path now proves two mounted RF=2 NVMe/TCP PVCs remain isolated
  when one generated blockvolume deployment is removed and restored: the
  affected volume reports `blocked/nvme_multipath_path_missing` with one live
  host path during loss, the untouched volume remains
  `ready/first_volume_verified` with two live host paths, both mounted pods keep
  their UIDs and continue I/O, both volumes restore to two live host paths, and
  final cleanup returns zero residue.
- Phase 115 NVMe/TCP Mounted Multi-Volume Path Churn Soak is closed. It turned
  the Phase 114 one-shot proof into a bounded repeated-transition proof by
  alternating path loss/restore across both mounted volumes for three cycles,
  while preserving pod identity, I/O, reason-code isolation, publish-target
  isolation, two-path restoration, and cleanup hygiene.
- Phase 116 packaged the closed Phase 100-115 evidence into a user-facing
  supported-lab NVMe/TCP release claim: README/docs wording, feature/status
  matrix, explicit non-claims, and pinned-image release-smoke instructions for
  matching `seaweed-block` and `seaweed-block-csi` images.
- Phase 117 has a pinned-image smoke gate and TestOps scenario ready. It should
  run when matching images exist. If images are not published, the result is
  artifact-blocked rather than a product failure.
- Phase 118 starts the NVMe/RDMA/RoCE implementation track with a transport
  seam inside `core/frontend/nvme.Target`. TCP remains the only implemented
  public path; RDMA is now a typed target-layer unsupported transport rather
  than just a CLI string rejection.
- Phase 119 used `C:\work\rdma\seaweed-mono-rdma-refresh` as the current
  RDMA/VFS/RustVolume/NIXL evidence source. That repo proves useful VFS/object
  acceleration and NIXL-shaped object compatibility, but it does not implement
  a Linux `nvme connect -t rdma` compatible target. The decision was to keep
  RoCE/NVMe-RDMA as a non-claim and first run a block NVMe/TCP performance
  baseline before spending more time on RoCE.
- Phase 120 is closed as a management-LAN/default-network baseline. It proved
  the implemented Kubernetes NVMe/TCP path and recorded sequential write/read
  MiB/s plus small-write IOPS on `192.168.1.181:4420`, but those numbers are
  not the authoritative 100GbE performance baseline.
- Phase 121 is closed. It mirrors the Rust volume RDMA pattern by making
  data-plane addresses explicit, queryable, and visible in status evidence
  before rerunning a high-speed NVMe/TCP baseline. It separates management IP
  from NVMe/TCP frontend/data-plane IP and still keeps RoCE/NVMe-RDMA
  unsupported for Block.
- Phase 122 is closed. It reran the Phase 120 measurement shape on the
  configured 100GbE TCP frontend IP and proved the live target as
  `10.0.0.1:4420` over `enp1s0np0`, with sequential write `115.11 MiB/s`,
  sequential read `250.98 MiB/s`, and small-write `606.64 IOPS`. The gate also
  fixed the gRPC observation wire gap for `frontend_ip` and
  `frontend_network_class`. This remains a baseline, not a RoCE, NVMe/RDMA, or
  performance SLO claim.
- Phase 123 is closed. It added an independent `iperf3` comparator over the
  same 10.0.0.x data-plane route and measured `network_baseline_mibps=4106.55`
  versus mounted Block NVMe/TCP read/write of `248.06 MiB/s` / `127.74 MiB/s`.
  The network is not the immediate bottleneck, but the remaining bottleneck is
  still `unknown` because target/backend/Kubernetes/test-shape are not split.
- Phase 124 is closed. It compared Block NVMe/TCP against a same-shape
  `local-path` PVC on the same app node. Block read (`273.50 MiB/s`) was not
  behind local-path read (`235.29 MiB/s`), but Block write (`118.74 MiB/s`) was
  only `0.366x` local-path write (`324.87 MiB/s`). The next bottleneck class is
  write-side `block_target_or_backend`, not network/RDMA.
- Phase 125 is closed. It profiled a 512MiB Block NVMe/TCP write and same-node
  local-path comparator: Block write `174.33 MiB/s`, local write
  `1147.98 MiB/s`, Block/local write ratio `0.152`, while Block read stayed
  comparable to local-path. Coarse pod-level CPU samples did not show target
  CPU saturation (`0.80%` peak), so the next direction is backend/sync write
  instrumentation.
- Phase 126 is closed. It added product-owned write-path timing/counter
  evidence on `/status/durable` and localized the mounted NVMe/TCP write gap to
  backend write cost: Block write `177.72 MiB/s`, local-path write
  `1115.47 MiB/s`, target/backend write bytes `588075008`,
  `backend_write_duration_ms=33186`, and `backend_sync_duration_ms=73`.
- Phase 127 is closed for source/component NVMe ANA Change Notice. OAES ANA
  Change Notice is conditional on an ANA provider, a parked AER completes when
  ANA change count advances, no-provider OAES remains zero, and the AER
  single-slot limit remains enforced.
- Phase 128 is closed for live Linux host validation of ANA Change Notice. The
  m02 kernel `nvme:nvme_async_event` tracepoint observed
  `NVME_AEN=0x0c0302` during standalone r1->r2 failover, the ANA log change
  count advanced, host path state refreshed, mounted I/O remained correct, and
  cleanup was clean.
- Phase 129 is closed for the Kubernetes NVMe mounted restage contract. A
  repeated `NodeStageVolume` call on an already-mounted NVMe staging path now
  refreshes publish context, connects missing paths for the same NQN, rejects
  NQN mismatch, and does not remount or reformat. It deliberately does not
  claim an automatic Kubernetes reconnect trigger.
- Phase 130 is closed for the CSI-node NVMe reconnect owner/trigger contract.
  The product now has an opt-in CSI node loop that invokes mounted NVMe
  reconnect from refreshed publish evidence and is disabled by default. This is
  a source/component gate, not the full live Kubernetes pod UID/I/O close gate.
- Phase 131 is closed for the live Kubernetes NVMe host-path reconnect gate.
  A mounted RF=2 NVMe/TCP PVC starts with two host paths, one path is removed
  with scoped `nvme disconnect -d`, CSI-node reconnects it, mounted I/O remains
  correct, and CRD/report/dashboard agree.
- Phase 132 is closed for live Kubernetes NVMe desired path-set replacement.
  A mounted RF=2 NVMe/TCP PVC starts with two desired paths, one generated
  frontend address is replaced, `SwBlockVolume.status.nvme.nvmeAddrs` changes
  old-to-new, CSI-node connects the new desired path, mounted I/O remains
  correct, and CRD/report/dashboard agree.
- Phase 133 is closed for live Kubernetes NVMe stale path pruning after desired
  path replacement. CSI-node now connects the new desired path and disconnects
  only stale host paths for the same NQN that are no longer desired, using
  scoped controller disconnects. Mounted pod UID/I/O are preserved and
  CRD/report/dashboard agree.
- Phase 134 is closed for durable backend write batching. The previous
  `backend_write_ops` counter was not enough because it measured
  `StorageBackend.Write` calls, not internal storage fan-out. The product now
  has bounded full-block `WriteBatch` execution, real `walstore` batch WAL
  append, strict-ACK batch disablement, and `/status/durable`
  `backend_storage_*` counters. The live NVMe/TCP gate observed
  `backend_storage_write_blocks=28872`,
  `backend_storage_write_calls=3634`, and
  `backend_storage_batch_calls=3613` with cleanup clean. The next NVMe work can
  measure wall-clock improvement and identify the next bottleneck; it should
  still avoid SLO, RoCE, or NVMe/RDMA claims.
- Phase 135 is closed for post-batch retriage. The comparable 512MiB profile
  proved batching stayed active at scale
  (`backend_storage_batch_calls=17953`,
  `backend_storage_batch_blocks=143555`), but write throughput remained around
  the Phase 126 range (`172.80 MiB/s` versus `177.72 MiB/s`) while local-path
  write was `1075.63 MiB/s`. The next backend work should split WAL
  append/copy/checksum/dirty-map costs before any NVMe/RDMA work.
- Phase 136 is closed for WAL append/copy/checksum profiling. The product now
  exposes `/status/durable` counters for WAL copy, record encode, checksum,
  append/write-at, and dirty-map update. The live 512MiB gate kept batching
  active and named `wal_encode` as the largest backend-internal cost
  (`753ms`), with WAL copy close behind (`593ms`). The next backend work
  should reduce WAL record encode/copy cost.
- Phase 137 is closed for reducing WAL record encode/copy cost. The WAL format
  and recovery semantics stayed unchanged, but the extra pre-encode block copy
  was removed and batch append now encodes directly into the coalesced pending
  buffer. The live gate reduced `wal_encode + wal_copy` from `1346ms` to
  `363ms`; the next backend work should inspect WAL append/write-at shape.
- Phase 138 is closed for WAL append/write-at shape profiling. The live gate
  showed the append path is issuing many small pwrite calls:
  `wal_append_writeat_calls=17979`,
  `wal_append_writeat_avg_bytes=33013`, and
  `wal_append_writeat_max_bytes=33072`, while wrap/padding was negligible. The
  next backend work should inspect batch/coalescing shape.
- Phase 139 is closed for WAL append batch-shape coalescing analysis. The live
  gate proved the 33KB write-at shape is imposed upstream:
  `backend_write_request_max_bytes=32768`,
  `backend_full_block_batch_max=8`, and
  `wal_append_writeat_max_bytes=33072`. The next work should inspect frontend
  request size before changing WAL append semantics.
- Phase 140 is closed for frontend request-size profiling. The live gate proved
  the 32KiB shape is the NVMe/TCP target's advertised H2C limit, not a WAL
  coalescing limit: `nvme_tcp_max_h2c_data_length_bytes=32768`,
  `target_write_request_max_bytes=32768`, and
  `backend_write_request_max_bytes=32768`. The next work should test a bounded
  `MaxH2CDataLength` candidate before changing defaults.
- Phase 141 is closed for the NVMe/TCP MaxH2C boundary. A 64KiB candidate is
  now wired as an explicit opt-in from Helm through blockmaster launcher to
  blockvolume/NVMe target, with ICResp/Identify consistency tests and a live
  Linux mounted writer/reader gate. The live request max moved to 65536 bytes
  and cleanup was clean. The default remains 32KiB; the next work should
  retriage the 64KiB opt-in write path before considering broader
  compatibility or default changes.
- Phase 142 is closed for large-H2C retriage. With the 64KiB opt-in enabled,
  the live target/backend request max stayed at 65536 bytes and full-block
  batch max rose to 16 blocks. The top remaining product-owned cost is now WAL
  append (`wal_append_duration_ms=300`), with WAL encode close behind
  (`wal_encode_duration_ms=289`). The next backend work should profile WAL
  append under the 64KiB shape before changing append semantics.
- Phase 143 is closed for large-H2C WAL append shape profiling. The live gate
  showed full-size write-at records (`wal_append_writeat_avg_bytes=65883`) and
  negligible wrap/padding (`wal_append_padding_bytes=13136` over ~566MiB), while
  WAL encode is nearly tied with append (`wal_encode_duration_ms=285` vs
  `wal_append_duration_ms=290`). The next work should profile the encode+append
  pair before optimizing either side.
- Phase 144 is closed for the encode/append pair profile. The large-H2C live
  gate showed `wal_encode_duration_ms=297` and `wal_append_duration_ms=295`, so
  the next implementation target is a narrow WAL record materialization
  reduction rather than isolated append or encode tuning.
- Phase 145 is closed for the first WAL materialization reduction. The batch
  path now uses `[]walEntry` values instead of allocating one `*walEntry` per
  block, preserving WAL bytes/recovery while removing a local allocation seam.
  Phase 146 measured that change as visible in the lab profile
  (`phase146_pair_improvement_pct=5.24`) and kept it, while preserving the
  non-claim that this is not a throughput/SLO release statement. The next
  backend gate should select a deeper WAL path: multi-block WAL records or
  vectored write-at, with durability/recovery invariants documented first.
- Phase 147 is closed for that design gate. It selected `multi_block_record`
  over vectored write-at because the current append path already coalesces
  encoded records into fewer `WriteAt` calls, while record count remains the
  structural encode/recovery cost. The current WAL format stays unchanged; the
  next phase may prototype a disabled-by-default multi-block record locally.
- Phase 148 is closed for the local prototype. Multi-block WAL records now have
  a disabled-by-default test gate with encode/decode, dirty-read, recovery-split,
  flusher-split, and `ScanLBAs` coverage. It is not wired into Kubernetes or the
  default blockvolume path; Phase 149 must profile record-count reduction before
  runtime opt-in wiring.
- Phase 149 is closed for the local profile. The same WriteBatch workload drops
  WAL encode ops from `2048` to `128`, while append/write-at calls remain `128`.
  This justifies wiring a disabled-by-default runtime opt-in for mounted NVMe/TCP
  profiling, but it is still not a performance/SLO claim.
- Phase 150 is closed for runtime opt-in wiring. The flag
  `--durable-wal-multiblock-records` reaches walstore only when explicitly set;
  Helm defaults omit it and explicit
  `blockmaster.durableWALMultiBlockRecords=true` renders it.
- Phase 151 is closed for the mounted NVMe/TCP opt-in profile. The live gate
  verifies writer/reader I/O, 64KiB target/backend request shape, and mounted
  multi-block record shape (`wal_encode_ops=9002` for `143570` written storage
  blocks). The opt-in stays default-off and is not a performance/SLO claim. The
  next gate must prove mounted restart/recovery compatibility for the new WAL
  entry type.
- Phase 152 is closed for mounted restart/recovery compatibility. It recovered
  `LSN=14545` after a force-deleted `blockvolume` restart with hostPath
  persistence and no WAL integrity fault.
- Phase 153 is closed for the release-boundary documentation gate. The opt-in
  remains source-gated, default-off, and explicitly not a performance, RoCE, or
  NVMe/RDMA claim.
- Phase 154 is closed for local durable-status `HeadLSN` cleanup. The bug was a
  diagnostic boundary mismatch: superblock WAL byte-position metadata was being
  reported as the LSN `HeadLSN` after recovery. Storage and durable-provider
  regressions now assert recovered `HeadLSN` is bounded by the recovered LSN.
- The standalone and opt-in Kubernetes single-path NVMe/RDMA gates now pass
  behind a source-gated Linux supported-lab boundary. Keep reconnect/failover,
  multipath, performance characterization, and object/NIXL acceleration as
  separate claims.

### Track E: Protocol / Backend Expansion

- Current: iSCSI and NVMe-oF are protocol-gated frontends. `walstore` remains
  the MVP backend. NVMe/TCP and opt-in NVMe/RDMA have Kubernetes supported-lab
  paths; RDMA uses kernel `nvmet-rdma` and a product-owned NBD-to-backend
  bridge.
- Next: make NVMe/RDMA reconnect/failover ownership explicit before any
  transparent-HA claim. Multipath and performance remain later, separate gates.
  Storage-engine boundary and smartwal/delta experiments also remain separate.
- Protocol hardening now has a dedicated working area under
  `internal/docs/protocol/`. New protocol semantics should update the control
  model, invariant ledger, and anti-pattern checklist there before release
  claims are made.
- Later: RDMA/KV-backed data-plane experiments and semantic storage protocols
  only after the block core is mature.
- Guardrail: do not let backend/library extraction weaken the user-visible
  block contract. A simpler data-plane seam is useful only if it preserves the
  product semantics the block layer needs: bounded buffers/streaming for large
  objects, explicit acknowledgement profile, durable frontier, fencing, retry
  behavior, and supportable failure evidence. If an abstraction becomes easier
  to test but cannot carry real volume I/O semantics, keep it experimental and
  do not build a product claim on it.

### Track F: Operations Layer

- Current: cluster operations inventory is closed for the supported alpha path:
  it discovers live Seaweed Block volumes from Kubernetes, maps them to
  PVC/PV/generated workloads, attaches per-replica status bundles, and names
  stale/orphan residue without relying on TestOps artifacts. It also serves as
  the proof surface for product-owned lifecycle, durable restart evidence, and
  same-node placement/attach evidence. Phase 19 closed the shared observation
  core for users/support/AI, JSON automation, and future dashboard use.
- Current: observation is now part of the first-user loop. Users can inspect
  cluster status, volume detail, timeline, static report artifacts, and a local
  read-only dashboard without SSHing into every node.
- Closed model-hardening slice: Phase 22 ManagedVolume Operations Model
  (`internal/docs/protocol/phase22-control-context-plan.md`) made PVC-backed
  volumes a first-class internal read model that composes K8s, CSI, authority,
  recovery, host path, workload, and evidence facts while keeping local
  controllers small and testable.
- Closed on 2026-06-04: Phase 35 turned the existing read-only operations model
  into Kubernetes-native status:
  - `SwBlockCluster` and `SwBlockVolume` CRDs,
  - a status-only controller that writes `.status`,
  - ManagedVolume Conditions projected into Kubernetes Conditions,
  - Kubernetes Events for ready, blocked, stale-evidence, and WAL-integrity
    transitions,
  - tests proving the controller has no mutating storage authority.
- Later: metrics, read-only dashboard hardening, conservative admin controls,
  enterprise operations, hosted validation, fleet automation, and cloud-scale
  test lifecycle.

### Track F2: ManagedVolume Model And Protocol Hardening

- Closed for current scope: V3 protocol principles have been pulled into
  `internal/docs/protocol/`: truth-domain ownership, anti-patterns, invariant
  ledger, engine design guidelines, and the Phase 22 ManagedVolume plan. The scope review in
  `internal/docs/protocol/operations-state-dependency-review.md` defines Phase
  22 as a PVC-backed ManagedVolume read model plus read-only operations
  alignment.
- Seed landed: `core/ops` now has an initial ManagedVolume projection model,
  typed facts, status priority, read-only/dry-run action contracts,
  `VolumeEvidence` and bundle-artifact bridges, and table tests for healthy
  first-volume, blocked loopback cross-node attach, CSI image-pull and mount
  blockers, node-loss reattach recovery, Stage 2 transparent multipath
  recovery, non-claims, and dual-primary invalid priority.
- Execution discipline: every Phase 22 D-step must carry TDD, internal review
  against the engine guidelines, and a regression command before it can close.
- Later: use ManagedVolume as the semantic core for operator
  Conditions/Events, read-only dashboard, and any future safe mutating admin
  workflows.

### Track F3: Operations Surface / Dashboard / Operator-Readiness

- Current: Phase 23 is closed for scope. Seaweed Block now exposes
  ManagedVolume Conditions, evidence refs, report/explain alignment, and a
  future-operator status contract from the shared read model.
- Current: Phase 24 is closed for scope. Seaweed Block can serve a local
  read-only dashboard/API surface over the same `ClusterEvidence` and
  ManagedVolume model used by `sw-block ops report`.
- Closed on 2026-05-22: Phase 25 packages that operations surface into the
  v0.3 Helm first-volume release story and validates docs/gates against it.
- Closed on 2026-06-04: Phase 35 implemented the first Kubernetes-native
  operator foundation slice over this surface: CRDs, read-only `.status`,
  Conditions, Events, stable Event identity, and read-only RBAC. It did not add
  mutating admin workflows.
- Seed landed:
  - `NewObservationDashboardHandler` serves `index.html`,
    `cluster-evidence.json`, `timeline.jsonl`, `summary.txt`, and `healthz`,
  - `sw-block ops dashboard` serves bundle-backed and master-api-backed
    evidence on a loopback address by default,
  - unsafe HTTP methods return `405` with a read-only boundary message,
  - `ops explain` now emits ManagedVolume Conditions, dry-run action
    preconditions, invariant refs, evidence refs, and non-claims,
  - Conditions carry additive `evidence_refs`,
  - `ManagedVolumeOperatorContractFromProjection` defines how Conditions map to
    future operator status and Kubernetes Events while keeping
    `mutation_allowed=false`,
  - `internal/docs/protocol/operator-readiness-contract.md` documents the
    future operator boundary,
  - replay tests cover first-volume, blocked, and recovery bundles.
- Later: production dashboard hardening and operator reconciliation can depend
  on this surface, but should not bypass ManagedVolume or mint their own truth.

### Product Semantics Rule

- Roadmap slices must preserve a sharp product question. For block storage, the
  question is not "can we model this state?" but "can an operator and workload
  safely use this volume under the documented topology and failure mode?"
- Passing tests are not enough when the tested abstraction has weaker semantics
  than the user-facing product. Such cases must be documented as non-claims,
  safe refusals, or experiments.
- Mature-product direction means each operations and availability slice should
  improve one of: provision, attach, mounted I/O, durability, fencing, reattach,
  cleanup, status, or support-bundle actionability.

### Top Light-Use Product Blockers

These are the main gaps between the current functional block substrate and a
credible light-use product:

- Product-owned generated workload lifecycle: scripts/TestOps still own too
  much apply/cleanup and run-scoped state management.
- Install/upgrade/uninstall: v0.2 script activation works and v0.3 Helm is the
  current packaging boundary. Operator lifecycle comes after the Helm contract
  is stable.
- Observation beyond one volume: the read-only CLI/dashboard now covers cluster
  inventory and first-volume evidence; users will still need richer lifecycle
  status, metrics, and production UI hardening.
- Safe admin controls: repair/promote/cleanup actions must wait until the
  read-only observation model is stable and release-gated.

### Track H: Stage 1.5 Product Usability Hardening

- Current: core product loops are proven for light-use alpha, but day-2
  usability still depends too much on internal context.
- Next:
  - add `kubectl get`-readable readiness/degraded/recovering/blocked conditions
    with stable reason codes through Phase 35 CRD status,
  - provide one-command support-bundle capture with minimum evidence for attach,
    failover, and cleanup diagnosis,
  - set conservative default timeout/retry profiles for iSCSI/NVMe/CSI paths,
  - add upgrade/rollback smoke gates for PVC attach/read continuity,
  - enforce capacity/replication preflight guards with operator-readable errors,
  - harden delete/residue cleanup auditing for sessions/targets/artifacts,
  - run multi-volume concurrency baselines (create/attach/delete) as product
    gates, not ad-hoc checks,
  - keep fail-closed blocker reasons directly mapped to operator runbook steps.
- Later: fold proven Stage 1.5 hardening into beta entry criteria.

### Track I: Enterprise Feature Gap Closure (Backup, UI, DR, Security)

- Current: Seaweed Block is intentionally narrow and honest for alpha/beta.
  Compared with mature Kubernetes block platforms, major product gaps still
  include:
  - backup/snapshot lifecycle as a first-class product workflow,
  - disaster-recovery orchestration across clusters/regions,
  - operator-facing control UI and guided diagnostics,
  - policy/RBAC/audit guardrails for admin actions,
  - richer day-2 automation (scheduled policies, safe runbooks, rollback UX).
- Reference signals from mature products:
  - Longhorn exposes recurring snapshot/backup jobs and UI-first operations.
  - Ceph/Rook exposes RBD snapshots and mirroring for DR workflows.
  - OpenEBS Replicated PV Mayastor exposes replicated PV operations and CSI
    snapshot integration.
  - Portworx positions backup + DR + enterprise security as core platform
    pillars.
- Next (priority order):
  - P1: backup/snapshot policy + restore workflow with support-bundle evidence,
  - P1: control-plane read UI over the observation API described in
    `ref/control-plane-observation-api-mvp.md`,
  - P2: admin action protocol (promote/repair/cleanup) with strict fencing and
    audit trail,
  - P2: cross-cluster DR contract (RPO/RTO class + failover/failback runbook),
  - P3: security hardening track (RBAC, secret handling, action audit,
    tenancy boundaries).
- Later: turn these tracks into explicit beta/production entry gates and avoid
  feature-claim drift between docs and runnable gates.

### Track G: Test Management Control Plane

- Current: runner scenarios write result/status bundles, but M01/M02 shared lab
  ownership is still mostly implicit.
- Closed: Phase 33 TestOps failure hardening. The runner and helpers now
  exercise negative paths, collect useful failure snapshots, assert no false
  Ready states, and prove cleanup/replay behavior after failed runs.
- Closed: Phase 34 test realism. Reduced self-proving gates by adding
  independent cross-checks, live status-endpoint-unreachable injection,
  restart convergence assertions, and one dirty storage failure: SmartWAL
  corruption. The D4 gate is release-relevant because it checks whether dirty
  storage evidence can still leak through as false `Ready=True`; it now passes
  on the core contract.
- Later: shared-drive control data for active runs, scenario library indexing,
  queueing, remote agents on lab nodes, matrix scheduling, hosted validation,
  and discovery-agent ingestion.

## Productized Operations Gap Priority

These are the current operation gaps in priority order. Phase 35 closed the P0
read-only status foundation; the remaining groups should become separate gated
phases rather than being mixed into the closed foundation.

### P0: Become Kubernetes-Native For Read-Only Status

1. CRD + status-only operator:
   `SwBlockCluster`, `SwBlockVolume`, and `.status` writes only. Status:
   closed in Phase 35.
2. Conditions writer:
   project `Ready`, `Blocked`, `Recovering`, `Recovered`, `EvidenceStale`, and
   `CleanupRequired` from ManagedVolume facts. Status: core vocabulary closed;
   cleanup projection remains follow-up.
3. Kubernetes Events:
   emit normal/warning Events such as `VolumeReady`,
   `CsiNodeImagePullFailed`, `WalIntegrityFault`, and `EvidenceStale`. Status:
   closed for read-only status Events.

### P1: Make Operations Actionable

4. Node readiness/preflight status:
   iSCSI, multipath, image readiness, hostPath readiness, and observed version
   under `SwBlockCluster.status.nodes[]`. Status: Phase 36 validated positive
   read-only node readiness and replay-only missing-image blockers. Live
   negative node evidence remains Phase 37.
5. Support-bundle pointers:
   keep the CLI collection path, but expose evidence refs and suggested
   commands from status. Status: closed in Phase 36.
6. Cleanup visibility:
   `CleanupRequired=True`, residue type, and safe next step. Do not automate
   cleanup yet. Status: closed in Phase 36.
7. Surface agreement:
   CRD status, Events, report, dashboard, operator-snapshot, and explain must
   agree on healthy, blocked, stale, cleanup-required, and multi-volume paths.
   Status: closed in Phase 36. Live operational follow-ups remain for
   build-host CSI image-import evidence, loopback publish-target documentation,
   and force-delete iSCSI node DB residue visibility.

### P2: Enter Mutating Lifecycle Carefully

8. Finalizers and delete safety for PVC/CRD lifecycle.
9. Upgrade/rollback status and drift reporting before upgrade execution.

### P3: Advanced Day-2 Features

10. Rebuild, reintegration, and failback.
11. Backup, snapshot, and restore.

NVMe ANA parity is important, but it should not be the next plan. It should
reuse the Phase 35 status foundation so protocol-specific path state does not
become another isolated status model.

## Capability And Model Maturity Snapshot

This section is the product-state checkpoint after the Phase 35/36 operations
push. It exists to stop the roadmap from becoming an unbounded sequence of
small operations fixes.

### Current Block Capabilities

The product can already demonstrate a narrow but real Kubernetes block-storage
loop:

- Helm install on supported lab clusters.
- CSI dynamic PVC provisioning.
- App pod mount, write, read, and replacement-reader verification.
- iSCSI frontend with Linux initiator and dm-multipath/ALUA gates.
- RF=3 multi-volume lab path with independent volume identity, primary, and
  publish target.
- CSI reattach recovery with pod recreate.
- Mounted transparent failover on the gated Stage-2 iSCSI ALUA path.
- HostPath restart persistence for supported gates, including authority
  preservation after promotion.
- Dirty-evidence protection: stale/unreachable/corrupt evidence does not become
  false `Ready=True`.
- Strict cleanup verification across Kubernetes resources, iSCSI sessions,
  iSCSI node DB records, multipath, dmsetup, product processes, and hostPath
  residue.

### Current Kubernetes And Operations Capabilities

- Helm is the primary install path; scripts remain development/fallback tools.
- `sw-block ops generate-helm-values` can derive Day-1 Helm values from the
  target Kubernetes cluster.
- Read-only CLI/report/dashboard/support-bundle flows are established.
- `SwBlockCluster` and `SwBlockVolume` CRDs exist with status subresources.
- The optional operator-status controller writes `.status` and Kubernetes
  Events only.
- Positive/read-only node readiness, support evidence refs, cleanup visibility,
  safe next-step hints, and surface agreement are QA-validated under the
  read-only model. Live negative node blockers remain Phase 37.

### Model Convergence State

The control-plane model is materially better than the earlier script-centered
shape, but it is not finished. See
`internal/docs/protocol/control-structure-effectiveness-review.md` for the
review standard that separates real capability closure from semantic-only model
work.

Converged:

- `ManagedVolume` is the shared semantic object for volume status.
- CRD status, Events, report, dashboard, `operator-snapshot.json`, and
  `ops explain` share the same Condition/reason vocabulary.
- The design pattern is now explicit:
  truth owner publishes facts; orchestration/status entity aggregates judgment;
  executor remains bounded; evidence/timeline explains why the judgment is
  allowed.
- Negative-first is enforced across status surfaces.

Still incomplete:

- Live node evidence does not yet derive all negative facts from real Kubernetes
  node/image/CSI-driver state.
- Some volume-side fault reasons still need stronger end-to-end propagation to
  user-visible surfaces.
- Mutating lifecycle actions now have a non-mutating lifecycle-owner contract
  shape: action + precondition + invariant + executor + evidence.
- PVC lifecycle, blockvolume lifecycle, and Kubernetes attach lifecycle still
  need a real lifecycle-owner component and API/admission proof before anything
  mutates Kubernetes lifecycle objects.

### Product Priority From Here

The operations work can continue forever unless the next phases are bounded by
product risk. The recommended order is:

1. **Phase 37: Live node evidence hardening.** Closed.
   Kubernetes Ready/SchedulingDisabled, CSI node pod readiness,
   CSIDriver/node-plugin registration, image-pull status, host-prereq replay,
   and loopback target cross-node blockers are now projected through the shared
   status surfaces.
2. **Phase 38: Lifecycle action model executable contract.** Closed.
   Future actions now have facts, preconditions, invariants, allowed executor,
   policy gate, evidence, and allow/reject decisions. It includes both a dry-run
   action gate and a rejected-action gate.
3. **Phase 39: Delete-safety status boundary.** Closed.
   Live QA proved the original RBAC-only finalizer boundary is not viable for
   CRD finalizers: CRDs have no usable HTTP `/finalizers` endpoint, so
   `metadata.finalizers` changes require main `patch swblockvolumes`
   authorization. The chosen path is to keep operator-status status/events-only
   and defer finalizer add/remove to a future lifecycle owner.
4. **Phase 40: Operator production hardening.** Closed.
   The v0.4 beta foundation released Helm + PVC + status/events-only
   operator-status, install drift visibility, delete-safety visibility, and
   schema/RBAC conformance gates.
5. **Phase 41: Lifecycle owner foundation.** Closed.
   Defines observer/lifecycle-owner/executor boundaries, keeps operator-status
   status/events-only, defers finalizer mutation, and exposes dry-run
   lifecycle-owner decisions for delete-safety. It does not ship deletion
   protection.
6. **Phase 42: real lifecycle-owner API/admission gate.** Closed.
   Before any finalizer add/remove, prove main-object patch confinement against
   a real Kubernetes API with admission/RBAC. Only then consider a first
   bounded mutation.
7. **Phase 43: first real lifecycle mutation.** Closed.
   Shipped the first bounded mutation: `SwBlockVolume` protection finalizer
   add/remove with delete-safety preconditions. It does not include cleanup,
   rebuild, failback, backup, or NVMe.
8. **Phase 44: delete lifecycle close gate and Operation Layer v0.5
   candidate.** Closed for code/QA; release skipped for now.
   Validated the full user path: install -> PVC -> status -> delete requested
   -> blocked/releasable -> finalizer behavior -> cleanup evidence -> support
   bundle -> uninstall zero residue. Matching release images and pinned-image
   smoke remain required before marking v0.5 released.
9. **Phase 46: returned-replica rebuild/reintegration productization.** Closed.
   High product value. Returned-replica facts now project through the lifecycle
   action model as visible, fenced, volume-scoped status/decision evidence
   (`internal/docs/finished-plans/phase46_finishedplan_returned_replica_reintegration_productization.md`).
   The phase intentionally stops short of automatic failback or broad rebuild
   execution.
10. **Phase 47: returned-replica executor admission.** Closed.
   First slice admits `authority.reintegrate_returned_replica` only as a
   dry-run, non-mutating action after exact fencing and frontier evidence is
   present
   (`internal/docs/finished-plans/phase47_finishedplan_returned_replica_executor_admission.md`).
   It is the bridge toward a future executor, not automatic failback.
11. **Phase 48: returned-replica live evidence close.** Closed.
   The live iSCSI returned-replica gate emits same-run managed-volume
   evidence for required frontier coverage and replay it through report/status
   surfaces before any mutating returned-replica executor is proposed.
12. **Phase 60-98: rebuild, failback, frontend publication, and workload close.** Closed through the live post-failback I/O gate.
   Phase 60 proved the existing datapath, Phase 61 added the executor call-site,
   Phase 62 added HTTP runtime transport, Phase 63 locked runtime target facts,
   Phase 64 added the opt-in blockvolume endpoint that starts recovery after
   primary/lineage validation, Phase 65 added terminal durable-frontier
   evidence, Phase 66 surfaces caught-up as publication preflight while keeping
   publication disabled, Phase 67 publishes only ACK eligibility after
   caught-up terminal evidence, Phase 68 surfaces frontend publication as
   disabled preflight on the ACK eligibility status, Phases 74-95 make
   failback executable only behind explicit policy and then prove it live, and
   Phases 96-98 connect terminal failback evidence to frontend publication and
   post-publication workload writer/reader I/O. Default automatic failback
   remains off.
13. **Backup/snapshot/restore and NVMe ANA parity.**
   Important, but they should reuse the status/action model rather than create
   another isolated control plane.

### Effort Shape

Approximate engineering effort if scope remains tight:

- Live node evidence hardening: small/medium. Mostly observation ingestion,
  projection tests, and live TestOps gates. Low mutation risk.
- Lifecycle action model review: medium. Mostly design and tests, but it should
  block later mutating work from scattering logic across listeners/scripts.
- Delete-safety status: closed. Keeps delete safety visible without widening
  operator-status mutation rights.
- Lifecycle-owner finalizers: medium/high. Cleaner than operator-status main
  patch, but requires a separate lifecycle-owner component plus real
  API/admission proof that only finalizers can be patched.
- Productized returned-replica rebuild/reintegration/failback: high. Phase 46
  closed the status/decision slice: returned replicas are visible, fenced, and
  volume-scoped across product surfaces. Phases 47-67 have advanced from
  dry-run admission to bounded runtime start, terminal completion evidence, and
  disabled publication preflight. Phase 67 adds the first bounded ACK
  eligibility status publication. Phase 68 adds frontend publication preflight
  while keeping the frontend mutation disabled. Frontend publication and
  failback remain separate gated work.
- Backup/snapshot/restore: high. Requires durable data semantics and user-facing
  restore guarantees.
- NVMe ANA parity: medium/high. Protocol-specific work, but cheaper if it uses
  the existing CRD/Condition/Event model.

## PR Cadence

- Prefer one coherent milestone PR, not one PR per tiny fix.
- Target one or two PRs per day at most.
- Keep minor doc/test cleanups batched unless they block current work.
- QA can push evidence-only PRs when assigned, but product code should stay
  milestone-based.

## Current Execution Pointer

- Active work should be tracked in `internal/docs/current-plan.md`.
- Phase 35 closed the Kubernetes-native read-only operator foundation:
  CRDs, status-only reconciliation, Conditions, Events, stable Event identity,
  and read-only boundary tests.
- Phase 36 Productized Operations Actionability is closed. Positive/read-only
  node readiness, support evidence pointers, cleanup visibility, and surface
  agreement are QA-validated under the read-only control-plane model. Live
  negative node evidence remains Phase 37.
- Phase 37 Live Node Evidence Hardening is closed.
- Phase 38 Lifecycle Action Model Executable Contract is closed.
- Phase 39 Delete-Safety Status Boundary is closed.
- Phase 40 Operator Production Hardening is closed and is the v0.4 beta release
  boundary.
- Phase 41 Lifecycle Owner Foundation is closed. It is non-mutating:
  finalizer add/remove is deferred, while lifecycle-owner dry-run decisions and
  delete-safety preconditions are made visible.
- Phase 42 Lifecycle Owner API / Admission Gate is closed. It proved the
  lifecycle-owner main-object patch boundary against a real Kubernetes
  API/admission surface and preserved the delete-safety decision model.
- Phase 43 First Bounded Finalizer Mutation is closed.
- Phase 44 Delete Lifecycle Close Gate is closed for code/QA. The team is
  intentionally skipping the v0.5 release smoke for now, so v0.5 is not marked
  released until matching images and pinned-image validation are completed.
- Phase 46 Returned-Replica Rebuild / Reintegration Productization is closed.
  Returned replicas are visible, fenced, action-gated, and QA-verifiable before
  any automatic rebuild/failback executor is enabled.
- Phase 47 Returned-Replica Executor Admission is closed. It keeps
  `authority.reintegrate_returned_replica` dry-run/non-mutating while proving
  exact fencing/frontier evidence and schema/RBAC conformance, including a live
  Kubernetes status-subresource server-side dry-run gate.
- Phase 48 Returned-Replica Live Evidence Close is closed. It connects the
  live iSCSI returned-replica run to the same managed-volume evidence/action
  surfaces used by Phase 47, without enabling rebuild/failback mutation.
- Phase 60 Rebuild Catch-up Datapath Gate is closed. It proved the existing
  engine/adapter/transport/recovery data path can move catch-up/rebuild bytes
  and converge durable content, but it did not connect the authority executor.
- Phase 61 Authority Executor Runtime Call-site is closed. It added the
  bounded runtime interface and `running/caught_up/blocked` status mapping,
  while preserving the non-claim that blockvolume RPC is not wired.
- Phase 62 Authority Executor HTTP Runtime Transport is closed. It adds an
  explicit HTTP runtime URL for `rebuild_traffic` execution and validates the
  executor-to-runtime transport contract. It still does not claim a live
  blockvolume endpoint.
- Phase 63 Rebuild Runtime Target Contract is closed. It schema-locks the
  runtime target facts (`runtimeEndpoint`, data address, session, epoch,
  endpoint version, and frontier hints), copies them from returned-replica
  status into `SwBlockReplicaRebuild.spec`, and makes target-owner /
  authority-executor fail closed when those facts are missing. It still does
  not call live `StartRebuild`.
- Phase 64 Blockvolume Runtime Rebuild Endpoint is closed. It adds an explicit
  opt-in blockvolume `/runtime/rebuild` endpoint, validates local primary
  readiness plus session/epoch/endpoint-version facts, starts
  `StartRebuild`/`StartCatchUp`, and keeps authority status `running` when the
  runtime reports only `runtimeState=started`.
- Phase 65 Runtime Terminal Evidence is closed. The runtime records terminal
  session status, the blockvolume endpoint returns `runtimeState=caught_up`
  with durable frontier evidence without restarting traffic, and the authority
  executor transitions `running -> caught_up`.
- Phase 66 Caught-up Publication Preflight is closed. `SwBlockReplicaRebuild`
  status now exposes `publicationDecision` / `publicationReason` /
  `publicationMutationAllowed`, with publication blocked until caught-up and
  disabled after caught-up.
- Phase 67 ACK Eligibility Publication is closed. After matching rebuild
  terminal evidence is `caught_up`, authority-executor can publish only
  `SwBlockReplicaEligibility.status` with `ack_eligibility_recorded`; frontend
  publication, failback, storage mutation, and primary authority changes remain
  explicitly out of scope.
- Phase 68 Frontend Publication Preflight is closed. `SwBlockReplicaEligibility`
  status now carries `frontendPublicationDecision`, `frontendPublicationReason`,
  and `frontendPublicationMutationAllowed`; the decision is currently
  `disabled` with mutation allowed false.
- Phase 88 Failback Deployed Suite Packaging is closed. The failback target
  owner, failback executor, and blockmaster gRPC runtime can be rendered as one
  explicit opt-in suite, still without claiming automatic failback or frontend
  publication.
- Phase 89 SwBlockVolume Authority Facts is closed. `SwBlockVolume.status` now
  exposes `primaryReplicaID`, `publishTarget`, `authorityEpoch`, and
  `authorityEndpointVersion`; operator-snapshot and summary surfaces expose the
  same facts. These fields are the observed inputs for the next failback target
  activation phase.
- Phase 90 Failback Target Authority Gate is closed. The target owner now
  refuses to create a target without current `SwBlockVolume.status` authority
  facts and stamps `expectedCurrentReplicaID` / `expectedCurrentEpoch` onto
  created disabled targets, while preserving the non-claims that frontend
  publication and automatic failback remain disabled until their own gates pass.
- Phase 91 Failback Target Activation Policy is closed. Target-owner activation
  is still default-off, but explicit policy plus runtime endpoint can stamp an
  enabled failback target for the executor handoff. The target owner still does
  not call the runtime or publish a frontend.
- Phase 92 Failback Target -> Executor Handoff is closed. A local/fake-runtime
  gate proves expected-current authority facts survive target creation into the
  executor runtime request and terminal evidence drives `failed_back` status.
  Live deployed failback remains a separate gate.
- Phase 93 Failback Handoff Isolation is closed. The local handoff now proves
  two volumes keep independent expected-current authority and target address
  facts through target creation and executor runtime requests.
- Phase 94 Failback Deployed gRPC Smoke is closed. The full opt-in Helm suite
  renders coherently and the executor-to-real-blockmaster gRPC smoke passes.
  This is still not a live Kubernetes PVC failback claim.
- Phase 95 Failback Live Deployed Suite Smoke is closed. The live k3s gate
  proves fresh images, Helm install, first PVC writer/reader, failback target
  creation, executor gRPC call to live blockmaster, terminal `failed_back`
  status, RBAC boundary, and zero-residue cleanup.
- Phase 96 Failback Frontend Publication Target is closed. Terminal
  `failed_back` evidence can create a disabled frontend-publication target with
  failback-source identity and target address facts.
- Phase 97 Frontend Publication Executor Call-site is closed. The executor can
  call a frontend-publication runtime only under explicit policy and can publish
  terminal status only from valid evidence.
- Phase 98 Failback Frontend Workload Close Gate is closed. The deployed
  opt-in suite now proves returned-replica failback -> frontend publication ->
  post-publication workload writer/reader I/O -> zero-residue cleanup in live
  k3s.
- Phase 99 NVMe ANA Baseline is closed. It pins the current protocol/CSI
  baseline and corrects stale audit wording: ANA log/Identify/provider and CSI
  single-path NVMe stage/unstage are present. Phase 100 closed the Kubernetes
  CSI NVMe multipath attach follow-up for the supported lab path.
- Operation milestone release readiness is active and blocked only on matching
  published images. Run `scripts/run-operation-milestone-release-readiness.ps1`
  plus the published-image Day-1 smoke before marking the operation milestone
  released. Development has moved ahead in parallel; this does not mark the
  operation milestone released.
- Phase 100 Kubernetes CSI NVMe Multipath Attach is closed. It implements and
  gates the path from dynamic PVC `protocol=nvme` and `replicationFactor=2` to
  master NVMe frontend grouping, CSI `nvmeAddrs` publish context, NodeStage
  multi-address connect, mounted writer/reader I/O, and zero NVMe residue after
  delete in the supported lab.
- Phase 101 NVMe Hardening And Soak is closed. It starts from the Phase 100
  supported-lab attach path and adds status-surface visibility,
  one-path-failure honesty, repeated stage/unstage residue checks, and bounded
  writer/reader soak before any broader NVMe claim.
- Phase 102 NVMe Release Artifact Smoke is blocked on matching release images.
  It does not add product
  behavior; it converts the Phase 100/101 source/lab claim into a
  published-image claim once matching release images exist and pass the gate.
- Phase 117 has the newer NVMe/TCP published-image smoke gate ready and remains
  artifact-blocked until matching `seaweed-block` and `seaweed-block-csi`
  images exist.
- Phase 118 NVMe/RDMA Transport Seam is implemented locally: the target has a
  TCP/RDMA selector seam, TCP remains the only implemented path, and RDMA stays
  a typed unsupported/public refusal.
- Phase 119 is closed as an evidence decision. It imported the current mono
  RDMA/VFS/RustVolume and NIXL evidence from
  `C:\work\rdma\seaweed-mono-rdma-refresh`, separated object/VFS acceleration
  from block NVMe/RDMA, and chose a block NVMe/TCP performance baseline as the
  next conservative step.
- Phase 120 is closed as a management-LAN/default-network baseline.
- Phase 121 is closed. It adds the data-plane address capability model needed
  before a real 100GbE NVMe/TCP baseline and before any future NVMe/RDMA work.
- Phase 122 is closed for the live 100GbE NVMe/TCP baseline. The target was
  `10.0.0.1:4420` over `enp1s0np0`; the baseline was `115.11 MiB/s` write,
  `250.98 MiB/s` read, and `606.64 IOPS` small write, with cleanup clean and
  no RDMA claim.
- Phase 123 is closed for NVMe/TCP bottleneck triage. The 10.0.0.x network
  comparator reached `4106.55 MiB/s`, far above mounted Block NVMe/TCP, so the
  next split is target/backend/Kubernetes/test-shape rather than RDMA.
- Phase 124 is closed for the NVMe/TCP target/backend/test-shape split.
  Same-shape local-path comparison narrowed the gap to Block write-side
  target/backend behavior.
- Phase 125 is closed for Block NVMe/TCP write-path profiling. It narrowed the
  gap to write-side backend/sync behavior rather than read path, network, or
  immediate RDMA work.
- Phase 126 is closed for Block NVMe/TCP backend write instrumentation.
  Product-owned `/status/durable` evidence localized the remaining write-side
  gap to backend writes, but Phase 127 intentionally closed the NVMe ANA
  Change Notice source/component gap before performance optimization.
- Phase 128 is closed for live Linux host AER/ANA notification evidence.
- Phase 129 is closed for the mounted NVMe restage primitive.
- Phase 130 is closed for the CSI-node NVMe reconnect owner/trigger contract.
- Phase 131 is closed for the live Kubernetes NVMe host-path reconnect gate.
- Phase 132 is closed for the Kubernetes NVMe desired path-set change close
  gate.
- Phase 133 is closed for Kubernetes NVMe stale host-path pruning after desired
  path replacement.
- Phase 134 is closed for durable backend write batching and product-owned
  `backend_storage_*` counters.
- Phase 135 is closed for post-batch NVMe/TCP write-path retriage and names
  WAL append/copy/checksum profiling as the next backend step.
- Phase 136 is closed for WAL append/copy/checksum profiling and names
  `wal_encode` / record-copy cost as the next backend step.
- Phase 137 is closed for WAL record encode/copy reduction and names
  `wal_append` / write-at shape as the next backend step.
- Phase 138 is closed for WAL write-at shape profiling and names small
  write/coalescing shape as the next backend step.
- Phase 139 is closed for WAL append batch-shape analysis and names frontend
  request size as the next backend/frontend seam to inspect.
- Phase 41-44 are the Operation Layer v0.5 release train: lifecycle-owner
  foundation, real API/admission proof, first bounded finalizer mutation, and
  delete lifecycle close gate.
- The release-train contract is
  `internal/docs/ref/operation-layer-v0.5-release-train.md`; the Phase 42 gate
  draft is `internal/docs/ref/phase42-lifecycle-owner-api-admission-gate.md`.
- The returned-replica operation loop has a coherent close point at Phase 98.
  The next large storage feature can start, but it should reuse the same
  fact -> judgment -> action -> evidence model rather than bypassing the
  Operation Layer.
- When the current plan closes, move it to `internal/docs/finished-plans/`
  with a phase/topic filename such as
  `phase1_finishedplan_frontend_protocol_readiness.md`.
- Keep deeper technical design in separate files only when it is needed for
  review or future maintenance.
- Keep long audits and historical references under `internal/docs/ref/`.
