# Current Plan: Phase 20 - Activation / Day-1 Ops MVP

Status: implementation started. D1 dev slice in progress; QA needed after the
activation chain is wired into TestOps.

Previous closed capability:

- `finished-plans/phase19_finishedplan_control_plane_observation_ai_readable_ops_mvp.md`
- `qa-assignments/control-plane-observation-ai-readable-ops-mvp-close-report.md`

## Product Question

Can a new Kubernetes user complete one understandable block-storage activation
loop without reading internal docs or SSHing into every node?

```text
preflight
-> install
-> node readiness
-> create PVC
-> volume ready
-> write/read app
-> status/timeline/evidence
-> cleanup
```

Expanded product loop:

```text
install Seaweed Block from one documented path
-> verify node readiness and blockers
-> create one PVC-backed volume through Kubernetes
-> run an app write/read check
-> inspect read-only CLI/status evidence
-> collect product evidence, inventory, logs, and support bundle
-> uninstall cleanly
```

This plan is about activation and day-1 operations. It should not add new HA,
backup, restore, repair, or mutating admin claims.

## Core Thesis

The product is now strong enough in recovery and observation that the next
highest-friction gap is the user journey around it. A good block product needs
more than a working CSI path: users need to install it, understand whether each
node is usable, create volumes, see what happened, and capture evidence when it
is not healthy.

The implementation should reuse the Phase 19 observation surface. Dashboard,
CLI, logs, and bundles should all describe the same facts with stable reason
codes.

## Scope

### D1: Install Loop

- Keep `scripts/install-k8s-alpha.sh` as the implementation substrate for this
  phase.
- Add a simpler documented wrapper path if needed. This should wrap preflight,
  local image build/import, install, rollout wait, and summary. It may be a
  script first; a `sw-block install k8s-alpha` CLI is optional and should not
  block the first gate.
  - Dev slice: `scripts/activate-k8s-alpha.sh` is the first wrapper path. It
    calls `preflight-k8s-alpha.sh`, `build-alpha-images.sh`,
    `install-k8s-alpha.sh`, applies the alpha StorageClass, captures rollout
    readiness, and writes `activation-summary.txt`.
  - Image path split:
    - `SW_BLOCK_ACTIVATION_IMAGE_MODE=local` (default) builds/imports
      `sw-block:local` and `sw-block-csi:local` for dev and QA work-tree
      validation.
    - `SW_BLOCK_ACTIVATION_IMAGE_MODE=published` skips local build/import,
      runs GHCR preflight, and installs `ghcr.io/seaweedfs/seaweed-block:alpha`
      plus `ghcr.io/seaweedfs/seaweed-block-csi:alpha` unless overridden.
      This is the PM/user-path test surface. Prefer `sha-<commit>` or release
      tags over mutable `:alpha` for close validation.

- Add a user-facing install path in README/docs that makes prerequisites,
  expected components, and verification commands explicit.
- Emit a concise install summary:
  - master ready,
  - CSI controller ready,
  - CSI node ready count,
  - StorageClass present,
  - selected protocol,
  - selected ACK profile,
  - known non-claims.
  - Dev slice: summary fields are stable key/value lines:
    `activation_status`, `master_ready_replicas`,
    `csi_controller_ready_replicas`, `csi_node_ready`,
    `storageclass_provider`, `protocol`, `ack_profile`, and next status /
    inventory commands.
- Failed install should explain the blocker: missing tool, missing k3s access,
  image import failure, pod not ready, or node prerequisite mismatch.
- QA slice: `testops/scenarios/activation-day1-install-chain.yaml` and
  `qa-assignments/activation-day1-install-validation.md` validate the D1
  install-to-ready path.

### D2: Node Readiness

- Provide a read-only node readiness view before claiming add-node automation.
- A node row should make clear:
  - Kubernetes node name,
  - CSI node pod status,
  - iSCSI / multipath prerequisite status where applicable,
  - whether the node is eligible for blockvolume placement,
  - frontend address mode: loopback or non-loopback,
  - blocker reason if not eligible.
- If this phase adds an `add-node` command, it must be conservative: preflight
  and explain eligibility first; mutating scheduling or rebalance behavior is
  out of scope unless separately gated.

### D3: Add-Volume Loop

- Document the canonical dynamic PVC path as the normal volume creation path.
- Provide a minimal user-facing "add volume" path. This can be a documented
  PVC YAML template first. A CLI helper is optional and, if added, must be a
  thin generator around Kubernetes PVC creation. It must not bypass PVC/CSI.

```bash
kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml
```

- The close gate must prove at least two volumes can be created and listed
  without attribution or port/status collisions.
- Add or improve a small command/view that maps:

```text
PVC -> PV -> volume_id -> replicas -> primary -> frontend -> events -> bundle
```

- The user should be able to create one PVC, watch it become ready, and see the
  same volume appear in CLI/dashboard evidence.
- Dev slice: `scripts/run-basic-app-example.sh` runs the canonical
  `examples/kubernetes/basic-app` PVC/writer/reader loop, collects
  `sw-block ops cluster` and `sw-block ops inventory` evidence, and writes
  `first-volume-summary.txt`.
- QA slice: `testops/scenarios/activation-day1-first-volume-chain.yaml` and
  `qa-assignments/activation-day1-first-volume-validation.md` validate the
  published-image Day-1 first-volume path.

### D4: Read-Only Status / Dashboard View

- Dashboard/status view is read-only for this plan.
- It may be a local web view, static report, or CLI-driven status page. Do not
  make the close depend on a full hosted UI unless that implementation is
  deliberately chosen.
- Product positioning:

```text
V2 UI shape:
cluster / servers / volumes / ops

V3 sw-block data model:
PVC / PV / volume_id / replica / primary / frontend / epoch /
event timeline / bundle / reason codes
```

- Build a read-only block dashboard/status view modeled after the SeaweedFS
  `/block/` information architecture, backed by the Phase 19 observation API,
  with no mutating admin actions.
- Dev slice: `sw-block ops report` renders a static read-only status page from
  either live `--master-api` evidence or a saved support bundle. It writes
  `index.html`, `cluster-evidence.json`, `timeline.jsonl`, and `summary.txt`
  from the shared observation core.
- Day-1 helper slice: `scripts/run-basic-app-example.sh` now generates
  `status/report/index.html` after writer/reader verification, so the first
  volume bundle contains a user-readable status page and machine-readable
  evidence together.
- Reuse lessons from the V2 SeaweedFS block UI where useful:
  - reference files:
    `C:\work\seaweedfs\weed\server\master_server_handlers_block_ui.go`,
    `C:\work\seaweedfs\weed\server\master_server.go`,
    `C:\work\seaweedfs\weed\storage\blockvol\blockapi\types.go`,
  - useful read-only shape: cluster cards, server table, volume table,
    replica details, health counters, barrier/failover/rebuild counters,
  - do not copy mutating V2 operations into this phase: create, delete,
    promote, expand, assign, or force actions.
- Minimum pages or panels:
  - cluster summary,
  - node readiness,
  - volumes table,
  - volume detail,
  - event timeline,
  - support bundle / log links.
- The dashboard must consume product-owned observation data, not scrape random
  pod logs as its primary source.
- Logs are supporting evidence, not authority.
- Mutating actions such as promote, repair, delete, rebuild, failback, and
  cleanup buttons are explicitly out of scope.
- Enterprise UI boundary: first ship observation. Add admin workflows only
  after each workflow has a separate product spec, RBAC/audit model, and strict
  QA gate.

### D5: Status / Logs / Bundle Loop

- The default CLI path should stay useful without the dashboard:

```text
sw-block ops cluster --master-api <addr>
sw-block ops volumes
sw-block ops describe volume <id>
sw-block ops timeline volume <id>
sw-block ops inventory --namespace <ns> --master <addr> --out <dir>
```

- Text output should be human-readable and AI-readable.
- JSON/JSONL output should remain stable enough for dashboard, CI, and support
  assistant use.
- Every unhealthy state should include a reason code and next inspection step.

### D6: README And User Path

- README must present Seaweed Block as an alpha/early-beta product loop, not a
  collection of internal gates.
- README must keep claims narrow:
  - not production-ready,
  - no full HA claim,
  - no backup/restore,
  - no upgrade safety,
  - no mutating admin operations,
  - no hosted dashboard unless implemented and gated.
- The documented quick path must match the commands the close gate runs.
- Dev slice: README and `docs/quickstart-kubernetes.md` were rewritten around
  the end-user path: activate, first PVC, writer/reader verification, local
  read-only report, troubleshooting evidence, and cleanup. Internal gate
  details remain in QA docs instead of the user tutorial.

### D7: TestOps Controller / Agent Evidence Loop

- Treat TestOps as product infrastructure for this phase, not as ad-hoc
  scripting. The current runner is already the Windows-side controller: it owns
  `run_id`, phase/action status, timeouts, cleanup, and the central result
  bundle.
- Add a controller/agent track rather than relying only on SSH:
  - controller stays on the Windows/dev workstation and runs scenarios,
    distributes binaries/scripts, owns cancellation, and writes the final
    bundle;
  - Linux node agents on m01/m02/tp01 collect local evidence and eventually
    execute bounded jobs;
  - SSH remains fallback until the agent proves stable.
- First useful agent capability:
  - `/healthz` with node name, version, uptime;
  - `collect_node_snapshot` for process, iSCSI sessions, multipath, NVMe,
    mounts, disk, kernel tail, network, containerd/k3s images, and kubelet
    status;
  - `collect_path` for local artifact pickup;
  - cancellation-aware bounded command execution later, after snapshot is
    proven.
- K8s/block failure auto-collect should become a gate rule:
  - on phase failure, collect node snapshots from involved nodes,
  - collect Kubernetes state (`kubectl get`, `describe`, events, selected logs,
    rendered manifests vs live objects),
  - collect product state (`sw-block ops cluster`, inventory, timeline/events),
  - then run cleanup and collect after-clean state.
- Why this is in the plan: Day-1 first-volume r1/r2 showed that without
  automatic evidence, a timeout is ambiguous; with `writer-describe.txt`, the
  root cause was immediately visible as loopback publish target vs cross-node
  writer placement.
- Open-source analogs to learn from, not copy blindly:
  - Sonobuoy: Kubernetes aggregator plus job/daemonset plugins; useful model
    for cluster-wide collection and per-node log gatherers.
  - Ansible Runner / AWX execution nodes: useful model for transmit -> worker
    -> process and central artifact handling.
  - Jenkins controller/agent: useful model for scheduling jobs onto prepared
    nodes.
  - Robot Framework remote libraries: useful model for remote capability
    exposure, but too generic for block-specific node snapshots.
- Non-goal for Phase 20: do not rewrite the runner. Add snapshot collection and
  failure auto-collect first; move remote exec to agent only after the evidence
  loop is stable.

## Non-Claims

This plan does not claim:

- production installer/operator lifecycle,
- full Helm/operator packaging unless explicitly chosen as the implementation,
- online upgrade or rollback safety,
- mutating dashboard actions,
- automatic repair/rebuild/failback,
- backup/snapshot/restore,
- new HA semantics beyond closed RF=3 gates,
- transparent node-loss failover beyond existing closed claims,
- broad distro/kernel compatibility.

## Hard Gate

The close gate should run from a fresh supported Kubernetes lab and prove:

1. One documented command path runs preflight, install, rollout wait, and
   install summary.
2. Node readiness view identifies at least one eligible node and explains any
   blockers.
3. A PVC is created through the documented Kubernetes path.
4. The generated product-owned volume becomes ready.
5. An app writes and reads data through the mounted PVC.
6. `sw-block` status/evidence explains PVC -> volume -> replica -> primary ->
   frontend -> timeline.
7. Product evidence and inventory/support bundle are collected.
8. Cleanup/uninstall leaves no active iSCSI sessions, stale blockvolume
   Deployments, stale port-forwards, or unexpected product pods.
9. README quick path matches the commands used by the gate.
10. Failure bundles include automatic K8s/product/node evidence sufficient to
    explain at least one failed first-volume run without manual SSH.

## Suggested Close Claim

Seaweed Block provides an alpha install-to-operate loop for Kubernetes users:
from a fresh supported lab, run one documented install path, create a
PVC-backed block volume, verify app write/read, inspect read-only status and
timeline evidence, collect support artifacts, and uninstall cleanly.

## Next Product Choices After This

- Backup / Snapshot / Restore MVP for data protection.
- Rebuild / Reintegration / Failback MVP for returned replicas.
- Production installer/operator lifecycle.
- Mutating admin controls with RBAC/audit hard gates.
