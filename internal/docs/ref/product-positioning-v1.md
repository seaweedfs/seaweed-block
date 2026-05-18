# Seaweed Block Product Positioning v1

Status: draft, proposed for internal alignment. This document is the source of
truth a product agent should read before talking about Seaweed Block, and the
first place a new user should land before running it.

It has three layers:

1. Positioning — the product story.
2. What's delivered — the validated claim surface as of the latest closed plan.
3. How a new user runs it — including specifics for the M01/M02 lab.

---

## 1) Positioning First, Matrix Second

Primary rule:

- Positioning is the product story.
- Claim matrix is the fact layer behind the story.

Do not lead with a giant capability table in user-facing surfaces.
Lead with the product promise, then back it with evidence links.

---

## 2) One-Line Positioning

English:

`Seaweed Block is a lightweight Kubernetes block storage product with enterprise protocol discipline and fail-closed recovery semantics.`

Chinese:

`Seaweed Block 是一个面向 Kubernetes 的轻量块存储产品，强调企业级协议纪律与可解释的 fail-closed 故障语义。`

---

## 3) Core Value Propositions (Public)

### A. Lightweight Kubernetes Experience

- Fast first-volume path: one preflight, one install, one PVC, writer + reader pods verify checksum.
- Product-owned lifecycle: no manual side-quest scripts on the happy path.
- Read-only cluster inventory with line-level cleanup attribution.

### B. Protocol Discipline

- Product behavior is constrained by CSI / iSCSI / NVMe contracts.
- Linux OS-initiator and Windows-initiator compatibility validated against the same target binary (Linux green; Windows substantively PASS).
- ALUA / multipath / CHAP / online resize / Prometheus metrics covered by closed plans.

### C. Fail-Closed Recovery Semantics

- On controlled failure, the product either:
  - recovers through the documented path with evidence, or
  - refuses unsafe promotion and explains why.
- The safe-refusal contract carries `failover_status: refused`, `data_check_after_failover=not_claimed`, and an `after_issue_evidence` line tied to live inventory — no hidden "green" based on weak semantics.
- For Kubernetes HA, the minimum product line is not "master changed primary."
  The minimum is app-path recovery: automatic promotion plus documented
  CSI/node reattach on pod recreate with post-failure checksum evidence. A
  later transparent-failover line must prove protocol multipath: iSCSI
  ALUA/dm-multipath or NVMe ANA/native multipath.

---

## 4) Target Users (ICP v1)

1. Platform teams running stateful workloads on Kubernetes, who want less
   operational weight than large storage stacks.
2. Edge/private-cluster operators needing explainable failure behavior rather
   than opaque automation.
3. Engineering teams that care about protocol-correct host behavior and
   supportable incident evidence.

---

## 5) What We Are Not Positioning As (For Now)

- Not a broad production HA platform claim.
- Not a full replacement pitch against large enterprise storage suites.
- Not a UI-first story.

Current direction is:

`small footprint + strict semantics + evidence-first operations`

---

## 6) What's Delivered (Validated Claim Surface)

Each row below is backed by a closed plan in
`internal/docs/finished-plans/` and a QA close report in
`internal/docs/qa-assignments/`. Run IDs on the M02 lab are recorded
in the corresponding close reports.

### 6.1 Light-Use Operations Loop  (phases 9–11)

| Capability | Status |
|---|---|
| Install alpha stack with one command | claimed |
| Create one PVC, write/read through a pod, replace pod, read back | claimed |
| `sw-block ops status` produces a self-describing per-volume bundle | claimed |
| `sw-block ops inventory` discovers volumes/PVCs/endpoints from the cluster | claimed |
| Inventory: PVC ↔ volume ↔ replica mapping, line-level cleanup attribution | claimed |
| TestOps coordinates shared-lab runs with active/history/lock records | claimed |

Closed plans: `phase9_finishedplan_light_use_operations_mvp.md`,
`phase10_finishedplan_light_use_install_lifecycle_operations_mvp.md`,
`phase11_finishedplan_cluster_ops_inventory_lifecycle_visibility_mvp.md`.

### 6.2 Product-Owned Lifecycle  (phase 12)

| Capability | Status |
|---|---|
| Generated `blockvolume` Deployment created by the product when a PVC is added | claimed |
| Scoped delete: deleting one PVC leaves the other PVC's workload intact | claimed |
| `lifecycle_owner=pvc-owner-ref` + `owner_ref=PersistentVolumeClaim/<ns>/<pvc>` in inventory | claimed |
| `scripts/apply-k8s-alpha-blockvolumes.sh` side-quest removed from the default path | claimed |

Closed plan: `phase12_finishedplan_product_owned_blockvolume_lifecycle_mvp.md`.

### 6.3 Durable Volume Restart And Reattach  (phase 13)

| Capability | Status |
|---|---|
| Generated RF=1 `blockvolume` survives its own workload restart with data still readable through the PVC | claimed (when `SW_BLOCK_LAUNCHER_STATE_HOSTPATH` is configured) |
| Restart durability proof: writer checksum before restart, reader checksum on a replacement pod after restart | claimed |
| Durable status `Latched=true Operational=true` exposed in `sw-block ops status` bundle | claimed |
| Failure-bundle path: bad hostPath emits `inventory_status: unhealthy` with actionable issues | claimed |

Closed plan: `phase13_finishedplan_durable_volume_restart_reattach_mvp.md`.

### 6.4 Multi-Node Attach And Placement  (phase 14)

| Capability | Status |
|---|---|
| Same-node RF=1 attach on a multi-node-capable cluster: app pods + generated `blockvolume` pinned to the same selected Kubernetes node | claimed |
| Negative fixture: cross-node loopback attach refused with explicit exit 45 + `unsupported-cross-node-loopback-attach.txt` | claimed |
| Inventory exposes `node=<name>` per replica so operators see the alignment | claimed |

Closed plan: `phase14_finishedplan_multi_node_attach_and_placement_mvp.md`.

### 6.5 Basic Mounted Failover And Reattach  (phase 15)

| Capability | Status |
|---|---|
| Two-logical-server dev/TestOps topology: one Kubernetes node can host two distinct Seaweed Block server identities | claimed |
| RF=2 PVC binds, two generated `blockvolume` Deployments become Ready, distinct frontend/status ports | claimed |
| RF=2 mounted app path: writer + reader checksums pass through the same PVC | claimed |
| Default single-logical-server RF=2 is safely refused (not partially placed) | claimed |
| Controlled primary failure produces a safe-refusal bundle when the peer is not promotion-ready | claimed |
| Inventory dual-primary safety: `conflicting_primary_replicas=...` issue surfaces when authority is contradictory | claimed |

Closed plan: `phase15_finishedplan_basic_mounted_failover_and_reattach_mvp.md`.

Delivered Phase 15 safe-refusal contract (the exact strings):

```text
failover_status: refused
ack_profile: best-effort
failure_class=primary-blockvolume-controlled-stop
before_primary_replica=<rN>
failed_replica=<rN>                ← derived from live inventory, equals before_primary_replica
candidate_ready=false
candidate_evidence=<peer's not-ready inventory row>
data_check_after_failover=not_claimed
reason=candidate_not_ready_for_primary
target_ready_replicas=0
after_issue_evidence=<actionable inventory issue>
```

### 6.6 Stage 1 Mounted Recovery ACK Profile  (phase 16)

| Capability | Status |
|---|---|
| RF=2 `best-effort` controlled recovery demo through CSI/pod recreate | claimed as demo only |
| RF=3 `sync-quorum` mounted recovery through CSI/pod recreate | claimed as Stage 1 beta recovery target |
| Master promotion gated by promotion evidence and durable frontier coverage | claimed |
| CSI/node re-stage on pod recreate moves the mounted app to the promoted frontend | claimed |
| Post-failure reader checksum proves recovered app-path data | claimed |
| Transparent in-place I/O continuation without pod recreate | not claimed |

Closed plan:
`phase16_finishedplan_stage1_mounted_recovery_ack_profile_mvp.md`.

Delivered Stage 1 RF=3 recovery marker:

```text
ack_profile: sync-quorum
claim_profile=beta-recovery
failover_status: promotion_pending -> promoted
before_primary_replica=r1
failed_replica=r1
promoted_replica=r2
frontier_covered=true
post_failure_primary_count=1
data_check_after_failover: pending_reader -> reader_checksum_passed
reader_verified=true
```

Interpretation: this is Kubernetes mounted recovery through CSI/pod recreate.
It is not transparent protocol multipath failover.

### 6.7 Protocol Substrate (carried forward from earlier phases)

| Capability | Status |
|---|---|
| iSCSI target + ALUA implicit + multipath compatibility | closed at phases 1–6 |
| iSCSI OS-initiator compatibility (Linux green, Windows substantively PASS) | closed at phase 6 |
| Snapshots (CoW), CHAP auth, online resize | closed at phase 5 |
| Prometheus metrics | closed at phase 5 |
| Fault/consistency tests on m01/M02 lab | closed at phases 2–5 |

---

## 7) Non-Claims (Authoritative List)

The product does NOT claim, as of phase 16 close:

- transparent production HA without pod recreate,
- node loss or host-disk failure survival,
- remote-node attach to a loopback-published `blockvolume`,
- automatic multi-node scheduling, rescheduling, or rebalancing,
- RF=2 quorum HA after primary failure,
- transparent in-place I/O continuation,
- Kubernetes CSI multipath host-path failover,
- sync-all durability,
- backup, restore, rebuild, promote, repair, or other mutating admin commands,
- broad uninstall safety / upgrade safety,
- performance SLOs,
- UI or operator-grade reconciliation.

If a new claim is added in a future plan, both `docs/operations-v1.md`
"Scope And Non-Claims" and this document MUST be updated in the same change.

---

## 8) How A New User Runs It

This section gives the canonical install + first-volume path. It mirrors
`docs/quickstart-kubernetes.md` ("First Volume In 10 Minutes" section). Run
each step in order on a single-node Kubernetes cluster.

### 8.1 Prerequisites

- A Linux Kubernetes node where privileged CSI pods are allowed and
  `iscsi_tcp` is loadable. k3s works.
- `open-iscsi` (`iscsiadm`) installed on the node.
- Docker + the ability to `k3s ctr images import` (or a registry your cluster can pull).
- `kubectl` configured against the cluster.

### 8.2 Default Install / First Volume

```bash
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

# Preflight (emits structured PASS/FAIL per check):
bash scripts/preflight-k8s-alpha.sh --local-k3s

# Build alpha images locally and import them into k3s containerd:
SW_BLOCK_IMPORT_K3S=1 \
SW_BLOCK_ARTIFACT_DIR=/tmp/sw-block-alpha-build \
  bash scripts/build-alpha-images.sh "$PWD"

# Run the first-volume demo end to end:
bash scripts/run-k8s-demo.sh "$PWD"
```

Expected final line:

```text
[app-demo] PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete
```

### 8.3 Inspect The Cluster

Operate the cluster after install:

```bash
# Port-forward blockmaster for the inventory command:
kubectl -n kube-system port-forward svc/blockmaster 9333:9333
```

In another terminal:

```bash
sw-block ops inventory \
  --namespace default \
  --master 127.0.0.1:9333 \
  --out /tmp/sw-block-inventory

cat /tmp/sw-block-inventory/volume-inventory-summary.txt
```

A healthy one-volume shape looks like:

```text
inventory_status: ok
volumes: total=1 ok=1 unhealthy=0 invalid=0
volume: id=pvc-... pvc=sw-block-demo-pvc rf=1 desired=1 observed=1 primary=r1 status=ok protocols=iscsi
replica: ... server=<node> node=<node> frontend=127.0.0.1:3260 status_addr=127.0.0.1:23260 support_bundle=volumes/pvc-.../r1
```

For an RF=2 development/TestOps topology, set
`SW_BLOCK_ALPHA_LOGICAL_SERVERS=2 SW_BLOCK_ALPHA_EXPECTED_SLOTS_PER_VOLUME=2` on
the installer. See `docs/operations-v1.md` §"RF=2 Mounted Failover Status".

### 8.4 Durable Restart Path

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export SW_BLOCK_LAUNCHER_STATE_HOSTPATH="/var/lib/sw-block/testops-${RUN_ID}-restart"
bash scripts/run-k8s-blockvolume-restart.sh "$PWD"
```

This proves a generated RF=1 `blockvolume` can be restarted while the same PVC
remains usable. See `docs/operations-v1.md` §5.

### 8.5 If Something Fails

```bash
ARTIFACT_DIR="$(ls -td /tmp/sw-block-app-demo-* | head -1)"
cat "$ARTIFACT_DIR/run.log"

VOLUME_ID="$(sed -n 's/.*--volume-id=\([^"[:space:]]*\).*/\1/p' "$ARTIFACT_DIR/generated-blockvolume.yaml" | head -1)"
STATUS_ADDR="$(sed -n 's/.*--status-addr=\([^"[:space:]]*\).*/\1/p' "$ARTIFACT_DIR/generated-blockvolume.yaml" | head -1)"

if [ -n "$VOLUME_ID" ] && [ -n "$STATUS_ADDR" ]; then
  sw-block ops status \
    --volume "$VOLUME_ID" \
    --master "127.0.0.1:9333" \
    --status-addr "$STATUS_ADDR" \
    --out "$ARTIFACT_DIR/ops-status"
else
  echo "ops-status-unavailable: no volume id/status address reached"
fi
```

Attach the whole artifact directory to any issue report.

### 8.6 Cleanup

The demo cleans up automatically. For a full alpha stack uninstall:

```bash
bash scripts/uninstall-k8s-alpha.sh "$PWD"
```

For user-facing scoped cleanup of just the demo PVC, see
`docs/quickstart-kubernetes.md` "Cleanup" section. Do NOT use global
`kubectl delete deploy -A -l app=sw-blockvolume` — that's a TestOps
guardrail, not user-facing cleanup.

---

## 9) Running On The M01 / M02 Lab

The internal M01/M02 lab is the canonical hardware substrate for QA and demos.
Seaweed Block's alpha all runs on M02 (single-node k3s). M01 is currently used
by the SRA/RDMA project, not as a Kubernetes node for seaweed_block.

### 9.1 Lab Coordinates

| Item | Value |
|---|---|
| M02 (k3s alpha lab) | `192.168.1.184`, user `testdev` |
| SSH key (controller-side) | `C:/work/dev_server/testdev_key` |
| Shared SMB share | `V:/share` on Windows = `/mnt/smb/work/share` on Linux |
| TestOps artifact root on M02 | `/mnt/smb/work/share/g15d-k8s/<run_id>-<scenario>/` |
| Default `KUBECONFIG` on M02 | `/etc/rancher/k3s/k3s.yaml` |
| TestOps control directory (when used) | `--control-dir` flag to `sw-testops` |

### 9.2 Connecting To M02

From a Windows controller (Git Bash / PowerShell):

```bash
ssh -i C:/work/dev_server/testdev_key testdev@192.168.1.184
```

If you need to forward an interactive `iscsiadm` session or expose
blockmaster's gRPC to a Windows tool, use SSH local forwarding:

```bash
ssh -i C:/work/dev_server/testdev_key -N -L 9333:127.0.0.1:9333 testdev@192.168.1.184
```

(For Windows iSCSI Initiator validation specifically, see
`internal/docs/qa-assignments/iscsi-os-windows-initiator-validation.md`.)

### 9.3 Staging The Source Tree On M02

The product working tree is shared with the dev agent at
`C:/work/seaweed_block`. To run on M02, ship the working tree to
`/tmp/seaweed_block` and proceed:

```bash
# From Windows controller (Git Bash):
ssh -i C:/work/dev_server/testdev_key testdev@192.168.1.184 \
  'rm -rf /tmp/seaweed_block && mkdir -p /tmp/seaweed_block'

cd C:/work/seaweed_block
tar --exclude=.git --exclude='*.exe' --exclude=node_modules \
    --exclude=results --exclude=.cache --exclude=tmp -cf - . \
  | ssh -i C:/work/dev_server/testdev_key testdev@192.168.1.184 \
        'cd /tmp/seaweed_block && tar -xf -'
```

On M02, the same `scripts/preflight-k8s-alpha.sh --local-k3s` + the canonical
quickstart commands apply (§8.2 above), with `KUBECONFIG=/etc/rancher/k3s/k3s.yaml`.

### 9.4 Running A QA Scenario Against M02 From The Controller

The `swblock` runner can drive scenarios on M02 over SSH:

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/<scenario-name> `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/<scenario>.yaml
```

Scenario artifacts land under both:

- The Windows result bundle at `V:/share/g15d-k8s/testops-runs/...`,
- The M02-side artifact dir at `/mnt/smb/work/share/g15d-k8s/<run_id>-*/`.

### 9.5 Lab Hygiene After A Run

The chains' `collect_and_cleanup` phases handle scoped cleanup. To audit
manually after a run:

```bash
ssh -i C:/work/dev_server/testdev_key testdev@192.168.1.184 '
  sudo -n iscsiadm -m session 2>&1 | head -1
  pgrep -af "blockmaster|blockvolume|blockcsi|iscsi-target" 2>/dev/null | head
  pgrep -af "kubectl.*port-forward.*svc/blockmaster" 2>/dev/null | head
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get deploy -A -l app=sw-blockvolume
  sudo -n ls -d /var/lib/sw-block/testops-* 2>/dev/null'
```

Expected after a clean run: no active iSCSI sessions, no sw-block processes,
no port-forwards, no `app=sw-blockvolume` Deployments, no run-scoped
`testops-*` paths.

Older `/var/lib/sw-block/pvc-*` directories from prior demos exist and are
out of scope for the testops cleanup guarantee — they are documented as a
non-claim (see `docs/operations-v1.md` "Upgrade and broad uninstall safety
are not claimed").

---

## 10) Where To Find Evidence

For each delivered claim:

| Source | What it gives you |
|---|---|
| `internal/docs/finished-plans/phase<N>_finishedplan_*.md` | The plan that closed the claim |
| `internal/docs/qa-assignments/*-close-report.md` | QA close report with HG-clause table, residue audit, run IDs |
| `internal/docs/qa-assignments/*-validation.md` | QA assignments (input to a close report) |
| `testops/scenarios/*.yaml` | Runner-native scenarios that validate the claim live |
| `docs/operations-v1.md` | User-facing operations manual; tracks the same claim/non-claim discipline |
| `docs/quickstart-kubernetes.md` | First-volume runbook for a new user |

Recent close reports a product agent should be ready to cite:

- `light-use-mvp-close-report.md` (phase 10)
- `cluster-ops-inventory-mvp-close-report.md` (phase 11)
- `product-owned-blockvolume-lifecycle-mvp-close-report.md` (phase 12)
- `durable-volume-restart-reattach-mvp-close-report.md` (phase 13)
- `multi-node-attach-placement-mvp-close-report.md` (phase 14)
- `mounted-failover-reattach-mvp-close-report.md` (phase 15)
- `stage1-mounted-recovery-ack-profile-mvp-close-report.md` (phase 16)

---

## 11) Public Messaging Template

### Short Paragraph (README / Site)

Seaweed Block is a lightweight Kubernetes block storage product for early lab
and small-cluster usage. It focuses on a clear first-volume workflow, protocol-
disciplined behavior across CSI/iSCSI/NVMe paths, and fail-closed recovery
semantics. When recovery is not yet safe to claim, Seaweed Block reports an
explicit refusal with actionable evidence instead of hiding risk behind partial
success.

### Non-Claim Footer (Mandatory)

This release does not claim production HA, broad distro compatibility, or
upgrade-safe operations. See `docs/operations-v1.md` "Scope And Non-Claims"
and the QA close reports under `internal/docs/qa-assignments/` for exact
scope.

---

## 12) Execution Rules (Internal)

1. Any new public claim must map to:
   - a gate/scenario,
   - QA evidence (close report with run IDs),
   - explicit non-claim boundary.
2. If semantics are weaker than the product promise, publish safe refusal
   rather than softening the promise silently.
3. Do not let tests become marketing language; tests validate claims, they do
   not define positioning.
4. When a plan closes, update both `docs/operations-v1.md` "Scope And
   Non-Claims" and §6/§7 of this document in the same change.

---

## 13) Role Of The Claim Matrix

The claim matrix (§6 and §7 above) should remain a fact document:

- capability,
- claim status (claimed / partial / non-claim),
- proof links (close report + scenario + plan),
- known gaps (the next plan in `internal/docs/current-plan.md`).

Use it as:

- release sign-off artifact,
- support alignment artifact,
- internal truth source for external messaging.

Do not use it as the primary homepage narrative.

---

## 14) Next Steps

1. Align README top section to this positioning.
2. Add a concise public-facing claim summary in `docs/` that mirrors §6/§7.
3. Keep this document as the authoritative claim/non-claim source under
   `internal/docs/ref/`.
4. When the next plan closes, append its row to §6, reconcile §7 if any
   non-claim was promoted, and update §10's recent close report list.
