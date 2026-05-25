# Phase 32 Runner Action Backlog Addendum

Date: 2026-05-25

Owner: QA. Source for D1a Workstream C.

This addendum extends `internal/docs/ref/testops-runner-action-backlog.md`
with concrete acceptance cases for each Phase 32-blocked runner primitive.
For each item: why product gates need it, current shell workaround,
expected parameters, one pass case + one fail case.

## P0 - Required for Phase 32 D3-D7 strict-PASS scenarios

### 1. `kubectl_wait_jsonpath`

**Why**: PVCs/Jobs/Deployment replica counters don't have
`.status.conditions[]` arrays. Today the runner-native PVC spike
`experimental-runner-native-pvc-loop.yaml` had to use long-running Pods
instead of one-shot writer/reader pods because of this gap. Phase 32 D3
status projection scenarios need to wait for `.status.phase=Bound` on PVCs
and `.status.observedGeneration` jumps on future CRDs.

**Current shell workaround**:

```yaml
- action: exec
  cmd: |
    for i in $(seq 1 60); do
      phase=$(kubectl -n default get pvc sw-block-example-pvc -o jsonpath='{.status.phase}' 2>/dev/null);
      [ "$phase" = "Bound" ] && exit 0;
      sleep 2;
    done; exit 1
```

**Expected parameters**:

```yaml
- action: kubectl_wait_jsonpath
  node: m02
  resource: "pvc/sw-block-example-pvc"
  namespace: "default"
  jsonpath: "{.status.phase}"
  expected: "Bound"
  timeout: "3m"
  poll_interval: "2s"
```

**Pass case**: PVC reaches `Bound` within timeout → exit 0.
**Fail case**: PVC stays in `Pending` (e.g., CSI driver crashed) → action
emits `last_value=Pending` and exits non-zero before scenario timeout.

### 2. `kubectl_wait_completed`

**Why**: One-shot writer/reader Pods that complete normally end up with
`status.phase=Succeeded` and `Ready=False`. `kubectl_wait_condition
Ready=True` never fires for them. Phase 32 D3 needs the canonical
"writer pod ran once and exited 0" assertion natively.

**Current shell workaround**: `exec: kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/X --timeout=4m`.

**Expected parameters**:

```yaml
- action: kubectl_wait_completed
  node: m02
  resource: "pod/sw-block-example-writer"
  namespace: "default"
  timeout: "5m"
```

**Pass case**: Pod reaches `Succeeded` → exit 0.
**Fail case**: Pod stays `Pending` (image pull fail) → action exits non-zero
with `last_phase=Pending` and `events_tail` capturing the
`ErrImagePull/ImagePullBackOff` event in the action's recorded output.

### 3. `helm_install` / `helm_uninstall`

**Why**: Every Day-1 scenario calls helm via raw `exec`. The exec strings
are 1-2KB each and hide the install knobs from cold readers. Phase 32 D3-D6
scenarios will multiply this pattern.

**Current shell workaround**: bash exec wrappers in every scenario.

**Expected parameters**:

```yaml
- action: helm_install
  node: m02
  release: "sw-block"
  namespace: "kube-system"
  chart: "charts/seaweed-block"
  values: "/abs/path/values.day1.yaml"
  wait: true
  timeout: "10m"
  artifacts:
    install_log: "/abs/path/helm-install.txt"
    status_after: "/abs/path/helm-status.after-install.txt"
```

**Pass case**: helm reaches `STATUS: deployed`, all required Deployments
Ready → exit 0.
**Fail case**: chart-image-skew flag emitted by chart but missing on image
→ blockmaster CrashLoopBackOff → helm `--wait` times out. Action captures
helm install log + blockmaster events to the named artifact paths so a
cold reviewer doesn't need SSH.

### 4. `sw_block_ops_report`

**Why**: Phase 28 G4/G5 and Phase 29/30 D3 spend ~80 lines of bash each
running `port-forward + sw-block ops report --master-api ... --out ...`
with retry. Phase 32 D3 and D4 status-projection scenarios will need this
3-4 times each.

**Current shell workaround**: long bash with port-forward,
`while true; do test -z; sleep; done` loops, and `sw-block ops report
--master-api` flag soup.

**Expected parameters**:

```yaml
- action: sw_block_ops_report
  node: m02
  cli: "/abs/path/sw-block"
  mode: "live"            # or "from-bundle"
  from_bundle: ""         # optional path
  master_deployment: "deploy/sw-blockmaster"
  master_namespace: "kube-system"
  master_port: 9333
  out: "/abs/path/report"
  timeout: "3m"
```

**Pass case**: action picks a Ready blockmaster pod, port-forwards, calls
`sw-block ops report`, returns exit 0 with `report=<out>/index.html`.
**Fail case**: blockmaster unreachable → action records
`master_unreachable=true reason=connection_refused` and writes a stub
report containing only the residue captured by the runner. Does NOT
silently succeed with empty content.

### 5. `assert_no_multipath_maps`

**Why**: Phase 27 left 2 orphan `mpath...` maps; the original
`verify-helm-cleanup.sh` missed them. The new bash-grep verifier works,
but the assert intent is too important to be implicit. Phase 32 D7
stale-evidence and Phase 30+ cleanup ownership both depend on a clean
multipath surface.

**Current shell workaround**: bash grep against `multipath -ll` filtered
by sw-block IQN substring.

**Expected parameters**:

```yaml
- action: assert_no_multipath_maps
  node: m02
  iqn_substr: "io.seaweedfs"
  include_orphan_mpath: true
```

**Pass case**: `multipath -ll` empty (or contains only non-sw-block maps).
**Fail case**: any `mpath... size=<N>` line matching the iqn substr or any
orphan `mpath... ##,##` entry → action reports the offending block(s) and
exits non-zero.

## P1 - Required for Phase 32 D7 + Phase 27 D5/D6 verification

### 6. `assert_alua_aas_transition`

**Why**: Phase 27 D6 follow-up surfaced this as a real product-claim gap.
The current verification is "did sg_rtpg print the words 'asymmetric
access state'" → tautology. The actual AAS value transition is what
matters.

**Current shell workaround**: parse `sg_rtpg` output with `sed`, compare
hex codes.

**Expected parameters**:

```yaml
- action: assert_alua_aas_transition
  node: m02
  before_file: "/abs/path/sg-rtpg.before.txt"
  after_file: "/abs/path/sg-rtpg.after.txt"
  expected_before:
    old_primary_aas: "0x00"
    promoted_aas: "0x02"
  expected_after:
    old_primary_aas: "missing"
    promoted_aas: "0x00"
```

**Pass case**: parsed AAS values match the four expected codes.
**Fail case**: any field mismatch, or AAS unparseable → action reports the
exact mismatch (e.g., `promoted_aas after expected 0x00 got 0x01`).

### 7. `iscsi_assert_io_rejected`

**Why**: Phase 27 D5 follow-up. `old_primary_stale_io_success_count=0` is
the strongest claim Phase 27 makes; the current evidence is a script-side
direct-read probe whose result *should* be probe-derived. Phase 32 D4
needs this as a named primitive to validate fencing claims.

**Current shell workaround**: shell `iscsiadm login` + raw read attempt
+ count failures.

**Expected parameters**:

```yaml
- action: iscsi_assert_io_rejected
  node: m02
  portal: "192.168.1.181:3260"
  iqn: "iqn.2026-05.io.seaweedfs:pvc-..."
  probe: "direct_read"   # or "scsi_write"
  expected: "rejected"   # connection refused / login fail / write_error
  timeout: "30s"
```

**Pass case**: write probe rejected (login refused or write returned
expected sense key) → exit 0 with `success_count=0`.
**Fail case**: any successful read/write → exit non-zero with
`success_count>0` (which is itself a Phase 27/32 product regression).

### 8. `collect_k8s_snapshot`

**Why**: Failure bundles for Phase 32 status scenarios need a standard
shape (see Workstream D doc). Today every scenario open-codes its own
collection; some miss CSI controller logs, some miss
generated-blockvolume logs.

**Current shell workaround**: ad-hoc bash with `kubectl get pods,deploy,...
-o yaml` per scenario.

**Expected parameters**:

```yaml
- action: collect_k8s_snapshot
  node: m02
  namespace: "default,kube-system"
  out: "/abs/path/k8s-snapshot"
  include:
    - nodes
    - pods
    - deploys
    - events
    - pvc
    - pv
    - storageclass
    - logs:sw-block-csi-controller
    - logs:sw-block-csi-node
    - logs:sw-blockmaster
    - logs:sw-blockvolume
```

**Pass case**: produces a directory matching the Workstream D shape.
**Fail case**: any required collection target missing → action records
`missing=<list>` and exits non-zero IF strict mode; in best-effort mode it
collects what it can and records partial in summary.

## P2 - Quality-of-life, not Phase 32 blockers

### 9. `helm_lint_template` (suggestion)

Combine `helm lint` + `helm template` + chart metadata checks already in
`helm-release-hygiene-chain.yaml`. Same shape as P0 helm actions.

### 10. `inject_partition` / `inject_netem` K8s wrappers

Both chaos primitives exist in the runner but no scenario uses them.
Phase 32 D7 stale-evidence scenarios would benefit from being able to
deliberately partition blockmaster from CSI without touching nodes
manually.

## Phase 32 Blocker Summary

| Primitive | Phase 32 gate(s) blocked | Priority |
|---|---|---|
| `kubectl_wait_jsonpath` | D3, D4, D6 | P0 |
| `kubectl_wait_completed` | D3, D4 | P0 |
| `helm_install` / `helm_uninstall` | D3-D6 | P0 |
| `sw_block_ops_report` | D3, D4, D5, D6 | P0 |
| `assert_no_multipath_maps` | D4 cleanup-residue blocker, D7 | P0 |
| `assert_alua_aas_transition` | D5, D6 multipath failover | P1 |
| `iscsi_assert_io_rejected` | D4 fencing blocker, D5 stale primary | P1 |
| `collect_k8s_snapshot` | D4 negative-bundle, all gates on failure | P0 |

Recommend dev land P0 actions in parallel with D2 status/CRD work so QA
can author D3-D7 scenarios that don't need to re-grow the bash wrappers
that Phase 30-31 already paid down.
