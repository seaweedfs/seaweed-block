# Phase 32 Failure Snapshot Standard

Date: 2026-05-25

Owner: QA. Source for D1a Workstream D.

Purpose: define the minimum content of a failure bundle for Kubernetes block
tests. A cold reviewer must be able to read the bundle from the SMB share and
identify the failure class **without SSHing into any node**.

This standard rolls up what Phase 27/28/29/30/31 bundles have actually
produced, and what their gaps were when QA needed to debug.

## Minimum Required Contents

A failure snapshot for a Phase 32 status scenario must contain ALL of the
following, organized by layer:

### Cluster topology

```text
<snapshot>/
  nodes/
    nodes.txt              kubectl get nodes -o wide
    nodes.yaml             kubectl get nodes -o yaml
  kube-system/
    pods.txt               kubectl get pods -n kube-system -o wide
    deploys.txt            kubectl get deploy -n kube-system -o wide
    services.txt
  default/                 (and any other test ns)
    pods.txt
    deploys.txt
    services.txt
```

### Workload state

```text
<snapshot>/
  pvc/
    pvc.txt                kubectl get pvc -A -o wide
    pvc.yaml               kubectl get pvc -A -o yaml
  pv/
    pv.txt                 kubectl get pv -o wide
    pv.yaml
  storageclass/
    sc.txt
    sc.yaml
  events/
    events.kube-system.txt  kubectl get events -n kube-system --sort-by=lastTimestamp
    events.default.txt
    events.all.yaml         all namespaces, structured form
```

### Product logs

For each Deployment/DaemonSet, capture **previous + current** logs:

```text
<snapshot>/logs/
  blockmaster/
    deploy.yaml
    pod.<name>.current.log
    pod.<name>.previous.log
  csi-controller/
    deploy.yaml
    pod.<name>.current.log
    pod.<name>.previous.log
    csi-attacher.current.log
    csi-provisioner.current.log
  csi-node/
    ds.yaml
    pod.<name>.<node>.current.log    # one per DS replica
    pod.<name>.<node>.previous.log
  blockvolume/
    deploy.<volume>-<replica>.yaml
    pod.<name>.current.log
    pod.<name>.previous.log
```

Previous-pod logs matter because most failure modes (image pull,
CrashLoopBackOff, OOMKilled) leave a terminated pod whose log is the only
evidence of what went wrong.

### Host-level state (collected via kubectl exec or SSH-once-per-snapshot)

```text
<snapshot>/host/<node>/
  iscsi-sessions.txt        sudo iscsiadm -m session
  iscsi-nodes.txt           sudo iscsiadm -m node
  multipath.txt             sudo multipath -ll
  multipath.config.txt      sudo multipath -t
  dmsetup.txt               sudo dmsetup ls
  dmsetup-deps.txt          sudo dmsetup deps
  processes.txt             sudo pgrep -af 'blockmaster|blockvolume|blockcsi|iscsi-target'
  hostpath.txt              ls -la /var/lib/sw-block/
  sg_rtpg.<device>.txt      where multipath maps exist
```

### Product evidence (when reachable)

```text
<snapshot>/product/
  cluster-evidence.json     sw-block ops cluster -o json
  inventory/                sw-block ops inventory
  report/
    index.html
    summary.txt
    cluster-evidence.json
    timeline.jsonl
    operator-snapshot.json
  explain.txt               sw-block ops explain volume <id>
```

### Critical: capture the unreachable state itself

If `sw-block ops cluster` / port-forward / dashboard can't be reached, the
snapshot **must record that fact** as a first-class artifact, not as a
missing file:

```text
<snapshot>/product/unreachable.txt
  master_unreachable=true
  reason=connection_refused
  port_forward_log=<inline tail>
  blockmaster_pod_state=ImagePullBackOff
  attempt_timestamp=...
```

This is what Phase 28 cycle 1 missed initially - silent absence of
`operator-snapshot.json` looked like a present-but-failed surface; cold
readers couldn't tell.

## Cold-Reader Diagnosis Checklist

A reviewer with only the snapshot directory (no SSH, no live cluster) must
be able to answer:

1. **Did the scenario reach the user-loop or fail at install?**
   - Check `events.kube-system.txt` for helm/install events vs
     `logs/blockmaster/pod.*.current.log` for runtime errors.
2. **What's the per-volume status?**
   - Read `product/report/summary.txt` if present; otherwise
     `product/unreachable.txt` explains why and `events.default.txt` shows
     PVC events.
3. **Is there a stable reason code?**
   - `product/explain.txt` line per blocked volume with `reason=<code>`.
4. **Which Kubernetes-layer signal triggered the blocker?**
   - `events.default.txt` for PVC/Pod events.
   - `logs/csi-node/pod.*.current.log` for staging/publish errors.
5. **Is the host clean or polluted?**
   - `host/<node>/iscsi-sessions.txt`, `multipath.txt`, `dmsetup.txt`,
     `processes.txt`, `hostpath.txt`.
6. **Did stale or partial product evidence make it into a Ready surface?**
   - Cross-check `product/report/summary.txt` `cleanup_status=` and
     `*_residue_count=` against `host/<node>/...` direct data.

## Acceptance Criteria for the Standard

A scenario emits a Phase-32-compliant failure bundle when:

- All sections above are populated (or the unreachable.txt explains why).
- Each file in the bundle is text or JSON (no binary, no proprietary
  format).
- File paths are stable across scenarios (so cold tooling can grep).
- Bundle root is under the documented SMB share path
  (`/v/share/g15d-k8s/<run-id>-<scenario>/...`).
- The bundle answers all 6 cold-reader checklist questions.

## Mapping to Today's Bundles

| Required content | Phase 28+ status |
|---|---|
| nodes/pods/deploys/services kubectl dumps | partial - scenarios that use the helper scripts capture these; pure runner-native scenarios don't yet |
| PVC/PV/StorageClass | partial |
| events (per ns, time-sorted) | gap - most failure bundles capture only some events, not the full `--sort-by` form |
| product Deployment logs (current) | yes |
| product Deployment logs (previous, after crash) | gap - rarely captured; `kubectl logs --previous` is not in current cleanup verifier |
| CSI sidecar logs (attacher / provisioner) | gap |
| iSCSI session + node DB | yes |
| multipath -ll + multipath -t config | partial - `-ll` yes, `-t` no |
| dmsetup ls + deps | partial - `ls` yes, `deps` no |
| host processes | yes |
| hostpath listing | yes |
| product report bundle | yes when reachable |
| product explain output | yes (in blocked-bundle scenarios) |
| unreachable.txt when product silent | gap - Phase 28 cycle 1 surfaced this; not yet a runner primitive |

## Recommended Implementation

The `collect_k8s_snapshot` runner action proposed in Workstream C should
emit exactly the structure above. Pair it with a "snapshot on failure"
hook in the runner so any failed scenario phase automatically captures the
bundle without requiring per-scenario boilerplate.

Pseudocode for the hook (existing runner already has `-artifacts` flag
shape):

```yaml
on_failure:
  - action: collect_k8s_snapshot
    namespaces: "default,kube-system"
    out: "{{ run_dir }}/failure-snapshot"
    include: ALL    # the full P0 list above
    if_unreachable: "record_unreachable_txt"
```

## Validation

When Phase 32 D4 (negative-status gates) is written, every blocked-path
scenario must produce a bundle that passes the cold-reader checklist
above. QA will spot-check by:

1. Take a failed run bundle.
2. Move it to a different machine with no lab access.
3. Try to answer the 6 questions purely from the bundle.

If any question requires going back to the lab, the bundle fails the
standard.
