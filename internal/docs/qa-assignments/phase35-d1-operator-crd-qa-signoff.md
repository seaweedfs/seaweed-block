# QA Sign-off - Phase 35 D1 Read-Only Operator CRD Contract

Verdict: **PASS.** The SwBlockCluster + SwBlockVolume CRDs and the
disabled-by-default status-only operator RBAC render cleanly, are accepted by
the live k3s API server (server-side dry-run), and hold the read-only contract:
status subresources present, full condition vocabulary, evidence refs, and no
storage-mutation RBAC. No findings.

Date: 2026-06-02

Source commit: `8332225 phase35: add read-only operator CRD contract`
(branch `phase33-testops-failure-hardening`)

## Checks

| Check | Result | Evidence |
|---|---|---|
| Unit tests (CRD contract enforcement) | PASS | `go test ./core/ops ./cmd/sw-block` — both ok |
| CRDs render with `--include-crds` | PASS | 18 objects, incl. 2 CustomResourceDefinitions |
| Live k3s API accepts CRDs + RBAC | PASS | `kubectl apply --dry-run=server` accepted all 18 objects, both CRDs validated server-side, no errors |
| Status subresource present | PASS | both CRDs: `subresources: status: {}` |
| Condition vocabulary complete | PASS | SwBlockVolume enum: `Ready, Recovered, Recovering, Blocked, CleanupRequired, EvidenceStale` |
| Evidence refs in schema | PASS | `status.conditions[].evidenceRefs` + top-level `status.evidenceRefs`; plus `reasonCode`/`reason` |
| RBAC read-only / no storage mutation | PASS | CRD resources `get/list/watch`; `.../status` `get/update/patch`; events `create`. No verbs on pods/pvc/deployments/storage |
| Disabled by default | PASS | default render (no `operatorStatus.create`) emits 0 operator-status objects; RBAC gated by `operatorStatus.create && rbac.create` |
| Nothing persisted (dry-run only) | PASS | post-run: no swblock CRDs, no operator-status ClusterRole on the cluster |

## Contract Detail

### CRDs (`charts/seaweed-block/crds/`)

```text
group: block.seaweedfs.com      (matches operator-snapshot.json crd_contract.group)
scope: Namespaced
version: v1alpha1 (served + storage)
subresources: status: {}
```

SwBlockVolume status: `status` enum (`ready/recovered/recovering/blocked/...`),
`reasonCode`, per-condition `type` (the 6-value vocabulary), `status`, `reason`,
`evidenceRefs`. This is the Kubernetes-native projection of the same
ManagedVolume model the operator-snapshot already exposes.

### RBAC (`templates/operator-status-rbac.yaml`)

```yaml
{{- if and .Values.operatorStatus.create .Values.operatorStatus.rbac.create }}
resources: ["swblockclusters", "swblockvolumes"]          verbs: [get, list, watch]
resources: ["swblockclusters/status", "swblockvolumes/status"]  verbs: [get, update, patch]
resources: ["events"]                                     verbs: [create]
```

The only writes are to the `/status` subresource (status publication) and
`events` (event emission). There are NO mutating verbs on storage resources
(pods, pvc, deployments) and NO `create/delete` on the CRDs themselves. This is
consistent with the read-only operator foundation: a status operator publishes
status + events, it does not mutate storage or lifecycle.

## Non-Claims Held

D1 adds the CRD shape and status RBAC only. It does NOT install a running
controller-manager, does not reconcile, and does not mutate storage. The
contract is the same read-only boundary the `operator-snapshot.json` already
honors, now expressed as Kubernetes-native CRDs. The CRDs are
disabled-by-default and require explicit `operatorStatus.create=true` to render.

## Lab State

Clean — the validation was server-side dry-run only; nothing was applied. No
swblock CRDs or operator-status RBAC persisted on the cluster.

## Bottom Line

- D1 read-only operator CRD contract: **PASS, no findings.**
- CRDs are schema-valid (k3s server accepts them), carry status subresource +
  full condition vocabulary + evidence refs, and the RBAC is read-only with no
  storage mutation.
- Disabled-by-default; opt-in via `operatorStatus.create=true`.
- D1 can close. Recommend QA re-validate live status publication (a running
  status reconciler writing real SwBlockVolume.status) when D2+ wires it — this
  D1 slice is contract + RBAC shape only, which is correct for the foundation.
