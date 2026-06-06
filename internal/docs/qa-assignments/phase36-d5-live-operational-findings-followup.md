# Phase 36 D5 Live Operational Findings Follow-up

Date: 2026-06-05

Source QA sign-off:
`internal/docs/qa-assignments/phase36-d5-surface-agreement-qa-signoff.md`.

Phase 36 D5 passed all surface-agreement gates. These findings did not block
D5 because the CRD status, Events, report, dashboard, operator-snapshot, and
explain surfaces remained internally consistent. They are still real operational
follow-ups because they affect live user ergonomics and future node-readiness
accuracy.

## F1: Local CSI Image Import Evidence Gap

Severity: P0 follow-up for developer/local-image workflows.

Observed by QA:

- `sw-block-csi:local` was present on remote k3s nodes but absent from the build
  host's k3s containerd during a live D5 first-volume path.
- The m02 CSI node entered `Init:ErrImageNeverPull`.
- Consumer pods scheduled on m02 could not attach because `CSINode m02` did not
  contain driver `block.csi.seaweedfs.com`.
- The status surface still showed the node as ready, reinforcing the open D2
  live-node-evidence gap.

Current source note:

- `scripts/build-alpha-images.sh` already imports and verifies both
  `sw-block:local` and `sw-block-csi:local` into local k3s when
  `SW_BLOCK_IMPORT_K3S=1` and `SW_BLOCK_IMPORT_K3S_NODES` is set, provided
  `local_k3s_available` succeeds.
- Phase 36 added a regression test for that branch and a visible skip log when
  local k3s is unavailable.

Required follow-up:

```text
owner: dev/testops
artifact: build artifacts from a remote-node image-import run
gate: both k3s-images-local-sw-block.txt and
      k3s-images-local-sw-block-csi.txt exist and contain the expected image
      tags on the build host
fallback: if local_k3s_available is false, the artifact must contain
          k3s_import node=local skipped reason=local_k3s_unavailable
          and the scenario must not claim the build host image is ready
```

This should be paired with the D2 live-node-evidence follow-up: a missing CSI
image or unregistered CSI driver on any Kubernetes node must project as a node
blocker instead of being masked by generic `node_ready`.

## F2: Loopback Publish Target Is Single-Node Only

Severity: P1 documentation/default-values follow-up.

Observed by QA:

- The default chart value can use `internalIP=127.0.0.1`.
- That is valid only when the consuming pod is scheduled on the same node as the
  iSCSI target.
- A cross-node consumer sees `iscsiadm` connection refused against its own
  loopback address.

Required follow-up:

```text
owner: docs/chart
artifact: README/quickstart/chart values note
gate: loopback values are documented as single-node/local-consumer only
gate: multi-node values generated from Kubernetes node InternalIP use real node IPs
```

Do not turn this into an automatic mutating fix in Phase 36.

## F3: Force-Delete Can Leave iSCSI Node DB Residue

Severity: P1 operations follow-up.

Observed by QA:

- Force-deleting a pod with a mounted volume can bypass orderly CSI unstage.
- The real cleanup verifier detects stale `io.seaweedfs` iSCSI node DB records.
- Manual cleanup plus verifier returned the lab to `cleanup_status=ok`.

Required follow-up:

```text
owner: ops/docs/testops
artifact: cleanup verifier evidence and user troubleshooting note
gate: verifier keeps failing when stale iSCSI node DB records remain
gate: docs prefer graceful pod deletion and name the scripted cleanup path
```

This is visibility and scripted cleanup, not an in-cluster automatic cleanup
claim.
