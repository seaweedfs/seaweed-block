# Phase 166 NVMe/RDMA Kubernetes Multipath Status

Status: **HOLD - implementation complete, live close gate infrastructure-blocked**

Phase 166 requires an already-mounted RF2 Kubernetes volume to have two live
RDMA controllers, keep I/O running while one non-primary target is unavailable,
reconnect that exact RDMA path, and replace one desired endpoint without
restaging. No TCP controller may satisfy an RDMA path.

## Implemented Contract

- CSI matches a desired path by exact `(transport, NQN, address)` and accepts
  only controllers whose state is `live`.
- Initial stage and mounted reconcile connect missing desired endpoints before
  pruning stale controllers.
- Initial-stage rollback is scoped to controllers created by that attempt; it
  does not disconnect pre-existing or unrelated controllers.
- A mounted volume rejects a transport change and requires restage instead of
  silently mixing TCP and RDMA.
- Publish context carries explicit `rdma` intent while preserving legacy TCP
  behavior when the transport field is absent.
- The close gate requires a non-primary outage, surviving-path mounted I/O,
  exact path restoration, endpoint replacement, cross-surface agreement, no
  TCP fallback, and scoped zero-residue cleanup.

## Evidence

| Check | Result | Evidence |
| --- | --- | --- |
| Source and script contracts | PASS | `go test ./core/csi ./cmd/blockcsi ./core/ops ./core/launcher ./scripts -count=1`; changed shell scripts pass `bash -n`; Phase 133 and 166 scenarios validate. |
| Kubernetes single-path RDMA regression | PASS | Phase 165 run `20260718-054945-77d5`: transport `rdma`, host controller `10.0.0.3`, writer/reader verified, no TCP fallback, clean detach and zero residue. |
| Shared RF2 reconnect logic over NVMe/TCP | PASS | Phase 133 run `20260718-061457-5ed4`, 35/35 actions: two initial paths; non-primary endpoint `192.168.1.184:4420` replaced by `192.168.1.184:4520`; surviving `192.168.1.181:4420` controller preserved; mounted I/O and Pod UID preserved; old path absent; CRD/report/dashboard/explain agree; cleanup clean. |
| RF2 RDMA mounted reconnect close gate | BLOCKED | The current lab has only two RoCE-capable nodes, m01 (`10.0.0.1`) and m02 (`10.0.0.3`), and both must host the RF2 targets. The initiator therefore shares a node with one target; Linux rejects that local RDMA self-connect with `Invalid argument`. tp01 is offline and is not a RoCE-capable replacement. |

The Phase 133 regression asserts stable behavior, not observation of a transient
stale controller. A valid result is that the old path is already absent when
sampled, provided the new desired path is live, the surviving controller is
unchanged, the final exact path set is correct, mounted I/O is continuous, and
`stale_old_path_pruned=true`.

## Unblock Requirement

Provide a third Ready Kubernetes node with working RoCE configuration and
reachability to both m01 and m02 RDMA frontend addresses. Both RF2 targets must
be remote from the initiator. Then run:

```powershell
C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results `
  -env app_node=<third-kubernetes-node> `
  -env app_ssh_addr=<third-node-management-ip> `
  C:\work\seaweed_block\testops\scenarios\nvme-rdma-k8s-multipath-reconnect-chain.yaml
```

Phase 166 closes only after that live gate emits
`phase166_nvme_rdma_k8s_multipath_reconnect_status=ok` and
`cleanup_status=ok`. Component regressions do not substitute for this result.
