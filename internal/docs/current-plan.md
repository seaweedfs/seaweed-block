# Current Plan: Phase 106 NVMe/TCP Cross-Node Non-Loopback Live Attach

Status: planned next.

## Why This Is Next

Phase 103 proved the host can support NVMe/TCP preflight. Phase 104 proved the
product must not claim RoCE/NVMe-RDMA. Phase 105 proved the topology boundary:
cross-node loopback NVMe/TCP evidence is blocked with
`publish_target_loopback_cross_node`, no false `Ready=True`, and no iSCSI-only
remediation.

The next useful NVMe feature is therefore the positive live path that Phase 105
explicitly did not claim:

```text
blockvolume publishes a non-loopback NVMe/TCP target on node B
CSI stages it for a workload on node A
writer/reader I/O succeeds
status surfaces Ready only after live evidence
```

## Product Goal

Make cross-node NVMe/TCP attach a real gated capability for the lab path, not a
model-only promise.

Required behavior:

- loopback target across nodes stays blocked by Phase 105;
- non-loopback target across nodes can be staged by CSI;
- the published NQN/NSID/address in master status, CSI publish context, node
  stage evidence, report, dashboard, and CRD status agree;
- app writer/reader verifies data through the mounted volume;
- cleanup leaves zero Kubernetes, NVMe, process, and host residue.

## D1: Publish Target Selection

Ensure NVMe frontend publication exposes a routable node address when the
workload may run on a different node. Do not regress the same-node loopback lab
path.

Required local checks:

```text
go test ./core/frontend/nvme ./core/ops ./cmd/blockvolume ./cmd/sw-block -count=1
```

## D2: CSI Publish / Stage Evidence

Confirm CSI publish context carries the non-loopback NVMe/TCP target and node
stage records the same NQN/NSID/address.

No status surface may claim Ready if publish context and stage evidence
disagree.

## D3: Live Cross-Node Writer / Reader Gate

Run a TestOps scenario that pins or schedules:

```text
blockvolume_node=m02
workload_node=m01
publish_target=<m02 routable IP>:4420
protocol=nvme
```

Expected evidence:

```text
writer_verified=true
reader_verified=true
status=ready
reason=first_volume_verified
nvme_target_loopback=false
ready_true_allowed_only_after_reader=true
```

## D4: Negative Regression

Keep the Phase 105 negative topology scenario in the suite:

```text
publish_target=127.0.0.1:4420
blockvolume_node=m02
workload_node=m01
status=blocked
reason=publish_target_loopback_cross_node
ready_true_count=0
```

## Non-Claims

Phase 106 still does not claim:

- RoCE/NVMe-RDMA;
- multi-path failover across real hosts;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA.
