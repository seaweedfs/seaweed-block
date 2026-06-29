# Current Plan: Phase 106 NVMe/TCP Cross-Node Non-Loopback Live Attach

Status: closed. D1/D2 publish-contract gate passed on
2026-06-29 (`nvme-tcp-cross-node-publish-chain`, run
`20260629-005939-14c1`, 18/18 PASS). D3 live cross-node writer/reader passed
on 2026-06-29 (`nvme-tcp-cross-node-live-attach-chain`, run
`20260629-021338-5089`, 31/31 PASS) with a separate strict cleanup audit
returning `cleanup_status=ok`.

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

Status: closed for the publish-contract slice. Implemented:

```text
blockvolume default NVMe bind remains loopback-only
--allow-external-nvme-bind is required for non-loopback NVMe/TCP
blockmaster accepts --launcher-external-nvme
launcher renders --allow-external-nvme-bind and non-loopback --nvme-listen
Helm exposes network.externalNVMe and ports.nvmeBase
generate-helm-values --protocol nvme emits externalNVMe, not externalISCSI/CHAP
```

Verified by `scripts/run-phase106-nvme-tcp-cross-node-publish-gate.sh`.

## D2: CSI Publish / Stage Evidence

Confirm CSI publish context carries the non-loopback NVMe/TCP target and node
stage records the same NQN/NSID/address.

No status surface may claim Ready if publish context and stage evidence
disagree.

Status: closed. The chart and values produce the intended non-loopback NVMe
target configuration, and D3 proved the mounted cross-node workload sees the
same routable NVMe target through the managed-volume report.

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

Status: closed. Run `20260629-021338-5089` proved:

```text
phase106_nvme_tcp_cross_node_live_status=ok
blockvolume_node=m01
app_node=m02
publish_target=192.168.1.181:4420
publish_target_loopback=false
protocol=nvme
managed_volume_status=ready
managed_volume_reason=first_volume_verified
writer_verified=true
reader_verified=true
```

The live scenario uses already-built local images when present to stay within
the TestOps runner wall-clock budget. A strict cleanup audit was then run
outside the scenario against the same lab teardown artifacts and returned all
residue counts zero.

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

Status: covered by Phase 105 and retained as the negative counterpart. Phase
106 did not weaken the loopback-cross-node block.

## Non-Claims

Phase 106 still does not claim:

- RoCE/NVMe-RDMA;
- multi-path failover across real hosts;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA.
