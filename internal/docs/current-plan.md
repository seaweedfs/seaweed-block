# Current Plan: Phase 105 Multi-Host NVMe/TCP Topology Boundary

Status: planned next.

## Why This Is Next

Phase 103 proved host transport preflight. Phase 104 proved the current product
is explicitly NVMe/TCP-only and must not claim RoCE/NVMe-RDMA.

The next useful NVMe feature is therefore not RoCE and not performance. It is
multi-host NVMe/TCP topology correctness:

```text
NVMe/TCP frontend address must be non-loopback for cross-node attach
CSI publish context must not hand a pod on node A a 127.0.0.1 target from node B
status must block unsafe topology instead of claiming Ready=True
```

This mirrors the earlier iSCSI loopback-cross-node blocker, but for NVMe/TCP.

## Product Goal

Add a topology boundary that distinguishes:

- same-node loopback NVMe/TCP attach: allowed under the supported lab path;
- cross-node loopback NVMe/TCP attach: blocked with a stable reason;
- cross-node non-loopback NVMe/TCP attach: eligible for a future live gate.

The intended reason code is:

```text
nvme_publish_target_loopback_cross_node
```

or a reused protocol-neutral reason if the existing model can carry it cleanly.

## D1: Model / Reason Contract

Define how ManagedVolume facts represent an NVMe publish target whose address is
loopback while the workload node differs from the blockvolume node.

Required local checks:

```text
go test ./core/ops ./cmd/sw-block -count=1
```

## D2: Report / Dashboard / Explain Agreement

The blocked state must appear consistently in:

- summary.txt;
- operator-snapshot.json;
- dashboard `/operator-snapshot.json`;
- `ops explain`;
- CRD status if the operator-status path consumes the same evidence.

No surface may show `Ready=True` for cross-node loopback NVMe/TCP.

## D3: TestOps Scenario

Add a scenario/gate that crafts or induces:

```text
protocol=nvme
publish_target=127.0.0.1:<port>
blockvolume_node=m02
workload_node=m01
```

Expected evidence:

```text
status=blocked
reason=nvme_publish_target_loopback_cross_node
ready_true_count=0
safe action is observe/read_only or dry_run
```

## D4: Live Follow-up

If the lab can schedule a real pod on a different node while the NVMe frontend is
loopback-bound, run the live negative gate. Otherwise close D3 as replay-only
with a clear live follow-up, the same way earlier host-prereq gates handled
environment-limited cases.

## Non-Claims

Phase 105 does not claim:

- multi-host NVMe/TCP attach works;
- RoCE/NVMe-RDMA works;
- performance or SLO;
- production HA.
