# Phase 164 QA Sign-Off: NVMe/RDMA Standalone Hardening

Status: **PASS** on 2026-07-18.

TestOps run: `20260718-015204-af29`.

Local bundle: `results/20260718-015204-af29`.

## Verdict

The supported Linux lab passed the complete standalone hardening gate against
the real Seaweed backend over the m01-to-m02 RoCE path. The gate used standard
Linux `nvme connect -t rdma`; it did not substitute a TCP client or test only
the kernel target.

## Gates

- Stable preflight refusal and non-root refusal: PASS.
- Partial NBD/configfs startup failure rollback: PASS.
- Live RDMA port conflict rolls back only the failed target: PASS.
- Aligned 4 KiB plus 1 MiB write/read checksums: PASS.
- FUA and explicit NVMe flush reaching durable sync evidence: PASS.
- Graceful stop, same-root restart, reconnect, and old-data readback: PASS.
- Two concurrent NQN/port/namespace/NBD targets with isolated data: PASS.
- Five bounded connect/read/disconnect cycles: PASS.
- Capability restart honesty and existing NVMe/TCP tests: PASS.
- Host controller, configfs, NBD, process, and workdir cleanup: PASS.

The first diagnostic run exposed a host namespace-settle race before the
second target write. The final gate independently verifies both host block
devices and their expected capacity before I/O; the formal TestOps rerun then
passed 24/24.

The repository-wide Go suite has one pre-existing unrelated failure:
`TestT2Process_ISCSI_ReopenAfterMove_ServesNewLineage` does not return to
Healthy within 30 seconds. The same iSCSI failure was recorded before Phase
164; all targeted NBD, NVMe/RDMA, NVMe/TCP, and blockvolume tests pass.

## Boundary

This sign-off supports a source-gated, standalone Linux NVMe/RDMA path. It does
not validate Kubernetes publication, CSI stage/unstage, mounted workload I/O,
failover, multipath, performance improvement, broad kernel/RNIC compatibility,
or a production SLO.
