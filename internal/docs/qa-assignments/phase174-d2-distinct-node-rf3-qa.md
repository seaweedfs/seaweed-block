# Phase 174 D2 Distinct-Node RF3 QA

Run this gate from Windows Git Bash. It cross-compiles one test-only binary,
uses m02 as primary, and runs persistent replica listeners on m01 and tp01.
Each replica writes to its own host's NVMe filesystem. The transport is the
shared `192.168.1.x` management LAN because tp01 has no RoCE interface; this is
not a 100 GbE performance claim.

```bash
cd /c/work/seaweed_block
SW_BLOCK_PHASE174_SSH_KEY=/c/work/dev_server/testdev_key \
  bash scripts/run-phase174-distinct-node-rf3-gate.sh "$PWD"
```

If the workstation Go toolchain is older than the module requirement, build
the exact assignment commit's Linux test binary on a Go 1.25 host and pass it
as `SW_BLOCK_PHASE174_PREBUILT_BINARY`. The gate copies but never modifies the
provided binary.

## Contract

- 16,384 deterministic 4 KiB writes and 64 MiB per measured run;
- 1, 4, and 8 writers, five runs each, after a persistent precondition run;
- foreground ACK remains `sync_quorum_rf3`;
- after the measured Sync, an explicitly excluded recovery phase probes each
  external replica, catches any lagging replica up from its durable `R+1`,
  or rebuilds it when `R < S` proves the WAL window has moved past it,
  completes the production-equivalent live-session handoff, and only then
  requires SyncAll;
- each remote process stops, reopens WALStore, recovers the exact expected
  frontier, and verifies final payload bytes independently;
- the primary also closes, reopens, recovers, and verifies bytes every run.

## Required Evidence

- `phase174_distinct_node_rf3_status=ok`;
- 15 primary result rows and six independently recovered remote results;
- `foreground_sync_quorum_preserved=true`;
- `post_measurement_recovery_verified=true`;
- `post_measurement_sync_all_verified=true`;
- `remote_replica_frontiers_and_bytes_equal=true`;
- `rf3_distinct_node_healthy=true`;
- peer queue saturation is reported, not hidden;
- every primary row reports two live probes; catch-up/rebuild counts and
  maximum LSN lag are preserved as evidence rather than inferred from the
  final barrier;
- no cross-ACK-profile throughput ratio, candidate selection, or product
  mutation;
- remote process/store cleanup leaves no gate residue.

This is D2 attribution/correctness evidence. It does not override the D1 local
HOLD and cannot authorize D4/D5 implementation.
