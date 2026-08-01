# Phase 174 D2 Distinct-Node RF3 QA Sign-Off

Verdict: **PASS for distinct-node RF3 correctness and attribution; no
architecture candidate selected.**

## Formal Run

- source: `7d75a47542a949963fd196b3635e99291011cd56`
- primary: m02, `/data/nvme/block` on `/dev/nvme0n1p1`
- replicas: m01 and tp01
- transport: real TCP over the `192.168.1.x` management LAN
- artifact:
  `/mnt/smb/work/share/g15d-k8s/20260801T102712Z-phase174-d2-rf3-distinct.tar.gz`
- SHA256:
  `fc2a220e0183fcb77ea21473ec63cef9ecf4b17ce776f43db3d39389dba48bfb`

The fixed work was 16,384 deterministic 4 KiB writes per measured run. The
foreground ACK profile remained `sync_quorum_rf3`. Probe, recovery, and the
final SyncAll were outside the measured foreground interval.

| Writers | Median MiB/s | Max/min | Queue-terminal rows | Rebuilds |
|---:|---:|---:|---:|---:|
| 1 | 57.819 | 1.598 | 5/5 | 5 |
| 4 | 13.054 | 1.010 | 0/5 | 2 |
| 8 | 15.023 | 1.127 | 0/5 | 1 |

Required evidence passed:

- 15/15 primary rows and 6/6 independently reopened remote WALStores;
- 30 live probes, two per row;
- 8 full rebuilds selected by the existing `R < S` rule;
- every remote store recovered `stable_lsn=head_lsn=104448` and verified
  final bytes;
- foreground sync-quorum, post-measurement recovery, and final SyncAll all
  passed;
- remote process/store residue count was zero.

## Findings

The first real run exposed a product defect: queue saturation cancelled old
work but immediately installed a fresh steady queue before recovery closed the
LSN gap. `deebcf9` now permits queue replacement only while the peer is
positively `Healthy`; saturation remains terminal until the recovery handoff.
The queue/recovery race tests passed 20 iterations under the Linux race
detector.

An intermediate run correctly returned typed `WALRecycled`: the replica was at
R=1024 while the primary tail/checkpoint had reached S=17408. The final gate
uses the engine's existing classification (`R < S` means rebuild, otherwise
catch up) instead of treating SyncAll as recovery.

No catch-up occurred because this WALStore configuration reports retention
zero and every observed lag was outside the retained window. Distinct-node
catch-up with an explicit retained window remains a D6 recovery gate; this D2
run does not claim it.

The one-writer shape is unstable (`1.598x`, above `1.25x`), and the transport is
the management LAN rather than 100 GbE. The four/eight-writer results are
attribution data only. No throughput ratio can authorize D4/D5 implementation.

## Attempt Record

- `d1ef486`: failed before workload due stale helper listeners from an earlier
  failed gate; failure cleanup now stops scoped helper PIDs before removing
  their directories.
- `d1ef486`: reached recovery and failed honestly with typed `WALRecycled`
  because the test helper fixed every lag to catch-up.
- `7d75a47`: PASS after using the existing R/S/H recovery classification.

