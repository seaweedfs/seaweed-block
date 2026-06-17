# Phase 34 D2 - Live Status Endpoint Unreachable Sign-off

Status: PASS on 2026-05-29.

Source scenario:

- `testops/scenarios/status-endpoint-unreachable-live-chain.yaml`

Validation run:

- Run ID: `20260529-182955-3bd2`
- Result: PASS, 29/29 actions
- Runtime: 1m57.542s

## What This Gate Proves

This gate upgrades the Phase 33 replay-only status-unreachable check into live
fault injection.

The test flow:

```text
Helm install
-> first PVC writer/reader verifies Ready
-> discover live replica status endpoint from ops inventory
-> reject only the status endpoint port
-> collect live ops report/explain/dashboard
-> assert Ready=Unknown and EvidenceStale/status_endpoint_unreachable
-> assert not Ready=True and not Blocked=True
-> restore iptables rule
-> uninstall and verify zero residue
```

## Injection Evidence

Fault artifact:

```text
status_endpoint_blocked=true
status_addr=192.168.1.181:23260
status_host=192.168.1.181
status_port=23260
volume_id=pvc-0937fc3e-07fd-46bf-81ac-dec2f89b5a98
replica_id=r1
frontend_addr=192.168.1.181:3260
```

The injected rule only targeted the status port:

```text
iptables -I INPUT -p tcp --dport 23260 -j REJECT --reject-with tcp-reset
```

The iSCSI data path port `3260` was not blocked.

## Status Surface Evidence

Report summary:

```text
managed_volume=pvc-0937fc3e-07fd-46bf-81ac-dec2f89b5a98 status=unknown reason=status_endpoint_unreachable
managed_volume_condition=Ready status=Unknown reason=status_endpoint_unreachable severity=warning
managed_volume_condition=EvidenceStale status=True reason=status_endpoint_unreachable severity=warning
```

Operator snapshot and dashboard agreed:

- `read_only=true`
- `mutation_allowed=false`
- `status=unknown`
- `reason_code=status_endpoint_unreachable`
- `ready_volume_count=0`
- `blocked_volume_count=0`
- `stale_volume_count=1`
- no `Ready=True`
- no `Blocked=True`

This preserves the intended distinction:

```text
status endpoint unreachable -> Unknown / EvidenceStale
known product blocker       -> Blocked
```

## Cleanup

Cleanup summary:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Iterations

Two scenario bugs were found and fixed before the passing run:

- The runner `exec` shell uses `/bin/sh`; local inline scripts cannot use
  `set -o pipefail`.
- The live status endpoint address is present in
  `status/inventory/volume-inventory.json` as `status_address`, not in the
  product cluster evidence replica object.

Both fixes are scenario-side only.

## Verdict

D2 PASS.

The live status endpoint unreachable behavior is now covered at L2 realism:
real Helm install, real PVC, real status-port block, live ops collection, and
read-only user surfaces that refuse to claim readiness without evidence.
