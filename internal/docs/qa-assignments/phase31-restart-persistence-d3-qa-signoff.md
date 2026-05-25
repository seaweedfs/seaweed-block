# QA Sign-off - Phase 31 D3 Helm Single-Node Restart Persistence

Verdict: **PASS**

Date: 2026-05-25

Validated source commit: `656e40d testops: add single-node restart persistence gate`
Branch carries the new restart-persistence commits:
- `72e32b6 docs: start phase31 restart persistence`
- `63a74fb docs: add phase31 restart claim checklist`
- `d788d93 helm: add restart persistence values mode`
- `656e40d testops: add single-node restart persistence gate`

## Scope

Independent QA replay of Phase 31 D3 only (RF=1 / single Kubernetes node /
hostPath restart persistence / pod recreate after restart). This sign-off is
not a Phase 27/29 multi-volume HA validation; D4 (RF3 promotion + restart) is
explicitly out of scope.

## Run Summary

| Scenario | QA run ID | Result | Dev baseline |
|---|---:|---|---|
| `helm-single-node-restart-persistence-chain.yaml` | `20260525-104016-f3d4` | 40/40 PASS | `20260525-103441-2e9c` (39/39) |

QA run scored 40/40 versus dev's 39/39 — scenario likely gained one
assertion between dev's run and the QA sync; both runs PASS strict.

Lab pre-state confirmed clean: no helm release, no iSCSI sessions, no
multipath, no dmsetup, no sw-block pods on m02.

## Hard-Claim Compliance

### Helm hostPath mode emitted

`values/values.restart.yaml`:

```yaml
restartPersistence:
    stateHostPath: /var/lib/sw-block/testops-20260525-104016-f3d4-restart-persistence
```

Per-run scoped hostpath, not a shared `/var/lib/sw-block/...` namespace.

### Writer/reader verified BEFORE restart

`basic-app/first-volume-summary.txt`:

```text
first_volume_status=ok
install_mode=helm-single-node-restart-persistence
writer_verified=true
reader_verified=true
```

`basic-app/reader.log`: `/data/demo.bin: OK`

### k3s/product restart actually happened

`restart/restart-start.txt`: `Mon May 25 05:41:21 PM UTC 2026`. Restart phase
ran inside the scenario.

### Same PVC data verified AFTER restart

This is the core claim. Verified by ID match:

- pre-restart: `pv=pvc-1c9c9113-a515-4067-8272-423c68bb5b9e`
  (writer + reader OK)
- post-restart `after-restart/reader-after-restart.log`:
  `/data/demo.bin: OK`
- post-restart report:
  `volume=pvc-1c9c9113-a515-4067-8272-423c68bb5b9e status=ok
   pvc=default/sw-block-example-pvc primary=r1@m02 rf=1`

Same volume ID, same PVC name, reader still OK across the restart boundary.

### ManagedVolume reaches Ready after restart

`after-restart/report/summary.txt`:

```text
managed_volume=pvc-1c9c9113-... status=ready reason=first_volume_verified
managed_volume_condition=Ready status=True reason=first_volume_verified severity=info
managed_volume_action=observe.collect_bundle mode=read_only side_effect=observe executor=ops
```

ManagedVolume Ready condition holds post-restart. Action stays within the
Phase 30 D2 contract boundary (`mode=read_only`).

### Cleanup clean

`cleanup/verify/cleanup-summary.txt`: `cleanup_status=ok`.

Post-run direct host audit on m02:

```text
helm release sw-block:           none
iscsiadm sessions:               No active sessions
multipath -ll:                   empty
dmsetup ls:                      No devices found
sw-block pods/deploys:           none
per-run hostpath
  /var/lib/sw-block/testops-20260525-104016-f3d4-restart-persistence:
                                 absent (cleaned)
```

## Hard-Gate Acceptance

| Requirement | Result |
|---|---|
| Scenario PASS strict | PASS (40/40) |
| `restartPersistence: hostpath` emitted in values | PASS |
| `stateHostPath` is per-run scoped | PASS |
| Writer/reader verified pre-restart | PASS |
| k3s/product restart phase executed | PASS |
| Same PVC + same PV identity post-restart | PASS |
| Reader checksum holds post-restart | PASS (`/data/demo.bin: OK`) |
| ManagedVolume Ready post-restart | PASS |
| Per-run hostpath cleaned after teardown | PASS |
| Lab residue (helm, iSCSI, multipath, dmsetup, pods) clean | PASS |

## Blocking Findings

**None.**

## Non-Blocking Findings

### N1: Pre-existing orphan `/var/lib/sw-block/pvc-*` dirs on m02

`ls /var/lib/sw-block/` on m02 shows 5 leftover `pvc-*` directories plus one
`.blk` file, all dated `May 9` (well before Phase 31). These are orphans
from much earlier alpha-script runs (pre-Phase-30) at the legacy non-scoped
hostpath. The Phase 31 D3 run did NOT add to this list - it used and cleaned
the per-run scoped `testops-<run-id>-restart-persistence` directory.

Not a Phase 31 regression. Worth a one-time lab-cleanup pass before nightly
or release validation runs so the lab matches "deterministic clean state"
expectations:

```bash
sudo rm -rf /var/lib/sw-block/pvc-* /var/lib/sw-block/*.blk
```

(The new Phase 31 scoped path scheme prevents this kind of orphan from
recurring.)

## Verdict

Phase 31 D3 sign-off **PASS**. The new Helm restart-persistence values mode
+ state-permissions initContainer combine to give a deterministic
"restart -> same PVC data" claim on the single-node alpha path.

D4 (RF3 promotion + restart, no old-primary-resurrection, epoch/publish
target monotonicity) remains a separate, larger QA cycle.
