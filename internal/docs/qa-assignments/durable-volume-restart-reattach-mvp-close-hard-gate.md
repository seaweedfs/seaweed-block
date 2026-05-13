# QA Assignment: Durable Volume Restart And Reattach MVP Close Gate

Status: ready for QA after dev gates:

- D4 restart gate: `20260512-211604-1339` PASS at `e90ce49`.
- D5 failure gate: `20260512-214412-860a` PASS at `aae2e53`.

Branch: `docs/post-merge-plan`.

## Product Claim Under Test

On the supported single-node alpha Kubernetes path, a user can configure a
durable host path and verify that a generated RF=1 iSCSI `blockvolume` survives
its own workload restart with data still readable through the PVC.

This close gate must not expand the claim to node loss, multi-node scheduling,
live RF=2/RF=3 Kubernetes operation, rebuild, failover while mounted,
production durability, backup/restore, upgrade safety, performance, or UI.

## Required Runs

Use a clean m02 single-node k3s lab or an equivalent lab that passes
`scripts/preflight-k8s-alpha.sh --local-k3s`.

Run the restart gate:

```bash
swblock run \
  -env product_root=/tmp/seaweed-block-durable-restart-qa \
  -env ssh_key=C:/work/dev_server/testdev_key \
  testops/scenarios/csi-rf1-durable-restart-chain.yaml
```

Run the failure gate:

```bash
swblock run \
  -env product_root=/tmp/seaweed-block-durable-restart-qa \
  -env ssh_key=C:/work/dev_server/testdev_key \
  testops/scenarios/csi-rf1-durable-restart-failure-chain.yaml
```

QA may use the Windows `swblock.exe` or a Linux `swblock` binary. If the runner
is launched from m02, use the m02 self-SSH key and equivalent `product_root`.

## Hard Gate Clauses

### HG-0: Runbook Entry

Pass: `docs/operations-v1.md` contains a dedicated durable restart section that
shows:

- `SW_BLOCK_LAUNCHER_STATE_HOSTPATH`,
- `scripts/run-k8s-blockvolume-restart.sh`,
- expected final PASS line,
- durable status checks for `Latched=true` and `Operational=true`,
- inventory collection after restart,
- cleanup/retained-state wording.

Fail: the restart path is only discoverable from internal TestOps YAML or the
runbook implies production HA/node-loss durability.

### HG-1: Manifest Uses Durable HostPath

Pass: the restart gate artifact `generated-blockvolume.yaml` shows:

- `hostPath:`,
- `type: DirectoryOrCreate`,
- `--durable-root=/var/lib/sw-block/<volume>/<replica>`,
- no `emptyDir:` under the generated state volume.

Fail: generated state still uses `emptyDir` or durable-root is absent.

### HG-2: PVC Data Survives Generated Blockvolume Restart

Pass: the restart gate shows:

- writer checksum succeeded before restart,
- `blockvolume-pod-ids.before-restart.tsv` and
  `blockvolume-pod-ids.after-restart.tsv` are both non-empty and different,
- `restart-blockvolume-status.log` reports successful rollout,
- reader checksum `/data/demo.bin: OK` succeeded after restart.

Fail: the proof bypasses Kubernetes PVC attach/read or only checks a local file.

### HG-3: Durable Status Evidence

Pass: `status-durable-after-blockvolume-restart.json` includes:

- `ReplicaID` for `r1`,
- non-zero `Epoch`,
- `Latched: true`,
- `Operational: true`.

Fail: the restart gate passes without durable status evidence or with
contradictory durable fields.

### HG-4: Inventory And Nested Support Bundle

Pass: `ops-inventory-after-restart/volume-inventory-summary.txt` maps the live
PVC to the generated workload and shows:

- `pvc=sw-block-demo-pvc`,
- `protocols=iscsi`,
- `lifecycle_owner=pvc-owner-ref`,
- `owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc`,
- `support_bundle=volumes/<volume>/<replica>`.

Pass also requires
`ops-inventory-after-restart/nested-ops-status-bundles.json` to contain
`"command": "sw-block ops status"` and a nested
`volume-status-summary.txt` with a `durable_entry:` line containing
`latched=true` and `operational=true`.

Fail: inventory is missing, cannot map PVC to support bundle, or contradicts
the durable status.

### HG-5: Cleanup And Retained-State Boundary

Pass: after the restart gate:

- no active Seaweed Block iSCSI sessions,
- no `blockmaster`, `blockvolume`, `blockcsi`, or `iscsi-target` processes,
- no generated `sw-blockvolume` Deployment,
- the run-scoped `/var/lib/sw-block/testops-<run>` durable hostPath is gone.

Fail: cleanup requires a broad manual sweep not documented as a TestOps
guardrail, or durable data retention/removal is ambiguous.

### HG-6: Failure Bundle For Bad Durable HostPath

Pass: the failure gate intentionally uses an unwritable durable hostPath and
still exits scenario PASS because it proves the failure is diagnosed. Required
evidence:

- user workflow exit code is non-zero,
- `run.log` records `launcher_state_hostpath=/proc/sw-block-testops-<run>`,
- `ops-inventory-on-failure/volume-inventory-summary.txt` has
  `inventory_status: unhealthy`,
- the summary still contains a `pvc=sw-block-demo-pvc` row,
- the summary contains at least one actionable issue such as
  `generated_deployment_missing`, `observed_replicas=0 desired_replicas=1`,
  `replica_degraded=r1 status=unhealthy`, or `ops_status=unhealthy`,
- `ops-inventory-on-failure/ops-inventory-bundle.json` says
  `"command": "sw-block ops inventory"`,
- the summary records `collection_error: ops_status` when the nested replica
  status endpoint cannot be reached.

Fail: the bad-hostPath run only times out, cleans everything without a bundle,
or reports a false `inventory_status: ok`.

### HG-7: Non-Claims Remain Honest

Pass: `docs/operations-v1.md`, `docs/quickstart-kubernetes.md`, and
`internal/docs/current-plan.md` keep the claim limited to single-node RF=1
generated blockvolume workload restart. They must explicitly avoid claiming:

- node loss,
- host-disk failure,
- multi-node scheduling,
- live RF=2/RF=3 Kubernetes lifecycle,
- rebuild/reintegration,
- backup/restore,
- upgrade/uninstall safety,
- performance SLO,
- UI.

Fail: any user-facing doc implies this is production HA or node-loss durability.

## Close Report Template

QA should write:

```text
QA Close — Durable Volume Restart And Reattach MVP

Verdict: PASS|FAIL
Product commit:
Runner commit:
Host/lab:

HG-0 runbook entry: PASS|FAIL
HG-1 durable hostPath manifest: PASS|FAIL
HG-2 PVC data survives blockvolume restart: PASS|FAIL
HG-3 durable status evidence: PASS|FAIL
HG-4 inventory/support bundle: PASS|FAIL
HG-5 cleanup boundary: PASS|FAIL
HG-6 bad-hostPath failure bundle: PASS|FAIL
HG-7 non-claims honest: PASS|FAIL

Restart run id:
Failure run id:
Residue audit:
Blocking findings:
Non-blocking findings:
```

Any single hard-gate `FAIL` blocks plan close.
