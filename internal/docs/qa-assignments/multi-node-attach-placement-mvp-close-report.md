# QA Close — Multi-Node Attach And Placement MVP

Formal close report against
`internal/docs/qa-assignments/multi-node-attach-placement-mvp-close-hard-gate.md`.

```text
Verdict:         PASS (strict) — re-issued after dev's sed fix at 7993e08
Product commit:  7993e08 on docs/post-merge-plan
Runner commit:   sw-test-runner-standalone @ 6ec7abd (swblock 15.9 MB Windows)
Host/lab:        m02 (192.168.1.184) — Ubuntu 24.04.3 LTS / k3s v1.34.4+k3s1

Happy run:       20260512-235006-2e98  state=pass  phases=6/6  actions=47/47
Negative run:    20260512-232353-60ea  state=pass  phases=5/5  actions=34/34

Prior verdict at 97a080d (superseded): FAIL (HG-4) due to scenario sed bug.
Fixed by dev at 7993e08 (anchored APP_NODE extraction to "[app-demo] app_node=").
This report reflects the re-run at 7993e08.
```

## HG clause table

```text
HG-0 documentation entry                  PASS
HG-1 contract and audit present           PASS
HG-2 app node pinning default             PASS
HG-3 blockvolume placement evidence       PASS
HG-4 functional attach                    PASS  (scenario state=pass after sed fix; see re-run note)
HG-5 inventory explains ownership         PASS
HG-6 negative fixture actionable          PASS
HG-7 cleanup hygiene                      PASS
HG-8 fast tests pin contract              PASS
HG-9 non-claims honest                    PASS
```

## Evidence

### HG-0 documentation entry — PASS

```text
docs/operations-v1.md           : 2 same-node references; line 32 explicitly disclaims "remote-node attach to a loopback-published blockvolume"
docs/quickstart-kubernetes.md   : 2 same-node references; line 489 explicitly disclaims "Remote-node attach to a loopback-published blockvolume is not claimed"
```

### HG-1 contract and audit present — PASS

```text
internal/docs/ref/multi-node-attach-placement-audit.md         present
internal/docs/ref/same-node-alpha-placement-contract.md        present
```

### HG-2 app node pinning default — PASS

From happy-gate `demo/run.log`:

```text
[app-demo] app_node=m02
[app-demo] pin_app_node=1
```

From `demo/demo-app.rendered.yaml`:

```yaml
nodeSelector:
  kubernetes.io/hostname: m02
```

From `demo/demo-app-reader.rendered.yaml`:

```yaml
nodeSelector:
  kubernetes.io/hostname: m02
```

All four required signals present.

### HG-3 blockvolume placement evidence — PASS

From happy-gate `demo/generated-blockvolume.yaml`:

```yaml
kubernetes.io/hostname: m02
- --iscsi-listen=127.0.0.1:3260
```

App node from run.log (`m02`) matches inventory replica `node=m02`:

```text
replica: volume=pvc-99ab4458-... replica=r1 server=m02 node=m02 ... frontend=127.0.0.1:3260 ...
```

Substantive alignment confirmed.

### HG-4 functional attach — PASS

All four gate bullets satisfied in QA-owned re-run `20260512-235006-2e98`:

```text
Scenario state                                             : pass (6/6 phases, 47/47 actions)
writer.log "wrote and verified /data/demo.bin"             : 1 match
reader.log "verified persisted /data/demo.bin"             : 1 match
run.log "controlled stop after reader verified"            : 1 match
demo/ops-inventory-reader-verified/* artifacts             : present
```

The prior run at 97a080d failed this clause because of a scenario
assertion-script sed bug (the regex `s/.*app_node=//p` matched both
`app_node=m02` and `pin_app_node=1`, and `tail -1` picked `1`). Dev fixed
the regex at 7993e08 by anchoring extraction to `^.*] app_node=`. This
re-run hits the corrected assertion and the scenario reaches state=pass.

### HG-5 inventory explains ownership — PASS

From happy-gate `demo/ops-inventory-reader-verified/volume-inventory-summary.txt`:

```text
inventory_status: ok
volumes: total=1 ok=1 unhealthy=0 invalid=0
volume: id=pvc-99ab4458-... namespace=default pvc=sw-block-demo-pvc pv=pvc-99ab4458-...
        rf=1 desired=1 observed=1 primary=r1 status=ok protocols=iscsi replicas=1
replica: ... server=m02 node=m02 ... lifecycle_owner=pvc-owner-ref
         owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc
         frontend=127.0.0.1:3260 status_addr=127.0.0.1:23260
         support_bundle=volumes/pvc-99ab4458-.../r1
issues: none
```

All required fields present:

```text
pvc=sw-block-demo-pvc                              : 1
node=m02                                           : 1
frontend=127.0.0.1:3260                            : 1
support_bundle=volumes/pvc-99ab4458-.../r1         : 1
nested-ops-status-bundles.json "command": "sw-block ops status"  : 1
```

### HG-6 negative fixture actionable — PASS

Negative scenario reached state=pass (5/5 phases / 34/34 actions). Demo exit
code 45 (gate-required). `demo/unsupported-cross-node-loopback-attach.txt`:

```text
issue=unsupported_cross_node_loopback_attach
app_node=sw-block-not-the-blockvolume-node
blockvolume_node=m02
frontend=127.0.0.1:3260
volume_id=pvc-244bb914-7cfe-4af2-b888-1b0fe5758ab3
replica_id=r1
reason=loopback frontend requires app pod and blockvolume on the same node
ops_inventory_dir=/mnt/.../demo/ops-inventory-unsupported-placement
```

All gate-required keys present with the exact shape. `run.log` has the
matching line: `[app-demo] unsupported cross-node loopback attach:
app_node=sw-block-not-the-blockvolume-node blockvolume_node=m02
frontend=127.0.0.1:3260`.

Inventory bundle at
`demo/ops-inventory-unsupported-placement/volume-inventory-summary.txt`
exists with `pvc=sw-block-demo-pvc` row present (the residual from the prior
demo PVC), plus a `pvc=unavailable` row showing the unplaced workload with
`orphan-blockvolume-deploy=...`, `heartbeat-without-placement=m02
state=unadmitted-by-master`, `replica_degraded=r1 status=unhealthy`. The
inventory honestly classifies the unsupported placement as residue.

### HG-7 cleanup hygiene — PASS

After both gates:

```text
iSCSI sessions:                                  No active sessions
iSCSI nodes DB:                                  No records found
blockmaster/blockvolume/blockcsi/iscsi-target:   none
kubectl port-forward svc/blockmaster:            none
app=sw-blockvolume Deployments:                  No resources found
```

### HG-8 fast tests pin contract — PASS

```text
$ go test ./core/launcher ./core/ops -count=1
ok  github.com/seaweedfs/seaweed-block/core/launcher    0.008s
ok  github.com/seaweedfs/seaweed-block/core/ops         0.034s
```

Contract-specific tests confirmed:

```text
core/launcher/k8s_renderer_test.go    : TestG15d_K8sRenderer_SameNodeLoopbackPlacementContract
core/ops/volume_inventory_test.go     : TestBuildVolumeInventory_SameNodeAttachEvidenceIsVisible
```

### HG-9 non-claims honest — PASS

`docs/operations-v1.md` "Scope And Non-Claims" lists explicitly:

```text
Not claimed:
- production HA,
- node loss or host-disk failure,
- remote-node attach to a loopback-published blockvolume,
- automatic multi-node scheduling, rescheduling, or rebalancing,
- live RF=2/RF=3 Kubernetes lifecycle,
- upgrade or broad uninstall safety,
- repair, rebuild, promote, backup, or restore commands,
- performance SLOs,
- UI or operator-grade reconciliation.
```

`docs/quickstart-kubernetes.md` parallels these: "Remote-node attach to a
loopback-published `blockvolume` is not claimed", "Failover while a PVC
remains mounted is not claimed", "NVMe-oF is not part of this alpha path",
"Operator-grade reconciliation is not claimed", "Upgrade and uninstall
safety are not claimed", "Performance numbers from this demo are not a
product SLO".

All eight HG-9 non-claim properties present in user-facing docs.

## Residue audit

See HG-7 above. Lab fully clean after both gates.

## Blocking findings

None. The prior blocker (HG-4 sed-regex bug in
`testops/scenarios/same-node-alpha-attach-chain.yaml` phase
`same_node_asserts` action 12) was fixed by dev at commit 7993e08:
extraction anchored to `^.*] app_node=` so `pin_app_node=1` no longer
collides with `app_node=m02`. QA re-ran the happy gate at 7993e08 and
the scenario now reaches state=pass with all assertions green.

## Non-blocking findings

None beyond the blocking item.

## Close recommendation

```text
PASS (strict) — the plan is clear to move from current-plan.md to
finished-plans/.
```

The product-side claim — same-node RF=1 attach with app pods and the
generated blockvolume pinned to the same Kubernetes node, normal CSI
attach + write + read, inventory explaining node + frontend +
support-bundle ownership, negative path producing an actionable
unsupported-cross-node bundle, and explicit non-claims for remote-node
attach / multi-node scheduling / RF=2/3 live — is fully demonstrated by
the artifacts. All 10 HG clauses PASS with concrete live evidence.
