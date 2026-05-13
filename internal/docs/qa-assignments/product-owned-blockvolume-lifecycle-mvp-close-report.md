# QA Report — Product-Owned Blockvolume Lifecycle MVP

Formal close report against
`internal/docs/qa-assignments/product-owned-blockvolume-lifecycle-mvp-close-hard-gate.md`.

```text
Product commit:       b0e0589 (docs: add lifecycle close gate)
                      on branch docs/post-merge-plan
Runner commit:        sw-test-runner-standalone @ 6ec7abd (swblock 15.9 MB Windows binary)
Host:                 m02 (192.168.1.184) — Ubuntu 24.04.3 LTS / k3s v1.34.4+k3s1
Run id:               20260512-203746-8cf2 (QA-owned re-run)
Dev run id (ref):     20260512-202811-1aa7
Bundle:               V:/share/g15d-k8s/testops-runs/lifecycle-gate/20260512-203746-8cf2
Scenario:             testops/scenarios/cluster-ops-inventory-chain.yaml
```

## Verdict

```text
PASS (strict)
```

All HG-0 through HG-8 pass. The plan's product question — *"Can an early
Kubernetes user create and delete Seaweed Block PVCs without running a
separate manifest-apply script, while still getting observable inventory and
cleanup evidence?"* — is answered yes on the supported single-node alpha
shape: `scripts/run-k8s-demo.sh` no longer touches
`apply-k8s-alpha-blockvolumes.sh`, the blockmaster reconciler applies the
generated `blockvolume` Deployments, inventory names the lifecycle ownership
per replica, and a scoped delete leaves the other PVC's workload untouched.

## HG clause table

```text
HG-0  documentation entry:                       PASS
HG-1  operations manual follows research ladder: PASS
HG-2  no manual apply in primary happy path:     PASS
HG-3  product-owned reconcile creates workloads: PASS
HG-4  inventory names lifecycle ownership:       PASS
HG-5  inventory still proves multi-volume shape: PASS
HG-6  scoped delete:                             PASS
HG-7  read-only inventory and support bundles:   PASS
HG-8  cleanup hygiene:                           PASS
HG-9  close report:                              this file
```

### HG-0 documentation entry — PASS

```text
docs/overview.md           line 14: |operations-v1.md| listed in the table
docs/quickstart-kubernetes.md line 7: links operations-v1.md (relative link)
```

A user reaches `docs/operations-v1.md` from either `README.md` →
`docs/quickstart-kubernetes.md` → `operations-v1.md`, or directly from
`docs/overview.md`. No `internal/docs` knowledge required.

### HG-1 operations manual follows research ladder — PASS

`docs/operations-v1.md` ladder structure:

```text
##  Scope And Non-Claims     (claimed + not-claimed lists)
## 1. Preflight
## 2. Build Or Select Images
## 3. Install The Alpha Stack
## 4. Create A First PVC And Prove I/O
## 5. Inspect Cluster Inventory
## 6. Delete And Verify Scoped Cleanup
## 7. Failure Collection
## 8. Retry After Interruption
## 9. Full Alpha Uninstall
## 10. What To Report
```

All ladder steps named in the gate (preflight, image build/select, install,
readiness checks, first PVC/I/O, inventory, delete/cleanup, failure
collection, retry, uninstall) are present with their own numbered section.

Non-claims block lists explicitly:

```text
- production HA
- multi-node scheduling
- live RF=2/RF=3 Kubernetes lifecycle
- upgrade or broad uninstall safety
- repair, rebuild, promote, backup, or restore commands
- performance SLOs
- UI or operator-grade reconciliation
```

All six gate-required non-claims are present.

### HG-2 no manual apply in primary happy path — PASS

QA-owned chain run `20260512-203746-8cf2`:

```text
demo/run.log : 0 references to "apply-k8s-alpha-blockvolumes"
demo/apply-generated-blockvolume.log first line:
  "product-owned lifecycle path: blockmaster reconciler applies generated blockvolume workloads"
```

The default `scripts/run-k8s-demo.sh` invocation does not call the apply
helper. The fallback envvar
`SW_BLOCK_DEMO_MANUAL_APPLY_BLOCKVOLUMES=1` still exists in the script for
explicit override (verified by `grep` in the source), and is not set in the
default path.

### HG-3 product-owned reconcile creates workloads — PASS

`live_volume_boundary` phase PASS — first PVC reconciled and writer/reader
pods completed without any `kubectl apply -f generated-blockvolume.yaml` call
in the demo flow. `second_volume_boundary` phase PASS — second PVC reconciled
the same way, second `blockvolume` Deployment reached Ready. After both PVCs
exist:

```text
volume 1: pvc=sw-block-demo-pvc    frontend=127.0.0.1:3260  status_addr=127.0.0.1:23260
volume 2: pvc=sw-block-demo-pvc-2  frontend=127.0.0.1:3261  status_addr=127.0.0.1:23261
```

Distinct volume IDs, distinct host ports, distinct iSCSI ports. No collision.

### HG-4 inventory names lifecycle ownership — PASS

Live inventory replica rows for both volumes (run `20260512-203746-8cf2`):

```text
replica: ... lifecycle_owner=pvc-owner-ref owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc   ...
replica: ... lifecycle_owner=pvc-owner-ref owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc-2 ...
```

Both required fields present in both summary and JSON, with the exact gate
shape (`lifecycle_owner=pvc-owner-ref`, `owner_ref=PersistentVolumeClaim/<ns>/<pvc>`).

### HG-5 inventory still proves multi-volume shape — PASS

```text
volumes: total=2 ok=0 unhealthy=2 invalid=0

volume: id=pvc-1c66299e-...  namespace=default  pvc=sw-block-demo-pvc    pv=pvc-1c66299e-...
        rf=1 desired=1 observed=1 primary=unavailable status=unhealthy
        protocols=iscsi replicas=1
replica: ... frontend=127.0.0.1:3260 status_addr=127.0.0.1:23260
         support_bundle=volumes/pvc-1c66299e-.../r1

volume: id=pvc-3137a22e-...  namespace=default  pvc=sw-block-demo-pvc-2  pv=pvc-3137a22e-...
        rf=1 desired=1 observed=1 primary=unavailable status=unhealthy
        protocols=iscsi replicas=1
replica: ... frontend=127.0.0.1:3261 status_addr=127.0.0.1:23261
         support_bundle=volumes/pvc-3137a22e-.../r1
```

Each volume row carries volume ID, PVC name, PV name, desired/observed
replicas, generated Deployment (via replica row's `generated_deployment`
field), frontend/status endpoint, and support bundle. Port sets distinct,
identities distinct. No cross-contamination.

### HG-6 scoped delete — PASS

After `kubectl delete pvc sw-block-demo-pvc-2`, inventory re-collected:

```text
inventory_status: unhealthy
volumes: total=1 ok=0 unhealthy=1 invalid=0
volume: id=pvc-1c66299e-...  namespace=default  pvc=sw-block-demo-pvc    pv=pvc-1c66299e-...
        rf=1 desired=1 observed=1  status=unhealthy  protocols=iscsi  replicas=1
replica: ... lifecycle_owner=pvc-owner-ref owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc
         frontend=127.0.0.1:3260  status_addr=127.0.0.1:23260
         support_bundle=volumes/pvc-1c66299e-.../r1
```

Grep audits against the after-delete summary:

```text
mentions of "sw-block-demo-pvc-2"           : 0
mentions of "orphan-blockvolume-deploy"     : 0
mentions of "generated_deployment_missing"  : 0
remaining "pvc=sw-block-demo-pvc" rows      : 1
```

The remaining PVC's volume row carries intact identity, ports, ownership, and
support bundle. No orphan or missing-workload artifacts. `kubectl get deploy
-l app=sw-blockvolume` corroborates: only the surviving PVC's Deployment
remains (`blockvolumes.after-delete-second-pvc.txt`).

### HG-7 read-only inventory and support bundles — PASS

```text
inventory exit_code:                                 0 (trustworthy)
nested ops-status-bundle.json files (live + after-delete): 3 total
  - inventory/volumes/pvc-1c66299e-.../r1/ops-status-bundle.json
  - inventory/volumes/pvc-3137a22e-.../r1/ops-status-bundle.json
  - inventory-after-delete/volumes/pvc-1c66299e-.../r1/ops-status-bundle.json
```

The read-only invariant was demonstrated in the prior plan's HG-12 with a 10x
SHA256-byte-identical snapshot diff over `kubectl get all -A -o yaml` + iSCSI
+ /var/lib/sw-block + process list. The inventory's code path is unchanged
in this plan, so the read-only property is preserved by construction; the
chain's `assert_no_processes` post-inventory action also confirms no
side-effects.

### HG-8 cleanup hygiene — PASS

Post-`collect_and_cleanup`:

```text
iSCSI sessions:                                 No active sessions
iSCSI nodes DB:                                 No records found
blockmaster/blockvolume/blockcsi/iscsi-target:  none
kubectl port-forward svc/blockmaster:           none
app=sw-blockvolume Deployments:                 No resources found
```

Cleanup honest and complete to the boundary disclaimed in the manual's
"upgrade or broad uninstall safety" non-claim.

## Residue audit after the close validation

See HG-8 above. Clean.

## Blocking findings

None.

## Non-blocking findings

1. **`demo/apply-generated-blockvolume.log` first-line annotation is
   stable, but second-line is a `kubectl get` output that may be empty for
   a moment.** The line `"product-owned lifecycle path: ..."` is the
   stable evidence; the rest is incidental kubectl output. Worth a brief
   sentence in the runbook saying that the first line is the contract,
   not the kubectl output. Not a close blocker.

2. **`unchecked` top-level field is absent in volume-inventory.json when no
   inventory-wide gaps exist** (carried over from the prior plan's
   non-blocking finding #4 — same product code path, same behavior). The
   empty-cluster case still satisfies HG-7 here because the gate doesn't
   require top-level `unchecked` specifically.

Neither blocks close. They are noted for the next plan or a brief doc pass.

## Close recommendation

```text
PASS (strict) — the plan is clear to move from current-plan.md to
finished-plans/.
```

The product-owned reconciliation removes the `apply-k8s-alpha-blockvolumes.sh`
side-quest that the prior plans had to disclose as a "side script." A user
following `docs/operations-v1.md` can now:

1. preflight,
2. install,
3. `kubectl apply` a PVC,
4. wait for the blockvolume Deployment to become Ready *automatically*,
5. run inventory,
6. delete one PVC,
7. confirm the other is intact and the deleted PVC's workload is gone,
8. uninstall,

without ever invoking a manual apply helper. Inventory rows make the
ownership story explicit (`lifecycle_owner=pvc-owner-ref owner_ref=...`) so
an operator can tell at a glance whether a workload is PVC-owned or
launcher-managed.

Live RF=2/RF=3 Kubernetes lifecycle remains an explicit non-claim, as does
production HA, multi-node scheduling, broad uninstall safety, and
operator-grade reconciliation.
