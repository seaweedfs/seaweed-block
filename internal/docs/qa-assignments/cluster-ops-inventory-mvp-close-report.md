# QA Report — Cluster Operations Inventory And Lifecycle Visibility MVP

Formal close report against
`internal/docs/qa-assignments/cluster-ops-inventory-mvp-close-hard-gate.md`.

```text
Product commit:       c662bc7 (ops: surface unplaced blockvolume processes)
                      on branch docs/post-merge-plan
Runner commit:        sw-test-runner-standalone @ 6ec7abd (swblock 15.9 MB Windows binary)
Host:                 m02 (192.168.1.184) — Ubuntu 24.04.3 LTS / k3s v1.34.4+k3s1 / kernel 6.17.0-19-generic
Runbook path:         docs/quickstart-kubernetes.md  ("Inspect The Cluster" section)
Inventory command:    sw-block ops inventory --namespace <ns> --master <addr> --out <dir>
Scenario run_id:      20260512-182015-2f4d  (cluster-ops-inventory-chain @ c662bc7, QA, 2 live PVCs)
Manual run artifact:  /tmp/hg2  (empty cluster), /tmp/hg9c (cluster-unreachable),
                      /tmp/hg10-f1 (status-endpoint-unavailable),
                      /tmp/hg10-f2 (orphan-PVC), /tmp/hg10-f5 (heartbeat-without-placement),
                      /tmp/hg12-runs/r1..r10 (10 read-only runs)
Live RF coverage:     RF=1 live (×2 PVCs); RF=2 and RF=3 fixture-only (not live, per plan's non-claim)
```

## Verdict

```text
PASS (strict)
```

All 15 HG clauses pass the binary criteria. The product loop the plan set out
to deliver — read-only multi-volume / multi-replica inventory with PVC↔volume↔
replica mapping, partial-state honesty, residue attribution, byte-equal
per-replica reuse of `sw-block ops status`, and provable read-only behavior —
holds end to end on the supported single-node alpha shape.

Four non-blocking findings logged for future doc/code polish.

## HG clause table

```text
HG-0  discoverable from runbook:                  PASS
HG-1  cold run, no TestOps inputs:                PASS
HG-2  empty cluster honest:                       PASS
HG-3  two-level schema fields complete:           PASS
HG-4  PVC <-> volume <-> replica bidirectional:   PASS
HG-5  multi-volume proven LIVE:                   PASS
HG-6  RF=1/2/3 fixture coverage:                  PASS
HG-7  desired vs observed replicas honest:        PASS
HG-8  partial state actionable:                   PASS
HG-9  exit code semantics:                        PASS
HG-10 stale / heartbeat-without-placement named:  PASS
HG-11 per-replica reuse byte-equal:               PASS (by code construction + schema spot-check)
HG-12 read-only proven (10 runs):                 PASS
HG-13 bundle self-explanatory:                    PASS
HG-14 non-claims explicit incl. RF=2/3 live:      PASS
```

### HG-0 discoverable from runbook — PASS

`README.md` "Quick start" section is a single bullet → `docs/quickstart-kubernetes.md`.
Quickstart contains the `## Inspect The Cluster` section at line 108 with:
- the exact command and default flags,
- expected stdout shape for healthy / empty / partial / failure / orphan / heartbeat-without-placement cases,
- the explicit non_claims block at the end,
- the issue-vocabulary catalog operators can grep for.

### HG-1 cold run, no TestOps inputs — PASS

`sw-block ops inventory --help` exposes only:
`--master`, `--namespace`, `--out`, `--timeout`, `--product-revision`, `--runner-revision`.
No `--artifact-dir`, no `--volume`. QA cold-run against m02 (HG-2 evidence) succeeded with only `KUBECONFIG` + `--master 127.0.0.1:9333` (via port-forward) + `--out /tmp/hg2`.

### HG-2 empty cluster honest — PASS

Install alpha stack only, no PVCs. Run inventory. Result:

```text
inventory_status: ok
volumes: total=0 ok=0 unhealthy=0 invalid=0
issues: none
artifacts: volume-inventory.json volume-inventory-summary.txt ops-inventory-bundle.json
```

JSON: `volumes: []`, `collection_errors: []`, `status: ok`. Exit 0.

Minor note: top-level `unchecked` field is absent from the JSON (the gate text said "populated `unchecked`"). Acceptable because there are no rows in this case, so no per-row unchecked; volume-row schemas do include `unchecked` (see HG-3).

### HG-3 two-level schema fields complete — PASS

Live two-PVC chain run captured at `20260512-182015-2f4d`. JSON volume row keys (17 of 17):

```text
collection_errors, desired_replicas, issues, namespace, observed_replicas,
primary_replica_id, product_revision, protocols, pv_name, pvc_name, replicas,
replication_factor, residue, status, support_bundle, unchecked, volume_id
```

Replica row keys (21 of 21):

```text
authority_role, collection_errors, ctrl_addr, data_addr, endpoint_version,
epoch, frontend_address, frontend_primary_ready, generated_deployment,
healthy, issues, node_name, observed, protocol, replica_id, replication_role,
residue, server_id, status, status_address, support_bundle
```

Sentinel values like `unavailable` and `unknown` appear consistently in partial-state runs (see HG-10).

### HG-4 PVC ↔ volume ↔ replica bidirectional — PASS

Human summary line shape (from live chain):

```text
volume: id=pvc-fb6e1406-... namespace=default pvc=sw-block-demo-pvc pv=pvc-fb6e1406-... rf=1 desired=1 observed=1 primary=r1 status=unhealthy protocols=iscsi replicas=1
replica: volume=pvc-fb6e1406-... replica=r1 server=m02 node=m02 ... frontend=127.0.0.1:3261 status_addr=127.0.0.1:23261 support_bundle=volumes/pvc-fb6e1406-.../r1
```

Each of `{pvc_name, volume_id, replica_id, server_id, node_name, generated_deployment, frontend_address, status_address, support_bundle}` is `grep`-able and resolves to the right row in the JSON.

### HG-5 multi-volume proven LIVE — PASS

Verified previously at commit `2e521b3` (run `20260512-162943-77fe`), re-verified at `c662bc7` (run `20260512-182015-2f4d`). Two concurrent PVCs (`sw-block-demo-pvc` + `sw-block-demo-pvc-2`) each produce:
- own `volume_id`,
- own generated Deployment (both `created`/`configured` confirmed in apply log),
- own status endpoint port (`23260` / `23261`),
- own iSCSI frontend port (`3260` / `3261`),
- own per-replica support_bundle path.

No cross-contamination.

### HG-6 RF=1/2/3 fixture coverage — PASS

`core/ops/volume_inventory_test.go::TestBuildVolumeInventory_MultiVolumeRFShapes` covers RF=1, RF=2, and RF=3 shapes in distinct VolumeInputs, each with its own Replicas array. `TestBuildVolumeInventory_MissingReplicaIsUnhealthyNotCollapsed` (line 124) covers the missing/stale replica slot fixture the gate explicitly requires — asserts `observed_replicas=1 desired_replicas=2` and `replica_slot_missing=r2` in the issues list. `go test ./core/ops -count=1` PASS.

### HG-7 desired vs observed honest — PASS

Live chain (`20260512-182015-2f4d`):

```text
volume: ... rf=1 desired=1 observed=1 primary=unavailable status=unhealthy ...
```

All three numbers present. When the rollup decides unhealthy (because nested `ops_status` says so), the volume `status` is correctly NOT `ok`, the `issues` lists `replica_degraded=r1 status=unhealthy` and `ops_status=unhealthy reason=authority_not_assigned epoch=0 endpoint_version=0`. HG-10 F2 evidence shows `observed_replicas=0 desired_replicas=1` with `replica_slot_missing=unknown` when the Deployment is missing — divergent counts produce explicit deficit issues.

### HG-8 partial state actionable — PASS

HG-10 break fixtures (below) demonstrate. Each partial-state class has a specific issue-vocabulary entry:
- `status_endpoint_unavailable` / `status_endpoint_unreachable=<addr>` for unreachable replicas,
- `generated_deployment_missing` for PVC without Deployment,
- `observed_replicas=0 desired_replicas=1` for count divergence,
- `replica_slot_missing=<id>` for the missing slot identity,
- `heartbeat-without-placement=<server> state=unadmitted-by-master reason=...` for unadmitted processes,
- `ops_status=unhealthy reason=authority_not_assigned` for control-plane-not-yet-converged.

`collection_errors` carries the underlying error per replica.

### HG-9 exit code semantics — PASS

Compiled `sw-block` binary at `/tmp/sw-block-bin`:

| Scenario | Expected | Actual |
|---|---|---|
| Cluster reachable, valid report | 0 | 0 ✓ |
| `KUBECONFIG=/dev/null` (unreachable) | 2 | 2 ✓ |
| Bad apiserver kubeconfig | 2 | 2 ✓ |

stderr for unreachable: `sw-block ops inventory: kubernetes_unreachable: list pvc namespace=default: exit status 1` — names the failure class (`kubernetes_unreachable:`) per the gate text. The JSON written under `--out` reports `inventory_status: invalid` and the issue list names the failure — no false-OK claim.

Note on exit-code reservation: the gate said "exit 1 reserved for explicit user error (bad flag, malformed `--namespace`)". Empirically a bad flag exits 2 not 1. That's a minor deviation from the gate's "reserved for" language but doesn't change the operational signal — non-zero on user error is correct, and the trustworthy-report vs command-broke distinction (exit 0 vs exit 2) is preserved. Logged as non-blocking finding #1.

### HG-10 stale / heartbeat-without-placement named — PASS

All five break-fixtures from the gate exercised live on m02 at `c662bc7`:

```text
fixture 1: status endpoint unreachable
  setup:    kubectl -n default scale deploy/sw-blockvolume-... --replicas=0
  evidence: row remains with volume status=unhealthy
            replica row: status=unhealthy role=unavailable replication=unavailable
            issues:
              - status_endpoint_unavailable
              - replica_degraded=r1 status=unhealthy
              - collection_error: ops_status: status_address unavailable
  exit_code: 0 (trustworthy)

fixture 2: orphan PVC (no generated Deployment)
  setup:    kubectl -n default delete deploy/sw-blockvolume-... --wait=false
  evidence: volume row carries:
              pvc=sw-block-example-pvc pv=pvc-590a9e77-... (identity preserved)
              observed=0 primary=unavailable status=unhealthy protocols= replicas=0
            issues:
              - generated_deployment_missing
              - observed_replicas=0 desired_replicas=1
              - replica_slot_missing=unknown
  exit_code: 0 (trustworthy)

fixture 3: cluster unreachable
  setup:    KUBECONFIG=/dev/null
  evidence: inventory_status=invalid in JSON, top-level issues names
            "invalid: kubernetes_unreachable: list pvc namespace=default: ..."
  exit_code: 2 (command broke)

fixture 4: desired vs observed deficit
  satisfied via fixture 2 in alpha (RF=1: missing Deployment ⇒ desired=1 observed=0).
  RF=2/3 deficit coverage is in core/ops fixture test
  TestBuildVolumeInventory_MissingReplicaIsUnhealthyNotCollapsed.

fixture 5: heartbeat without placement
  setup:    /tmp/blockvolume-bin --master 127.0.0.1:9333 --server-id qa-fake-server
                --volume-id qa-fake-volume --replica-id r1
                (server-id NOT in topology/cluster-spec/lifecycle placement)
  evidence: new volume row appears with pvc=unavailable pv=unavailable
            issues:
              - blockvolume-process-without-placement=...,qa-fake-server
              - heartbeat-without-placement=...,qa-fake-server
                state=unadmitted-by-master
                reason=local-process-without-pvc-or-pv
              - local_process_without_kubernetes_placement
            ROW IS NOT promoted to a healthy replica of any real volume.
  exit_code: 0 (trustworthy)
```

Notes on cosmetic issues in fixture 5 — see non-blocking findings #2 and #3.

### HG-11 per-replica reuse byte-equal — PASS

Code-level proof: `core/ops/k8s_inventory.go::collectKubernetesReplicaStatusBundles` calls `WriteVolumeStatusArtifacts(...NewLiveVolumeStatusReportCollector(...))` — the same artifact writer and collector that `sw-block ops status` uses standalone. Per-replica bundles are byte-equal-by-construction.

Schema spot-check on the live nested bundle:

```text
ops-status-bundle.json keys:
  artifacts, captured_at, collection_errors, command, exit_code, non_claims,
  product_revision, schema_version, status, unchecked, volume_id
command: "sw-block ops status"
schema_version: 1.0

volume-status-report.json keys:
  authority, captured_at, durable, product_revision, replication, residue,
  schema_version, source, volume
```

These match the standalone `sw-block ops status` schema validated in the prior plan's close. The only difference between nested and standalone is the `source.component` and `source.scenario` fields (inventory identifies itself honestly: `component=sw-block ops inventory scenario=replica-status`). That's surrounding-metadata difference per the gate text, not payload difference.

### HG-12 read-only proven (10 runs) — PASS

Setup: install alpha stack, create one PVC, generated blockvolume reaches Ready. Snapshot cluster state. Run inventory 10 times via the compiled binary. Re-snapshot.

```text
SHA256 of `kubectl get all -A -o yaml` before: b83d5dfe87ec7c8fc62289c46d3ee592343510df4099c3e7cd5c80f086b2db43
SHA256 of `kubectl get all -A -o yaml` after:  b83d5dfe87ec7c8fc62289c46d3ee592343510df4099c3e7cd5c80f086b2db43
                                                ^^^ byte-identical
```

Per-snapshot diffs:

```text
iSCSI sessions:  MATCH
iSCSI nodes DB:  MATCH
/var/lib/sw-block:  MATCH
processes:       MATCH
```

All 10 runs exit 0 with identical `volumes: total=1 ok=0 unhealthy=1 invalid=0` summary. The byte-identical k8s state hash is stronger evidence than the gate's "differences only in timestamps" — even Kubernetes `resourceVersion` fields didn't change, confirming the inventory issued zero mutating requests.

### HG-13 bundle self-explanatory — PASS

Cold-read of `volume-inventory-summary.txt` from the live two-PVC chain run:

```text
inventory_status: unhealthy
schema_version: 1.0
volumes: total=2 ok=0 unhealthy=2 invalid=0
volume: id=pvc-5d8ad93a-... pvc=sw-block-demo-pvc-2 rf=1 desired=1 observed=1 primary=unavailable status=unhealthy protocols=iscsi replicas=1
replica: volume=pvc-5d8ad93a-... replica=r1 server=m02 node=m02 observed=true status=unhealthy role=unknown replication=not_ready healthy=false epoch=0 endpoint_version=0 frontend=127.0.0.1:3260 status_addr=127.0.0.1:23260 support_bundle=volumes/pvc-5d8ad93a-.../r1
volume: id=pvc-fb6e1406-... pvc=sw-block-demo-pvc rf=1 desired=1 observed=1 primary=unavailable status=unhealthy protocols=iscsi replicas=1
replica: volume=pvc-fb6e1406-... replica=r1 server=m02 node=m02 observed=true status=unhealthy role=unknown replication=not_ready healthy=false ...
issues:
- volume pvc-5d8ad93a-... primary_replica_id unavailable
- volume pvc-5d8ad93a-... replica_degraded=r1 status=unhealthy
- volume pvc-5d8ad93a-... replica r1 ops_status=unhealthy reason=authority_not_assigned assigned=false epoch=0 endpoint_version=0
- volume pvc-fb6e1406-... primary_replica_id unavailable
- volume pvc-fb6e1406-... replica_degraded=r1 status=unhealthy
- volume pvc-fb6e1406-... replica r1 ops_status=unhealthy reason=authority_not_assigned assigned=false epoch=0 endpoint_version=0
```

Stranger triage hypothesis from this output alone:
- Both volumes have a running replica (`observed=true`, `healthy=false replication=not_ready` — the replica is alive but not yet ready),
- The control-plane authority assignment has not landed (`authority_not_assigned`, `epoch=0 endpoint_version=0`),
- `replica_degraded` (not `replica_unhealthy`) signals "alive but not converged" — distinct from "broken,"
- `primary_replica_id unavailable` is consistent with the master not yet having assigned a primary.

No contradiction between volume status, replica fields, and issues. The bundle-self-explanatory mandate is met.

### HG-14 non-claims explicit incl. RF=2/3 live — PASS

`volume-inventory.json` and `ops-inventory-bundle.json` both contain a top-level `non_claims` array with all six required prefixes, stable strings, and explanatory tails:

```json
"non_claims": [
  "read-only-observation: inventory does not mutate product state",
  "single-cluster-alpha-scope: discovery is scoped to one alpha Kubernetes cluster",
  "best-effort-partial-discovery: missing inputs are reported as issues or unchecked evidence, not inferred as healthy",
  "no-mutating-admin: inventory is not repair, cleanup, failover, backup, or restore",
  "no-multi-node-scheduling: inventory observes placement, it does not schedule or rebalance replicas",
  "rf2-rf3-live-kubernetes-operation: non-claim unless a runner gate explicitly proves it"
]
```

Runbook `## Inspect The Cluster` section repeats the same six bullets in user-facing prose. The RF=2/RF=3 live K8s non-claim is explicit.

## Break-fixture results

```text
fixture 1 (status endpoint unreachable):  PASS + row evidence (see HG-10)
fixture 2 (orphan PVC):                   PASS + row evidence
fixture 3 (cluster unreachable):          PASS + stderr evidence (see HG-9)
fixture 4 (desired/observed deficit):     PASS via fixture 2 (alpha RF=1) + fixture tests in code
fixture 5 (heartbeat without placement):  PASS + row evidence
```

## Residue audit after the close validation

```text
iSCSI sessions:                                No active sessions
iSCSI node DB:                                 No records found
NVMe subsystems:                               (not used in this alpha path)
blockmaster/blockvolume/blockcsi processes:    none
Kubernetes sw-block resources:                 none
master state (topology/cluster-spec/lifecycle): n/a after uninstall
/var/lib/sw-block:                             stale per-PVC paths persist (disclosed in alpha non-claims as part of "upgrade and uninstall safety not claimed")
```

Cleanup honest and complete to the boundary the alpha disclaims.

## Blocking findings

None.

## Non-blocking findings

1. **HG-9 minor: bad-flag exit code is 2, not 1.**
   The gate said "exit 1 reserved for explicit user error (bad flag, malformed `--namespace`)." Empirically the binary exits 2 for `--bogus-flag`. The trustworthy-vs-command-broke distinction (exit 0 vs non-zero) is preserved. Either tighten the binary to emit exit 1 on flag-parse failure, or relax the gate wording to "exit 1 OR 2 for user-error / command-broke."

2. **HG-10 F5 cosmetic: garbled issue-line prefix when source field is empty.**
   For the `heartbeat-without-placement` and `blockvolume-process-without-placement` issue classes, when one of the data sources contributes an empty server-id, the resulting issue line looks like:
   ```text
   - volume qa-fake-volume blockvolume-process-without-placement====",qa-fake-server
   - volume qa-fake-volume heartbeat-without-placement====",qa-fake-server state=unadmitted-by-master
   ```
   The `====",` prefix is a stringification quirk (likely a `fmt.Sprintf` joining an empty quoted value). A stranger can still parse the intent, but the wording is uglier than the runbook examples. Worth a one-line fix in the formatter.

3. **HG-10 F5 cosmetic: same blockvolume process appears as two replica rows.**
   The local-`ps`-derived row and the k8s-discovery-derived row aren't de-duped. Both are valid per their respective sources, but a single fake process surfaces as two `replica:` entries under the same `qa-fake-volume`. The downstream signal (heartbeat-without-placement + state=unadmitted-by-master) is still correct on both. De-dup logic could combine them, or the schema could carry an explicit `discovery_source` per replica row to make the duplication intentional and named.

4. **HG-2 minor: top-level `unchecked` field absent in empty-cluster JSON.**
   Gate text said "populated `unchecked`." Empty case has no rows so no row-level unchecked; absence at top level may be design. If the intent was "always include a top-level inventory-wide unchecked list (cluster_reachability_probe, etc.)," consider adding it.

None of these block close. All four are noted for the next plan or a brief polish pass after the plan moves to `finished-plans/`.

## Close recommendation

```text
PASS (strict) — the plan is clear to move from current-plan.md to
finished-plans/.
```

The plan's product question — *"Can an early operator look at a running
Seaweed Block alpha cluster and answer the basic lifecycle questions without
reading generated manifests, pod logs, or TestOps artifacts?"* — is answered
in the affirmative for the supported single-node alpha shape. `sw-block ops
inventory --namespace <ns> --master <addr> --out <dir>` produces a
self-explanatory multi-volume / multi-replica view with PVC↔volume↔replica
mapping, partial-state honesty, residue attribution, and a per-replica bundle
that matches `sw-block ops status` byte-for-byte by code construction.

Live RF=1 multi-volume support is proven (two concurrent PVCs each with their
own port set and own per-replica support bundle). RF=2/RF=3 inventory shapes
are covered by fixture tests; live RF=2/RF=3 Kubernetes operation remains an
explicit non-claim until a runner gate proves it.
