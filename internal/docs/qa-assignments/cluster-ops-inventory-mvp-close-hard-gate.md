# QA Hard Gate: Cluster Operations Inventory And Lifecycle Visibility MVP

Status: standing gate. Applies to the active plan
`internal/docs/current-plan.md`
("Cluster Operations Inventory And Lifecycle Visibility MVP",
revised at commit `2901559` to be multi-volume and RF-aware).
D1 contract implementation landed at commit `7ed77a3`.

This is QA's hard gate, written from the point of view of an early operator
who has never seen this product before and is staring at a running alpha
cluster. It sits **on top of** the plan's own Gates 1-9 (D1-D7) and raises
the bar from "boxes checked" to "an operator can answer real questions
without grep'ing scenario artifacts."

The plan's revision after the first draft of this gate explicitly added:

- multi-volume contract and tests (not just multiple-rows: multiple PVCs,
  multiple `volume_id`s),
- RF=1 / RF=2 / RF=3 schema and fixture coverage,
- per-replica row structure (each volume carries a `replicas:` array),
- master-placement reality: `blockvolume` dynamically registers via
  heartbeat, but master only operates on replicas admitted by topology /
  cluster-spec / lifecycle placement; heartbeat alone is not authority.

This gate enforces those revisions at close time.

## Mandate

QA will not sign close based on green CI. QA runs the documented operator
command by hand, cold, against a live alpha cluster on the supported
single-node shape, with no TestOps artifact paths supplied. If any clause
below is fuzzy, the close stops until the doc / scenario / bundle is fixed.

Each HG clause is binary: PASS or FAIL. No soft categories. If any single
HG clause fails, the close is blocked.

## Gate Clauses

### Step 0 — "Where do I find this command?"

**HG-0  Discoverable from the runbook, not from source.**
A new operator can find the command from `README.md` → user-facing doc
without reading `cmd/sw-block` source or scenario YAML.

- Fail: the command exists only in code comments / scenario YAML / a slack
  thread; the runbook references it but does not show the exact invocation
  with default flags.
- Pass: `docs/quickstart-kubernetes.md` has an "Inspect The Cluster" (or
  equivalent) section with the exact command, default flag set, and
  expected stdout shape. The Cluster section is reachable in two clicks
  from `README.md`.

### Step 1 — Run from cold, no TestOps inputs

**HG-1  Runs without artifact-dir, scenario context, or volume hints.**
Plan's P0: "Inventory Discovery Must Not Depend On Test Artifacts."

- Fail: the command requires `--artifact-dir`, `--volume`, or a generated
  YAML to function on a cluster.
- Pass: with only `KUBECONFIG` and (if needed) `--master <addr>`, the
  command produces a valid inventory or a clear "nothing to inventory"
  result. Verified by QA against a cluster QA started fresh, no prior
  TestOps run.

### Step 2 — Empty cluster is honest

**HG-2  Empty case has explicit summary, not silent zero rows.**

- Fail: running on a cluster with no Seaweed Block volumes produces an
  empty stdout, exit 0, no message — indistinguishable from a hang.
- Pass: empty case emits a one-line summary
  (`inventory_status: ok` and `volumes: total=0 ...`) on stdout and writes
  a valid JSON file with `volumes: []`, populated `non_claims`, empty
  `collection_errors`. Exit 0.

### Step 3 — Two-level schema, every field populated

**HG-3  All required volume-level and replica-level fields present.**

Plan §D1 enumerates 17 volume fields and 20 replica fields. Every row at
every level must carry every field, with documented sentinels for absent
values.

- Fail: a volume row is missing one of the 17 fields, a replica row inside
  `replicas:` is missing one of the 20 fields, or a field is silently
  `null`/empty without an explicit `unavailable` / `unknown` sentinel
  or a matching entry in the row's `unchecked` list.
- Pass: a one-volume one-replica row from a healthy demo has every named
  field populated at both levels; absent fields use documented sentinels;
  schema doc names the sentinel and the conditions for each field. Volume
  row's `replicas:` array is present even when empty.

### Step 4 — Identity mapping is three-way

**HG-4  Operator can map PVC → volume → replicas, and back.**
Plan's P0: "Volume Identity Must Be Human-Mappable."

- Fail: rows show `volume_id` only. An operator who knows the PVC name has
  to grep the JSON to find the volume. An operator who has a `replica_id`
  has no path back to the PVC.
- Pass: the human summary on stdout shows
  `<namespace>/<pvc>  →  <volume_id>  RF=<n>  observed=<m>/<n>` per
  volume, and a `replicas:` sub-block lists
  `<replica_id>@<server_id>/<node_name>  role=...  protocol=...  status=...`
  per replica. Any of `{pvc_name, volume_id, replica_id, server_id,
  node_name, generated_deployment}` can be `grep`-ed and resolves
  bidirectionally in the JSON.

### Step 5 — Multi-volume case proven LIVE, not just fixtures

**HG-5  At least two concurrent PVCs / `volume_id`s in a live inventory.**
Plan's P0: "Inventory Must Be Multi-Volume And Replica-Aware."

- Fail: the multi-volume case is only proven by fixture tests in D6; the
  live runner-native gate or the QA cold run shows only one volume.
- Pass: a runner-native gate or a QA-personal run creates two PVCs on the
  same cluster, runs inventory, and the output shows two distinct volume
  rows with correct attribution (each PVC → its own `volume_id`, its own
  generated Deployment, its own status endpoint port, its own iSCSI IQN).
  No row inherits state from the other. If the alpha path cannot create
  two PVCs reliably, that limitation is explicit in the runbook AND the
  inventory's `non_claims` AND the close report — close still blocked on
  HG-5 unless QA confirms the limitation is honestly named.

### Step 6 — RF=1 / RF=2 / RF=3 fixture coverage

**HG-6  Schema and contract tests cover RF=1, RF=2, and RF=3 shapes.**

Plan §D6: "fixture tests for RF=1, RF=2, and RF=3 volume shapes." The
live alpha path may only run RF=1, but the contract must not assume
single-replica.

- Fail: schema tests only exercise RF=1, or RF=2/RF=3 fixtures share code
  paths that would silently collapse multi-replica state.
- Pass: D6 fast tests include at least one fixture per RF, plus a
  `missing/stale replica slot` fixture that verifies a volume row
  correctly reports `observed_replicas < desired_replicas` and names the
  affected replica in the per-volume `issues:` list. QA spot-checks at
  least one RF=2 and one RF=3 fixture output cold and confirms each
  replica is independently addressable.

### Step 7 — Desired vs observed replicas reported honestly

**HG-7  `replication_factor`, `desired_replicas`, `observed_replicas`
all present and consistent.**

- Fail: only one number is emitted, or `observed_replicas` always equals
  `desired_replicas` regardless of cluster reality.
- Pass: volume row carries all three. When they diverge, the volume's
  top-level `status` is not `ok`, the `issues:` list names the deficit
  (`replica_slot_missing=<replica_id>` or
  `replica_unhealthy=<replica_id>`), and the affected replica row in
  `replicas:` carries its own `issues:` entry. A volume with
  `desired_replicas=2 observed_replicas=1` cannot have volume `status=ok`.

### Step 8 — Partial state is actionable

**HG-8  Partial rows name what is broken, in machine-readable form.**
Plan's P0: "Partial Failure Must Be Actionable."

- Fail: an unreachable status endpoint produces a row that looks healthy,
  or `issues: []` with the actual problem only in stdout free-text.
- Pass: a row with an unreachable endpoint has `status` ≠ `ok`,
  `issues:` lists the specific class and identity
  (for example `collection_error: GET http://127.0.0.1:23260/status...`,
  `generated_deployment_missing`, or `pvc_unbound_for_more_than_60s`), and
  `collection_errors` carries the underlying transport/parse error. The
  top-of-stdout summary names how many rows are degraded.

### Step 9 — Exit code semantics are binary

**HG-9  Exit code distinguishes "report is trustworthy" from "command
broke."**

- Fail: any unhealthy row makes the command exit non-zero, OR an
  unreachable cluster produces exit 0 with an empty report.
- Pass:
  - exit 0 = the report itself is trustworthy (even if some rows are
    `unhealthy` or carry `issues`);
  - exit 2 = the command could not produce a trustworthy report
    (KUBECONFIG unset, master gRPC unreachable, output dir unwritable);
  - exit 1 reserved for explicit user error (bad flag, malformed
    `--namespace`).

  Documented in the runbook and in `--help`.

### Step 10 — Stale state and master-placement residue are named

**HG-10  Orphan / residue rows are explicit, with attribution that
respects the master/topology reality.**

Reuses the prior plan's `cleaned_by=testops-guardrail` convention. New for
this plan: the master only operates on replicas admitted by
topology/cluster-spec/lifecycle placement. A heartbeating `blockvolume`
process that is NOT in accepted placement is a residue class, not a
healthy replica.

- Fail: an orphaned PVC (no generated Deployment), a leftover iSCSI
  session against a deleted volume, a `blockvolume` Deployment without a
  matching PVC, or a `blockvolume` process heartbeating without an
  authority assignment silently disappears or gets reported as a healthy
  replica.
- Pass: each residue class is a row (or a `residue:` field on the closest
  identity row) with an explicit shape, e.g.:
  ```text
  orphan-pvc:<ns>/<pvc>                       state=no-generated-deployment age=<duration>
  orphan-iscsi-session:<iqn>                  state=session-without-volume   cleaned_by=testops-guardrail
  orphan-blockvolume-deploy:<name>            state=no-matching-pvc
  heartbeat-without-placement:<server_id>     state=unadmitted-by-master     reason=topology-or-cluster-spec-mismatch
  ```
  Inventory does not claim cleanup it didn't perform; it names the
  testops-guardrail boundary the same way the prior plan's
  `cleanup-attribution.txt` does. Inventory does not silently promote a
  heartbeating-but-unadmitted process to a healthy replica.

### Step 11 — Per-replica reuse is real, not duplicated

**HG-11  D3 per-replica bundle reuses `sw-block ops status` semantics.**

Plan §D3: "for each discovered live replica with a status endpoint, call
or reuse the existing `ops status` collector and attach the per-replica
report to the inventory bundle."

- Fail: the inventory's per-replica `volume-status-report.json` /
  `volume-status-summary.txt` changes field meaning or issue semantics from
  what
  `sw-block ops status --volume <id> --master <addr> --status-addr <addr> --out <dir>`
  produces standalone for the same replica.
- Pass: QA runs both for at least one healthy replica and one degraded
  replica and compares normalized reports. The normalized comparison ignores
  expected run-local metadata (`captured_at`, output paths, and wrapper
  source labels) but must match volume/replica identity, authority,
  replication, durable evidence, residue evidence, issue classification, and
  collection errors. Differences allowed only in surrounding inventory metadata
  (volume aggregation, file paths), not in per-replica evidence semantics.

### Step 12 — Read-only contract proven

**HG-12  10 consecutive runs produce zero cluster state change.**

Plan's non-claim: "Mutating repair commands. Automatic cleanup."

- Fail: any cluster object (k8s resource, blockvolume process state, iSCSI
  session, durable file on disk, master state) changes as a result of
  running the inventory command.
- Pass: QA snapshots cluster state, runs the inventory command 10 times
  (with the documented default output mode and, if implemented, without
  `--out`; with and without `--namespace`), snapshots again. `diff` of the
  snapshots shows no change beyond timestamps.
  Snapshot includes: `kubectl get all -A -o yaml`,
  `sudo iscsiadm -m session`, `sudo iscsiadm -m node`,
  `ls /var/lib/sw-block`, process list, and a sample of master state via
  the read-only `EvidenceService.QueryVolumeStatus` RPC.

### Step 13 — Bundle is self-explanatory to a stranger

**HG-13  Inventory bundle's summary alone tells the triage story.**

Mirrors HG-6 of the prior plan's gate but for the new two-level inventory
format.

- Fail: bundle's summary needs a cover letter to be useful, or a
  maintainer has to open three files to form a triage hypothesis for a
  multi-volume, mixed-RF, mixed-health inventory.
- Pass: QA re-runs Part C-style inspection against a *failing* multi-
  volume inventory bundle (not a synthetic). Bundle includes at least:
  two volumes, at least one volume with `desired > observed`, and at
  least one partial row from HG-8's break fixtures. Pass only if
  `inventory-summary.txt` plus the row-level `issues:` lines tell the
  triage story without follow-up. Volume-level summary explicitly names
  which volumes are degraded and why; replica-level summary makes per-
  replica state directly readable.

### Step 14 — Claims match, RF=2/RF=3 live explicitly disclaimed

**HG-14  Non-claims are explicit in the inventory output AND in the
runbook, with RF=2/RF=3 live operation specifically called out.**

Plan §Out-of-scope: "Claiming live RF=2/RF=3 Kubernetes lifecycle until a
runner gate proves that path; RF=2/RF=3 inventory shapes are still
required in contract tests."

- Fail: the inventory or runbook implies that live Kubernetes RF=2/RF=3
  works without a runner gate that proves it; OR the inventory implies
  repair, multi-node scheduling, durability, HA, or operator-grade
  semantics.
- Pass: every inventory bundle carries a `non_claims:` block listing at
  minimum:
  - `read-only-observation` (not repair),
  - `single-cluster-alpha-scope`,
  - `best-effort-partial-discovery`,
  - `no-mutating-admin`,
  - `no-multi-node-scheduling`,
  - `rf2-rf3-live-kubernetes-operation` — explicitly listed as a
    non-claim unless a runner gate has been merged and is referenced by
    artifact path; QA confirms the listed gate exists and is green
    before allowing the non-claim to be removed from this list.

  Runbook's "Inspect The Cluster" section repeats these in user-facing
  prose and links each to the relevant alpha limitation line. When the
  live alpha path cannot create two PVCs, this is also called out as a
  documented limitation (see HG-5).

## QA Process Commitment

- Validation host: m02 (`192.168.1.184`) k3s lab, or a user-provided
  equivalent single-node Kubernetes lab that passes
  `docs/quickstart-kubernetes.md` preflight.
- QA does not sign off on close based on "all gates green in CI." QA runs
  the documented inventory command by hand, cold, as an operator who has
  not seen the dev process and has not consulted scenario artifact
  directories.
- QA deliberately exercises the break fixtures in "Required Break
  Fixtures" below and rejects the close if any break produces a bundle
  that does not help an operator triage.
- For HG-6 (RF schema coverage) QA spot-checks at least one RF=2 and one
  RF=3 fixture output cold; QA does not need to bring up live RF=2/RF=3
  on the alpha path for this gate, but QA verifies that any RF=2/RF=3
  live claim made elsewhere has a runner gate to back it.
- QA reports findings against this gate in the close report. Any HG-clause
  failure must be quoted verbatim in the verdict.

## Required Break Fixtures For HG-7 / HG-8 / HG-9 / HG-10

QA exercises these break fixtures, not friendly synthetic errors. Each
must produce row(s) whose `status`, `issues`, and `collection_errors`
together tell the operator what is wrong without grepping logs.

1. **Status endpoint unreachable** (HG-8 / HG-9). Scale the generated
   `blockvolume` Deployment to 0 replicas after PVC is bound. Run
   inventory. Expected: row remains, volume `status` ≠ ok, the affected
   replica row's `issues` names `status_endpoint_unreachable=<addr>`,
   `collection_errors` carries the connection refused/timeout error.
   Inventory exit code 0 (report is trustworthy).
2. **Orphan PVC** (HG-8 / HG-10). Create a PVC against the SeaweedBlock
   StorageClass, then delete the generated `blockvolume` Deployment
   before any pod attaches. Run inventory. Expected: volume row carries
   `issues: [generated_deployment_missing]`, identity (`namespace`,
   `pvc_name`, `pv_name`) is still populated, `replicas: []`. Inventory
   exit 0.
3. **Cluster unreachable** (HG-9). Set `KUBECONFIG=/dev/null` (or point at
   an unreachable apiserver). Run inventory. Expected: exit 2, stderr
   names the failure class (`kubernetes_unreachable: <error>`), no
   false-OK JSON written, no partial bundle that claims success.
4. **Desired vs observed deficit** (HG-7). Where the alpha path supports
   it (or via fixture if live RF=2 is not available), produce a volume
   with `desired_replicas=2 observed_replicas=1`. Expected: volume
   `status` ≠ ok, `issues` names `replica_slot_missing=<id>`, the
   missing replica is reflected (or not present) in the `replicas:`
   array with a clear absence marker.
5. **Heartbeat without placement** (HG-10). Start a `blockvolume` process
   that points `--master` at the running master but uses a `--server-id`
   not in `--topology` / `--cluster-spec`. Run inventory. Expected:
   `heartbeat-without-placement:<server_id> state=unadmitted-by-master`
   row appears as residue, NOT as a healthy replica of any volume.

## Report Template

```text
QA Report -- Cluster Operations Inventory And Lifecycle Visibility MVP

Product commit:
Runner commit:
Host:
Runbook path:
Inventory command (default form):
Scenario run_id:
Manual run artifact (cold operator follow):
Live RF coverage in this run: RF=1 / RF=2 / RF=3 (mark which were live, which were fixture-only)

Verdict:
  PASS / FAIL

HG clause table:
  HG-0  discoverable from runbook:                  PASS/FAIL + evidence
  HG-1  cold run, no TestOps inputs:                PASS/FAIL + evidence
  HG-2  empty cluster honest:                       PASS/FAIL + evidence
  HG-3  two-level schema fields complete:           PASS/FAIL + evidence
  HG-4  PVC <-> volume <-> replica bidirectional:   PASS/FAIL + evidence
  HG-5  multi-volume proven LIVE:                   PASS/FAIL + evidence
  HG-6  RF=1/2/3 fixture coverage:                  PASS/FAIL + evidence
  HG-7  desired vs observed replicas honest:        PASS/FAIL + evidence
  HG-8  partial state actionable:                   PASS/FAIL + evidence
  HG-9  exit code semantics:                        PASS/FAIL + evidence
  HG-10 stale / heartbeat-without-placement named:  PASS/FAIL + evidence
  HG-11 per-replica reuse byte-equal:               PASS/FAIL + evidence
  HG-12 read-only proven (10 runs):                 PASS/FAIL + evidence
  HG-13 bundle self-explanatory:                    PASS/FAIL + evidence
  HG-14 non-claims explicit incl. RF=2/3 live:      PASS/FAIL + evidence

Break-fixture results:
  fixture 1 (status endpoint unreachable):  PASS/FAIL + row evidence
  fixture 2 (orphan PVC):                   PASS/FAIL + row evidence
  fixture 3 (cluster unreachable):          PASS/FAIL + stderr evidence
  fixture 4 (desired/observed deficit):     PASS/FAIL + row evidence
  fixture 5 (heartbeat without placement):  PASS/FAIL + row evidence

Residue audit after the close validation:
  iSCSI sessions:
  iSCSI node DB:
  NVMe subsystems:
  blockmaster/blockvolume/blockcsi processes:
  Kubernetes sw-block resources:
  master state (topology/cluster-spec/lifecycle placement):

Blocking findings:
  - ...

Non-blocking findings:
  - ...
```

## Open Adjustments

If any HG clause is judged too strict for alpha, it must be downgraded
**before** dev starts D1-D7, not at close time. Late downgrades are not
accepted: that is the failure mode this gate exists to prevent. The
preceding plan's close report exercised that lesson — soft categories
introduced at close time had to be retracted.

If the live alpha path genuinely cannot bring up RF=2/RF=3 (which the
current plan's Out-of-scope already concedes), the *correct* response is
NOT to downgrade HG-6 (fixture coverage) or HG-14 (non-claim discipline).
The correct response is:
- keep HG-6 fixture coverage at full RF=1/2/3 strength,
- keep HG-14 RF=2/RF=3-live as an explicit non-claim, AND
- if HG-5 (live multi-volume) cannot be satisfied because two RF=1 PVCs
  cannot coexist on the alpha path, that itself is a usability
  limitation that must be named in the runbook AND in the close report.

## Non-Claims For This Gate

This gate validates the **read-only inventory and lifecycle visibility
loop** on the supported single-node shape. It does not validate:

- repair / mutating admin actions,
- automatic cleanup,
- Kubernetes operator-grade reconciliation,
- web UI / dashboard,
- Prometheus metrics pipeline,
- live multi-node scheduling / placement policy,
- live RF=2/RF=3 Kubernetes lifecycle (gate enforces this stays an
  explicit non-claim unless a runner gate proves it),
- upgrade / uninstall safety,
- performance / SLO under load.

Those remain non-claims of the active plan; this gate does not silently
extend them.
