# Current Plan: Light-Use Operations MVP

Status: active. Reframed on 2026-05-11 from the smaller
`Operations Layer Usability Seed` after review.

## Product Question

Can an early user or developer use Seaweed Block as a functional light block
product without reading internal scripts and logs?

The honest answer today:

- The core datapath is close to a functional MVP: CSI dynamic PVC, iSCSI/NVMe
  frontend paths, mounted failover, restart, returned-replica evidence, and
  beta-hardening gates are real.
- The operational product is not yet a light-use product. Install, lifecycle
  ownership, status, diagnosis, and support-bundle workflow are still too
  script/testops-shaped.

This plan closes the first user-visible operations loop. It does not try to
finish the whole enterprise operations vision.

## User Experience After This Plan

As a user or developer, I should be able to:

1. Follow the existing alpha/K8s workflow to create a PVC and run an app pod.
2. Use product-owned CLI/read-only status to answer:
   - what volume am I looking at?
   - what frontend target is serving it?
   - what authority/replication/durable state does the product report?
   - is there visible iSCSI/NVMe host residue?
   - what issue summary should I attach to a bug?
3. Produce one support bundle directory with:
   - machine-readable JSON,
   - human-readable summary,
   - bundle metadata/provenance,
   - clear exit code.
4. Have TestOps validate that the command and bundle shape do not drift.

This is the minimum useful operations loop:

```text
PVC/app problem -> run status command -> read summary -> attach bundle -> dev/QA
can triage without asking for raw logs first
```

## Top Blocking Issues For Light-Use Product

These block a credible light-use product if left unresolved.

### P0: Product-Owned Lifecycle Is Still Thin

Current scripts/TestOps still own too much lifecycle:

- generated `blockvolume` deployment ownership,
- cleanup of generated workloads,
- run-scoped state directories,
- some residue cleanup checks.

User-visible risk: a user can create a PVC but cannot trust normal Kubernetes
deletion/retry behavior without our harness discipline.

This plan does not solve the full controller/operator gap. It must make the gap
visible and avoid pretending TestOps cleanup is the product.

### P0: Operator Status Is Not Yet One Closed Workflow

The read-only status report exists, and the CLI now exists, but the complete
support bundle must be self-describing and validated.

User-visible risk: users can collect JSON, but dev/QA still need context to know
what command, product revision, and non-claims produced it.

This is the main close-loop item for the current plan.

### P1: Install/Upgrade/Uninstall Story Is Not Productized

Current install paths are alpha scripts and TestOps scenarios.

User-visible risk: a user can reproduce our tests, but cannot install and
operate the product as a normal K8s add-on.

Push to next plan: packaging/installer/operator track.

### P1: Observation API/UI Is CLI-First Only

We have read-only API surfaces and CLI summaries, not a UI or metrics dashboard.

User-visible risk: users cannot monitor over time or see cluster-wide health.

Push to next plan: metrics/API/UI track after the status bundle is stable.

## Current Scope

Finish a closed read-only operations loop.

In scope:

- `sw-block ops status` command for one volume,
- collection from blockmaster and blockvolume read-only status surfaces,
- local iSCSI/NVMe host residue observation,
- explicit `unchecked` residue classes when this CLI cannot safely observe a
  class,
- machine-readable report JSON,
- human-readable summary,
- self-describing bundle metadata,
- TestOps command-boundary gate,
- operator guide and non-claims.

Required TestOps support for closing the loop:

- simple control data for the shared M01/M02 lab,
- active run record with run id, scenario, state, current phase, artifact dir,
  product/runner commit, target nodes, ports if known, and updated timestamp,
- resource-group lock metadata for tests that share global lab resources such
  as `node:m02`, `iscsi:m02`, `nvme:m02`, or `k3s:m02`,
- terminal move from active to history,
- enough CLI/status surface for dev/QA to answer:
  - what is running,
  - which build is running,
  - which resources it owns,
  - where artifacts are,
  - whether the run is stale.

Out of scope for this plan:

- mutating admin commands,
- automatic repair,
- Kubernetes operator/controller,
- UI/dashboard,
- Helm/installer,
- performance monitoring,
- fleet agent,
- cloud-scale test controller.
- full TestOps scheduler or remote agent.

## Deliverables

### D1: Operator Command

```text
sw-block ops status \
  --volume <id> \
  --master <host:port> \
  --status-addr <host:port|url> \
  --out <dir>
```

Exit code contract:

- `0`: report collected, parsed, and classified clean,
- `1`: report collected but unhealthy/incomplete/residue or collection-error
  evidence detected,
- `2`: required input is invalid, artifact writing failed, or the report
  identity/schema is invalid.

### D2: Support Bundle

The output directory should contain:

- `volume-status-report.json`,
- `volume-status-summary.txt`,
- `ops-status-bundle.json`.

`ops-status-bundle.json` is the remaining close-loop item. It should record:

- schema version,
- command name,
- captured time,
- volume id,
- product revision,
- runner revision,
- exit classification,
- artifact list,
- unchecked residue classes,
- non-claims.

### D3: TestOps Gates

Fast gates, no live lab needed:

- `operations-volume-status-report-component-gate`
- `operations-volume-status-cli-gate`

These prove the report contract, summary/classifier, artifact writer, and real
CLI command boundary.

### D4: Operator Guide

The guide must explain:

- when to run the command,
- how to read the summary,
- which fields indicate authority/replication/durable risk,
- what residue and `unchecked` mean,
- what this command does not prove.

### D5: TestOps Control Data For Shared Lab Runs

The runner should maintain minimal shared-drive control data when executing
scenario gates that consume M01/M02 resources:

```text
testops-control/
  active/<run_id>.json
  history/<run_id>.json
  locks/<resource>.lock
  events.jsonl
```

The first version is not a scheduler. It is visibility and safety:

- create an active record at run start,
- update state/current phase at phase boundaries,
- record artifact dir and known commit evidence,
- refuse or clearly report conflicting resource locks,
- release locks and move the record to history on terminal exit,
- leave stale active records visible if the runner crashes.

This supports the product usability loop because QA/dev cannot validate user
experience reliably if M01/M02 has invisible old tests, stale ports, or unknown
builds still running.

## Current Progress

Completed:

- `RenderVolumeStatusSummary(report)` deterministic summary.
- `ClassifyVolumeStatusReport(report)` exit-code intent.
- `VolumeStatusReportIssues(report)` issue list.
- `WriteVolumeStatusArtifacts(ctx, dir, collector)` artifact writer.
- Live read-only collector for:
  - blockmaster `EvidenceService.QueryVolumeStatus`,
  - blockvolume `/status`,
  - blockvolume `/status/peers`,
  - blockvolume `/status/durable`.
- `sw-block ops status` command.
- Local host iSCSI/NVMe residue observation.
- Explicit `unchecked` residue classes for process/K8s/storage paths.
- Operator guide.
- Component and CLI TestOps gates.

Remaining in this plan:

1. Add `ops-status-bundle.json`.
2. Gate the bundle metadata in `operations-volume-status-cli-gate`.
3. Update operator guide with the bundle workflow.
4. Add minimal TestOps control data for shared lab visibility and stale-run
   detection.
5. Gate the control-data behavior with component tests and one QA validation
   assignment.
6. Run focused tests and scenario validation.
7. Close this plan into `finished-plans/`.

## Deferred Roadmap Items

Move these to future plans, not this one:

- K8s install/upgrade/uninstall packaging.
- Controller/operator-owned generated `blockvolume` lifecycle.
- Cluster-wide `sw-block ops list` / `sw-block ops volume list`.
- Mutating admin commands such as force detach, cleanup, rebuild, or promote.
- Prometheus metrics and dashboard/UI.
- Agent-based fleet operations.
- Hosted/cloud TestOps controller.
- Advanced TestOps scheduler, queueing, or remote agent.

## Gate To Close

This plan closes only when:

1. A user can run one read-only product command for one volume.
2. The command emits report, summary, and bundle metadata.
3. The bundle is self-describing enough to attach to an issue.
4. TestOps validates the command-boundary bundle shape.
5. Shared-lab TestOps runs publish enough active/history/lock control data that
   dev/QA can see what is running and avoid colliding scenarios.
6. The operator guide documents the exact workflow and non-claims.
7. No mutating product control-plane action is added.

## Success Statement

After this plan, Seaweed Block is still not a full operations product. But it
has its first real operator loop:

```text
create/use a volume -> observe it with a product CLI -> collect a support bundle
-> validate the bundle shape -> triage with clear non-claims
```

That is the foundation for the next plans: install/lifecycle ownership and
cluster-wide observation.
