# Operations Guide: Reading `VolumeStatusReport`

Status: first operator-facing reading guide for the read-only operations status
report. This guide explains what the report can support today and what it must
not be used to decide.

This guide is intentionally conservative. A `VolumeStatusReport` is evidence,
not authority.

## What The Report Is

`VolumeStatusReport` is a schema-controlled JSON object assembled from existing
status facts:

- master volume assignment/status,
- local blockvolume status projection,
- replication peer status,
- durable lineage status,
- host/Kubernetes residue facts,
- product and runner provenance.

The report is read-only. Collection must not promote, demote, detach, clean,
restart, create sessions, remove sessions, create Kubernetes resources, or
delete Kubernetes resources.

## Minimal Command

The first operator-facing command is:

```text
sw-block ops status \
  --volume <volume-id> \
  --master <blockmaster-grpc-addr> \
  --status-addr <blockvolume-loopback-status-addr-or-url> \
  --out <artifact-dir>
```

Example:

```text
sw-block ops status \
  --volume v1 \
  --master 127.0.0.1:18080 \
  --status-addr 127.0.0.1:23260 \
  --out /tmp/sw-block-ops-v1
```

The command writes:

- `volume-status-report.json`
- `volume-status-summary.txt`
- `ops-status-bundle.json`

It also prints the human-readable summary to stdout.

`ops-status-bundle.json` is the top-level support-bundle manifest. It records
the command name, captured time, volume id, product/runner revisions, exit
classification, artifact list, unchecked residue classes, collection errors,
and non-claims. Attach the whole output directory to an issue; start triage from
the bundle manifest and summary, then inspect the raw report only when needed.

Exit codes:

- `0`: report collected, parsed, and classified clean.
- `1`: report collected but unhealthy, incomplete, residue, or collection-error
  evidence was observed.
- `2`: required inputs were missing, artifact writing failed, or the report
  identity/schema is invalid.

`--master` and `--status-addr` are read-only sources. Both are required for the
first command version so a clean exit cannot hide missing master
frontend/publication facts or missing local blockvolume authority, replication,
and durable facts.

`--status-addr` points at the existing `blockvolume --status-addr` HTTP
surface. That status surface is loopback-only and unauthenticated by design; do
not expose it on a public interface. Use SSH port forwarding or a trusted
operator-side tunnel when collecting remotely.

Current limitation: the live command collects host-side iSCSI and NVMe
initiator residue using local read-only commands. It reports process,
Kubernetes, and storage-path residue classes as unchecked; release gates that
need those facts should keep using TestOps cleanup/residue scenarios.

## Safe Operator Questions

The report may be used to answer these questions:

- Which `volume_id` and `replica_id` did this host report?
- Which frontend protocols and identities were advertised?
- Did the local replica report `authority_role=primary`, `superseded`, or
  `unknown`?
- What `epoch` and `endpoint_version` did the local replica report?
- Did the frontend report `frontend_primary_ready=true`?
- Which replication peers were known, and what state did each peer report?
- Which durable lineage was latched: `replica_id`, `epoch`,
  `endpoint_version`, `latched`, `operational`, and `closed`?
- Was host or Kubernetes residue observed at collection time?
- Which product and runner revisions produced the evidence?

## Unsafe Conclusions

The report must not be used alone to conclude:

- It is safe to force detach a volume.
- It is safe to delete a durable path.
- It is safe to promote or demote a replica.
- A stale primary is fully fenced.
- A host is permanently dead.
- A volume is highly available.
- A filesystem or application workload is durable.
- A cleanup action is authorized.
- A rollback, clone, backup, or restore point exists.

Those decisions need a separate admin protocol with fencing, quorum/authority
semantics, and action-specific preconditions.

## Field Reading Rules

### `captured_at`

Use as the evidence timestamp. Do not treat old reports as current truth.

If a report is older than the operational question being asked, recollect.

### `product_revision` and `runner_revision`

Use to verify provenance. A report without matching expected revisions is still
evidence, but not release-gate evidence.

If `product_revision=unavailable`, do not use the report to prove a specific
commit passed.

### `volume`

Use `volume.volume_id`, `volume.replica_id`, `protocols`, and `frontends` to
confirm the target shape the product exposed.

Do not infer that a host initiator is connected only because a frontend is
listed. Frontends describe product exposure, not client-side attachment.

### `authority`

Use `authority_role`, `epoch`, `endpoint_version`, `assigned`,
`frontend_primary_ready`, and `healthy` to understand the local replica's
reported role.

Do not use `authority_role=primary` alone as permission to serve writes in an
external workflow. Serving writes is a product data-path behavior, not an
operator action.

Do not use `authority_role=superseded` alone as proof that all remote clients
have drained.

### `replication`

Use peer entries to understand the local replica's view of peer health and
lineage. A missing peer list is evidence that the peer source was unavailable
or empty, not proof that no peer exists.

`last_error=unavailable` means the current product source did not provide a
structured peer error. It does not mean "no error."

### `durable`

Use durable entries to inspect latched replica identity and epoch lineage.

`latched=true` and `operational=true` are useful evidence that the durable
backend recognized its identity. They are not enough to authorize deletion,
rollback, or force rejoin.

### `residue`

Use residue arrays to block unsafe manual action and to guide cleanup
investigation.

Empty residue arrays mean "nothing was observed by this collector at this
time." They do not prove the environment is globally clean.

## Escalation Paths

If the report shows conflicting or unsafe evidence:

- `authority_role=primary` on more than one replica:
  stop and inspect authority/fencing logs; do not issue cleanup.
- durable lineage differs from authority lineage:
  stop and inspect durable backend identity; do not promote or delete.
- frontend is ready while durable is not operational:
  treat as a product bug or incomplete status; collect product logs.
- residue remains after a supposedly clean run:
  block release-gate pass and collect host/Kubernetes state.
- product revision is missing or mismatched:
  rerun with pinned build/provenance before using as milestone evidence.

## Minimal Release-Gate Use

A release gate may use the report as supporting evidence when all of these are
true:

- expected `product_revision` is present,
- expected `runner_revision` or runner provenance is present when applicable,
- schema version is supported,
- key arrays are present as arrays, not `null`,
- report artifact is collected into the run bundle,
- scenario-specific assertions validate the fields required by that scenario.

The report remains supporting evidence. The release gate still needs workload
or component assertions appropriate to the scenario.

