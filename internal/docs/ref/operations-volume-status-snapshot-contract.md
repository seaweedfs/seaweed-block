# Operations Contract: Volume Status Snapshot

Status: selected first operations-layer contract. This is a contract target for
the next active plan, not an operator implementation.

## Purpose

Operators need one stable read-only view that answers:

```text
What volume is this host serving, what role does it believe it has, what
frontend facts are exposed, what durable lineage is latched, and what residue
or peer condition should block unsafe action?
```

Today these facts exist across status endpoints, lifecycle records, runner
artifacts, and host initiator state. The first operations-layer step is to make
that shape explicit before building admin actions around it.

## Existing Inputs

Current product surfaces:

- master `QueryVolumeStatus` / `StatusResponse`
  - assigned volume/replica facts,
  - protocol frontend targets:
    - protocol,
    - address,
    - iSCSI IQN/LUN,
    - NVMe NQN/NSID.
- `GET /status?volume=<id>`
  - `VolumeID`
  - `ReplicaID`
  - `Epoch`
  - `EndpointVersion`
  - `Healthy`
  - `FrontendPrimaryReady`
  - `AuthorityRole`
  - `ReplicationRole`
- `GET /status/durable?volume=<id>`
  - `VolumeID`
  - `ReplicaID`
  - `VolumeCount`
  - per-volume durable status:
    - `VolumeID`
    - `Path`
    - `Impl`
    - `ReplicaID`
    - `Epoch`
    - `EndpointVersion`
    - `Latched`
    - `Operational`
    - `Evidence`
    - `Closed`
- `GET /status/peers?volume=<id>`
  - primary-side peer readiness when a peer source is installed.
  - current raw peer fields:
    - `ReplicaID`
    - `State`
    - `Epoch`
    - `EndpointVersion`
    - `DataAddr`
    - `CtrlAddr`
    - `SessionID`
    - `ProbeInFlight`
    - `Closed`
- TestOps artifacts:
  - product git revision,
  - runner git revision,
  - scenario provenance,
  - iSCSI session state,
  - NVMe subsystem state,
  - K8s resource state,
  - cleanup actions.

## Snapshot Shape

The contract should produce an append-only JSON object with these sections.

### Header

- `schema_version`
- `captured_at`
- `source`
  - `component`
  - `host`
  - `scenario`
- `product_revision`
  - required when captured from TestOps,
  - otherwise explicit `unavailable`.
- `runner_revision`, when captured by TestOps

### Volume

- `volume_id`
- `replica_id`
- `protocols`
  - `iscsi`
  - `nvme`
- `frontends`
  - `protocol`
  - `addr`
  - `iqn`, for iSCSI
  - `nqn`, for NVMe
  - `lun` / `nsid`

### Authority

- `epoch`
- `endpoint_version`
- `authority_role`
  - `primary`
  - `superseded`
  - `unknown`
- `frontend_primary_ready`
- `healthy`

### Replication

- `replication_role`
  - `none`
  - `recovering`
  - `not_ready`
  - `replica_ready`
  - `unknown`
- `peers`
  - `replica_id`
  - `state`
  - `data_addr`
  - `ctrl_addr`
  - `healthy`, derived as `state == "healthy"` until the product exposes an
    explicit boolean
  - `epoch`
  - `endpoint_version`
  - `session_id`
  - `probe_in_flight`
  - `closed`
  - `last_error`, explicit `unavailable` until the product exposes one

### Durable

- `durable`
  - `impl`
  - `path`
  - `replica_id`
  - `epoch`
  - `endpoint_version`
  - `latched`
  - `operational`
  - `closed`
  - `evidence`

### Residue

Residue is diagnostic and should not trigger destructive action by itself.

- `host_initiator`
  - `iscsi_sessions`
  - `nvme_subsystems`
- `processes`
  - V3 process matches
- `kubernetes`
  - blockmaster resources
  - blockvolume resources
  - CSI resources
  - PVC/PV residue
- `storage_paths`
  - durable roots observed
  - unmanaged paths, if any

## Safety Rules

- Snapshot is read-only.
- Snapshot must not mutate authority, lifecycle, storage, sessions, or K8s.
- Snapshot must be append-only; new fields are allowed, existing field meaning
  is not.
- Missing optional inputs must be represented explicitly as unavailable, not
  silently omitted.
- A future `force_detach` or cleanup command must not be built only on this
  snapshot. It needs fencing semantics and a separate admin protocol.

## Non-Claims

- This is not an operator.
- This is not a production API.
- This does not authorize force detach.
- This does not prove HA.
- This does not replace TestOps run bundles.
- This does not expose a public unauthenticated endpoint.

## First Implementation Target

For the next plan, start with a component-level collector that can assemble the
snapshot from existing in-process/status-test data:

- status projection,
- durable status,
- peer status,
- synthetic residue inputs.

Then add a runner-native component gate that asserts:

- primary projection includes frontend-ready authority facts,
- returned replica can be durable-ready but frontend-fenced,
- durable lineage is visible without log scraping,
- missing peer/durable inputs are represented explicitly.
