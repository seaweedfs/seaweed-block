# Protocol And State-Machine Catalog

This page catalogs the state machines and mini-protocols that should be
documented with Mermaid diagrams. It links to the current source references and
names the code or gate area that owns the behavior.

The catalog is intentionally broader than the current release claim. Some
protocols are implemented and gated; others are planned but need the same
fact -> judgment -> action -> evidence structure before becoming product
claims.

## Existing Source References

- [Runtime State Machines](../runtime-state-machines.md)
- `internal/docs/protocol/README.md`
- `internal/docs/protocol/invariant-ledger.md`
- `internal/docs/protocol/managed-volume-operations-model.md`
- `internal/docs/protocol/operator-readiness-contract.md`
- `internal/docs/protocol/finalizer-delete-safety-contract.md`
- `internal/docs/ref/lifecycle-owner-control-contract.md`
- `internal/docs/ref/operation-layer-v0.5-release-train.md`

## Catalog

| # | Protocol / state machine | Current source | Main code area | Mermaid status |
|---|---|---|---|---|
| 1 | Control-plane placement to authority publication | `docs/runtime-state-machines.md` | `core/host/master`, launcher/authority code | existing diagram |
| 2 | Data-plane write / sync / WAL boundary | `docs/runtime-state-machines.md` | `cmd/blockvolume`, `core/storage` | existing diagram |
| 3 | Recovery / replication peer lifecycle | `docs/runtime-state-machines.md`, `internal/docs/ref/rf2-promotion-ready-recovery-contract.md` | recovery / replication paths | existing high-level diagram; needs productized rebuild version |
| 4 | Frontend eligibility / stale-owner fencing | `docs/runtime-state-machines.md`, `internal/docs/protocol/invariant-ledger.md` | frontend projection, authority bridge | existing high-level diagram |
| 5 | CSI node stage/publish flow | `docs/runtime-state-machines.md`, `core/csi` | `cmd/blockcsi`, `core/csi` | existing sequence diagram |
| 6 | ManagedVolume readiness projection | `internal/docs/protocol/managed-volume-operations-model.md` | `core/ops` | needs detailed Mermaid |
| 7 | Kubernetes node readiness evidence | `internal/docs/protocol/operator-readiness-contract.md` | `core/ops` live node evidence | needs detailed Mermaid |
| 8 | Cleanup visibility / cleanup verifier evidence | `internal/docs/protocol/operator-readiness-contract.md`, `internal/docs/ref/operation-layer-v0.5-release-train.md` | cleanup verifier, `core/ops/cleanup_evidence.go` | needs detailed Mermaid |
| 9 | Delete-safety decision model | `internal/docs/protocol/finalizer-delete-safety-contract.md` | `core/ops` delete-safety projection | initial diagram in [State Machines](state-machines.md) |
| 10 | Lifecycle-owner finalizer add/release | `internal/docs/ref/lifecycle-owner-finalizer-strategy.md`, `internal/docs/ref/lifecycle-owner-control-contract.md` | `core/ops/lifecycle_owner_controller.go` | initial sequence in [State Machines](state-machines.md) |
| 11 | Kubernetes admission boundary for lifecycle-owner | `internal/docs/ref/phase42-lifecycle-owner-api-admission-gate.md` | Helm VAP/RBAC templates | needs VAP decision diagram |
| 12 | Action model evaluator | `internal/docs/protocol/engine-design-guidelines.md`, Phase 38 docs | `core/ops/action_model.go` | needs Mermaid decision tree |
| 13 | Status writer / CRD schema conformance | Phase 40 status API docs | `core/ops/kubernetes_status_writer.go` | needs request/validation diagram |
| 14 | SmartWAL corruption fail-closed path | Phase 34 finished plan and QA sign-offs | `core/storage`, `cmd/blockvolume`, `core/ops` projection | needs detailed failure-path diagram |
| 15 | iSCSI ALUA / dm-multipath path state | `internal/docs/ref/iscsi-alua-technical-note.md`, `stage2-transparent-multipath-host-failover-spec.md` | iSCSI frontend and host path evidence | needs protocol diagram |
| 16 | NVMe ANA parity path | `internal/docs/ref/nvme-ana-technical-note.md`, `nvme-ana-parity-plan.md` | future NVMe frontend path | planned diagram |
| 17 | Returned-replica rebuild / reintegration | roadmap + recovery refs | recovery + future lifecycle action owner | planned diagram |
| 18 | TestOps run lifecycle and evidence bundle | `internal/docs/ref/testops-control-data-contract.md`, `testops/` | runner/scenarios | needs run-state diagram |

## Diagram Priority

The first detailed Mermaid expansions should be:

1. Operation layer v0.5: facts -> judgment -> action -> admission -> evidence.
2. ManagedVolume readiness projection: how ready/blocked/unknown wins.
3. CSI lifecycle: PVC -> CreateVolume -> `SwBlockVolume` CR -> status/finalizer.
4. SmartWAL dirty-failure path: storage fault -> local readiness block ->
   projection no false Ready.
5. TestOps gate lifecycle: live/replay/adversarial gates and evidence bundles.

## Rule For Adding A Diagram

Every Mermaid diagram should name:

```text
state names
transition trigger
owner component
evidence artifact
failure state
QA gate
```

If a diagram cannot name the evidence artifact or gate, it is probably still a
design sketch rather than a product behavior.

