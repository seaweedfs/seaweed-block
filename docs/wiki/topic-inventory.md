# Topic Inventory

This inventory lists the topics the engineering wiki should eventually cover.
It is broader than current release scope.

Status meanings:

| Status | Meaning |
|---|---|
| `deep` | implementation-grade: background, contract, ownership, state machine, code map, evidence, failures, checklist, QA |
| `tutorial` | strong explanatory page, but may still miss one or two implementation-grade sections |
| `summary` | page exists but cannot yet guide a safe code change |
| `mapped` | topic appears in catalog/coverage but has no dedicated deep page |
| `missing` | no useful wiki coverage yet |
| `future` | intentionally deferred feature area |

## Product And Release

| Topic | Status | Current page |
|---|---|---|
| Product positioning / claim boundary | mapped | `docs/releases/`, coverage matrix |
| Release train and image/chart skew | mapped | release docs, Phase 40 references |
| User quickstart and Day-1 path | mapped | quickstart, README |
| Feature/status table for public readers | mapped | README/release docs |
| Docker volume plugin idea | future | roadmap only |
| Windows developer path / WSL / remote lab | missing | scattered notes only |

## Kubernetes / CSI / Operator Layer

| Topic | Status | Current page |
|---|---|---|
| CSI lifecycle / SwBlockVolume identity | tutorial | `deep-dives/csi-lifecycle.md` |
| CSI ControllerPublish/NodeStage/NodePublish details | summary | CSI page |
| CSI DeleteVolume and CR delete ordering | summary | CSI page + operation layer page |
| SwBlockVolume CRD field ownership | summary | CSI page + operation layer page |
| operator-status CRD status writer | summary | operation layer page |
| lifecycle-owner finalizer add/release | deep | operation layer page |
| Kubernetes VAP/RBAC admission boundary | summary | operation layer page |
| delete-safety state machine | tutorial | state machines + operation layer |
| cleanup verifier / residue taxonomy | mapped | state machines + support bundle page |
| node readiness / CSI evidence | deep draft | `deep-dives/live-node-evidence.md` |
| host prereq projection | mapped | Phase 37 D4/D5 docs |
| loopback/cross-node blocker | mapped | Phase 37 D5 docs |
| install drift status | mapped | roadmap/release docs |
| Kubernetes Events identity/dedupe | mapped | Phase 35 docs |
| CRD schema conformance/envtest gap | mapped | Phase 39/40 docs |

## Storage / Data Plane

| Topic | Status | Current page |
|---|---|---|
| WAL / frontier / barrier concepts | deep draft | `deep-dives/wal-frontier-recovery.md` |
| SmartWAL dirty-failure path | tutorial | `deep-dives/smartwal-dirty-failure.md` |
| SmartWAL file layout and corruption injection | summary | SmartWAL page |
| durable root / hostPath persistence | mapped | roadmap/ref docs |
| blockvolume process layout and flags | missing | historical tutorial only |
| frontend projection/readiness | mapped | SmartWAL + ManagedVolume pages |
| local storage adapter readiness | missing | source/code only |
| SCSI command handling / sense errors | missing | `core/frontend/iscsi` only |
| dirty restart / recovery fail-closed | summary | SmartWAL page |

## Authority / Recovery

| Topic | Status | Current page |
|---|---|---|
| authority / epoch / promotion | tutorial | `deep-dives/authority-promotion.md` |
| stale-primary fencing | summary | authority page |
| restart authority persistence | summary | authority page |
| recovery/catch-up/rebuild mechanics | mapped | returned-replica page + historical tutorial |
| returned-replica rebuild productization | tutorial initial | `deep-dives/returned-replica-rebuild.md` |
| failback policy | future | roadmap only |
| promotion readiness probes | summary | authority page |
| multi-volume authority isolation | mapped | QA/phase docs |

## Protocol Frontends

| Topic | Status | Current page |
|---|---|---|
| iSCSI basics | summary | iSCSI ALUA page |
| iSCSI ALUA / dm-multipath | deep draft | `deep-dives/iscsi-alua-multipath.md` |
| iSCSI OS initiator compatibility | mapped | ref/QA docs |
| iSCSI CHAP / auth path | mapped | chart values + quickstart |
| iSCSI stale path write rejection | mapped | ALUA/failover QA docs |
| NVMe-oF basics | missing | ref docs only |
| NVMe ANA parity | future | roadmap/ref docs |
| protocol selection in CSI | mapped | roadmap/ref docs |

## Operations / Observability

| Topic | Status | Current page |
|---|---|---|
| ManagedVolume readiness | tutorial | `deep-dives/managed-volume-readiness.md` |
| support bundle / report / dashboard | deep draft | `deep-dives/support-bundle-cold-reader.md` |
| operator-snapshot.json | mapped | operations docs |
| Events and reason-code vocabulary | mapped | release docs |
| action model evaluator | mapped | protocol catalog |
| negative-first rule | tutorial | ManagedVolume + SmartWAL pages |
| cold-reader explain path | mapped | Phase 32/36/44 docs |
| cleanup visibility | mapped | Phase 36/39/44 docs |
| action precondition/evidence model | mapped | Phase 38 docs |

## TestOps / QA

| Topic | Status | Current page |
|---|---|---|
| TestOps authoring | tutorial | `deep-dives/testops-authoring.md` |
| scenario registry | mapped | TestOps page |
| evidence realism levels | tutorial | TestOps page |
| release smoke gates | mapped | release docs |
| failure bundle standard | mapped | Phase 32 docs |
| live-vs-mock CRD/RBAC gap | mapped | Phase 35-40 docs |
| dirty-failure scenario design | mapped | Phase 34 docs |
| runner action reference | missing | source/testops only |

## Priority Gaps

The highest-value missing or summary-only pages are:

1. CRD schema/RBAC/envtest conformance.
2. release engineering and image/chart skew prevention.
3. NVMe-oF / ANA background.
4. blockvolume process layout and frontend readiness.
5. runner action reference.
6. WAL SmartWAL file-layout detail diagrams.
7. support bundle artifact examples from real runs.

## Review Rule

When reviewing this wiki, do not ask only whether a topic is present. Ask:

```text
Could a developer implement or safely change this feature from the page?
Could QA derive the required evidence and failure reasons from the page?
Could a cold reviewer map the user claim to code and gates?
```

If the answer is no, the page remains `summary` even if it is accurate.
