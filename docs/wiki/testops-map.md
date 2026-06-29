# QA and TestOps Map

Seaweed Block uses TestOps and QA sign-offs as product evidence, not just test
coverage. The important question is what each gate proves.

## Test Realism Levels

| Level | Meaning | Useful for |
|---|---|---|
| L0 self-proof | helper writes a value and test greps it | avoid for product claims |
| L1 replay | existing evidence is replayed through report/status logic | projection correctness |
| L2 live injection | real failure is induced and product observes it | status and recovery claims |
| L3 adversarial | live failure plus concurrency/timing/isolation | release-critical claims |

Phase 34 moved dirty-failure testing away from self-proof and toward live
injection.

## Scenario Families

| Family | Examples | Claim area |
|---|---|---|
| Day-1 install | `helm-first-volume-via-sw-block-cli-chain.yaml` | Helm + first PVC |
| Multi-volume RF=3 | `helm-multi-volume-rf3-*.yaml` | identity isolation and HA gates |
| Restart persistence | `helm-*-restart-*.yaml` | authority and data persistence |
| Failure evidence | `status-endpoint-unreachable-*`, `helm-smartwal-corrupt-restart-chain.yaml` | no false Ready |
| Lifecycle owner | `lifecycle-owner-*.yaml` | admission/action/finalizer boundary |
| Cleanup | `cleanup-residue-chain.yaml` and cleanup verifier | zero-residue contract |
| Protocol | `iscsi-*`, `nvme-*` | frontend-specific gates |

## Release Evidence

Release claims should point to:

- a finished plan in `internal/docs/finished-plans/`,
- one or more QA sign-offs in `internal/docs/qa-assignments/`,
- a release note in `docs/releases/`,
- if applicable, immutable image tags/digests.

If a feature has only unit tests and no live or replay gate, call it an
implementation detail, not a product capability.

## Current High-Value Gates

| Gate | Why it matters |
|---|---|
| SmartWAL corruption restart | proves dirty evidence cannot become false Ready |
| live node/CSI evidence | prevents node and CSI blockers from being masked |
| status API conformance | catches CRD schema/RBAC bugs missed by mocks |
| lifecycle-owner admission | proves mutation boundary on a real Kubernetes API |
| delete lifecycle close | proves the operation-layer pieces compose end-to-end |

## QA Writing Rule

A QA sign-off should state:

```text
what was run
which source commit/image
which evidence was observed
what would have failed the gate
blocking findings
non-blocking findings
residue result
```

This makes the sign-off useful months later when a developer needs to know why
a state transition or guard exists.

