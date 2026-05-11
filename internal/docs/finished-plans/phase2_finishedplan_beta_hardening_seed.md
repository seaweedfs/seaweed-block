# Finished Plan: Beta Hardening Seed Gate

Status: historical reference. Closed on 2026-05-11 by the runner-native
`beta-hardening-gate` suite.

This phase turned the protocol-ready block product into a beta-hardening seed:
one runner-native suite covering iSCSI, NVMe, CSI protocol selection, soak,
component lifecycle, restart, returned-replica evidence, diagnostics, and
cleanup residue.

Current work is tracked in `../current-plan.md`.

## Close Evidence

QA validated the first full green run:

- suite: `testops/suites/beta-hardening-gate.yaml`
- run id: `20260511-031605-8258`
- product commit: `8822f20e91c2b88727ead9e49f9bf75eec28c791`
- runner commit: `cf65daaf2ce5cf500e1efa48b411f7cb66dbac0b`
- result: PASS
- wall clock: 1305.86 seconds
- `swblock validate-bundle --profile beta-hardening --expect-commit 8822f20`
  returned VALID.

All 10 children passed:

1. `iscsi-p6-alua-failover`
2. `nvme-p4-multipath-failover`
3. `nvme-p5-csi-protocol`
4. `iscsi-p8-compat-soak`
5. `csi-lifecycle-component`
6. `csi-rf1-durable-restart`
7. `operations-status-diagnostics`
8. `returned-replica-component`
9. `iscsi-returned-replica`
10. `cleanup-residue`

## What Closed

- iSCSI/NVMe/CSI protocol readiness is now included in a broader beta seed
  gate.
- Returned-replica evidence is field-level and defensible:
  - returned r1 remains non-primary and frontend-fenced,
  - r1 local recovery frontier is captured,
  - r2 remains primary and durable operational at epoch >= 2,
  - r2 sees r1 as a healthy peer.
- Component/operations chains now emit product provenance evidence, so suite
  provenance is not inferred from the controller checkout.
- Runner suite provenance now derives `product_commit` from child evidence and
  fails on mixed or missing child evidence after the suite commit is
  established.
- `runner_commit` is captured from Go build metadata, not from the caller's
  current working directory.
- Cleanup residue is a real suite child and validates no stale iSCSI/NVMe/V3/K8s
  residue at the end of the run.

## Important Non-Claims

- This is not production HA.
- This is not broad distro or kernel compatibility.
- This is not performance readiness.
- This is not a multi-node/cloud-scale claim.
- This is not an operations/operator completeness claim.
- This is one full green, not yet a back-to-back repeatability claim.

## Follow-Up Decisions

- Request one back-to-back QA run before calling the beta seed suite stable.
- Keep `protocol-release-gate` as the smaller periodic frontend regression
  suite.
- Use `beta-hardening-gate` for milestone readiness, not default developer
  iteration.
- Reduce future suite cost by lowering integration-only assertions into
  component/contract tests whenever possible.
