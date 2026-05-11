# Finished Plan: Fast Gates And Operations Contract Prep

Status: historical reference. Closed on 2026-05-11 after the fast gates and the
first operations status report contract were validated on m02.

This phase turned the repeatable beta milestone suite into a more practical
developer workflow and seeded the operations layer with a read-only volume
status report contract.

This report is observability evidence. It is not a block/data snapshot.

Current work is tracked in `../current-plan.md`.

## Close Evidence

Baseline milestone evidence inherited from phase 3:

- `beta-hardening-gate` run 1:
  `20260511-031605-8258`, PASS, 10/10, `21m46s`, bundle VALID.
- `beta-hardening-gate` run 2:
  `20260511-040412-ac38`, PASS, 10/10, `22m55s`, bundle VALID.
- Product commit for that repeatability claim:
  `8822f20e91c2b88727ead9e49f9bf75eec28c791`.
- Runner commit for that repeatability claim:
  `cf65daaf2ce5cf500e1efa48b411f7cb66dbac0b`.

Fast gate validation:

| Gate | Run ID | Product Commit | Result | Wall Clock | Bundle |
| --- | --- | --- | --- | ---: | --- |
| `nvme-p5-protocol-component-gate` | `20260510-214940-d726` | `b926b7e50c522665b66a81a2990a3fe925364365` | PASS | `1.486s` | collected |
| `csi-rf1-durable-restart-component-gate` | `20260510-214947-206b` | `b926b7e50c522665b66a81a2990a3fe925364365` | PASS | `1.034s` | collected |
| `operations-volume-status-report-component-gate` | `20260510-232649-03fd` | `c8a27ac4ca35e4686420ce068bb67811b9a95fd9` | PASS | `1.081s` | collected |

The operations gate was initially named
`operations-volume-status-snapshot-component-gate`. It was renamed to
`status-report` and revalidated to avoid conflict with real block/data snapshot
terminology.

All three fast gates passed `swblock validate`.

## What Closed

1. Developers now have runner-native component gates for:
   - CSI/NVMe/iSCSI protocol propagation,
   - RF=1 durable restart contract shape,
   - the first operations volume status report contract.
2. `testops/README.md` documents which gate to run:
   - package-local `go test`,
   - fast runner-native component gate,
   - child integration chain,
   - full beta milestone suite.
3. The full `beta-hardening-gate` remains a milestone/release gate, not the
   default debugging loop.
4. The first operations contract is documented at
   `../ref/operations-volume-status-report-contract.md`.
5. `core/ops.BuildVolumeStatusReport` assembles the report from existing
   read-only facts:
   - master `StatusResponse` frontend targets,
   - local `StatusProjection`,
   - replication peer status,
   - durable volume status,
   - synthetic residue inputs.
6. A reviewer caught the valid `lun:0` omission risk; JSON-level tests now keep
   zero-valued frontend identity fields visible.

## Important Non-Claims

- This phase does not deliver a block/data snapshot.
- This phase does not deliver an operator.
- This phase does not add force-detach or cleanup commands.
- This phase does not claim production HA.
- This phase does not change the beta milestone suite.
- The operations status report is read-only evidence, not authority or a
  mutation precondition by itself.

## Follow-Up Decisions

- Move from pure status-report builder to a read-only collection surface.
- Keep the current direct DTO imports acceptable for the seed, but consider a
  thinner schema/input DTO boundary if the operations package grows.
- Continue lowering expensive suite assertions into fast component gates when
  failures are concrete and componentizable.
- Reserve QA for long suite repeatability, trust-critical validation, and
  ambiguous lab behavior.
