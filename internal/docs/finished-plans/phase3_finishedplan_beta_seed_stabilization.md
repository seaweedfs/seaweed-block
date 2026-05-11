# Finished Plan: Beta Seed Stabilization And Cost Reduction

Status: historical reference. Closed on 2026-05-11 by two back-to-back
`beta-hardening-gate` PASS runs on real hardware.

This phase turned the first green beta-hardening seed suite into a repeatable
milestone gate and produced the first cost-reduction targets for the next
iteration.

Current work is tracked in `../current-plan.md`.

## Close Evidence

The repeatability claim is tied to this exact product/runner pair:

- Product commit:
  `8822f20e91c2b88727ead9e49f9bf75eec28c791`
- Runner commit:
  `cf65daaf2ce5cf500e1efa48b411f7cb66dbac0b`
- Suite:
  `testops/suites/beta-hardening-gate.yaml`
- Validator:
  `swblock validate-bundle --profile beta-hardening --expect-commit 8822f20`

Back-to-back QA runs:

| Run | Run ID | Result | Children | Wall Clock | Bundle Validation |
| --- | --- | --- | --- | ---: | --- |
| 1 | `20260511-031605-8258` | PASS | 10/10 | `21m46s` | VALID |
| 2 | `20260511-040412-ac38` | PASS | 10/10 | `22m55s` | VALID |

No manual cleanup was performed between runs. The second run's pre-clean and
the final `cleanup-residue` child both passed. QA reported no residue after the
second run.

## Delivery Gate Result

All close criteria passed:

1. `beta-hardening-gate` passed twice back-to-back without manual cleanup.
2. The second bundle validated with `--profile beta-hardening`.
3. Both runs recorded the same product commit and runner commit.
4. The final `cleanup-residue` child passed.
5. No repeatability failure required a scoped product follow-up.
6. The beta suite cost map was documented with concrete componentization
   targets.

## Cost Map

Reference: `../ref/beta-hardening-suite-cost-map.md`.

The first green suite cost was `1305.86s` (`21m46s`). Top drivers:

| Child | Wall Clock | Share | Decision |
| --- | ---: | ---: | --- |
| `iscsi-p8-compat-soak` | `659s` | `50.5%` | Keep as release/periodic soak. |
| `nvme-p5-csi-protocol` | `273s` | `20.9%` | Keep K8s proof; lower protocol-shape checks. |
| `csi-rf1-durable-restart` | `165s` | `12.6%` | Keep K8s restart proof; lower script/render/status checks. |
| `nvme-p4-multipath-failover` | `138s` | `10.6%` | Keep kernel multipath proof. |

Follow-up work already seeded after this repeatability baseline:

- `testops/scenarios/nvme-p5-protocol-component-gate.yaml`
- `testops/scenarios/csi-rf1-durable-restart-component-gate.yaml`

Those commits are forward work after the repeatability claim. They do not alter
the historical `8822f20` beta milestone evidence.

## What Closed

- `beta-hardening-gate` is now a repeatable milestone readiness suite.
- The suite's provenance is defensible:
  - product commit comes from child evidence,
  - runner commit comes from Go build metadata,
  - missing or mixed child product evidence fails the suite.
- The suite has a final cleanup-residue child and no longer relies on implicit
  operator cleanup.
- Cost-reduction targets are explicit enough to drive the next active plan.

## Important Non-Claims

- This is not production HA.
- This is not broad distro or kernel compatibility.
- This is not performance readiness.
- This is not an operator.
- This is not a cloud-scale or multi-node AWS claim.
- This does not replace user feedback or an iterative release program.

## Follow-Up Decisions

- Keep `beta-hardening-gate` as the milestone suite.
- Keep `protocol-release-gate` as the smaller frontend protocol regression
  suite.
- Do not use the 20-23 minute beta suite as the default developer loop.
- Promote fast component gates for known protocol/lifecycle shape contracts.
- Start operations-layer work from contracts and diagnostics before operator
  implementation.

