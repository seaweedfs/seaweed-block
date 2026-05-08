# V3 TestOps Scenarios

This directory holds product-owned scenario content for the standalone
`sw-test-runner` stack.

The intended boundary is:

- Product repo owns the test intent: which scripts or checks represent the
  product gate, what artifacts prove each hop, and what cleanup must be true.
- Standalone runner owns orchestration: target cleanup, command execution,
  build/import coordination, progress checks, run-bundle layout, and artifact
  collection.
- QA can run the same YAML locally or from CI; developers can run it before
  pushing to avoid stale-image / stale-process / scattered-artifact loops.

Example:

```bash
swblock run testops/scenarios/nvme-p5-csi-protocol-chain.yaml \
  --env product_root=/path/to/seaweed_block/on/m02
```

The scenario still shells out to existing bash payloads. That is deliberate:
the first migration step is to move orchestration and evidence collection into
the runner without rewriting the product smoke tests.
