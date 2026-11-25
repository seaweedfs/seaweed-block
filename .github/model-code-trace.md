# Model citations for mapped files

The spec-trace workflow reads `model-code-trace.tsv` and the validator from
the target branch. A PR cannot replace its own mapping or checker.

Each non-comment line has two tab-separated columns: an exact repository
path and `Module.Action`. Repeat a path to list alternative actions that
cover it. Paths are case-sensitive; directories and glob patterns are not
expanded. Missing, empty or malformed mappings refuse the check.

The reviewed map uses the neutral public module names: `SnapshotRemoval`,
`RestoreTarget`, `HolderInterface`, `EngineAdapter`, `StopAndRestart`,
`MoveAdmission`, `Promotion`, `ProbeRound` and `FencingRound`.
Actions name top-level definitions used by the corresponding model's `Next`.
The mapping is the source of accepted citations; private model names and
storage locations do not belong in the public map or PR description.

For every changed mapped path, including deletions, the PR's `Model:` line
must contain a listed action or `NONE (pure function of Module)` for that
module. Separate multiple citations with semicolons, for example:

```text
Model: EngineAdapter.Begin; MoveAdmission.Revoke; NONE (pure function of RestoreTarget)
```

Unmapped paths keep the existing spec-trace policy. The pure-function
declaration is a review claim; CI checks its module, not its truth.

This branch is a trial. Populate the canonical mapping and run the complete
shell self-test on Linux before enabling it by merging the workflow.
