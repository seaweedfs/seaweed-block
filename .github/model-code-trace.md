# Model citations for mapped files

The spec-trace workflow reads `model-code-trace.tsv` and the validator from
the target branch. A PR cannot replace its own mapping or checker.

Each non-comment line has two tab-separated columns: an exact repository
path and `Module.Action`. Repeat a path to list alternative actions that
cover it. Paths are case-sensitive; directories and glob patterns are not
expanded. Missing, empty or malformed mappings refuse the check.

The reviewed map uses the neutral public module names: `SnapshotRemoval`,
`RestoreTarget`, `HolderInterface`, `EngineAdapter`, `StopAndRestart`,
`MoveAdmission`, `Promotion`, `ProbeRound`, `FencingRound`, `VolumeCreate`
and `ReplicationPeer`.
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

## Adding a model or file

1. Have the model owner confirm its neutral public module name and the
   top-level action definitions used by `Next`. Use those names in public
   code, mapping rows and PR descriptions.
2. Add an exact-path TSV row for each implementation and enforcement file
   that the action covers. Split actions get separate rows. Include receiving
   guards as well as senders; a reviewer checks that each file really enforces
   the cited action. Environment actions without a Go site and mutant test
   obligations are not implementation mappings.
3. Add controls for accepted and refused citations when extending the
   checker. For mapping additions, check the real map against representative
   affected paths. Run `python3 -B .github/scripts/model-trace-check-test.py`
   and `bash .github/scripts/spec-trace-check-test.sh` on Linux.
4. Submit the map and its evidence for review with the model change or before
   its callers land. The PR description cites the applicable public actions.
   The new rows govern later PRs once they are on the target branch; changing
   a PR's own map never changes the check that evaluates that PR.
