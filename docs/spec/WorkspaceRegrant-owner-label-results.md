# Workspace owner-label simulation results

An assignment carries the owner of its published line. Reconstructing that
owner from a later store read can label a line with a different server when
the store view lags. An unknown moved owner remains empty, so the volume
refuses adoption; only a move-less line can use placement as its owner.

`NoForeignAdoption` checks that a process does not adopt another server's line.
`TargetNotFencedByOwnLine` checks that a target is not fenced by its own line
because its owner label was reconstructed incorrectly.

## Results and limits

Status: **SIMULATION ONLY**. The reported search used 3 seeds x 20,000 traces,
depth 120. Counterexample lengths count states, including the initial state;
they are not trace identifiers.

| Configuration | Reported result |
|---|---|
| `WR_id4_fix_label.cfg` | No violation in the simulations |
| `WR_id4_fix_label_ownline.cfg` | No violation in the simulations |
| `WR_id4_RED_label.cfg` | `NoForeignAdoption` violated; counterexamples contain 14, 17, 15 states |
| `WR_id4_RED_label_safety.cfg` | No safety-invariant violation in the simulations; lease validation stops the adopter |
| `WR_id4_fix_label_volguard.cfg` | `NoForeignAdoption` still violated; counterexamples contain 14, 17, 15 states; a fresh process has no recorded foreign line |

A counterexample demonstrates a failure in the model. No sampled violation
does not prove an invariant. Exhaustive checking is **PENDING**; no exhaustive
state count or safety proof is claimed. These results are separate from the
Exhausted bounds table.
