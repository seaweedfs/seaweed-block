# Workspace owner-label model: round 4 pins

This is a citation of claude06's external model and its reported simulations,
not an exhaustive public-spec result. Source bundle:
`V:/share/rdma/claude06-sw-tla-regrant-r0-20261122/`.

`WorkspaceRegrant.tla` round 4 maps `StreamLabelFromStore = FALSE` to
`Publisher.captureAssignmentOwner` -> immutable `AssignmentInfo` ->
`services.assignmentFact` -> `Host.workspaceLineOwner`. The lease view derives
the same owner metadata (`INV-LEASE-VIEW-001`). A moved unknown owner is empty;
only a move-less line falls back to placement (`INV-AUTH-HOLDER-IDENTITY-001`).
The TRUE switch injects a send-time owner store that can lag. This is a model
fault schedule, not evidence of a reachable pre-issued publication in current
Go code; the real P5 trace showed the old host refusing both foreign facts.

## Results and limits

Status: **SIMULATION ONLY**. `summary-r4.tsv` reports 3 seeds x 20,000 traces,
depth 120, with these exact configurations:

| Configuration | Reported result |
|---|---|
| `WR_id4_fix_label.cfg` | No violation in the simulations |
| `WR_id4_fix_label_ownline.cfg` | No violation in the simulations |
| `WR_id4_RED_label.cfg` | `NoForeignAdoption` violated at traces 14, 17, 15 |
| `WR_id4_RED_label_safety.cfg` | No safety-invariant violation in the simulations; lease layer stops the adopter |
| `WR_id4_fix_label_volguard.cfg` | `NoForeignAdoption` still violated at traces 14, 17, 15; a fresh process has no recorded foreign line |

The result interpretation is in `REGRANT-REPORT.md`, "Round 4: adoption fence".
A simulation finding is a counterexample; no sampled violation does not prove
an invariant. Exhaustive round 4 is **PENDING** in claude06's next Azure slot,
bundle `combo10`, results expected under
`V:/share/rdma/claude06-tlc-azure-results-20261120/tlc-combo10-*`.
Date depends on the open budget decision. No exhaustive state count or GO claim
is made here. This pending model is not added to the Exhausted bounds table.

## Exact source pins

| Artifact (relative to source bundle) | SHA-256 |
|---|---|
| `WorkspaceRegrant.tla` | `d92922331afa2bb127d4f29b9d6fc266354c30b8d11a9a0bd3931e67d2c27d1c` |
| `cfg/WR_id4_fix_label.cfg` | `d716ed5a9d4fce039e2dda084901c789f85f5a44e03d2db28d30a17312ad199f` |
| `cfg/WR_id4_fix_label_ownline.cfg` | `49188ae6776114c9c4358bac0e0dbf8187622feba25a69c1aaf97ed0da8bcdc1` |
| `cfg/WR_id4_RED_label.cfg` | `d22cf52f84bc6b2eed4f0696e4f84285daba2fe5ad0111bbae3358da7a3d1d50` |
| `cfg/WR_id4_RED_label_safety.cfg` | `ca1b0b66fdeb4fef31d43a5efb4e3bb5ef501270f1a6441c8250fb084d4f0e69` |
| `cfg/WR_id4_fix_label_volguard.cfg` | `86fad07c3a9e321ea5dd52a1e4b104c61bdc34a3ce08b4bc123d9b2d97e47f65` |
| `summary-r4.tsv` | `4ca8155ff7c3922d5ff6b0880c41ef59ef13169cc1af9cc630f7fd06256cdbd3` |
| `REGRANT-REPORT.md` | `eab94a6d2e5d81ad9318cb56a90b77958b1a73719258c9500cf71f9c95fc286a` |
