# NeverStarted r2: exhaustive results for the never-started delete

A chunk volume whose line was granted but whose HEAD was never claimed can be
deleted (`INV-WS-DELETE-NO-LIVE-HOLDER-001`, `TERM-LIVE-HOLDER-001`). A rerun
of a delete whose token already holds the reservation (or a committed one) is
admitted without a new verdict; the catalog's HEAD compare-and-swap is the
fence. The model checks this head behaviour, the earlier heads it replaces, and
mutations of each guard.

Assumptions (not modelled): one delete process per volume at a time; the gap
between HEAD deleted and the volume marked deleted; an interrupted delete is
re-run by the operator.

## Results

Status: **EXHAUSTIVE** (TLC, every configuration finished). Depth and distinct
state counts are as reported by TLC.

**Must not violate** (the head's behaviour: FixDeleting + FixToken). Invariants
checked: TypeOK, NoTwoHolders, NoValidityBehindLine, NoServedWriteLost,
DeleteNeverStrandsALiveHolder, NoRefusalContradiction,
NoLiveHolderWhileDeleting, RerunCanFinish, RerunCanSettle,
SkipOnlyReplaysADecision.

| Configuration | Distinct states | Depth | Result |
|---|---|---|---|
| `safe_head_nosnap` | 25,831,776 | 50 | No error |
| `safe_head` (snapshot revert on) | 48,737,939 | 50 | No error |

**Must violate** (each fires as expected):

| Configuration | What it is | Violated |
|---|---|---|
| `f1_head_stuck` | earlier head: a delete stopped after its HEAD mark | RerunCanFinish |
| `f1b_fix_deleting_settle` | earlier head: a delete stopped before its mark, then the holder claims | RerunCanSettle |
| `mut_no_head_fence` | the catalog's mark is a blind write | NoServedWriteLost |
| `mut_fix_mark_no_fence` | HEAD marked without its fence | NoLiveHolderWhileDeleting |
| `mut_blind_claim` | a claim that ignores deleting | DeleteNeverStrandsALiveHolder |
| `mut_admit_claimed` | a claimed HEAD admitted | NoServedWriteLost |
| `mut_lc_admits_ns` | the lifecycle delete admits a never-started line | DeleteNeverStrandsALiveHolder |
| `mut_token_any` | the skip for any token while admitted | SkipOnlyReplaysADecision |
| `mut_token_rerun_trusts` | a skipped rerun's catalog step trusts the admission | NoLiveHolderWhileDeleting |
| `mut_mem_restart`, `mut_mem_restart_two_holders` | empty lease memory after restart plus a barrier-less move | NoValidityBehindLine |
| `mut_mem_restart_nth` | the same, NoTwoHolders alone | NoTwoHolders |
| `mut_move_ignores_lease` | a move on HEAD alone while a lease is valid | NoValidityBehindLine |
| `mut_move_ignores_lease_nth` | the same, NoTwoHolders alone | NoTwoHolders |
| `wit_fix_other_token_admitted` | witness: a new token is decided by the verdict | WitnessOtherTokenAdmitted |
| `wit_fix_stale_deleting_admits_live` | witness: a stale deleting read admits; the catalog refuses | WitnessStaleDeletingAdmitsLive |
| `wit_fix_stale_read_joins_deleting` | witness: a stale read joins the mark | WitnessStaleReadJoinsDeleting |
| `wit_fix_rerun_after_mark_finished` | witness: a delete stopped after its mark is finished by a rerun | WitnessRerunAfterMarkFinished |
| `wit_token_rerun_admits_live` | witness: the skip admits over a live HEAD; the catalog gives it back | WitnessRerunAdmittedLive |

21 of 21 configurations as expected. A violated witness shows that the state
it names is reachable (the fix is not vacuous); it is not a defect.
