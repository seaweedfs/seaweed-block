-------------------------- MODULE RetentionGCTiming --------------------------
(***************************************************************************)
(* S9 (first slice): GC timing with an explicit clock (grace rule).       *)
(* One snapshot/fork copy operation reads HEAD at t0 (chunk k is rooted by *)
(* HEAD), may write its copy intent naming k at any time before its        *)
(* deadline t0 + D, and publishes before the deadline or refuses           *)
(* (catalog.go:891-899). The host may drop k from HEAD at any time.        *)
(* GC runs marks that take up to MD ticks: per volume it reads HEAD and    *)
(* snapshots first and lists intents last (gc.go:157, 162, 179, 188). A    *)
(* chunk                                                                   *)
(* is deleted only when unrooted at a mark whose start is >= Grace after   *)
(* the start of the first mark that saw it unrooted (two-mark rule,        *)
(* gc.go:262-300). The code refuses Grace <= deadline (gc.go:89) and a     *)
(* mark with duration + deadline >= Grace (gc.go:255).                     *)
(***************************************************************************)
EXTENDS Integers, TLC

CONSTANTS D, G, MD, MaxT,
          MarkGuard     \* TRUE (code): a mark whose duration + D >= G is discarded (gc.go:255)

VARIABLES t, headHasK, op, gc, cand, deleted, published
vars == <<t, headHasK, op, gc, cand, deleted, published>>

Init == /\ t = 0 /\ headHasK = TRUE
        /\ op = [st |-> "idle", t0 |-> 0]
        /\ gc = [st |-> "idle", start |-> 0, sawRoot |-> FALSE]
        /\ cand = -1            \* start of the first mark that saw k unrooted (-1: none)
        /\ deleted = FALSE /\ published = FALSE

Tick == t < MaxT /\ t' = t + 1 /\ UNCHANGED <<headHasK, op, gc, cand, deleted, published>>
\* the host's next cut drops k from HEAD
Drop == headHasK /\ headHasK' = FALSE /\ UNCHANGED <<t, op, gc, cand, deleted, published>>

\* the copy op reads HEAD (k is a root while it is in HEAD)
OpRead == op.st = "idle" /\ headHasK /\ op' = [st |-> "read", t0 |-> t] /\ UNCHANGED <<t, headHasK, gc, cand, deleted, published>>
OpIntent == op.st = "read" /\ t < op.t0 + D /\ op' = [op EXCEPT !.st = "intent"] /\ UNCHANGED <<t, headHasK, gc, cand, deleted, published>>
\* deadline re-checked immediately before the publish; the snapshot then roots k
OpPublish == op.st = "intent" /\ t < op.t0 + D
             /\ op' = [op EXCEPT !.st = "done"] /\ published' = TRUE
             /\ UNCHANGED <<t, headHasK, gc, cand, deleted>>
OpRefuse == op.st \in {"read", "intent"} /\ t >= op.t0 + D
            /\ op' = [op EXCEPT !.st = "refused"] /\ UNCHANGED <<t, headHasK, gc, cand, deleted, published>>

IntentRoot == op.st = "intent"
SnapRoot == published

\* mark: (1) read HEAD and snapshots ...
MarkStart == gc.st = "idle" /\ gc' = [st |-> "listed", start |-> t, sawRoot |-> headHasK \/ SnapRoot]
             /\ UNCHANGED <<t, headHasK, op, cand, deleted, published>>
\* ... (2) list intents, at most MD ticks later; decide k
MarkEnd == /\ gc.st = "listed"
           /\ LET rooted == gc.sawRoot \/ IntentRoot
                  late == t - gc.start > MD                     \* the mark took longer than MD
                  void == late \/ (MarkGuard /\ (t - gc.start) + D >= G)
              IN IF void
                 THEN /\ cand' = -1 /\ deleted' = deleted       \* a failed mark discards coverage
                 ELSE IF rooted
                      THEN /\ cand' = -1 /\ deleted' = deleted
                      ELSE IF cand >= 0 /\ gc.start - cand >= G
                           THEN /\ deleted' = TRUE /\ cand' = cand
                           ELSE /\ cand' = (IF cand >= 0 THEN cand ELSE gc.start) /\ deleted' = deleted
           /\ gc' = [st |-> "idle", start |-> 0, sawRoot |-> FALSE]
           /\ UNCHANGED <<t, headHasK, op, published>>

Next == Tick \/ Drop \/ OpRead \/ OpIntent \/ OpPublish \/ OpRefuse \/ MarkStart \/ MarkEnd
Spec == Init /\ [][Next]_vars

\* GC never deletes a chunk that a published snapshot, an intent, or HEAD names
NoLiveDelete == deleted => ~(published \/ IntentRoot \/ headHasK)
W_Publish == ~published
W_Delete == ~deleted
=============================================================================
