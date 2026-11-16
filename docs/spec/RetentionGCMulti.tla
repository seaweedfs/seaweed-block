--------------------------- MODULE RetentionGCMulti ---------------------------
(***************************************************************************)
(* S9 (remainder): GC marks across two volumes, with a fork and a snapshot *)
(* delete. Chunk k is rooted by P's snapshot M. A fork F <- P/M claims M,  *)
(* writes its intent in F, creates F's HEAD (which names k) and drops its  *)
(* intent and claim (catalog.go:1151-1311). Then M may be deleted          *)
(* (retention or DeleteSnapshot: markDeleting needs no live claim); an     *)
(* unnamed deleting attempt stays a root until Grace after its delete mark *)
(* (gc_copy.go:172-181). A GC mark visits the volumes in ListVolumes order *)
(* (F before P), one step per volume, within MD ticks of its start.        *)
(***************************************************************************)
EXTENDS Integers, TLC

CONSTANTS G, MD, MaxT

VARIABLES t, mst, mdel, fork, gc, cand, deleted
vars == <<t, mst, mdel, fork, gc, cand, deleted>>

Init == /\ t = 0 /\ mst = "active" /\ mdel = 0
        /\ fork = "idle"          \* idle -> claimed -> intent -> headed -> done
        /\ gc = [st |-> "idle", start |-> 0, seenF |-> FALSE]
        /\ cand = -1 /\ deleted = FALSE

Tick == t < MaxT /\ t' = t + 1 /\ UNCHANGED <<mst, mdel, fork, gc, cand, deleted>>
ForkStep == /\ fork # "done" /\ mst = "active"
            /\ fork' = CASE fork = "idle" -> "claimed" [] fork = "claimed" -> "intent"
                       [] fork = "intent" -> "headed" [] OTHER -> "done"
            /\ UNCHANGED <<t, mst, mdel, gc, cand, deleted>>
\* M deleted once no live claim holds it (claim live until F's HEAD exists)
DeleteM == /\ mst = "active" /\ fork \in {"idle", "headed", "done"}
           /\ mst' = "deleting" /\ mdel' = t
           /\ UNCHANGED <<t, fork, gc, cand, deleted>>

RootF == fork \in {"intent", "headed", "done"}            \* F's intent, then F's HEAD
RootP == mst = "active" \/ (mst = "deleting" /\ t - mdel <= G)

MarkF == gc.st = "idle" /\ gc' = [st |-> "F", start |-> t, seenF |-> RootF] /\ UNCHANGED <<t, mst, mdel, fork, cand, deleted>>
MarkP == /\ gc.st = "F" /\ t - gc.start <= MD
         /\ LET rooted == gc.seenF \/ RootP IN
            IF rooted THEN cand' = -1 /\ deleted' = deleted
            ELSE IF cand >= 0 /\ gc.start - cand >= G THEN deleted' = TRUE /\ cand' = cand
            ELSE cand' = (IF cand >= 0 THEN cand ELSE gc.start) /\ deleted' = deleted
         /\ gc' = [st |-> "idle", start |-> 0, seenF |-> FALSE]
         /\ UNCHANGED <<t, mst, mdel, fork>>
\* a mark that overran MD is discarded (no coverage)
MarkVoid == /\ gc.st = "F" /\ t - gc.start > MD
            /\ gc' = [st |-> "idle", start |-> 0, seenF |-> FALSE] /\ cand' = -1
            /\ UNCHANGED <<t, mst, mdel, fork, deleted>>

Next == Tick \/ ForkStep \/ DeleteM \/ MarkF \/ MarkP \/ MarkVoid
Spec == Init /\ [][Next]_vars

\* GC never deletes k while F's HEAD (or intent) or an unexpired M names it
NoLiveDelete == deleted => ~(RootF \/ RootP)
W_Delete == ~deleted
=============================================================================
