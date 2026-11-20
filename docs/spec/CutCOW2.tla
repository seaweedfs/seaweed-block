------------------------------- MODULE CutCOW2 -------------------------------
(***************************************************************************)
(* S4c r2: copy-before-write in the cut window, with the save as I/O and   *)
(* an abort. Proposed design, not implemented (2026-11-18).                *)
(*                                                                         *)
(* As r1 (CutCOW): barrier vs COW; the barrier fixes H and D, then (COW)   *)
(* releases; a chunk copy is CopyStart + CopyFinish and a write to a chunk *)
(* being copied waits. New in r2:                                          *)
(*  - the writer's save is I/O too: SaveStart claims the chunk (state      *)
(*    saving, it reads the at-H content), SaveFinish writes it to the side *)
(*    store. The write itself waits until its chunk is saved. The in-     *)
(*    flight save lives in `pend`, outside the cut record, as real I/O     *)
(*    outlives a cut record that someone else tears down.                  *)
(*  - an abort (save error, cancel, Close): AbortStart invalidates the cut *)
(*    and releases waiters; AbortFinish deletes the side store and ends    *)
(*    the window only once no saving or copying owner is left (the join).  *)
(* Mut:                                                                    *)
(*  noCopy       a write in the window never saves                         *)
(*  noChunkLock  a write proceeds while its chunk is being copied          *)
(*  abortNoJoin  AbortFinish does not wait for saving/copying owners: a    *)
(*               save still in I/O completes into a side store that has    *)
(*               already been deleted, i.e. into whatever window is next   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Chunks, MaxLSN, MaxCuts, MaxCrash, MaxAborts, Mode, Mut

NONE == -1
Zero == [c \in Chunks |-> 0]
NoSide == [c \in Chunks |-> NONE]
Idle == [pc |-> "idle", H |-> 0, D |-> {}, copied |-> {}, copying |-> {}, saving |-> {}, buf |-> Zero]

VARIABLES hist, synced, vis, ext, side, pend, head, cut, dirty, cuts, crashes, aborts
vars == <<hist, synced, vis, ext, side, pend, head, cut, dirty, cuts, crashes, aborts>>

VA(h, c, L) == LET ws == {i \in 1..L : i <= Len(h) /\ h[i] = c}
               IN IF ws = {} THEN 0 ELSE CHOOSE m \in ws : \A k \in ws : k <= m
ValueAt(c, L) == VA(hist, c, L)
WrittenAfter(h, L) == {h[i] : i \in {j \in (L + 1)..Len(h) : j >= 1}}

Init == /\ hist = <<>> /\ synced = 0 /\ vis = Zero /\ ext = Zero /\ side = NoSide /\ pend = NoSide
        /\ head = [lsn |-> 0, img |-> Zero] /\ cut = Idle /\ dirty = {}
        /\ cuts = 0 /\ crashes = 0 /\ aborts = 0

Open == Mode = "cow" /\ cut.pc = "copying"            \* a live COW window (not aborting)
\* c still needs a save before a write to it may publish
NeedSave(c) == Open /\ c \in cut.D \ (cut.copied \cup cut.copying \cup cut.saving) /\ side[c] = NONE

Blocked(c) == \/ Mode = "barrier" /\ cut.pc \in {"barrier", "copying"}
              \/ Mode = "cow" /\ cut.pc = "barrier"
              \/ Open /\ c \in cut.copying /\ Mut # "noChunkLock"     \* wait for the copier
              \/ Open /\ c \in cut.saving                             \* wait for the saving owner
              \/ NeedSave(c) /\ Mut # "noCopy"                        \* a save must come first

Write(c) ==
    /\ Len(hist) < MaxLSN /\ ~Blocked(c)
    /\ LET n == Len(hist) + 1 IN
       /\ hist' = Append(hist, c)
       /\ vis' = [vis EXCEPT ![c] = n]
       /\ dirty' = dirty \cup {c}
    /\ UNCHANGED <<synced, ext, side, pend, head, cut, cuts, crashes, aborts>>

\* a writer becomes the owner of a pending candidate and reads its current (= at-H) content
SaveStart(c) == /\ NeedSave(c) /\ Mut # "noCopy" /\ Len(hist) < MaxLSN /\ pend[c] = NONE
                /\ cut' = [cut EXCEPT !.saving = @ \cup {c}]
                /\ pend' = [pend EXCEPT ![c] = vis[c]]
                /\ UNCHANGED <<hist, synced, vis, ext, side, head, dirty, cuts, crashes, aborts>>

\* the save's I/O completes: into the live window's side store; discarded if the window is aborting;
\* an orphan (only reachable with abortNoJoin) writes into whatever side store exists now
SaveFinish(c) == /\ pend[c] # NONE
                 /\ side' = IF cut.pc = "aborting" THEN side ELSE [side EXCEPT ![c] = pend[c]]
                 /\ cut' = [cut EXCEPT !.saving = @ \ {c}]
                 /\ pend' = [pend EXCEPT ![c] = NONE]
                 /\ UNCHANGED <<hist, synced, vis, ext, head, dirty, cuts, crashes, aborts>>

Sync == /\ synced < Len(hist) /\ synced' = Len(hist)
        /\ UNCHANGED <<hist, vis, ext, side, pend, head, cut, dirty, cuts, crashes, aborts>>

\* Local capture/save excludes materialization of this chunk only. Other
\* chunks remain flushable while the point is open or its owners drain.
Flush(c) == /\ c \notin (cut.copying \cup cut.saving)
            /\ ext[c] # vis[c] /\ ext' = [ext EXCEPT ![c] = vis[c]]
            /\ UNCHANGED <<hist, synced, vis, side, pend, head, cut, dirty, cuts, crashes, aborts>>

CutBegin == /\ cut.pc = "idle" /\ cuts < MaxCuts /\ Len(hist) > head.lsn
            /\ synced' = Len(hist)
            /\ cut' = [Idle EXCEPT !.pc = "barrier", !.H = Len(hist)]
            /\ UNCHANGED <<hist, vis, ext, side, pend, head, dirty, cuts, crashes, aborts>>

FixD == /\ cut.pc = "barrier"
        /\ cut' = [cut EXCEPT !.pc = "copying", !.D = dirty]
        /\ UNCHANGED <<hist, synced, vis, ext, side, pend, head, dirty, cuts, crashes, aborts>>

CopyStart(c) == /\ cut.pc = "copying" /\ c \in cut.D \ (cut.copied \cup cut.copying \cup cut.saving)
                /\ cut' = [cut EXCEPT !.copying = @ \cup {c}]
                /\ UNCHANGED <<hist, synced, vis, ext, side, pend, head, dirty, cuts, crashes, aborts>>

CopyFinish(c) == /\ c \in cut.copying
                 /\ cut' = IF cut.pc = "aborting" THEN [cut EXCEPT !.copying = @ \ {c}]
                           ELSE [cut EXCEPT !.copying = @ \ {c}, !.copied = @ \cup {c},
                                            !.buf[c] = IF side[c] # NONE THEN side[c] ELSE vis[c]]
                 /\ UNCHANGED <<hist, synced, vis, ext, side, pend, head, dirty, cuts, crashes, aborts>>

CutCAS == /\ cut.pc = "copying" /\ cut.copied = cut.D /\ cut.copying = {} /\ cut.saving = {}
          /\ head' = [lsn |-> cut.H, img |-> [c \in Chunks |-> IF c \in cut.D THEN cut.buf[c] ELSE head.img[c]]]
          /\ dirty' = WrittenAfter(hist, cut.H)
          /\ side' = NoSide /\ cut' = Idle /\ cuts' = cuts + 1
          /\ UNCHANGED <<hist, synced, vis, ext, pend, crashes, aborts>>

\* abort: invalidate and release waiters at once; the dirty set is kept (no finishCut)
AbortStart == /\ cut.pc = "copying" /\ aborts < MaxAborts
              /\ cut' = [cut EXCEPT !.pc = "aborting"]
              /\ aborts' = aborts + 1
              /\ UNCHANGED <<hist, synced, vis, ext, side, pend, head, dirty, cuts, crashes>>
\* the join: delete the side store and end the window once no owner is in I/O (abortNoJoin: at once)
AbortFinish == /\ cut.pc = "aborting"
               /\ Mut = "abortNoJoin" \/ (cut.saving = {} /\ cut.copying = {})
               /\ side' = NoSide /\ cut' = Idle
               /\ UNCHANGED <<hist, synced, vis, ext, pend, head, dirty, cuts, crashes, aborts>>

Crash == /\ crashes < MaxCrash
         /\ LET h == SubSeq(hist, 1, synced)
                img == [c \in Chunks |-> VA(h, c, Len(h))]
            IN /\ hist' = h /\ vis' = img /\ ext' = img /\ dirty' = WrittenAfter(h, head.lsn)
         /\ side' = NoSide /\ pend' = NoSide /\ cut' = Idle
         /\ crashes' = crashes + 1
         /\ UNCHANGED <<synced, head, cuts, aborts>>

Next == \/ \E c \in Chunks : Write(c) \/ SaveStart(c) \/ SaveFinish(c) \/ Flush(c) \/ CopyStart(c) \/ CopyFinish(c)
        \/ Sync \/ CutBegin \/ FixD \/ CutCAS \/ AbortStart \/ AbortFinish \/ Crash
Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
HeadImage == \A c \in Chunks : head.img[c] = ValueAt(c, head.lsn)
BufFrozen == \A c \in cut.copied : cut.buf[c] = ValueAt(c, cut.H)
SideFrozen == cut.pc = "copying" => \A c \in Chunks : side[c] # NONE => side[c] = ValueAt(c, cut.H)
SideOnlyInCut == cut.pc = "idle" => side = NoSide
ReadsNewest == \A c \in Chunks : vis[c] = ValueAt(c, Len(hist))
HeadLeSynced == head.lsn <= synced /\ head.lsn <= Len(hist)
TypeOK == /\ cut.pc \in {"idle", "barrier", "copying", "aborting"}
          /\ cut.copied \subseteq cut.D /\ cut.copying \subseteq cut.D /\ cut.saving \subseteq cut.D
          /\ cut.copied \cap cut.copying = {} /\ cut.copying \cap cut.saving = {}

\* Witnesses (expected VIOLATED = reachable):
W_AbortWhileSaving == ~(cut.pc = "aborting" /\ cut.saving # {})
W_SaveUsedByCopier == ~(Open /\ \E c \in cut.copied : side[c] # NONE)
W_CASAfterAbort == ~(aborts >= 1 /\ cuts >= 1)
=============================================================================
