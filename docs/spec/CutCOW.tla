------------------------------- MODULE CutCOW  -------------------------------
(***************************************************************************)
(* S4b: copy-before-write (COW) in the cut window (roadmap 1b), against    *)
(* today's barrier cut. Proposed design, not implemented (2026-11-18).    *)
(*                                                                         *)
(* Today (Mode "barrier", chunk_layer.go captureLocked, main afe1d32d):    *)
(* the capture barrier is held while every candidate chunk is copied, so   *)
(* no write lands between H and the end of the copy (the stall: 1.50 s at  *)
(* 3 GiB dirty, lab measurement).                                         *)
(* COW (Mode "cow"): the barrier only fixes H (the cut's Sync) and the     *)
(* candidate set D, then releases. A write to a chunk of D that is not     *)
(* yet copied first saves that chunk's current (= at-H) content to a side  *)
(* file; the copier reads the side file if present, else the live chunk.   *)
(* The side file is local and is dropped after the HEAD CAS and on crash.  *)
(*                                                                         *)
(* Content is abstracted to "the LSN of the write a chunk holds", as in    *)
(* CutCapture. vis = what the read path returns (WAL over extent); ext =   *)
(* the extent (the flusher moves vis to ext).                              *)
(*                                                                         *)
(* Mut (each must violate a safety invariant; "none" must hold):           *)
(*  noCopy       a write in the window never saves the old content         *)
(*  copyAfter    the side file is written with the NEW content             *)
(*  sideReuse    the side file is not dropped after the CAS                *)
(*  earlyRelease writes are admitted between fixing H and fixing D         *)
(*  copyAtFlush  the old content is saved when the flusher overwrites the  *)
(*               extent, not when the write becomes visible                *)
(*  noChunkLock  (r1) a write proceeds while its chunk is being copied     *)
(* r1: the copy of one chunk is two steps (CopyStart, CopyFinish): a real  *)
(* copy reads 1,024 blocks. The fix: a write to a chunk in "copying" waits. *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Chunks, MaxLSN, MaxCuts, MaxCrash, Mode, Mut

NONE == -1
Zero == [c \in Chunks |-> 0]
NoSide == [c \in Chunks |-> NONE]
Idle == [pc |-> "idle", H |-> 0, D |-> {}, copied |-> {}, copying |-> {}, buf |-> Zero]

VARIABLES hist,     \* the current lineage: hist[i] = the chunk written at LSN i
          synced,   \* highest synced LSN
          vis,      \* read-visible content per chunk
          ext,      \* extent content per chunk
          side,     \* side file: saved at-H content per chunk, or NONE
          head,     \* published HEAD: [lsn, img]
          cut,      \* the cut in progress
          dirty,    \* chunks changed since HEAD (the dirty set)
          cuts, crashes,
          crashedWithSide   \* ghost: a crash dropped a non-empty side file
vars == <<hist, synced, vis, ext, side, head, cut, dirty, cuts, crashes, crashedWithSide>>

VA(h, c, L) == LET ws == {i \in 1..L : i <= Len(h) /\ h[i] = c}
               IN IF ws = {} THEN 0 ELSE CHOOSE m \in ws : \A k \in ws : k <= m
ValueAt(c, L) == VA(hist, c, L)
WrittenAfter(h, L) == {h[i] : i \in {j \in (L + 1)..Len(h) : j >= 1}}

Init == /\ hist = <<>> /\ synced = 0 /\ vis = Zero /\ ext = Zero /\ side = NoSide
        /\ head = [lsn |-> 0, img |-> Zero] /\ cut = Idle /\ dirty = {}
        /\ cuts = 0 /\ crashes = 0 /\ crashedWithSide = FALSE

\* Write admission. "barrier": no write from CutBegin until the CAS. "cow": no write while H and D are
\* being fixed (pc "barrier"); mutant earlyRelease admits writes there too.
Blocked == \/ Mode = "barrier" /\ cut.pc \in {"barrier", "copying"}
           \/ Mode = "cow" /\ cut.pc = "barrier" /\ Mut # "earlyRelease"

Write(c) ==
    /\ Len(hist) < MaxLSN /\ ~Blocked
    /\ ~(Mode = "cow" /\ c \in cut.copying /\ Mut # "noChunkLock")   \* r1: the per-chunk wait
    /\ LET n == Len(hist) + 1
           inWin == cut.pc = "copying" /\ c \in cut.D \ (cut.copied \cup cut.copying)
           save == Mode = "cow" /\ inWin /\ side[c] = NONE /\ Mut \notin {"noCopy", "copyAtFlush"}
       IN /\ hist' = Append(hist, c)
          /\ side' = IF save THEN [side EXCEPT ![c] = IF Mut = "copyAfter" THEN n ELSE vis[c]] ELSE side
          /\ vis' = [vis EXCEPT ![c] = n]
          /\ dirty' = dirty \cup {c}
    /\ UNCHANGED <<synced, ext, head, cut, cuts, crashes, crashedWithSide>>

Sync == /\ synced < Len(hist) /\ synced' = Len(hist)
        /\ UNCHANGED <<hist, vis, ext, side, head, cut, dirty, cuts, crashes, crashedWithSide>>

\* The flusher writes the visible content to the extent. Mutant copyAtFlush saves the extent's old
\* content here (too late: the read path already returns the new write).
Flush(c) ==
    /\ ext[c] # vis[c]
    /\ ext' = [ext EXCEPT ![c] = vis[c]]
    /\ side' = IF Mode = "cow" /\ Mut = "copyAtFlush" /\ cut.pc = "copying" /\ c \in cut.D \ cut.copied /\ side[c] = NONE
               THEN [side EXCEPT ![c] = ext[c]] ELSE side
    /\ UNCHANGED <<hist, synced, vis, head, cut, dirty, cuts, crashes, crashedWithSide>>

\* The barrier: the cut's own Sync fixes H.
CutBegin == /\ cut.pc = "idle" /\ cuts < MaxCuts /\ Len(hist) > head.lsn
            /\ synced' = Len(hist)
            /\ cut' = [Idle EXCEPT !.pc = "barrier", !.H = Len(hist)]
            /\ UNCHANGED <<hist, vis, ext, side, head, dirty, cuts, crashes, crashedWithSide>>

\* D is fixed; in "cow" the barrier is released here.
FixD == /\ cut.pc = "barrier"
        /\ cut' = [cut EXCEPT !.pc = "copying", !.D = dirty, !.copied = {}]
        /\ UNCHANGED <<hist, synced, vis, ext, side, head, dirty, cuts, crashes, crashedWithSide>>

\* The copier reads one candidate: the side file if present, else the live chunk (the normal read path).
CopyStart(c) == /\ cut.pc = "copying" /\ c \in cut.D \ (cut.copied \cup cut.copying)
                /\ cut' = [cut EXCEPT !.copying = @ \cup {c}]
                /\ UNCHANGED <<hist, synced, vis, ext, side, head, dirty, cuts, crashes, crashedWithSide>>
\* the read completes: the side file if present, else the live chunk as it is now
CopyFinish(c) == /\ c \in cut.copying
                 /\ cut' = [cut EXCEPT !.copying = @ \ {c}, !.copied = @ \cup {c},
                                       !.buf[c] = IF side[c] # NONE THEN side[c] ELSE vis[c]]
                 /\ UNCHANGED <<hist, synced, vis, ext, side, head, dirty, cuts, crashes, crashedWithSide>>

\* Upload + HEAD CAS at H; the dirty set keeps the chunks written after H; the side file is dropped.
CutCAS == /\ cut.pc = "copying" /\ cut.copied = cut.D /\ cut.copying = {}
          /\ head' = [lsn |-> cut.H, img |-> [c \in Chunks |-> IF c \in cut.D THEN cut.buf[c] ELSE head.img[c]]]
          /\ dirty' = WrittenAfter(hist, cut.H)
          /\ side' = IF Mut = "sideReuse" THEN side ELSE NoSide
          /\ cut' = Idle
          /\ cuts' = cuts + 1
          /\ UNCHANGED <<hist, synced, vis, ext, crashes, crashedWithSide>>

\* Power loss: unsynced writes are gone; the cut in flight and the side file are dropped; recovery
\* rebuilds content and the dirty set from the synced prefix (WAL replay), never from the side file.
Crash == /\ crashes < MaxCrash
         /\ LET h == SubSeq(hist, 1, synced)
                img == [c \in Chunks |-> VA(h, c, Len(h))]
            IN /\ hist' = h /\ vis' = img /\ ext' = img
               /\ dirty' = WrittenAfter(h, head.lsn)
         /\ side' = NoSide /\ cut' = Idle
         /\ crashes' = crashes + 1
         /\ crashedWithSide' = (crashedWithSide \/ side # NoSide)
         /\ UNCHANGED <<synced, head, cuts>>

Next == \/ \E c \in Chunks : Write(c) \/ Flush(c) \/ CopyStart(c) \/ CopyFinish(c)
        \/ Sync \/ CutBegin \/ FixD \/ CutCAS \/ Crash
Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* HEAD is the exact image of the lineage at HEAD.lsn: every uploaded chunk is its content as of H.
HeadImage == \A c \in Chunks : head.img[c] = ValueAt(c, head.lsn)
\* Every copied candidate holds its content as of H (the direct form of HeadImage, before the CAS).
BufFrozen == \A c \in cut.copied : cut.buf[c] = ValueAt(c, cut.H)
\* A saved side-file entry holds the chunk's content as of H.
SideFrozen == cut.pc = "copying" => \A c \in Chunks : side[c] # NONE => side[c] = ValueAt(c, cut.H)
\* The side file exists only inside a cut window.
SideOnlyInCut == cut.pc = "idle" => side = NoSide
\* COW never changes what a read returns: every chunk shows its newest write.
ReadsNewest == \A c \in Chunks : vis[c] = ValueAt(c, Len(hist))
\* HEAD never covers more than is synced.
HeadLeSynced == head.lsn <= synced /\ head.lsn <= Len(hist)
TypeOK == /\ cut.pc \in {"idle", "barrier", "copying"} /\ cut.copied \subseteq cut.D /\ cut.copying \subseteq cut.D /\ cut.copied \cap cut.copying = {} /\ cut.D \subseteq Chunks

\* Witnesses (expected VIOLATED = reachable, for non-vacuity):
\* a write landed in a candidate chunk before it was copied, and the side file took the old content
W_WriteInWindow == ~(Mode = "cow" /\ cut.pc = "copying" /\ \E c \in cut.D \ cut.copied : side[c] # NONE)
\* a cut is ready to CAS with at least one candidate taken from the side file
W_CASAfterWindowWrite == ~(Mode = "cow" /\ cut.pc = "copying" /\ cut.copied = cut.D /\ side # NoSide)
\* a crash dropped a non-empty side file (and HeadImage still holds afterwards)
W_CrashDroppedSide == ~crashedWithSide
=============================================================================
