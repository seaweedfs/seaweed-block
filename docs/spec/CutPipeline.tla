----------------------------- MODULE CutPipeline -----------------------------
(***************************************************************************)
(* S4 (remainder): the cut pipeline at main 1e8b0e9d.                      *)
(*  - precopy rounds upload chunks before the barrier, tagged with the     *)
(*    chunk's mutation generation (chunkMut, chunk_layer.go:107-111, 349); *)
(*    at the barrier a precopied upload is reused only if the generation   *)
(*    is unchanged, else the chunk is copied under the barrier;            *)
(*  - two-level manifest: each segment object lists its chunks; a segment  *)
(*    is rewritten iff one of its chunks is in the cut's set, else the old *)
(*    segment object is reused; the root (HEAD) CAS publishes them         *)
(*    (#151, docs/chunk-manifests.md);                                     *)
(*  - restart without a bound bitmap starts reconciling (hashMode): every  *)
(*    chunk is compared against HEAD; cuts are refused ("reconciling")     *)
(*    until the compare finishes (chunk_layer.go:95);                     *)
(*  - a crash before the root CAS leaves only orphan uploads.              *)
(* Per-object upload retries retain the same key/bytes and intent. Failed *)
(* attempts make no abstract state change (Spec permits stuttering); the  *)
(* atomic Cut below still requires all objects before HEAD publication.  *)
(* Backoff/deadline timing and ambiguous-reply byte checks are qualified  *)
(* by native TestChunkCut*Upload* controls, not by this abstraction.       *)
(* Chunk content = the LSN of the write it holds.                          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS MaxLSN, MaxCuts, MaxPre,
          GenCheck,        \* RED FALSE: reuse a precopied upload without comparing generations
          SegRewrite,      \* RED FALSE: reuse the old segment object even if a chunk in it changed
          RefuseReconcile  \* RED FALSE: allow a cut while the reconcile compare is incomplete

Chunks == {"a", "b", "c"}
Segs == {"s1", "s2"}
SegOf == [c \in Chunks |-> IF c = "c" THEN "s2" ELSE "s1"]

VARIABLES hist, mut, dirty, pre, head, recon, cuts, pres, restarted
vars == <<hist, mut, dirty, pre, head, recon, cuts, pres, restarted>>

Val(c, L) == LET ws == {i \in 1..L : hist[i] = c} IN IF ws = {} THEN 0 ELSE CHOOSE m \in ws : \A k \in ws : k <= m
Cur(c) == Val(c, Len(hist))
None == [v |-> -1, g |-> -1]
Init == /\ hist = <<>> /\ mut = [c \in Chunks |-> 0] /\ dirty = {}
        /\ pre = [c \in Chunks |-> None]
        /\ head = [lsn |-> 0, img |-> [c \in Chunks |-> 0], seg |-> [s \in Segs |-> [c \in Chunks |-> 0]]]
        /\ recon = [c \in Chunks |-> TRUE]      \* TRUE: this chunk's dirty state is known
        /\ cuts = 0 /\ pres = 0 /\ restarted = FALSE

Write(c) == /\ Len(hist) < MaxLSN
            /\ hist' = Append(hist, c) /\ mut' = [mut EXCEPT ![c] = @ + 1] /\ dirty' = dirty \cup {c}
            /\ UNCHANGED <<pre, head, recon, cuts, pres, restarted>>

\* precopy: upload the chunk's current content outside the barrier, tagged with its generation
Precopy(c) == /\ c \in dirty /\ pres < MaxPre
              /\ pre' = [pre EXCEPT ![c] = [v |-> Cur(c), g |-> mut[c]]]
              /\ pres' = pres + 1
              /\ UNCHANGED <<hist, mut, dirty, head, recon, cuts, restarted>>

\* restart with a lost/unbound bitmap: dirty state unknown, reconcile every chunk against HEAD
Restart == /\ ~restarted
           /\ recon' = [c \in Chunks |-> FALSE] /\ dirty' = {} /\ pre' = [c \in Chunks |-> None]
           /\ restarted' = TRUE
           /\ UNCHANGED <<hist, mut, head, cuts, pres>>
\* the bounded background compare: one chunk against HEAD
Reconcile(c) == /\ ~recon[c]
                /\ recon' = [recon EXCEPT ![c] = TRUE]
                /\ dirty' = IF Cur(c) # head.img[c] THEN dirty \cup {c} ELSE dirty
                /\ UNCHANGED <<hist, mut, pre, head, cuts, pres, restarted>>

\* the barrier + capture + manifest + root CAS, as one step (a crash before it leaves orphans only)
Cut == /\ cuts < MaxCuts /\ Len(hist) > head.lsn
       /\ RefuseReconcile => \A c \in Chunks : recon[c]
       /\ LET L == Len(hist)
              D == dirty
              up(c) == IF c \notin D THEN head.img[c]
                       ELSE IF pre[c] # None /\ (~GenCheck \/ pre[c].g = mut[c]) THEN pre[c].v
                       ELSE Val(c, L)
              img == [c \in Chunks |-> up(c)]
              seg == [s \in Segs |-> IF SegRewrite /\ \E c \in D : SegOf[c] = s
                                      THEN [c \in Chunks |-> IF SegOf[c] = s THEN img[c] ELSE 0]
                                      ELSE IF ~SegRewrite /\ head.lsn = 0
                                      THEN [c \in Chunks |-> IF SegOf[c] = s THEN img[c] ELSE 0]
                                      ELSE head.seg[s]]
          IN /\ head' = [lsn |-> L, img |-> img, seg |-> seg]
             /\ dirty' = {}                   \* finishCut: no write lands inside the atomic cut
             /\ pre' = [c \in Chunks |-> None]
       /\ cuts' = cuts + 1
       /\ UNCHANGED <<hist, mut, recon, pres, restarted>>

Next == (\E c \in Chunks : Write(c) \/ Precopy(c) \/ Reconcile(c)) \/ Restart \/ Cut
Spec == Init /\ [][Next]_vars

\* HEAD (root and every segment) is the exact image of the history at HEAD.lsn
HeadImage == \A c \in Chunks : head.img[c] = Val(c, head.lsn)
SegConsistent == \A c \in Chunks : head.seg[SegOf[c]][c] = head.img[c]
W_TwoCuts == cuts < 2
=============================================================================
