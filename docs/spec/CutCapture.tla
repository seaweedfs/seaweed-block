------------------------------ MODULE CutCapture ------------------------------
(***************************************************************************)
(* S4 (with the S6 failover slice): which chunks must the next cut upload? *)
(* Compares the dirty BITMAP (core/storage/chunk_layer.go at 1e8b0e9d)     *)
(* with a proposed per-chunk LAST-WRITE-LSN map, across RF=1 restart,      *)
(* crash mid-cut, and RF=2 failover to a replica.                          *)
(*                                                                         *)
(* Writes get LSN = position in `hist` (the current lineage). A cut takes  *)
(* a barrier at frontier L, captures the chunks it believes changed since  *)
(* HEAD at their value as of L, then CASes HEAD {lsn L, image}. Chunk      *)
(* content is abstracted to "the LSN of the write it holds".               *)
(*                                                                         *)
(* Track:                                                                  *)
(*  "bitmap"      dirty set in memory; persisted before a WAL checkpoint   *)
(*                passes its entries (BeforeCheckpointAdvance, :353-361);  *)
(*                recovery = persisted set + chunks of WAL entries above   *)
(*                the checkpoint; after a cut, bits stay only for chunks   *)
(*                written after the barrier (finishCut :1542-1559). Not    *)
(*                replicated: a promoted replica starts with an empty set. *)
(*  "bitmapHash"  as bitmap, but a node whose bitmap is not bound to the   *)
(*                current HEAD starts in hashMode (:95, :186-216): every   *)
(*                chunk is a candidate for the next cut.                   *)
(*  "lsnmap"      lastLSN[c] = LSN of the last write to c; replicas apply  *)
(*                the same writes with the same LSNs; dirty-since-HEAD =   *)
(*                {c : lastLSN[c] > HEAD.lsn}; persisted like the bitmap.  *)
(*                A BASE-rebuilt replica seeds lastLSN = base for all.     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Chunks, MaxLSN, MaxCuts, Track, Mode,
          PersistBeforeCkpt,   \* RED FALSE: checkpoint passes entries before the dirty state is durable
          ClearAtCapture,      \* RED TRUE: clear dirty state at the barrier, before the HEAD CAS
          ReplDurableOnly,     \* TRUE: in rf2 a cut covers only the replicated-durable frontier
          BaseLSN              \* rf2: R was BASE-rebuilt at this LSN (0: R replicated from the start)

Nodes == {"P", "R"}
Zero == [c \in Chunks |-> 0]

VARIABLES hist, prim, alive, applied, synced, ckpt,
          dmem, ddisk, mmem, mdisk,   \* bitmap (mem/disk) and lsn map (mem/disk) per node
          bound,                      \* the persisted dirty state is bound to the current HEAD (bitmap header)
          head, cut, cuts, events
vars == <<hist, prim, alive, applied, synced, ckpt, dmem, ddisk, mmem, mdisk, bound, head, cut, cuts, events>>

ValueAt(c, L) == LET ws == {i \in 1..L : i <= Len(hist) /\ hist[i] = c} IN IF ws = {} THEN 0 ELSE CHOOSE m \in ws : \A k \in ws : k <= m
Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A s \in S : s <= m
Min2(a, b) == IF a < b THEN a ELSE b

Init ==
    /\ hist = [i \in 1..BaseLSN |-> CHOOSE c \in Chunks : TRUE]
    /\ prim = "P" /\ alive = [n \in Nodes |-> TRUE]
    /\ applied = [n \in Nodes |-> BaseLSN] /\ synced = [n \in Nodes |-> BaseLSN] /\ ckpt = [n \in Nodes |-> BaseLSN]
    /\ dmem = [n \in Nodes |-> IF n = "P" /\ BaseLSN > 0 THEN {CHOOSE c \in Chunks : TRUE} ELSE {}]
    /\ ddisk = dmem
    /\ mmem = [n \in Nodes |-> IF n = "P" /\ BaseLSN > 0 THEN [c \in Chunks |-> ValueAt(c, BaseLSN)]
                                ELSE IF BaseLSN > 0 THEN [c \in Chunks |-> BaseLSN] ELSE Zero]
    /\ mdisk = mmem
    /\ bound = [n \in Nodes |-> n = "P"]
    /\ head = [lsn |-> 0, img |-> Zero]
    /\ cut = [pc |-> "idle", L |-> 0, D |-> {}, cap |-> Zero]
    /\ cuts = 0
    /\ events = 0

Repl == Mode = "rf2"

\* a VM write on the primary (WALStore write -> chunk layer OnExtentWrite, :299-310)
Write(c) == /\ Len(hist) < MaxLSN /\ alive[prim]
            /\ hist' = Append(hist, c)
            /\ applied' = [applied EXCEPT ![prim] = Len(hist) + 1]
            /\ dmem' = [dmem EXCEPT ![prim] = @ \cup {c}]
            /\ mmem' = [mmem EXCEPT ![prim][c] = Len(hist) + 1]
            /\ UNCHANGED <<prim, alive, synced, ckpt, ddisk, mdisk, bound, head, cut, cuts, events>>

\* the replica applies the next shipped write with the same LSN (ApplyEntry)
ReplApply == /\ Repl /\ alive["R"] /\ prim = "P" /\ applied["R"] < Len(hist)
             /\ LET i == applied["R"] + 1 IN
                /\ applied' = [applied EXCEPT !["R"] = i]
                /\ mmem' = [mmem EXCEPT !["R"][hist[i]] = i]
             /\ UNCHANGED <<hist, prim, alive, synced, ckpt, dmem, ddisk, mdisk, bound, head, cut, cuts, events>>

Sync(n) == /\ alive[n] /\ synced[n] < applied[n]
           /\ synced' = [synced EXCEPT ![n] = applied[n]]
           /\ UNCHANGED <<hist, prim, alive, applied, ckpt, dmem, ddisk, mmem, mdisk, bound, head, cut, cuts, events>>

\* the flusher passes WAL entries <= synced; the dirty state is saved first (BeforeCheckpointAdvance)
Checkpoint(n) == /\ alive[n] /\ ckpt[n] < synced[n]
                 /\ ckpt' = [ckpt EXCEPT ![n] = synced[n]]
                 /\ ddisk' = IF PersistBeforeCkpt THEN [ddisk EXCEPT ![n] = dmem[n]] ELSE ddisk
                 /\ mdisk' = IF PersistBeforeCkpt THEN [mdisk EXCEPT ![n] = mmem[n]] ELSE mdisk
                 /\ UNCHANGED <<hist, prim, alive, applied, synced, dmem, mmem, bound, head, cut, cuts>>
                 /\ events' = events

\* the dirty state a node can rebuild after a restart: persisted + WAL entries above the checkpoint
WalChunks(n) == {hist[i] : i \in {j \in (ckpt[n] + 1)..synced[n] : j <= Len(hist)}}
WalMap(n) == [c \in Chunks |-> LET ws == {i \in (ckpt[n] + 1)..synced[n] : i <= Len(hist) /\ hist[i] = c}
                               IN IF ws = {} THEN mdisk[n][c] ELSE Max(ws)]

\* RF=1: the primary crashes and restarts; unsynced writes are gone; a cut in flight is lost
Restart == /\ ~Repl /\ events < 1 /\ alive["P"]
           /\ hist' = SubSeq(hist, 1, synced["P"])
           /\ applied' = [applied EXCEPT !["P"] = synced["P"]]
           /\ dmem' = [dmem EXCEPT !["P"] = IF Track = "bitmapHash" /\ ~bound["P"] THEN Chunks ELSE ddisk["P"] \cup WalChunks("P")]
           /\ mmem' = [mmem EXCEPT !["P"] = WalMap("P")]
           /\ cut' = [pc |-> "idle", L |-> 0, D |-> {}, cap |-> Zero]
           /\ events' = events + 1
           /\ UNCHANGED <<prim, alive, synced, ckpt, ddisk, mdisk, bound, head, cuts>>

\* RF=2: P dies, R is promoted with what it applied (acked writes are replicated-durable)
Failover == /\ Repl /\ events < 1 /\ prim = "P"
            /\ prim' = "R" /\ alive' = [alive EXCEPT !["P"] = FALSE]
            /\ hist' = SubSeq(hist, 1, applied["R"])
            /\ dmem' = [dmem EXCEPT !["R"] = IF Track = "bitmapHash" THEN Chunks ELSE {}]   \* R has no bitmap for HEAD
            /\ cut' = [pc |-> "idle", L |-> 0, D |-> {}, cap |-> Zero]
            /\ events' = events + 1
            /\ UNCHANGED <<applied, synced, ckpt, ddisk, mmem, mdisk, bound, head, cuts>>

Frontier == IF Repl /\ prim = "P" /\ ReplDurableOnly THEN Min2(synced["P"], synced["R"]) ELSE synced[prim]
Dirty(n) == IF Track = "lsnmap" THEN {c \in Chunks : mmem[n][c] > head.lsn} ELSE dmem[n]

\* the barrier: frontier L, the believed dirty set, the captured values as of L
CutBegin == /\ cut.pc = "idle" /\ cuts < MaxCuts /\ alive[prim] /\ Frontier > head.lsn
            /\ LET L == Frontier
                   D == Dirty(prim)
               IN /\ cut' = [pc |-> "cas", L |-> L, D |-> D, cap |-> [c \in Chunks |-> IF c \in D THEN ValueAt(c, L) ELSE head.img[c]]]
                  /\ dmem' = IF ClearAtCapture THEN [dmem EXCEPT ![prim] = @ \ D] ELSE dmem
            /\ UNCHANGED <<hist, prim, alive, applied, synced, ckpt, ddisk, mmem, mdisk, bound, head, cuts, events>>

\* the HEAD CAS (refused below HEAD's remote_lsn: lsn_behind), then finishCut keeps the bits of
\* chunks written after the barrier and persists the set bound to the new HEAD
CutCAS == /\ cut.pc = "cas" /\ alive[prim]
          /\ head' = [lsn |-> cut.L, img |-> cut.cap]
          /\ LET still == {c \in Chunks : \E i \in (cut.L + 1)..Len(hist) : hist[i] = c}
             IN dmem' = [dmem EXCEPT ![prim] = IF ClearAtCapture THEN @ ELSE (@ \ cut.D) \cup still]
          /\ bound' = [n \in Nodes |-> n = prim]
          /\ cut' = [pc |-> "idle", L |-> 0, D |-> {}, cap |-> Zero]
          /\ cuts' = cuts + 1
          /\ UNCHANGED <<hist, prim, alive, applied, synced, ckpt, ddisk, mmem, mdisk, events>>

Next == \/ \E c \in Chunks : Write(c)
        \/ ReplApply \/ Restart \/ Failover \/ CutBegin \/ CutCAS
        \/ \E n \in Nodes : Sync(n) \/ Checkpoint(n)
Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* HEAD is the exact image of the current lineage at HEAD.lsn: the cut uploaded every chunk changed since HEAD
HeadImage == \A c \in Chunks : head.img[c] = ValueAt(c, head.lsn)
\* HEAD never covers more than the current primary has durably
HeadLeDurable == ~alive[prim] \/ (head.lsn <= Len(hist) /\ head.lsn <= synced[prim])
W_CutAfterEvent == ~(events > 0 /\ cuts >= 2)
=============================================================================
