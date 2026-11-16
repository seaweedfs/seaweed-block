----------------------------- MODULE WALRecovery -----------------------------
(***************************************************************************)
(* S3, walstore layout: a ring WAL of K pages in front of an extent.       *)
(* Records of 1 or 2 pages are appended to the page cache, written back to *)
(* the durable image page by page in ARBITRARY order, and made durable as  *)
(* a whole by Sync. The flusher checkpoints the oldest record into the     *)
(* extent and persists the superblock {tail, head, checkpoint}. Then the   *)
(* host crashes at any point, optionally with ONE durable record page      *)
(* corrupted, and recovery runs once.                                      *)
(*                                                                         *)
(* Rule selects recovery:                                                  *)
(*   "current"     core/storage/recovery.go recoverWAL at 5c293821: scan   *)
(*                 each physical range, stop the RANGE at the first decode *)
(*                 failure, continue with the next range, replay all.      *)
(*   "holeFail"    fail closed when any LSN in (checkpoint, max] is missing.*)
(*   "pr155"       PR #155 at 089d2d9 (walstore, warning-only): every range *)
(*                 that stops on a bad entry cuts replay at its decoded     *)
(*                 predecessor + 1 (default checkpoint + 1); scan-order replay.*)
(*   "watermark"   each record carries durable_lsn (highest LSN whose Sync *)
(*                 had completed when it was written). W = max over valid  *)
(*                 records. Contiguous prefix R from checkpoint: R < W ->  *)
(*                 fail closed; else replay only up to R (never past a hole).*)
(*   "watermarkSB" as watermark, plus Sync persists W in the superblock    *)
(*                 (second fsync) before it returns.                       *)
(*   "watermarkFull" watermarkSB, and recovery decodes every page position *)
(*                 instead of stopping a range at its first bad entry.     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS K, MaxLSN, LBAs, Rule, CorruptOn, MaxCkpt,
          CkptLag,      \* TRUE (code): persistCheckpoint writes the PRE-advance tail (walstore.go:402-405, flusher.go:223-227)
          ReuseBound,   \* "mem" (code): the writer reuses space released by the in-memory tail; "persisted": root fix
          JumpOn,       \* an AdvanceFrontier / BASE jump (in memory; persisted by the next Sync, walstore.go:866-898)
          JumpPersistedBySync, \* TRUE: Sync persists the pending jump before it acknowledges; FALSE: the
                       \* rebuild path (AdvanceFrontier stays in memory until MarkBaseComplete + Sync)
          Forge        \* a 2-page record's payload page may carry bytes that decode as a valid record

Zero == [k |-> "zero", lsn |-> 0, part |-> 0, size |-> 0, lba |-> "-", dl |-> 0, fake |-> FALSE]
Pad  == [k |-> "pad",  lsn |-> 0, part |-> 0, size |-> 0, lba |-> "-", dl |-> 0, fake |-> FALSE]
Pages == 0..(K - 1)

VARIABLES
    memTail, pendJ, syncing,   \* in-memory tail; pending jump LSN; LSNs a Sync made durable but not yet acked
    cache, disk,     \* page cache and durable image of the WAL pages
    recs,            \* appended records, in LSN order: [lsn, lba, start (logical), size, dl]
    head,            \* in-memory logical head (next page)
    synced,          \* highest LSN whose Sync completed (acknowledged durable)
    sb,              \* durable superblock [tail, head, ckpt, w]
    extent,          \* durable extent: lba -> LSN of the write it holds (0 = never)
    phase, corrupt,  \* "run" | "crashed" | "recovered"; corrupted page or -1
    res              \* recovery result [failed, state (lba -> lsn), replayed (set of LSN)]

vars == <<memTail, pendJ, syncing, cache, disk, recs, head, synced, sb, extent, phase, corrupt, res>>

NextLSN == Len(recs) + 1

Init == /\ memTail = 0 /\ pendJ = 0 /\ syncing = 0
        /\ cache = [p \in Pages |-> Zero] /\ disk = [p \in Pages |-> Zero]
        /\ recs = <<>> /\ head = 0 /\ synced = 0
        /\ sb = [tail |-> 0, head |-> 0, ckpt |-> 0, w |-> 0]
        /\ extent = [l \in LBAs |-> 0]
        /\ phase = "run" /\ corrupt = -1
        /\ res = [failed |-> FALSE, state |-> [l \in LBAs |-> 0], replayed |-> {}, forged |-> FALSE, lastWriter |-> [l \in LBAs |-> 0]]

\* WAL append (walstore.go write path -> wal append): a 2-page record never
\* straddles the physical end; the remainder is padded and it starts the next lap.
AppendRec(l, sz) ==
    /\ phase = "run" /\ NextLSN <= MaxLSN
    /\ LET p == head % K
           padN == IF p + sz > K THEN K - p ELSE 0
           st == head + padN
           rec(i) == [k |-> "rec", lsn |-> NextLSN, part |-> i, size |-> sz, lba |-> l, dl |-> synced, fake |-> FALSE]
       IN /\ st + sz - (IF ReuseBound = "persisted" THEN sb.tail ELSE memTail) <= K   \* ring space
          /\ cache' = [q \in Pages |->
                          IF padN > 0 /\ q >= p THEN Pad
                          ELSE IF q = st % K THEN rec(1)
                          ELSE IF sz = 2 /\ q = (st + 1) % K THEN (IF Forge THEN [rec(2) EXCEPT !.fake = TRUE] ELSE rec(2))
                          ELSE cache[q]]
          /\ recs' = Append(recs, [lsn |-> NextLSN, lba |-> l, start |-> st, size |-> sz, dl |-> synced, jump |-> FALSE])
          /\ head' = st + sz
    /\ UNCHANGED <<memTail, pendJ, syncing, disk, synced, sb, extent, phase, corrupt, res>>

\* AdvanceFrontier (walstore.go AdvanceFrontier): skip one LSN without writing it; the jump is
\* recorded in memory and persisted as the checkpoint by the next Sync (when JumpPersistedBySync)
JumpFrontier == /\ JumpOn /\ phase = "run" /\ NextLSN <= MaxLSN /\ pendJ = 0 /\ sb.ckpt = Len(recs)
                /\ recs' = Append(recs, [lsn |-> NextLSN, lba |-> "-", start |-> head, size |-> 0, dl |-> synced, jump |-> TRUE])
                /\ pendJ' = NextLSN
                /\ UNCHANGED <<memTail, syncing, cache, disk, head, synced, sb, extent, phase, corrupt, res>>

\* the kernel writes one dirty page back, in any order
Writeback == /\ phase = "run"
             /\ \E p \in Pages : /\ cache[p] # disk[p] /\ disk' = [disk EXCEPT ![p] = cache[p]]
             /\ UNCHANGED <<memTail, pendJ, syncing, cache, recs, head, synced, sb, extent, phase, corrupt, res>>

\* Sync, step 1: fsync of the WAL pages (durable, not yet acknowledged)
Sync == /\ phase = "run" /\ synced < Len(recs) /\ syncing = 0
        /\ disk' = cache /\ syncing' = Len(recs)
        /\ UNCHANGED <<memTail, pendJ, cache, recs, head, synced, sb, extent, phase, corrupt, res>>
\* Sync, step 2: the second fsync (superblock: pending jump as checkpoint, head; W for the
\* watermark rules), then the acknowledgement (walstore.go:866-905)
SyncAck == /\ phase = "run" /\ syncing > 0
           /\ LET j == JumpPersistedBySync /\ pendJ > sb.ckpt /\ pendJ <= syncing
                  w == Rule \in {"watermarkSB", "watermarkFull", "prefixFullSB"}
              IN /\ sb' = [sb EXCEPT !.ckpt = IF j THEN pendJ ELSE @, !.head = IF j THEN head ELSE @,
                                     !.w = IF w THEN syncing ELSE @]
                 /\ pendJ' = IF j THEN 0 ELSE pendJ
           /\ synced' = syncing /\ syncing' = 0
           /\ UNCHANGED <<memTail, cache, disk, recs, head, extent, phase, corrupt, res>>

\* flusher: apply the oldest retained record to the extent, fsync it, then
\* persist the superblock (tail past it, current head, checkpoint LSN)
Checkpoint == /\ phase = "run" /\ sb.ckpt < Len(recs) /\ sb.ckpt < MaxCkpt
              /\ LET r == recs[sb.ckpt + 1]
                     end == IF r.jump THEN memTail ELSE r.start + r.size
                 IN /\ extent' = IF r.jump THEN extent ELSE [extent EXCEPT ![r.lba] = r.lsn]
                    \* persistCheckpoint writes {ckpt, live head, tail}; with CkptLag the tail is the
                    \* PRE-advance one and the in-memory tail advances afterwards
                    /\ sb' = [sb EXCEPT !.tail = IF CkptLag THEN memTail ELSE end, !.head = head, !.ckpt = r.lsn]
                    /\ memTail' = end
                    /\ pendJ' = IF r.jump THEN 0 ELSE pendJ
              /\ UNCHANGED <<syncing, cache, disk, recs, head, synced, phase, corrupt, res>>

Crash == /\ phase = "run" /\ phase' = "crashed"
         /\ \/ corrupt' = -1
            \/ CorruptOn /\ \E p \in Pages : disk[p].k = "rec" /\ corrupt' = p
         /\ UNCHANGED <<memTail, pendJ, syncing, cache, disk, recs, head, synced, sb, extent, res>>

-----------------------------------------------------------------------------
(* recovery *)

Bad(p) == p = corrupt
\* a valid record whose first page is at physical p, inside [p, e)
Forged(p) == disk[p].k = "rec" /\ disk[p].part = 2 /\ disk[p].fake /\ ~Bad(p)
FakeRec(p) == [k |-> "rec", lsn |-> disk[p].lsn + 1, part |-> 1, size |-> 1, lba |-> "y", dl |-> disk[p].dl, fake |-> TRUE]
View(p) == IF Forged(p) THEN FakeRec(p) ELSE disk[p]
ValidAt0(p, e) ==
    /\ disk[p].k = "rec" /\ disk[p].part = 1 /\ ~Bad(p)
    /\ disk[p].size = 2 => (p + 1 < e /\ disk[p + 1].k = "rec" /\ disk[p + 1].part = 2
                             /\ disk[p + 1].lsn = disk[p].lsn /\ ~Bad(p + 1))
ValidAt(p, e) == Forged(p) \/ ValidAt0(p, e)

\* scan [pos, e) (recovery.go:96-206): the records decoded in scan order, and
\* whether the range stopped on a BAD entry (torn or corrupt; trailing zeros are
\* not bad) together with the LSN of its decoded predecessor in the range
\* (pred starts at the checkpoint, as #155 does at recovery.go:201 of 089d2d9)
RECURSIVE Scan(_, _, _)
Scan(pos, e, pred) ==
    IF pos >= e THEN [recs |-> <<>>, bad |-> FALSE, pred |-> pred]
    ELSE IF disk[pos].k = "pad" THEN Scan(pos + 1, e, pred)
    ELSE IF ValidAt(pos, e)
         THEN LET r == Scan(pos + View(pos).size, e, View(pos).lsn)
              IN [r EXCEPT !.recs = <<View(pos)>> \o @]
    ELSE [recs |-> <<>>, bad |-> disk[pos].k # "zero", pred |-> pred]

\* physical ranges from the superblock (recovery.go:57-93). For head == tail the
\* code walks record footers backward from the physical end to find the start
\* of the high run (findDefensiveHighRunStart, recovery.go:250-283, and
\* previousValidWALRecord); a 1-page footer stands for the size field.
PrevStart(c) ==
    IF c >= 1 /\ disk[c - 1].k = "pad" THEN c - 1
    ELSE IF c >= 1 /\ disk[c - 1].k = "rec" /\ disk[c - 1].part = 1 /\ disk[c - 1].size = 1 /\ ValidAt(c - 1, K) THEN c - 1
    ELSE IF c >= 2 /\ disk[c - 1].k = "rec" /\ disk[c - 1].part = 2 /\ disk[c - 2].size = 2 /\ disk[c - 2].lsn = disk[c - 1].lsn /\ ValidAt(c - 2, K) THEN c - 2
    ELSE -1
RECURSIVE Walk(_, _, _)
Walk(c, earliest, data) == LET p == PrevStart(c)
                           IN IF p < 0 THEN [e |-> earliest, data |-> data]
                              ELSE Walk(p, p, data \/ disk[p].k = "rec")
HighRun == LET w == Walk(K, K, FALSE) IN IF w.data /\ w.e > 0 /\ w.e < K THEN w.e ELSE 0
DefStarts == {HighRun}
RangesFrom(ds) ==
    LET pH == sb.head % K
        pT == sb.tail % K
    IN IF sb.head = sb.tail /\ Rule = "rootLww" /\ pH # 0 THEN <<<<pH, K>>, <<0, pH>>, <<0, 0>>>>   \* #159
       ELSE IF sb.head = sb.tail THEN <<<<ds, K>>, <<0, ds>>, <<0, 0>>>>
       ELSE IF pH > pT THEN <<<<pT, pH>>, <<pH, K>>, <<0, pT>>>>
       ELSE <<<<pT, K>>, <<0, pH>>, <<pH, pT>>>>

Scans(ds) == LET r == RangesFrom(ds) IN [i \in 1..3 |-> Scan(r[i][1], r[i][2], sb.ckpt)]
\* retained records in scan order (LSN above the checkpoint)
\* "watermarkFull": every page position is decoded on its own (records are
\* self-checking), so no range break can hide a durable record
FullSeq == LET ps == {p \in Pages : ValidAt(p, K) /\ disk[p].lsn > sb.ckpt}
               RECURSIVE Build(_)
               Build(S) == IF S = {} THEN <<>> ELSE LET m == CHOOSE x \in S : \A y \in S : x <= y
                                                     IN <<View(m)>> \o Build(S \ {m})
           IN Build(ps)
FoundSeq(ds) == IF Rule \in {"watermarkFull", "lwwFull", "prefixFull", "prefixFullSB"} THEN FullSeq ELSE LET s == Scans(ds)
                    all == s[1].recs \o s[2].recs \o s[3].recs
                IN SelectSeq(all, LAMBDA f : f.lsn > sb.ckpt)

Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A s \in S : s <= m
Min(S) == CHOOSE m \in S : \A s \in S : m <= s
SeqSet(q) == {q[i] : i \in 1..Len(q)}
RECURSIVE PrefixIn(_, _)
PrefixIn(n, L) == IF n + 1 \in L THEN PrefixIn(n + 1, L) ELSE n

\* replay into the dirty map: scan order, unconditional replace (recovery.go:158-190,
\* dirty_map.go putAt) for "current"/"holeFail"/"pr155"; by LSN for the watermark rules
RECURSIVE FoldScan(_, _, _)
FoldScan(q, i, st) == IF i > Len(q) THEN st ELSE FoldScan(q, i + 1, [st EXCEPT ![q[i].lba] = q[i].lsn])
ByLSN(F) == [l \in LBAs |-> LET mine == {f.lsn : f \in {g \in F : g.lba = l}}
                            IN IF mine = {} THEN extent[l] ELSE Max(mine)]

Recover ==
    /\ phase = "crashed"
    /\ \E ds \in DefStarts :
        LET q == FoundSeq(ds)
            F == SeqSet(q)
            L == {f.lsn : f \in F}
            R == PrefixIn(sb.ckpt, L)
            W == Max({f.dl : f \in F} \cup {sb.w})
            hole == \E n \in (sb.ckpt + 1)..Max(L) : n \notin L
            sc == Scans(ds)
            cuts == {sc[i].pred + 1 : i \in {j \in 1..3 : sc[j].bad}}
            cutoff == IF cuts = {} THEN Max(L) + 1 ELSE Min(cuts)
            keep == CASE Rule = "pr155" -> SelectSeq(q, LAMBDA f : f.lsn < cutoff)
                    []   Rule \in {"watermark", "watermarkSB", "watermarkFull", "prefixFull", "prefixFullSB"} -> SelectSeq(q, LAMBDA f : f.lsn <= R)
                    []   OTHER -> q
            failed == CASE Rule = "holeFail" -> hole
                      []   Rule \in {"watermark", "watermarkSB", "watermarkFull", "prefixFullSB"} -> R < W
                      []   OTHER -> FALSE                       \* current; pr155 is warning-only for walstore
            st == IF Rule \in {"watermark", "watermarkSB", "watermarkFull", "lww", "lwwFull", "prefixFull", "prefixFullSB", "rootLww"} THEN ByLSN(SeqSet(keep))
                  ELSE FoldScan(keep, 1, extent)
        IN res' = [failed |-> failed, state |-> st, replayed |-> {f.lsn : f \in SeqSet(keep)},
                   forged |-> \E f \in SeqSet(keep) : f.fake /\ f.part = 1,
                   lastWriter |-> ByLSN(SeqSet(keep))]
    /\ phase' = "recovered"
    /\ UNCHANGED <<memTail, pendJ, syncing, cache, disk, recs, head, synced, sb, extent, corrupt>>

Next == \/ \E l \in LBAs, sz \in {1, 2} : AppendRec(l, sz)
        \/ Writeback \/ Sync \/ SyncAck \/ Checkpoint \/ JumpFrontier \/ Crash \/ Recover

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* I1: every synced write is recovered (or superseded by a later write that is), or recovery fails closed *)
I1 == phase = "recovered" =>
        (res.failed \/ \A i \in 1..synced : recs[i].jump \/ res.state[recs[i].lba] >= recs[i].lsn)
(* I2: crash only, no corruption: recovery never reports corruption *)
I2 == (phase = "recovered" /\ corrupt = -1) => ~res.failed
(* I0Prefix (informational): a successful recovery never replays past a hole *)
I0Prefix == (phase = "recovered" /\ ~res.failed) =>
        \A n \in res.replayed : \A m \in (sb.ckpt + 1)..n : m \in res.replayed

(* recovery never replays bytes forged inside a user data payload *)
NoForgedReplay == (phase = "recovered" /\ ~res.failed) => ~res.forged
(* replayed state equals the last writer by LSN among the replayed records (review of #155) *)
ReplayLastWriter == (phase = "recovered" /\ ~res.failed) => res.state = res.lastWriter

\* witnesses (expected violated): recovery succeeds after a wrap with replayed records
W_NoWrapRecovery == ~(phase = "recovered" /\ ~res.failed /\ head > K /\ res.replayed # {})
W_NoCorruptFailClosed == ~(phase = "recovered" /\ corrupt # -1 /\ res.failed)
\* a recovery after a persisted jump that replays a synced record above it
W_NoJumpRecovery == ~(phase = "recovered" /\ ~res.failed /\ \E i \in 1..synced : recs[i].jump /\ sb.ckpt >= recs[i].lsn
                       /\ \E k \in (i + 1)..synced : ~recs[k].jump /\ recs[k].lsn \in res.replayed)
=============================================================================
