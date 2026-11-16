-------------------------- MODULE SmartWALRecovery --------------------------
(***************************************************************************)
(* S3, smartwal layout (core/storage/smartwal at 5c293821): data is        *)
(* written IN PLACE to the extent block of its LBA, then a metadata record *)
(* {lsn, lba, crc of the data, durable_lsn} goes to ring slot lsn mod C    *)
(* (store.go:266-288, ring.go:45-55). Sync is one fsync (store.go:314-331).*)
(* Unsynced extent blocks and ring slots are written back in ARBITRARY     *)
(* order; a slot may also be written back TORN (magic intact, bad CRC).    *)
(* Crash at any point, optionally with ONE durable extent block or ring    *)
(* slot corrupted. Recovery may run twice: after the first recovery the    *)
(* store continues, may AdvanceFrontier (an in-memory jump that writes no  *)
(* slots), writes, syncs and crashes again (a schedule from review of     *)
(* PR #155).                                                               *)
(* Each write has a unique issue number `ts` that stands for its data/CRC. *)
(*                                                                         *)
(* Rule:                                                                   *)
(*  "current"   runRecovery (recovery.go:39-110): valid slots only (a bad  *)
(*              CRC is skipped as torn, ring.go:62-82); per LBA the        *)
(*              highest-LSN record; extent must match its crc else fail    *)
(*              closed (85d93753); frontier = max verified LSN.            *)
(*  "holeFail"  also fail closed on any missing LSN in the window below Hi.*)
(*  "pr155"     PR #155 at 089d2d9: a torn slot is a mid-history hole      *)
(*              (wal_corrupt) unless it is the slot right after the highest*)
(*              valid LSN; its LSN is inferred from its position.          *)
(*  "pr155o"    the pr155 torn-slot rule alone (extent mismatches are not   *)
(*              judged), to isolate the positional inference (a schedule  *)
(*              from review of PR #155).                                   *)
(*  "watermark" W = max durable_lsn over valid records; a missing LSN      *)
(*              <= W not covered by a gap marker, or an extent mismatch at *)
(*              a record <= W, fails closed; above W it is a torn tail.    *)
(*  "ordered"   watermark, and the record is durable before the data write.*)
(* Marker: AdvanceFrontier persists a gap record covering the skipped LSNs.*)
(* Repair: before the store continues, recovery durably replaces every    *)
(*         slot it treated as a torn tail (torn, or any record above W of  *)
(*         an LBA whose extent mismatches) with a gap record for that LSN, *)
(*         re-attests the last record of such an LBA with the CRC of the   *)
(*         bytes the extent holds, and the frontier is max(W, verified and *)
(*         attested last writers), so a later                              *)
(*         watermark cannot reclassify it as corruption.                   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS C, MaxLSN, LBAs, Rule, CorruptOn, MaxTs, Jump, Marker, Rounds, Repair, CorruptExtent

NoRec == [k |-> "zero", lsn |-> 0, lo |-> 0, lba |-> "-", crc |-> 0, dl |-> 0]
Slots == 0..(C - 1)
Pgs == {<<"e", l>> : l \in LBAs} \cup {<<"r", s>> : s \in Slots}
Ordered == Rule = "ordered"

VARIABLES cache, disk, nxt, ts, synced, pend, lastSynced, phase, corrupt, round, res
vars == <<cache, disk, nxt, ts, synced, pend, lastSynced, phase, corrupt, round, res>>

Init == /\ cache = [p \in Pgs |-> IF p[1] = "e" THEN 0 ELSE NoRec]
        /\ disk = cache
        /\ nxt = 1 /\ ts = 0 /\ synced = 0
        /\ pend = [l \in LBAs |-> 0] /\ lastSynced = [l \in LBAs |-> 0]
        /\ phase = "run" /\ corrupt = <<"-", "-">> /\ round = 1
        /\ res = [failed |-> FALSE, state |-> [l \in LBAs |-> 0], frontier |-> 0, everCorrupt |-> FALSE]

Write(l) == /\ phase = "run" /\ nxt <= MaxLSN /\ ts < MaxTs
            /\ LET r == [k |-> "rec", lsn |-> nxt, lo |-> nxt, lba |-> l, crc |-> ts + 1, dl |-> synced]
               IN /\ cache' = [cache EXCEPT ![<<"e", l>>] = ts + 1, ![<<"r", nxt % C>>] = r]
                  /\ disk' = IF Ordered THEN [disk EXCEPT ![<<"r", nxt % C>>] = r] ELSE disk
            /\ nxt' = nxt + 1 /\ ts' = ts + 1
            /\ pend' = [pend EXCEPT ![l] = ts + 1]
            /\ UNCHANGED <<synced, lastSynced, phase, corrupt, round, res>>

\* in-memory frontier jump (no slots written); with Marker a gap record covering
\* the skipped LSNs goes to the slot of the last skipped LSN
Advance == /\ phase = "run" /\ round > 1 /\ nxt + Jump <= MaxLSN
           /\ cache' = IF Marker
                       THEN [cache EXCEPT ![<<"r", (nxt + Jump - 1) % C>>] =
                               [k |-> "gap", lsn |-> nxt + Jump - 1, lo |-> nxt, lba |-> "-", crc |-> 0, dl |-> synced]]
                       ELSE cache
           /\ nxt' = nxt + Jump
           /\ UNCHANGED <<disk, ts, synced, pend, lastSynced, phase, corrupt, round, res>>

Writeback == /\ phase = "run"
             /\ \E p \in Pgs : /\ cache[p] # disk[p]
                               /\ \/ disk' = [disk EXCEPT ![p] = cache[p]]
                                  \/ p[1] = "r" /\ disk' = [disk EXCEPT ![p] = [NoRec EXCEPT !.k = "torn"]]
             /\ UNCHANGED <<cache, nxt, ts, synced, pend, lastSynced, phase, corrupt, round, res>>

Sync == /\ phase = "run" /\ (cache # disk \/ synced < nxt - 1)
        /\ disk' = cache /\ synced' = nxt - 1
        /\ lastSynced' = pend
        /\ UNCHANGED <<cache, nxt, ts, pend, phase, corrupt, round, res>>

Crash == /\ phase = "run" /\ phase' = "crashed"
         /\ \/ corrupt' = <<"-", "-">>
            \/ CorruptOn /\ \E p \in Pgs : (IF p[1] = "e" THEN CorruptExtent ELSE disk[p].k = "rec") /\ corrupt' = p
         /\ UNCHANGED <<cache, disk, nxt, ts, synced, pend, lastSynced, round, res>>

-----------------------------------------------------------------------------
Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A s \in S : s <= m
SlotOK(s) == disk[<<"r", s>>].k \in {"rec", "gap"} /\ corrupt # <<"r", s>>
Valid == {disk[<<"r", s>>] : s \in {t \in Slots : SlotOK(t) /\ disk[<<"r", t>>].k = "rec"}}
Gaps == {disk[<<"r", s>>] : s \in {t \in Slots : SlotOK(t) /\ disk[<<"r", t>>].k = "gap"}}
Torn == {s \in Slots : disk[<<"r", s>>].k = "torn" \/ corrupt = <<"r", s>>}
Covered == UNION {g.lo..g.lsn : g \in Gaps}
ExtentVal(l) == IF corrupt = <<"e", l>> THEN -1 ELSE disk[<<"e", l>>]
Last(l) == LET mine == {r \in Valid : r.lba = l}
           IN IF mine = {} THEN NoRec ELSE CHOOSE r \in mine : \A q \in mine : q.lsn <= r.lsn
Hi == Max({r.lsn : r \in Valid} \cup {g.lsn : g \in Gaps})
Window == (IF Hi > C THEN Hi - C + 1 ELSE 1)..Hi
Present == {r.lsn : r \in Valid} \cup Covered
Missing == {n \in Window : n \notin Present}
\* positional LSN of a slot relative to Hi (PR #155 revision 089d2d9, smartwal/recovery.go:122)
Implied(s) == Hi - ((Hi - s) % C)
W == Max({r.dl : r \in Valid} \cup {g.dl : g \in Gaps})

Recover ==
    /\ phase = "crashed"
    /\ LET mismatch(l) == Last(l) # NoRec /\ ExtentVal(l) # Last(l).crc
           tornMid == \E s \in Torn : (Hi - s) % C # C - 1 /\ Implied(s) >= 1 /\ Implied(s) \notin Covered
           failed == CASE Rule = "current"  -> \E l \in LBAs : mismatch(l)
                     []   Rule = "holeFail" -> (\E l \in LBAs : mismatch(l)) \/ Missing # {}
                     []   Rule \in {"pr155", "pr155o"} -> (\E l \in LBAs : mismatch(l) /\ Rule = "pr155") \/ tornMid
                     []   OTHER -> \/ \E l \in LBAs : mismatch(l) /\ Last(l).lsn <= W
                                   \/ \E n \in Missing : n <= W
           verified == {Last(l).lsn : l \in {m \in LBAs : Last(m) # NoRec /\ ~mismatch(m)}}
           \* "current" keeps the code's frontier (max verified last-writer, recovery.go:96);
           \* the proposals count every valid record that is not a torn tail, and W
           good == {r.lsn : r \in {q \in Valid : ~\E l \in LBAs : mismatch(l) /\ Last(l) = q}}
           attested == IF Repair THEN {Last(l).lsn : l \in {m \in LBAs : mismatch(m) /\ Last(m).lsn > W}} ELSE {}
           fr == IF Rule \in {"watermark", "ordered"} THEN Max(verified \cup Covered \cup {W} \cup attested) ELSE Max(verified \cup Covered)
       IN /\ res' = [failed |-> failed, state |-> [l \in LBAs |-> ExtentVal(l)], frontier |-> fr,
                     everCorrupt |-> res.everCorrupt \/ corrupt # <<"-", "-">>]
          \* the store continues from the recovered image (store.go:179-201): nextLSN = frontier+1
          /\ IF ~failed /\ round < Rounds
             THEN /\ phase' = "run" /\ round' = round + 1
                  /\ LET settled(q) == Repair /\ q[1] = "r" /\ (q[2] \in Torn \/ (disk[q].k = "rec" /\ disk[q].lsn > W /\ mismatch(disk[q].lba)))
                         img == [p \in Pgs |-> IF p[1] = "e" THEN ExtentVal(p[2])
                                               ELSE IF settled(p) /\ disk[p].k = "rec" /\ Last(disk[p].lba) = disk[p] /\ ExtentVal(disk[p].lba) >= 0
                                                    \* re-attest the bytes the extent now holds (the accepted torn tail)
                                                    THEN [disk[p] EXCEPT !.crc = ExtentVal(disk[p].lba), !.dl = W]
                                               ELSE IF settled(p) THEN [k |-> "gap", lsn |-> Implied(p[2]), lo |-> Implied(p[2]), lba |-> "-", crc |-> 0, dl |-> W]
                                               ELSE disk[p]]
                     IN /\ cache' = img /\ disk' = img
                  /\ nxt' = fr + 1 /\ synced' = fr
                  /\ pend' = [l \in LBAs |-> ExtentVal(l)] /\ lastSynced' = [l \in LBAs |-> ExtentVal(l)]
                  /\ corrupt' = <<"-", "-">>
             ELSE /\ phase' = "recovered"
                  /\ UNCHANGED <<cache, disk, nxt, synced, pend, lastSynced, round, corrupt>>
    /\ UNCHANGED <<ts>>

Next == (\E l \in LBAs : Write(l)) \/ Advance \/ Writeback \/ Sync \/ Crash \/ Recover
Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* checked after the final recovery (state = what the volume would serve)
I1 == phase = "recovered" => (res.failed \/ \A l \in LBAs : res.state[l] >= lastSynced[l])
I1Frontier == phase = "recovered" => (res.failed \/ res.frontier >= synced)
I2 == (phase = "recovered" /\ ~res.everCorrupt /\ corrupt = <<"-", "-">>) => ~res.failed
W_NoRecovery == ~(phase = "recovered" /\ ~res.failed /\ round = Rounds /\ synced >= 2)
=============================================================================
