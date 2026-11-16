------------------------------ MODULE WALReplica ------------------------------
(***************************************************************************)
(* S3 extension: does the synced watermark break replicated mode (RF=2)?   *)
(* Scope: recovery classification and promotion eligibility only (full     *)
(* failover is S6). The WAL is abstracted to one slot per LSN (the page    *)
(* layout is WALRecovery.tla); unsynced slots reach disk in any order.     *)
(*                                                                         *)
(* Primary P writes locally and ships records in order to replica R; R     *)
(* writes them to its own WAL and Syncs on its own schedule, then acks.    *)
(* A write is acknowledged to the client once P synced it and R acked it.  *)
(* Optionally R joined by BASE rebuild at LSN B (RebaseShipLSN): its WAL   *)
(* legitimately lacks 1..B. As in walstore at 5c29382 the jump is durable  *)
(* only as the superblock checkpoint written by the next Sync              *)
(* (walstore.go:880-935); live-lane records above it may reach disk first. *)
(*                                                                         *)
(* Constants:                                                              *)
(*   DlLocal   c1 TRUE: R stamps its own completed-Sync LSN;               *)
(*             c2 FALSE (RED): R stores the primary's watermark verbatim.  *)
(*   Marker    g1 TRUE: the BASE install persists a gap marker covering    *)
(*             1..B; g2 FALSE (RED): no marker.                            *)
(*   SBWatermark  Sync also persists W out of band (WALRecovery "watermarkSB").*)
(*   JumpInStamp  the stamp counts the unpersisted BASE jump as synced     *)
(*             (an in-memory frontier bump before the Sync that persists it).*)
(*   PromoteGate  promotion only considers nodes that did not fail closed. *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS MaxLSN, B, DlLocal, Marker, SBWatermark, CorruptOn, PromoteGate, JumpInStamp

Nodes == {"P", "R"}
None == [k |-> "none", lo |-> 0, hi |-> 0, dl |-> 0]
Rec(n, d) == [k |-> "rec", lo |-> n, hi |-> n, dl |-> d]
LSNs == 1..MaxLSN

VARIABLES cache, disk, synced, sbw, ckpt, next, ship, racked, acked, phase, corrupt, res, promoted
vars == <<cache, disk, synced, sbw, ckpt, next, ship, racked, acked, phase, corrupt, res, promoted>>

\* P has already written and synced 1..B (acknowledged in degraded RF=1) when
\* R is rebuilt from a durable BASE image at B; R's jump is not yet persisted
Init ==
    /\ LET pre == [n \in LSNs |-> IF n <= B THEN Rec(n, n - 1) ELSE None]
           rwal == [n \in LSNs |-> IF B > 0 /\ Marker /\ n = B THEN [k |-> "gap", lo |-> 1, hi |-> B, dl |-> 0] ELSE None]
       IN /\ cache = [x \in Nodes |-> IF x = "P" THEN pre ELSE rwal]
          /\ disk = [x \in Nodes |-> IF x = "P" THEN pre ELSE [n \in LSNs |-> None]]
    /\ synced = [x \in Nodes |-> IF x = "P" \/ JumpInStamp THEN B ELSE 0]
    /\ sbw = [x \in Nodes |-> IF SBWatermark /\ x = "P" THEN B ELSE 0]
    /\ ckpt = [x \in Nodes |-> 0]
    /\ next = B + 1 /\ ship = <<>> /\ racked = 0 /\ acked = 0   \* acked = by P and R (1..B were RF=1 only)
    /\ phase = "run" /\ corrupt = <<"-", 0>>
    /\ res = [x \in Nodes |-> [failed |-> FALSE, frontier |-> 0]]
    /\ promoted = "-"

PWrite == /\ phase = "run" /\ next <= MaxLSN
          /\ cache' = [cache EXCEPT !["P"][next] = Rec(next, synced["P"])]
          /\ ship' = Append(ship, [lsn |-> next, dl |-> synced["P"]])
          /\ next' = next + 1
          /\ UNCHANGED <<disk, synced, sbw, ckpt, racked, acked, phase, corrupt, res, promoted>>

RRecv == /\ phase = "run" /\ ship # <<>>
         /\ LET m == Head(ship)
            IN cache' = [cache EXCEPT !["R"][m.lsn] = Rec(m.lsn, IF DlLocal THEN synced["R"] ELSE m.dl)]
         /\ ship' = Tail(ship)
         /\ UNCHANGED <<disk, synced, sbw, ckpt, next, racked, acked, phase, corrupt, res, promoted>>

Writeback(x) == /\ phase = "run"
                /\ \E n \in LSNs : cache[x][n] # disk[x][n] /\ disk' = [disk EXCEPT ![x][n] = cache[x][n]]
                /\ UNCHANGED <<cache, synced, sbw, ckpt, next, ship, racked, acked, phase, corrupt, res, promoted>>

Top(x) == LET have == {n \in LSNs : cache[x][n].k # "none"} IN IF have = {} THEN 0 ELSE CHOOSE m \in have : \A h \in have : h <= m

Jump(x) == IF x = "R" THEN B ELSE 0
Sync(x) == /\ phase = "run" /\ (Top(x) > synced[x] \/ ckpt[x] < Jump(x))
           /\ LET s1 == IF Top(x) > Jump(x) THEN Top(x) ELSE Jump(x)
              IN /\ disk' = [disk EXCEPT ![x] = cache[x]]
                 /\ ckpt' = [ckpt EXCEPT ![x] = IF Jump(x) > @ THEN Jump(x) ELSE @]   \* WALCheckpointLSN, second fsync
                 /\ synced' = [synced EXCEPT ![x] = s1]
                 /\ sbw' = IF SBWatermark THEN [sbw EXCEPT ![x] = s1] ELSE sbw
                 /\ racked' = IF x = "R" THEN s1 ELSE racked          \* R's durable ack
           /\ UNCHANGED <<cache, next, ship, acked, phase, corrupt, res, promoted>>

Ack == /\ phase = "run"
       /\ LET a == IF synced["P"] < racked THEN synced["P"] ELSE racked
          IN /\ a > acked /\ acked' = a
       /\ UNCHANGED <<cache, disk, synced, sbw, ckpt, next, ship, racked, phase, corrupt, res, promoted>>

\* both nodes lose power; at most one durable record on one node is corrupted
Crash == /\ phase = "run" /\ phase' = "crashed"
         /\ \/ corrupt' = <<"-", 0>>
            \/ CorruptOn /\ \E x \in Nodes, n \in LSNs : disk[x][n].k # "none" /\ corrupt' = <<x, n>>
         /\ UNCHANGED <<cache, disk, synced, sbw, ckpt, next, ship, racked, acked, res, promoted>>

-----------------------------------------------------------------------------
Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A s \in S : s <= m
ValidSlots(x) == {n \in LSNs : disk[x][n].k # "none" /\ corrupt # <<x, n>>}
Covered(x) == UNION {disk[x][n].lo..disk[x][n].hi : n \in ValidSlots(x)}
RECURSIVE PrefixOf(_, _)
PrefixOf(x, n) == IF n + 1 \in Covered(x) THEN PrefixOf(x, n + 1) ELSE n
Wm(x) == Max({disk[x][n].dl : n \in ValidSlots(x)} \cup {sbw[x]})

\* watermark rule (WALRecovery "watermark"/"watermarkSB"); R's extent holds 1..B from BASE
Recover == /\ phase = "crashed"
           /\ res' = [x \in Nodes |-> LET R == PrefixOf(x, ckpt[x])
                                      IN [failed |-> R < Wm(x), frontier |-> R]]
           /\ phase' = "recovered"
           /\ UNCHANGED <<cache, disk, synced, sbw, ckpt, next, ship, racked, acked, corrupt, promoted>>

\* promotion picks the highest recovered frontier among the candidates
Promote == /\ phase = "recovered" /\ promoted = "-"
           /\ LET cand == IF PromoteGate THEN {x \in Nodes : ~res[x].failed} ELSE Nodes
              IN /\ cand # {}
                 /\ promoted' = CHOOSE x \in cand : \A y \in cand : res[y].frontier <= res[x].frontier
           /\ phase' = "promoted"
           /\ UNCHANGED <<cache, disk, synced, sbw, ckpt, next, ship, racked, acked, corrupt, res>>

Next == PWrite \/ RRecv \/ Ack \/ Crash \/ Recover \/ Promote
        \/ \E x \in Nodes : Writeback(x) \/ Sync(x)
Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* I2 per node: crash only, no corruption -> no node reports wal_corrupt
I2 == (phase \in {"recovered", "promoted"} /\ corrupt = <<"-", 0>>) => \A x \in Nodes : ~res[x].failed
\* I3: a node that recovered as wal_corrupt is never promoted, and every
\* acknowledged write survives on some eligible node
I3 == /\ phase = "promoted" => ~res[promoted].failed
      /\ phase \in {"recovered", "promoted"} => \E x \in Nodes : ~res[x].failed /\ res[x].frontier >= acked
W_NoPromotion == ~(phase = "promoted" /\ acked >= 2)
=============================================================================
