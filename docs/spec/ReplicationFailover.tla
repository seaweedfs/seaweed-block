------------------------- MODULE ReplicationFailover -------------------------
(***************************************************************************)
(* S6 (first slice): RF=2 failover with chunk HEAD and snapshot handoff.   *)
(* Primary P ships writes to replica R; R acks what it made durable; the   *)
(* client gets an ack once P and R are durable. The master promotes R at   *)
(* epoch E+1 only with evidence (R durable >= every acked LSN). R rejects  *)
(* shipping from a lower epoch, so a partitioned old primary can no longer *)
(* get acks. Cuts (chunk HEAD) cover only the replicated-durable frontier  *)
(* and are epoch-fenced CASes (S2). A snapshot request in flight at the    *)
(* failover is retried on the new primary with the same request id: it     *)
(* publishes to the same attempt path (exclusive create, cannot be         *)
(* cancelled), so whichever create lands first is the snapshot.            *)
(* Chunk content is abstracted to "the LSN of the write it holds"; one     *)
(* chunk per LBA, LBAs {a, b}.                                             *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS MaxLSN,
          ReplFrontier,   \* RED FALSE: a cut or snapshot covers P's local durable LSN, not the replicated one
          EvidenceGate,   \* RED FALSE: promote R without checking it has every acked write
          EpochFenceShip  \* RED FALSE: R accepts shipping from the old epoch after promotion

LBAs == {"a", "b"}

VARIABLES hist, pDur, rApp, rDur, acked, prim, epoch, pEpoch, head, snap, pend, rhist
vars == <<hist, pDur, rApp, rDur, acked, prim, epoch, pEpoch, head, snap, pend, rhist>>

Val(h, c, L) == LET ws == {i \in 1..L : i <= Len(h) /\ h[i] = c} IN IF ws = {} THEN 0 ELSE CHOOSE m \in ws : \A k \in ws : k <= m
Img(h, L) == [c \in LBAs |-> Val(h, c, L)]

Init == /\ hist = <<>> /\ rhist = <<>>      \* P's log and R's log
        /\ pDur = 0 /\ rApp = 0 /\ rDur = 0 /\ acked = 0
        /\ prim = "P" /\ epoch = 1 /\ pEpoch = 1
        /\ head = [lsn |-> 0, img |-> Img(<<>>, 0), epoch |-> 1]
        /\ snap = [st |-> "none", lsn |-> 0, img |-> Img(<<>>, 0)]   \* the published snapshot (attempt path)
        /\ pend = [on |-> FALSE, lsn |-> 0, img |-> Img(<<>>, 0)]   \* old primary's uncancellable create in flight

\* ---- primary P (old epoch) ----
PWrite(c) == /\ Len(hist) < MaxLSN /\ hist' = Append(hist, c)
             /\ UNCHANGED <<pDur, rApp, rDur, acked, prim, epoch, pEpoch, head, snap, pend, rhist>>
PSync == pDur < Len(hist) /\ pDur' = Len(hist) /\ UNCHANGED <<hist, rApp, rDur, acked, prim, epoch, pEpoch, head, snap, pend, rhist>>
\* R applies the next shipped write (refused from the old epoch after promotion)
Ship == /\ rApp < Len(hist) /\ (prim = "P" \/ ~EpochFenceShip)
        /\ rApp' = rApp + 1 /\ rhist' = Append(rhist, hist[rApp + 1])
        /\ UNCHANGED <<hist, pDur, rDur, acked, prim, epoch, pEpoch, head, snap, pend>>
RSync == rDur < rApp /\ rDur' = rApp /\ UNCHANGED <<hist, pDur, rApp, acked, prim, epoch, pEpoch, head, snap, pend, rhist>>
\* the client ack: durable on P and R (old primary still acks if R accepted its shipping)
Ack == /\ (prim = "P" \/ ~EpochFenceShip)   \* after the fence R acks nothing for the old epoch
       /\ LET a == IF pDur < rDur THEN pDur ELSE rDur IN a > acked /\ acked' = a
       /\ UNCHANGED <<hist, pDur, rApp, rDur, prim, epoch, pEpoch, head, snap, pend, rhist>>

Frontier == IF ReplFrontier THEN (IF pDur < rDur THEN pDur ELSE rDur) ELSE pDur
\* P's cut: epoch-fenced CAS (fails once the new primary has taken HEAD over)
PCut == /\ prim = "P" /\ head.epoch = pEpoch /\ Frontier > head.lsn
        /\ head' = [lsn |-> Frontier, img |-> Img(hist, Frontier), epoch |-> pEpoch]
        /\ UNCHANGED <<hist, pDur, rApp, rDur, acked, prim, epoch, pEpoch, snap, pend, rhist>>
\* P starts the snapshot: its create is sent and may land at any later time
PSnap == /\ prim = "P" /\ snap.st = "none" /\ ~pend.on /\ Frontier > 0
         /\ pend' = [on |-> TRUE, lsn |-> Frontier, img |-> Img(hist, Frontier)]
         /\ UNCHANGED <<hist, pDur, rApp, rDur, acked, prim, epoch, pEpoch, head, snap, rhist>>
Land == /\ pend.on
        /\ snap' = IF snap.st = "none" THEN [st |-> "published", lsn |-> pend.lsn, img |-> pend.img] ELSE snap
        /\ pend' = [pend EXCEPT !.on = FALSE]
        /\ UNCHANGED <<hist, pDur, rApp, rDur, acked, prim, epoch, pEpoch, head, rhist>>

\* ---- failover ----
Promote == /\ prim = "P" /\ (EvidenceGate => rDur >= acked)
           /\ prim' = "R" /\ epoch' = epoch + 1
           /\ UNCHANGED <<hist, pDur, rApp, rDur, acked, pEpoch, head, snap, pend, rhist>>
\* R's takeover CAS of HEAD at the new epoch (S2), from the HEAD it observed
Takeover == /\ prim = "R" /\ head.epoch < epoch
            /\ head' = [head EXCEPT !.epoch = epoch]
            /\ UNCHANGED <<hist, pDur, rApp, rDur, acked, prim, epoch, pEpoch, snap, pend, rhist>>
RWrite(c) == /\ prim = "R" /\ head.epoch = epoch /\ Len(rhist) < MaxLSN
             /\ rhist' = Append(rhist, c) /\ rApp' = rApp + 1
             /\ UNCHANGED <<hist, pDur, rDur, acked, prim, epoch, pEpoch, head, snap, pend>>
RCut == /\ prim = "R" /\ head.epoch = epoch /\ rDur > head.lsn
        /\ head' = [lsn |-> rDur, img |-> Img(rhist, rDur), epoch |-> epoch]
        /\ UNCHANGED <<hist, pDur, rApp, rDur, acked, prim, epoch, pEpoch, snap, pend, rhist>>
\* the retried snapshot request on R, same request id: same attempt path, exclusive create
RSnap == /\ prim = "R" /\ head.epoch = epoch /\ snap.st = "none" /\ rDur > 0
         /\ snap' = [st |-> "published", lsn |-> rDur, img |-> Img(rhist, rDur)]
         /\ UNCHANGED <<hist, pDur, rApp, rDur, acked, prim, epoch, pEpoch, head, pend, rhist>>

Next == \/ \E c \in LBAs : PWrite(c) \/ RWrite(c)
        \/ PSync \/ Ship \/ RSync \/ Ack \/ PCut \/ PSnap \/ Land
        \/ Promote \/ Takeover \/ RCut \/ RSnap
Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
Lineage == IF prim = "R" THEN rhist ELSE hist
\* the promoted primary holds every acknowledged write
AckedSurvive == prim = "R" => (\A i \in 1..acked : i <= Len(rhist) /\ rhist[i] = hist[i])
\* HEAD is a prefix image of the surviving lineage and never ahead of its durable LSN
HeadOnLineage == /\ head.img = Img(Lineage, head.lsn)
                 /\ (prim = "R" /\ head.epoch = epoch) => head.lsn <= rDur
\* the published snapshot is a prefix image of the surviving lineage
SnapOnLineage == snap.st = "published" => snap.img = Img(Lineage, snap.lsn)
\* the old primary acks nothing after the promotion that the new primary lacks
NoStaleAck == prim = "R" => acked <= Len(rhist)
W_Handoff == ~(prim = "R" /\ snap.st = "published" /\ head.epoch = epoch /\ head.lsn > 0)
=============================================================================
