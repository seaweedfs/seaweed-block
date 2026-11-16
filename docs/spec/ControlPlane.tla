----------------------------- MODULE ControlPlane -----------------------------
(***************************************************************************)
(* S5 (first slice): the blockmaster authority store and guarded moves.    *)
(* One volume, its durable record {epoch, holder, move journal}, two move  *)
(* requests (request ids) that clients retry, durable Puts whose outcome   *)
(* may be unknown (landed or not), and blockmaster restarts that reload    *)
(* the record (core/authority at 1e8b0e9d).                                *)
(*                                                                         *)
(* A move (applyWithWorkspaceMove, authority.go:435-575, prepare           *)
(* workspace.go:287-335), under Publisher.mu:                              *)
(*   1. an issued journal entry for the request id -> replay its record;   *)
(*   2. a reserved, unissued entry -> resume with the SAME epoch;          *)
(*   3. else reserve: epoch = max(published, journal) + 1, persist the     *)
(*      journal with the OLD assignment (first Put), then                  *)
(*   4. persist the new assignment with the entry marked issued (2nd Put). *)
(* A failed Put leaves the outcome unknown: the volume is marked uncertain *)
(* and refuses workspace changes until a restart reloads the store         *)
(* (workspace.go:254-269, authority.go:563-569).                           *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Rids, MaxRestarts, MaxTries,
          UncertainGuard,  \* RED FALSE: a failed Put does not mark the volume uncertain
          ReserveFirst     \* RED FALSE: mint and issue in one Put, without the reservation step

Target(r) == r      \* each request moves the volume to its own holder (model value)

VARIABLES store, mem, uncertain, pend, tries, restarts, issued
vars == <<store, mem, uncertain, pend, tries, restarts, issued>>

Rec0 == [epoch |-> 1, holder |-> "h0", moves |-> {}]

Init == /\ store = Rec0 /\ mem = Rec0 /\ uncertain = FALSE
        /\ pend = [r \in Rids |-> 0] /\ tries = [r \in Rids |-> 0]
        /\ restarts = 0 /\ issued = <<>>

Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A s \in S : s <= m
Entry(rec, r) == {m \in rec.moves : m.rid = r}
NextEpoch(rec) == Max({rec.epoch} \cup {m.e : m \in rec.moves}) + 1
Issue(rec, r, e) == [epoch |-> e, holder |-> Target(r),
                     moves |-> {m \in rec.moves : m.rid # r} \cup {[rid |-> r, e |-> e, ok |-> TRUE]}]
Logged(new) == IF new.epoch # store.epoch \/ new.holder # store.holder
               THEN Append(issued, [e |-> new.epoch, holder |-> new.holder]) ELSE issued

\* a durable Put: lands and is acknowledged, fails without landing, or lands with its reply lost
Put(new, okNext) ==
    \/ /\ store' = new /\ mem' = new /\ issued' = Logged(new) /\ okNext /\ uncertain' = uncertain
    \/ /\ store' = store /\ mem' = mem /\ issued' = issued /\ uncertain' = (UncertainGuard \/ uncertain)
       /\ pend' = pend
    \/ /\ store' = new /\ issued' = Logged(new) /\ mem' = mem /\ uncertain' = (UncertainGuard \/ uncertain)
       /\ pend' = pend

\* steps 1-3 (one critical section up to the first Put)
Request(r) ==
    /\ ~uncertain /\ pend[r] = 0 /\ tries[r] < MaxTries
    /\ tries' = [tries EXCEPT ![r] = @ + 1]
    /\ restarts' = restarts
    /\ LET ent == Entry(mem, r) IN
       IF \E m \in ent : m.ok
       THEN /\ UNCHANGED <<store, mem, uncertain, pend, issued>>          \* replay
       \* prepare refuses unless the current line is the request's expected one (workspace.go:291-295):
       \* every request here expects the initial assignment (epoch 1)
       ELSE IF mem.epoch # 1
       THEN /\ UNCHANGED <<store, mem, uncertain, pend, issued>>
       ELSE IF ent # {}
       THEN LET e == (CHOOSE m \in ent : TRUE).e                            \* resume the reservation
            IN Put(Issue(mem, r, e), pend' = pend)
       ELSE LET e == NextEpoch(mem)
            IN IF ReserveFirst
               THEN Put([mem EXCEPT !.moves = @ \cup {[rid |-> r, e |-> e, ok |-> FALSE]}], pend' = [pend EXCEPT ![r] = e])
               ELSE Put(Issue(mem, r, e), pend' = pend)

\* step 4, still under the lock (nothing else runs between the two Puts; a restart may)
IssueStep(r) ==
    /\ pend[r] # 0 /\ ~uncertain
    /\ LET e == pend[r] IN
       Put(Issue(mem, r, e), pend' = [pend EXCEPT ![r] = 0])
    /\ UNCHANGED <<tries, restarts>>

Restart == /\ restarts < MaxRestarts
           /\ restarts' = restarts + 1
           /\ mem' = store /\ uncertain' = FALSE /\ pend' = [r \in Rids |-> 0]
           /\ UNCHANGED <<store, tries, issued>>

\* the lock: while a request holds a pending reservation, no other request starts
Next == \/ \E r \in Rids : (\A q \in Rids : pend[q] = 0) /\ Request(r)
        \/ \E r \in Rids : IssueStep(r)
        \/ Restart
Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* no epoch is ever issued to two holders, and issued epochs strictly increase
UniqueEpoch == \A i, j \in 1..Len(issued) : i < j => issued[i].e < issued[j].e
\* each request id is issued at most one epoch
Idempotent == \A r \in Rids : Cardinality({i \in 1..Len(issued) : issued[i].holder = Target(r)}) <= 1
\* every issued epoch is journaled with its request (the grant record exists before the epoch is visible)
JournalComplete == store.epoch > 1 => \E m \in store.moves : m.e = store.epoch /\ m.ok
W_Moved == Len(issued) = 0
=============================================================================
