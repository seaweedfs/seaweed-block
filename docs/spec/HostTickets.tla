---------------------------- MODULE HostTickets ----------------------------
\* M7 waiting -> admitted: timer cuts wait for the one normal slot in order of
\* their tickets (flock-held files in the pool directory, ordered by due time).
\*
\* - Processes may die at any step (the kernel drops their locks; their files
\*   stay) or stall at any step, the probe included (they take no further step
\*   and keep their locks).
\* - A ticket is taken by create, lock, then an inode re-check (path still
\*   names the locked file). A prober removes a ticket in two steps: it locks a
\*   lockable ticket, then unlinks it only if the path still names it, and
\*   unlocks. A creator that meets the prober's lock retries the lock.
\* - Skip rule: a waiter may pass any earlier ticket while the slot is free and
\*   no urgent hold is seen. This over-approximates "the same ticket stayed
\*   ahead for T_skip", false skips of a slow live head included. Ticket order
\*   is fairness only; the slot lock is the exclusion.
\* - The waiter's exclusive probe of urgent-pending is held from the probe to
\*   the slot try-lock. Urgent work takes urgent-pending shared with a bounded
\*   wait and, if the probe is still held, proceeds without the hold.
\*
\* Mutants (constants set TRUE must violate):
\*   NoSkip             -> BoundedWait       (a stalled head blocks every waiter)
\*   NoInodeCheck       -> NoLostTicket      (a creator keeps a lock on an unlinked file)
\*   ExclusiveRefuses   -> UrgentNotRefused  (a probe refuses an urgent admission)
\*   UrgentBlocksForever -> UrgentNotBlocked (a stalled prober blocks urgent work for ever)
EXTENDS Integers, FiniteSets, TLC
CONSTANTS N, MaxGen, NoSkip, NoInodeCheck, ExclusiveRefuses, UrgentBlocksForever, Faults

Procs == 1..N
Due(p) == p                       \* distinct due times: lower runs first
FileIds == {<<p, g>> : p \in Procs, g \in 1..MaxGen}
DueOf(f) == Due(f[1])
None == <<0, 0>>

VARIABLES
  dir,        \* ticket files present in the directory
  lockOf,     \* file -> locking process (0: unlocked)
  pc,         \* process step
  mine,       \* the process's current ticket file (or None)
  reaping,    \* the file a process has locked to remove it (or None)
  gen,        \* tickets created so far by the process
  alive, stalled,
  slot,       \* holder of the one normal slot (0: free)
  probing,    \* processes holding the exclusive urgent-pending probe
  upc,        \* urgent work: idle, trying, held (with urgent-pending), nohold, done
  urgentRefused,
  outOfOrder, \* history: an admission passed an earlier live ticket
  outOfOrderBadly, \* history: ... while the slot was taken or an urgent hold was seen
  faults
vars == <<dir, lockOf, pc, mine, reaping, gen, alive, stalled, slot, probing, upc, urgentRefused, outOfOrder, outOfOrderBadly, faults>>

Init == /\ dir = {} /\ lockOf = [f \in FileIds |-> 0]
        /\ pc = [p \in Procs |-> "idle"] /\ mine = [p \in Procs |-> None]
        /\ reaping = [p \in Procs |-> None] /\ gen = [p \in Procs |-> 0]
        /\ alive = [p \in Procs |-> TRUE] /\ stalled = [p \in Procs |-> FALSE]
        /\ slot = 0 /\ probing = {} /\ upc = "idle" /\ urgentRefused = FALSE
        /\ outOfOrder = FALSE /\ outOfOrderBadly = FALSE /\ faults = 0

Live(p) == alive[p] /\ ~stalled[p]
UrgentHold == upc = "held"
Rest == <<gen, alive, stalled, upc, urgentRefused, outOfOrder, outOfOrderBadly, faults>>

\* ---- taking a ticket: create, lock (retry while a prober holds it), re-check
Create(p) == /\ Live(p) /\ pc[p] = "idle" /\ gen[p] < MaxGen /\ reaping[p] = None
             /\ LET f == <<p, gen[p] + 1>> IN
                  /\ dir' = dir \cup {f} /\ mine' = [mine EXCEPT ![p] = f]
                  /\ gen' = [gen EXCEPT ![p] = @ + 1]
             /\ pc' = [pc EXCEPT ![p] = "created"]
             /\ UNCHANGED <<lockOf, reaping, alive, stalled, slot, probing, upc, urgentRefused, outOfOrder, outOfOrderBadly, faults>>
Lock(p) == /\ Live(p) /\ pc[p] = "created" /\ lockOf[mine[p]] = 0
           /\ lockOf' = [lockOf EXCEPT ![mine[p]] = p]
           /\ pc' = [pc EXCEPT ![p] = "locked"]
           /\ UNCHANGED <<dir, mine, reaping, slot, probing>> /\ UNCHANGED Rest
Recheck(p) == /\ Live(p) /\ pc[p] = "locked"
              /\ IF mine[p] \in dir \/ NoInodeCheck
                   THEN /\ pc' = [pc EXCEPT ![p] = "waiting"]
                        /\ UNCHANGED <<lockOf, mine>>
                   ELSE /\ lockOf' = [lockOf EXCEPT ![mine[p]] = 0]   \* path gone: start over
                        /\ mine' = [mine EXCEPT ![p] = None]
                        /\ pc' = [pc EXCEPT ![p] = "idle"]
              /\ UNCHANGED <<dir, reaping, slot, probing>> /\ UNCHANGED Rest

\* ---- a waiter removes a ticket it can lock, in two steps
ReapLock(q, f) == /\ Live(q) /\ pc[q] = "waiting" /\ reaping[q] = None
                  /\ f \in dir /\ f # mine[q] /\ lockOf[f] = 0
                  /\ lockOf' = [lockOf EXCEPT ![f] = q] /\ reaping' = [reaping EXCEPT ![q] = f]
                  /\ UNCHANGED <<dir, pc, mine, slot, probing>> /\ UNCHANGED Rest
ReapFinish(q) == /\ Live(q) /\ reaping[q] # None
                 /\ LET f == reaping[q] IN
                      /\ dir' = IF f \in dir THEN dir \ {f} ELSE dir   \* unlink only if the path still names it
                      /\ lockOf' = [lockOf EXCEPT ![f] = 0]
                 /\ reaping' = [reaping EXCEPT ![q] = None]
                 /\ UNCHANGED <<pc, mine, slot, probing>> /\ UNCHANGED Rest

\* ---- admission: head (or skip), probe held until the slot try-lock, slot
Earlier(p) == {f \in dir : DueOf(f) < Due(p)}
LiveEarlier(p) == {f \in Earlier(p) : lockOf[f] # 0 /\ lockOf[f] # p /\ alive[lockOf[f]] /\ reaping[lockOf[f]] # f}
CanSkip == ~NoSkip /\ slot = 0 /\ ~UrgentHold
Head(p) == LiveEarlier(p) = {} \/ CanSkip
BeginProbe(p) == /\ Live(p) /\ pc[p] = "waiting" /\ reaping[p] = None /\ mine[p] \in dir
                 /\ ~UrgentHold /\ Head(p) /\ slot = 0
                 /\ \A f \in Earlier(p) : lockOf[f] # 0 \/ f = mine[p]   \* dead (unlocked) tickets are reaped first
                 /\ probing' = probing \cup {p} /\ pc' = [pc EXCEPT ![p] = "probing"]
                 \* the order decision is made here; a ticket that appears after it is not passed
                 /\ outOfOrder' = (outOfOrder \/ LiveEarlier(p) # {})
                 /\ outOfOrderBadly' = (outOfOrderBadly \/ (LiveEarlier(p) # {} /\ ~CanSkip))
                 /\ UNCHANGED <<dir, lockOf, mine, reaping, slot>>
                 /\ UNCHANGED <<gen, alive, stalled, upc, urgentRefused, faults>>
Admit(p) == /\ Live(p) /\ pc[p] = "probing"
            /\ probing' = probing \ {p}
            /\ IF slot = 0 /\ ~UrgentHold
                 THEN /\ slot' = p /\ pc' = [pc EXCEPT ![p] = "admitted"]
                      /\ dir' = dir \ {mine[p]}
                      /\ lockOf' = [lockOf EXCEPT ![mine[p]] = 0]
                      /\ mine' = [mine EXCEPT ![p] = None]
                 ELSE /\ pc' = [pc EXCEPT ![p] = "waiting"]
                      /\ UNCHANGED <<slot, dir, lockOf, mine>>
            /\ UNCHANGED <<reaping, gen, alive, stalled, upc, urgentRefused, outOfOrder, outOfOrderBadly, faults>>
Release(p) == /\ Live(p) /\ pc[p] = "admitted" /\ slot = p
              /\ slot' = 0 /\ pc' = [pc EXCEPT ![p] = "done"]
              /\ UNCHANGED <<dir, lockOf, mine, reaping, probing>> /\ UNCHANGED Rest

\* ---- urgent work (one episode): bounded wait for the shared hold, then on without it
UrgentStart == /\ upc = "idle" /\ upc' = "trying"
               /\ UNCHANGED <<dir, lockOf, pc, mine, reaping, gen, alive, stalled, slot, probing, urgentRefused, outOfOrder, outOfOrderBadly, faults>>
UrgentHoldIt == /\ upc = "trying" /\ probing = {}
                /\ upc' = "held"
                /\ UNCHANGED <<dir, lockOf, pc, mine, reaping, gen, alive, stalled, slot, probing, urgentRefused, outOfOrder, outOfOrderBadly, faults>>
UrgentBusy == /\ upc = "trying" /\ probing # {}
              /\ IF ExclusiveRefuses
                   THEN /\ urgentRefused' = TRUE /\ upc' = "done"
                   ELSE /\ ~UrgentBlocksForever          \* bounded wait over: proceed without the hold
                        /\ upc' = "nohold" /\ UNCHANGED urgentRefused
              /\ UNCHANGED <<dir, lockOf, pc, mine, reaping, gen, alive, stalled, slot, probing, outOfOrder, outOfOrderBadly, faults>>
UrgentEnd == /\ upc \in {"held", "nohold"} /\ upc' = "done"
             /\ UNCHANGED <<dir, lockOf, pc, mine, reaping, gen, alive, stalled, slot, probing, urgentRefused, outOfOrder, outOfOrderBadly, faults>>
UrgentStep == UrgentHoldIt \/ UrgentBusy \/ UrgentEnd

\* ---- faults: death drops every kernel lock of the process; a stall keeps them.
\* A stalled slot holder is outside M7 (the supervisor's premise).
Die(p) == /\ alive[p] /\ faults < Faults
          /\ alive' = [alive EXCEPT ![p] = FALSE] /\ faults' = faults + 1
          /\ lockOf' = [f \in FileIds |-> IF lockOf[f] = p THEN 0 ELSE lockOf[f]]
          /\ slot' = IF slot = p THEN 0 ELSE slot
          /\ probing' = probing \ {p}
          /\ reaping' = [reaping EXCEPT ![p] = None]
          /\ UNCHANGED <<dir, pc, mine, gen, stalled, upc, urgentRefused, outOfOrder, outOfOrderBadly>>
Stall(p) == /\ Live(p) /\ slot # p /\ faults < Faults
            /\ stalled' = [stalled EXCEPT ![p] = TRUE] /\ faults' = faults + 1
            /\ UNCHANGED <<dir, lockOf, pc, mine, reaping, gen, alive, slot, probing, upc, urgentRefused, outOfOrder, outOfOrderBadly>>

Step(p) == Create(p) \/ Lock(p) \/ Recheck(p) \/ BeginProbe(p) \/ Admit(p) \/ Release(p)
           \/ ReapFinish(p) \/ (\E f \in FileIds : ReapLock(p, f))
Next == (\E p \in Procs : Step(p) \/ Die(p) \/ Stall(p)) \/ UrgentStart \/ UrgentStep
Spec == Init /\ [][Next]_vars /\ (\A p \in Procs : WF_vars(Step(p))) /\ WF_vars(UrgentStep)

\* ---- properties
TypeOK == slot \in 0..N /\ probing \subseteq Procs
\* The slot lock is the exclusion: at most one live holder, and it is the slot's owner.
SingleHolder == \A p, q \in Procs : (alive[p] /\ alive[q] /\ pc[p] = "admitted" /\ pc[q] = "admitted") => p = q
\* Ticket order is fairness only: an admission passes an earlier live ticket
\* only through the skip rule (slot free, no urgent hold seen).
OrderedAdmission == ~outOfOrderBadly
\* A creator that passed its re-check is visible to every prober.
NoLostTicket == \A p \in Procs : (alive[p] /\ pc[p] \in {"waiting", "probing"}) => mine[p] \in dir
UrgentNotRefused == ~urgentRefused
\* Urgent work always gets going, whatever the waiters do.
UrgentNotBlocked == (upc = "trying") ~> (upc \in {"held", "nohold", "done"})
\* Every live waiter is eventually admitted (or stops being live).
BoundedWait == \A p \in Procs : (pc[p] = "waiting" /\ Live(p)) ~> (pc[p] \in {"admitted", "done"} \/ ~Live(p))
\* Witness (expected violated): a skip past a live earlier ticket is reachable.
NoFalseSkip == ~outOfOrder
=============================================================================
