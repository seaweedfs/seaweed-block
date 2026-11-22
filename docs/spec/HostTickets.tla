---------------------------- MODULE HostTickets ----------------------------
\* M7 waiting -> admitted: timer cuts wait for the one normal slot in order of
\* their tickets (flock-held files in the pool directory, ordered by due time).
\* Processes may die at any step (the kernel drops their locks; their files
\* stay) or stall (they take no further step and keep their locks). A ticket
\* is taken by create, lock, then an inode re-check; a prober removes a ticket
\* it can lock. A waiter may skip an earlier ticket that stays ahead while the
\* slot is free and no urgent work is pending (T_skip, abstracted as "the
\* holder is stalled"). Urgent work takes urgent-pending shared; a waiter
\* probes it exclusively.
\*
\* Mutants (constants set TRUE must violate):
\*   NoSkip           -> BoundedWait     (a stalled head blocks every waiter)
\*   NoInodeCheck     -> NoLostTicket    (a creator keeps a lock on an unlinked file)
\*   ExclusiveRefuses -> UrgentNotRefused (a probe refuses an urgent admission)
EXTENDS Integers, FiniteSets, TLC
CONSTANTS N, MaxGen, NoSkip, NoInodeCheck, ExclusiveRefuses, Faults, MaxUrgent

Procs == 1..N
Due(p) == p                       \* distinct due times: lower runs first
FileIds == {<<p, g>> : p \in Procs, g \in 1..MaxGen}
DueOf(f) == Due(f[1])

VARIABLES
  dir,        \* ticket files present in the directory
  lockOf,     \* file -> locking process (0: unlocked)
  pc,         \* process step
  mine,       \* the process's current ticket file (or <<0,0>>)
  gen,        \* tickets created so far by the process
  alive, stalled,
  slot,       \* holder of the one normal slot (0: free)
  probing,    \* processes inside an urgent-pending probe
  urgent,     \* urgent work holding urgent-pending
  urgentRefused,
  faults,
  urgents,    \* urgent episodes so far (bounded, so urgent work cannot starve waiters)
  badAdmit    \* history: an admission happened while an earlier ticket was live and not stalled
vars == <<dir, lockOf, pc, mine, gen, alive, stalled, slot, probing, urgent, urgentRefused, faults, urgents, badAdmit>>
None == <<0, 0>>

Init == /\ dir = {} /\ lockOf = [f \in FileIds |-> 0]
        /\ pc = [p \in Procs |-> "idle"] /\ mine = [p \in Procs |-> None]
        /\ gen = [p \in Procs |-> 0]
        /\ alive = [p \in Procs |-> TRUE] /\ stalled = [p \in Procs |-> FALSE]
        /\ slot = 0 /\ probing = {} /\ urgent = FALSE /\ urgentRefused = FALSE
        /\ faults = 0 /\ urgents = 0 /\ badAdmit = FALSE

Live(p) == alive[p] /\ ~stalled[p]

\* -- taking a ticket: create, lock, re-check
Create(p) == /\ Live(p) /\ pc[p] = "idle" /\ gen[p] < MaxGen
             /\ LET f == <<p, gen[p] + 1>> IN
                  /\ dir' = dir \cup {f} /\ mine' = [mine EXCEPT ![p] = f]
                  /\ gen' = [gen EXCEPT ![p] = @ + 1]
             /\ pc' = [pc EXCEPT ![p] = "created"]
             /\ UNCHANGED <<lockOf, alive, stalled, slot, probing, urgent, urgentRefused, faults, urgents, badAdmit>>
Lock(p) == /\ Live(p) /\ pc[p] = "created" /\ lockOf[mine[p]] = 0
           /\ lockOf' = [lockOf EXCEPT ![mine[p]] = p]
           /\ pc' = [pc EXCEPT ![p] = "locked"]
           /\ UNCHANGED <<dir, mine, gen, alive, stalled, slot, probing, urgent, urgentRefused, faults, urgents, badAdmit>>
Recheck(p) == /\ Live(p) /\ pc[p] = "locked"
              /\ IF mine[p] \in dir \/ NoInodeCheck
                   THEN /\ pc' = [pc EXCEPT ![p] = "waiting"]
                        /\ UNCHANGED <<lockOf, mine>>
                   ELSE /\ lockOf' = [lockOf EXCEPT ![mine[p]] = 0]   \* path gone: start over
                        /\ mine' = [mine EXCEPT ![p] = None]
                        /\ pc' = [pc EXCEPT ![p] = "idle"]
              /\ UNCHANGED <<dir, gen, alive, stalled, slot, probing, urgent, urgentRefused, faults, urgents, badAdmit>>

\* -- a prober removes a ticket it can lock (a dead holder's, or one not yet locked)
Reap(q, f) == /\ Live(q) /\ pc[q] = "waiting" /\ f \in dir /\ f # mine[q] /\ lockOf[f] = 0
              /\ dir' = dir \ {f}
              /\ UNCHANGED <<lockOf, pc, mine, gen, alive, stalled, slot, probing, urgent, urgentRefused, faults, urgents, badAdmit>>

\* -- admission: head (earlier tickets all skippable), urgent-pending probe, free slot
Skippable(p, f) == ~NoSkip /\ lockOf[f] # 0 /\ stalled[lockOf[f]] /\ slot = 0 /\ ~urgent
Head(p) == \A f \in dir : DueOf(f) < Due(p) => Skippable(p, f)
BeginProbe(p) == /\ Live(p) /\ pc[p] = "waiting" /\ mine[p] \in dir /\ Head(p) /\ slot = 0
                 /\ probing' = probing \cup {p} /\ pc' = [pc EXCEPT ![p] = "probing"]
                 /\ UNCHANGED <<dir, lockOf, mine, gen, alive, stalled, slot, urgent, urgentRefused, faults, urgents, badAdmit>>
Admit(p) == /\ Live(p) /\ pc[p] = "probing"
            /\ probing' = probing \ {p}
            /\ IF ~urgent /\ slot = 0 /\ Head(p)
                 THEN /\ slot' = p /\ pc' = [pc EXCEPT ![p] = "admitted"]
                      /\ badAdmit' = (badAdmit \/ \E f \in dir : DueOf(f) < Due(p) /\ lockOf[f] # 0 /\ Live(lockOf[f]))
                      /\ dir' = dir \ {mine[p]}
                      /\ lockOf' = [lockOf EXCEPT ![mine[p]] = 0]
                      /\ mine' = [mine EXCEPT ![p] = None]
                 ELSE /\ pc' = [pc EXCEPT ![p] = "waiting"]
                      /\ UNCHANGED <<slot, dir, lockOf, mine, badAdmit>>
            /\ UNCHANGED <<gen, alive, stalled, urgent, urgentRefused, faults, urgents>>
Release(p) == /\ Live(p) /\ pc[p] = "admitted" /\ slot = p
              /\ slot' = 0 /\ pc' = [pc EXCEPT ![p] = "done"]
              /\ UNCHANGED <<dir, lockOf, mine, gen, alive, stalled, probing, urgent, urgentRefused, faults, urgents, badAdmit>>

\* -- urgent work: takes urgent-pending shared (blocking: waits out a probe instant)
UrgentOn == /\ ~urgent /\ urgents < MaxUrgent
            /\ IF probing # {} /\ ExclusiveRefuses
                 THEN urgentRefused' = TRUE /\ UNCHANGED urgent
                 ELSE /\ probing = {} \/ ExclusiveRefuses
                      /\ urgent' = TRUE /\ UNCHANGED urgentRefused
            /\ urgents' = urgents + 1
            /\ UNCHANGED <<dir, lockOf, pc, mine, gen, alive, stalled, slot, probing, faults, badAdmit>>
UrgentOff == /\ urgent /\ urgent' = FALSE
             /\ UNCHANGED <<dir, lockOf, pc, mine, gen, alive, stalled, slot, probing, urgentRefused, faults, urgents, badAdmit>>

\* -- faults: death drops every kernel lock of the process; a stall keeps them.
\* A stalled slot holder is outside M7 (the supervisor's premise), so only a
\* process that does not hold the slot stalls.
Die(p) == /\ alive[p] /\ faults < Faults
          /\ alive' = [alive EXCEPT ![p] = FALSE] /\ faults' = faults + 1
          /\ lockOf' = [f \in FileIds |-> IF lockOf[f] = p THEN 0 ELSE lockOf[f]]
          /\ slot' = IF slot = p THEN 0 ELSE slot
          /\ probing' = probing \ {p}
          /\ UNCHANGED <<dir, pc, mine, gen, stalled, urgent, urgentRefused, urgents, badAdmit>>
Stall(p) == /\ Live(p) /\ slot # p /\ pc[p] \notin {"probing"} /\ faults < Faults
            /\ stalled' = [stalled EXCEPT ![p] = TRUE] /\ faults' = faults + 1
            /\ UNCHANGED <<dir, lockOf, pc, mine, gen, alive, slot, probing, urgent, urgentRefused, urgents, badAdmit>>

Step(p) == Create(p) \/ Lock(p) \/ Recheck(p) \/ BeginProbe(p) \/ Admit(p) \/ Release(p)
           \/ (\E f \in FileIds : Reap(p, f))
Next == (\E p \in Procs : Step(p) \/ Die(p) \/ Stall(p)) \/ UrgentOn \/ UrgentOff
Spec == Init /\ [][Next]_vars /\ \A p \in Procs : WF_vars(Step(p)) /\ WF_vars(UrgentOff)

\* ---- properties
TypeOK == slot \in 0..N /\ probing \subseteq Procs
\* Never admit while an earlier ticket is live and not stalled.
OrderedAdmission == ~badAdmit
\* A creator that passed its re-check is visible to every prober.
NoLostTicket == \A p \in Procs : (alive[p] /\ pc[p] \in {"waiting", "probing"}) => mine[p] \in dir
UrgentNotRefused == ~urgentRefused
\* Every live, never-stalled waiter is eventually admitted.
BoundedWait == \A p \in Procs : (pc[p] = "waiting" /\ Live(p)) ~> (pc[p] \in {"admitted", "done"} \/ ~Live(p))
=============================================================================
