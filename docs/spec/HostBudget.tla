----------------------------- MODULE HostBudget -----------------------------
\* Active coordinated reservations only: fallback, pacing, RSS and retained
\* crash scratch bytes are outside this abstraction. Crash frees active slots.
EXTENDS Integers, FiniteSets, TLC
CONSTANTS N, SlotCount, Payload, Spill, EarlyRelease, AtomicConfig
Owners == 1..N
Slots == 1..SlotCount
VARIABLES phase, owner, slot, saved, cfg, initializer
vars == <<phase, owner, slot, saved, cfg, initializer>>
Init == /\ cfg = "absent"
        /\ initializer = 0
        /\ phase = [o \in Owners |-> "free"]
        /\ owner = [s \in Slots |-> 0]
        /\ slot = [o \in Owners |-> 0]
        /\ saved = [o \in Owners |-> 0]
Admit(o,s) == /\ cfg = "ready"
              /\ phase[o] = "free" /\ owner[s] = 0
              /\ (o = N \/ s < SlotCount)
              /\ phase' = [phase EXCEPT ![o] = "active"]
              /\ owner' = [owner EXCEPT ![s] = o]
              /\ slot' = [slot EXCEPT ![o] = s]
              /\ UNCHANGED <<saved,cfg,initializer>>
Save(o) == /\ phase[o] = "active" /\ saved[o] < Spill
           /\ saved' = [saved EXCEPT ![o] = @ + 1]
           /\ UNCHANGED <<phase,owner,slot,cfg,initializer>>
Abort(o) == /\ phase[o] = "active"
            /\ phase' = [phase EXCEPT ![o] = "draining"]
            /\ owner' = IF EarlyRelease THEN [owner EXCEPT ![slot[o]] = 0] ELSE owner
            /\ UNCHANGED <<slot,saved,cfg,initializer>>
Join(o) == /\ phase[o] \in {"active","draining"}
           /\ phase' = [phase EXCEPT ![o] = "joined"]
           /\ UNCHANGED <<owner,slot,saved,cfg,initializer>>
Release(o) == /\ phase[o] = "joined"
              /\ owner' = [owner EXCEPT ![slot[o]] = 0]
              /\ phase' = [phase EXCEPT ![o] = "free"]
              /\ slot' = [slot EXCEPT ![o] = 0]
              /\ saved' = [saved EXCEPT ![o] = 0]
              /\ UNCHANGED <<cfg,initializer>>
Crash(o) == /\ phase[o] # "free"
            /\ owner' = [owner EXCEPT ![slot[o]] = 0]
            /\ phase' = [phase EXCEPT ![o] = "free"]
            /\ slot' = [slot EXCEPT ![o] = 0]
            /\ saved' = [saved EXCEPT ![o] = 0]
              /\ UNCHANGED <<cfg,initializer>>
BeginConfig(o) == /\ cfg = "absent"
                  /\ cfg' = IF AtomicConfig THEN "writing" ELSE "partial"
                  /\ initializer' = o
                  /\ UNCHANGED <<phase,owner,slot,saved>>
PublishConfig == /\ cfg \in {"writing","partial"}
                 /\ cfg' = "ready" /\ initializer' = 0
                 /\ UNCHANGED <<phase,owner,slot,saved>>
CrashInitializer == /\ cfg \in {"writing","partial"}
                    /\ cfg' = "absent" /\ initializer' = 0
                    /\ UNCHANGED <<phase,owner,slot,saved>>
Next == (\E o \in Owners: BeginConfig(o)) \/ PublishConfig \/ CrashInitializer \/
        (\E o \in Owners,s \in Slots: Admit(o,s)) \/
        (\E o \in Owners: Save(o) \/ Abort(o) \/ Join(o) \/ Release(o) \/ Crash(o))
Spec == Init /\ [][Next]_vars
NoPartialConfig == cfg # "partial"
ConfigBeforeAdmission == (\E o \in Owners: phase[o] # "free") => cfg = "ready"
HeldUntilJoined == \A o \in Owners: phase[o] # "free" => owner[slot[o]] = o
RECURSIVE Total(_)
Total(n) == IF n=0 THEN 0 ELSE saved[n]+Total(n-1)
PayloadBound == Cardinality({o \in Owners: phase[o] # "free"})*Payload <= SlotCount*Payload
SpillBound == Total(N) <= SlotCount*Spill
=============================================================================
