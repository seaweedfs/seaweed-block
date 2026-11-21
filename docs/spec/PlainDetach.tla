----------------------------- MODULE PlainDetach -----------------------------
\* Idle-gated admission only. HEAD CAS, authority, physical I/O and timing
\* remain the existing detach/cut obligations, outside this abstraction.
EXTENDS Naturals, TLC
CONSTANT IdleGuard
VARIABLES phase, readers
vars == <<phase, readers>>
Init == /\ phase = "serving" /\ readers = 0
BeginIO == /\ phase = "serving" /\ readers < 2
           /\ readers' = readers + 1 /\ UNCHANGED phase
EndIO == /\ readers > 0 /\ readers' = readers - 1 /\ UNCHANGED phase
Plain == /\ phase = "serving" /\ (~IdleGuard \/ readers = 0)
         /\ phase' = "stopped" /\ UNCHANGED readers
Publish == /\ phase = "stopped" /\ phase' = "detached" /\ UNCHANGED readers
Abort == /\ phase = "stopped" /\ phase' = "serving" /\ UNCHANGED readers
Next == BeginIO \/ EndIO \/ Plain \/ Publish \/ Abort
Spec == Init /\ [][Next]_vars
IdleAtStop == phase \in {"stopped", "detached"} => readers = 0
=============================================================================
