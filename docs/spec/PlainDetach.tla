----------------------------- MODULE PlainDetach -----------------------------
\* Idle-gated admission only. HEAD CAS, authority, physical I/O and timing
\* remain the existing detach/cut obligations, outside this abstraction.
EXTENDS Naturals, TLC
CONSTANT IdleGuard
VARIABLES phase, readers, closed
vars == <<phase, readers, closed>>
Init == /\ phase = "serving" /\ readers = 0 /\ closed = FALSE
BeginIO == /\ phase = "serving" /\ ~closed /\ readers < 2
           /\ readers' = readers + 1 /\ UNCHANGED <<phase, closed>>
EndIO == /\ readers > 0 /\ readers' = readers - 1 /\ UNCHANGED <<phase, closed>>
Plain == /\ phase = "serving" /\ (~IdleGuard \/ readers = 0)
         /\ phase' = "stopped" /\ UNCHANGED <<readers, closed>>
Publish == /\ phase = "stopped" /\ phase' = "detached" /\ UNCHANGED <<readers, closed>>
Abort == /\ phase = "stopped" /\ phase' = "serving" /\ UNCHANGED <<readers, closed>>
Close == /\ ~closed /\ closed' = TRUE /\ UNCHANGED <<phase, readers>>
Next == Close \/ BeginIO \/ EndIO \/ Plain \/ Publish \/ Abort
Spec == Init /\ [][Next]_vars
IdleAtStop == phase \in {"stopped", "detached"} => readers = 0
=============================================================================
