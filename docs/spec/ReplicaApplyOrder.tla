------------------------- MODULE ReplicaApplyOrder -------------------------
EXTENDS Integers, TLC

\* Two admitted sessions, one LBA, three versions. Bytes are represented by
\* their LSN. Storage faults, restart persistence and authority are external
\* obligations; this model checks successful apply ordering and probe dedupe.
CONSTANTS SharedVersions, PrepareEachEvent, MonotoneFacts, PreserveUnknownConfidence
VARIABLES applied, local, bytes, seen, stage, active, starts, replicaR, factsKnown, knownDecision
vars == <<applied, local, bytes, seen, stage, active, starts, replicaR, factsKnown, knownDecision>>
Max(a,b) == IF a > b THEN a ELSE b
Init == /\ applied = 0 /\ local = [s \in 1..2 |-> 0]
        /\ bytes = 0 /\ seen = 0 /\ stage = 0
        /\ active = FALSE /\ starts = 0 /\ replicaR = 0
        /\ factsKnown = TRUE /\ knownDecision = TRUE

\* One shared mutex covers stale check, physical apply and version publication.
Apply(s,n) == LET floor == IF SharedVersions THEN applied ELSE local[s]
              IN /\ bytes' = IF n > floor THEN n ELSE bytes
                 /\ applied' = Max(applied,n)
                 /\ local' = [local EXCEPT ![s] = Max(@,n)]
                 /\ seen' = Max(seen,n)
                 /\ UNCHANGED <<stage, active, starts, replicaR, factsKnown, knownDecision>>

\* A probe's two semantic events must observe the same reserved session.
FirstEvent == /\ stage = 0 /\ stage' = 1 /\ starts' = 1
              /\ active' = PrepareEachEvent
              /\ UNCHANGED <<applied, local, bytes, seen, replicaR, factsKnown, knownDecision>>
SecondEvent == /\ stage = 1 /\ stage' = 2
               /\ starts' = IF active THEN starts ELSE starts + 1
               /\ active' = TRUE
               /\ UNCHANGED <<applied, local, bytes, seen, replicaR, factsKnown, knownDecision>>
Complete == /\ stage = 2 /\ stage' = 3 /\ replicaR' = 3
            /\ active' = FALSE
            /\ UNCHANGED <<applied, local, bytes, seen, starts, factsKnown, knownDecision>>
DelayedProbe == /\ stage = 3 /\ stage' = 4
                /\ replicaR' = IF MonotoneFacts THEN replicaR ELSE 1
                /\ starts' = IF replicaR' < 3 THEN starts + 1 ELSE starts
                /\ active' = (replicaR' < 3)
                /\ UNCHANGED <<applied, local, bytes, seen, factsKnown, knownDecision>>
\* Retaining durable R does not give an all-zero probe known confidence.
MissingProbe == /\ stage = 3 /\ stage' = 5
                /\ factsKnown' = FALSE
                /\ knownDecision' = ~PreserveUnknownConfidence
                /\ UNCHANGED <<applied, local, bytes, seen, active, starts, replicaR>>
FreshAck == /\ stage = 5 /\ stage' = 3
            /\ factsKnown' = TRUE /\ knownDecision' = TRUE
            /\ UNCHANGED <<applied, local, bytes, seen, active, starts, replicaR>>
Next == (\E s \in 1..2, n \in 1..3 : Apply(s,n))
        \/ FirstEvent \/ SecondEvent \/ Complete \/ DelayedProbe \/ MissingProbe \/ FreshAck
Spec == Init /\ [][Next]_vars
NoRegression == bytes = seen
NoDuplicateRecovery == starts <= 1
NoFalseKnown == ~factsKnown => ~knownDecision
=============================================================================
