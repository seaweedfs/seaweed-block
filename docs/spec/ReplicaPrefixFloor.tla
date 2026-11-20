----------------------- MODULE ReplicaPrefixFloor -----------------------
EXTENDS Integers, TLC
CONSTANTS UseFloor, CertifiedOnly
VARIABLES data, seen, versions, durable, prefix
vars == <<data, seen, versions, durable, prefix>>
Addr(n) == IF n = 2 THEN 2 ELSE 1
Max(a,b) == IF a > b THEN a ELSE b
Covered(p) == \A n \in 1..p : data[Addr(n)] >= n
Init == /\ data = [i \in 1..2 |-> 0] /\ seen = data /\ versions = data
        /\ durable = 0 /\ prefix = 0
Apply(n) == LET i == Addr(n)
                threshold == Max(versions[i], IF UseFloor THEN prefix ELSE 0)
            IN /\ data' = IF n > threshold THEN [data EXCEPT ![i] = n] ELSE data
               /\ seen' = [seen EXCEPT ![i] = Max(@,n)]
               /\ versions' = IF n > threshold THEN [versions EXCEPT ![i] = n] ELSE versions
               /\ UNCHANGED <<durable,prefix>>
Sync == /\ durable' = Max(durable, Max(data[1],data[2]))
        /\ UNCHANGED <<data,seen,versions,prefix>>
Certify(p) == /\ p > prefix /\ p <= durable
              /\ (~CertifiedOnly \/ Covered(p))
              /\ prefix' = p
              /\ versions' = [i \in 1..2 |-> IF versions[i] <= p THEN 0 ELSE versions[i]]
              /\ UNCHANGED <<data,seen,durable>>
\* Session changes, ordinary live writes, disconnects and Sync do not certify.
RestartSession == UNCHANGED vars
Next == (\E n \in 1..3 : Apply(n)) \/ Sync \/ (\E p \in 1..3 : Certify(p)) \/ RestartSession
Spec == Init /\ [][Next]_vars
NoRegression == data = seen
CertifiedCoverage == Covered(prefix)
OnlySuffix == \A i \in 1..2 : versions[i] = 0 \/ versions[i] > prefix
=============================================================================