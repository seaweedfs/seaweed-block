----------------------- MODULE PrecopyFixedAdmission -----------------------
\* Admission only: data/authority/publication safety is covered by CutCOW2.
EXTENDS Integers, TLC
CONSTANTS OmitTailGuard, OmitStallGuard, OmitFreezeCap
Allowance == 12
FreezeLimit == 50
Margin == 1
VARIABLES fixed, tail, previous, estimate, phase, declared, round
vars == <<fixed,tail,previous,estimate,phase,declared,round>>
Init == /\ fixed \in {1,13,60}
        /\ tail \in {0,1,3,4,20}
        /\ previous = 0 /\ estimate = fixed + tail + Margin
        /\ phase = "decide" /\ declared = 0 /\ round = 1
Exception == /\ (OmitTailGuard \/ tail <= Allowance \div 4)
             /\ (OmitStallGuard \/ (previous > 0 /\ estimate >= previous))
             /\ (OmitFreezeCap \/ estimate <= FreezeLimit)
Eligible == estimate <= Allowance \/ Exception
Commit == /\ phase = "decide" /\ Eligible
          /\ phase' = "committed" /\ declared' = estimate
          /\ UNCHANGED <<fixed,tail,previous,estimate,round>>
Copy == /\ phase = "decide" /\ ~Eligible /\ round < 5
        /\ \E nextTail \in 0..tail:
              /\ tail' = nextTail /\ previous' = estimate
              /\ estimate' = fixed + nextTail + Margin
        /\ round' = round + 1
        /\ UNCHANGED <<fixed,phase,declared>>
Refuse == /\ phase = "decide" /\ ~Eligible /\ round = 5
          /\ phase' = "refused"
          /\ UNCHANGED <<fixed,tail,previous,estimate,declared,round>>
Done == /\ phase # "decide" /\ UNCHANGED vars
Next == Commit \/ Copy \/ Refuse \/ Done
Spec == Init /\ [][Next]_vars
SafeCommit == phase = "committed" =>
                /\ declared = estimate /\ estimate <= FreezeLimit
                /\ (estimate > Allowance =>
                      /\ tail <= Allowance \div 4
                      /\ previous > 0 /\ estimate >= previous)
=============================================================================
