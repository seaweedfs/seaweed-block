------------------------- MODULE GCClockSkew -------------------------
EXTENDS Integers, TLC
CONSTANTS G, S, Q, MaxT, IgnoreSkew, IgnoreResolution, AbortFuture
VARIABLES t, bias, rounded, stamp, phase, removed, removedOld
vars == <<t,bias,rounded,stamp,phase,removed,removedOld>>
Born == 10
Init == /\ t = Born /\ bias \in {-S,0,S} /\ rounded \in {0,Q}
        /\ stamp = Born+bias-rounded /\ phase = "check"
        /\ removed = FALSE /\ removedOld = FALSE
Allowance == (IF IgnoreSkew THEN 0 ELSE S)+(IF IgnoreResolution THEN 0 ELSE rounded)
Check == /\ phase = "check"
         /\ removed' = (removed \/ (t-stamp > G+Allowance))
         /\ removedOld' = (removedOld \/ ~(AbortFuture /\ stamp > t))
         /\ phase' = "done"
         /\ UNCHANGED <<t,bias,rounded,stamp>>
Tick == /\ phase = "done" /\ t < MaxT /\ t' = t+1 /\ phase' = "check"
        /\ UNCHANGED <<bias,rounded,stamp,removed,removedOld>>
Next == Check \/ Tick
Spec == Init /\ [][Next]_vars
NoEarlyExpiry == removed => t-Born > G
FutureProtects == stamp > t => ~removed
IndependentProgress == phase = "done" => removedOld
EventuallyEligibleAtBound == (phase = "done" /\ t = MaxT) => removed
=============================================================================
