------------------------ MODULE FrozenDetachBudget ------------------------
\* One externally frozen attempt; physical latency and orchestrator retries
\* are outside this model. Authority is stable during the modeled attempt;
\* mid-cut authority/CAS races remain CutCOW2/publication proof obligations.
EXTENDS Integers, TLC
CONSTANTS OmitElapsed, OmitBarrierGuard, OmitStageGuard,
          OmitOverrideRecord, OmitAuthority
Limit == 50
VARIABLES phase, elapsed, estimate, known, force, authority, admitted,
          uploaded, published, overBudget, atElapsed, atEstimate, atKnown
vars == <<phase,elapsed,estimate,known,force,authority,admitted,
          uploaded,published,overBudget,atElapsed,atEstimate,atKnown>>
Init == /\ phase = "forecast" /\ elapsed \in {0,30,60}
        /\ estimate \in {0,5,30,60} /\ known \in BOOLEAN
        /\ force \in BOOLEAN /\ authority \in BOOLEAN
        /\ admitted = FALSE /\ uploaded = FALSE /\ published = FALSE
        /\ overBudget = FALSE /\ atElapsed = 0 /\ atEstimate = 0
        /\ atKnown = FALSE
AuthorityOK == authority \/ OmitAuthority
FrozenEstimateKnown == known /\ estimate > 0
MaxRefreezeQuiesce == IF FrozenEstimateKnown /\ estimate < Limit THEN Limit-estimate ELSE 0
Fits == FrozenEstimateKnown /\ estimate + (IF OmitElapsed THEN 0 ELSE elapsed) <= Limit
Eligible == AuthorityOK /\ (force \/ Fits)
Forecast == /\ phase = "forecast" /\ Eligible /\ phase' = "stage"
            /\ UNCHANGED <<elapsed,estimate,known,force,authority,admitted,
                           uploaded,published,overBudget,atElapsed,atEstimate,atKnown>>
RefuseForecast == /\ phase = "forecast" /\ ~Eligible /\ phase' = "refused"
                  /\ UNCHANGED <<elapsed,estimate,known,force,authority,admitted,
                                 uploaded,published,overBudget,atElapsed,atEstimate,atKnown>>
Barrier == /\ phase = "stage" /\ phase' = "barrier"
           /\ elapsed' \in {elapsed,elapsed+20}
           /\ estimate' \in {estimate,estimate+30} /\ known' \in BOOLEAN
           /\ uploaded' = (uploaded \/ OmitStageGuard)
           /\ UNCHANGED <<force,authority,admitted,published,overBudget,atElapsed,atEstimate,atKnown>>
Admit == /\ phase = "barrier" /\ (Eligible \/ (OmitBarrierGuard /\ AuthorityOK))
         /\ phase' = "admitted" /\ admitted' = TRUE
         /\ atElapsed' = elapsed /\ atEstimate' = estimate /\ atKnown' = known
         /\ overBudget' = (force /\ ~OmitOverrideRecord)
         /\ UNCHANGED <<elapsed,estimate,known,force,authority,uploaded,published>>
RefuseBarrier == /\ phase = "barrier" /\ ~Eligible /\ ~OmitBarrierGuard
                 /\ phase' = "refused"
                 /\ UNCHANGED <<elapsed,estimate,known,force,authority,admitted,
                                uploaded,published,overBudget,atElapsed,atEstimate,atKnown>>
Upload == /\ phase = "admitted" /\ phase' = "uploaded" /\ uploaded' = TRUE
          /\ UNCHANGED <<elapsed,estimate,known,force,authority,admitted,
                         published,overBudget,atElapsed,atEstimate,atKnown>>
Publish == /\ phase = "uploaded" /\ AuthorityOK /\ phase' = "done"
           /\ published' = TRUE
           /\ UNCHANGED <<elapsed,estimate,known,force,authority,admitted,
                          uploaded,overBudget,atElapsed,atEstimate,atKnown>>
Done == /\ phase \in {"done","refused"} /\ UNCHANGED vars
Next == Forecast \/ RefuseForecast \/ Barrier \/ Admit \/ RefuseBarrier \/ Upload \/ Publish \/ Done
Spec == Init /\ [][Next]_vars
NoUploadBeforeAdmission == uploaded => admitted
BudgetAtAdmission == admitted /\ ~force => atKnown /\ atEstimate > 0 /\ atElapsed + atEstimate <= Limit
OverrideVisible == admitted /\ force => overBudget
AuthorityAtPublication == published => authority
ReportSlackSound == MaxRefreezeQuiesce >= 0 /\
    (MaxRefreezeQuiesce > 0 => known /\ estimate > 0 /\ MaxRefreezeQuiesce + estimate = Limit) /\
    (~FrozenEstimateKnown => MaxRefreezeQuiesce = 0)
=============================================================================
