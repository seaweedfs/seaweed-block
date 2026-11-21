------------------------------ MODULE RestoreIndependence ------------------------------
(***************************************************************************)
(* Independent restore (O-REC-2): restore a snapshot into a volume that   *)
(* ends up owning all its chunks, with no lineage to the source.          *)
(* Source volume V with HEAD and one sealed snapshot S; target T restored  *)
(* from S. T's HEAD maps indexes to fids; a fid is owned by V (a "@V/"     *)
(* ref) or by T. Steps: 0 claim S; 1 fork (T HEAD = S's fids, all V-owned) *)
(* ; 2 hydrate (local, not modelled beyond "local" flags); 3 OWNING =     *)
(* batch cuts, each re-uploads up to Batch V-owned indexes as T-owned fids *)
(* (intent -> upload -> HEAD CAS); 4 marker after the last V ref is gone,  *)
(* then drop the claim. A GC (machine E, reduced: deletes only fids that   *)
(* no root reaches) may run at any point; V may be deleted at any point;   *)
(* the restore process may crash and a new owner resumes from REMOTE facts *)
(* only (forced move). Mutants are constants (FALSE = the design).        *)
(* Reuse: the fid/owner/root vocabulary of the chunk GC model; this       *)
(* spec models only what the restore adds, not the superseded list/grace.  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
  NIdx,              \* indexes of the volume (1..NIdx)
  Batch,             \* indexes per independence batch cut
  MaxCrash,          \* restore-process crashes (each followed by a resume)
  MaxGc,             \* GC rounds
  \* ---- mutants (FALSE = the design) ----
  DropClaimAtFork,   \* r0 mutant: the claim on S is dropped right after the fork
  CasBeforeUpload,   \* batch CAS swaps a ref before its own upload is durable
  ListForeign,       \* the batch lists the replaced V fid for GC (rule 8 broken)
  MarkerEarly,       \* marker written before the last V ref is gone
  MarkerBeforeCas,   \* marker written before the final batch's HEAD CAS
  ResumeLocal,       \* a new owner trusts a local "done" flag instead of remote facts
  SnapDuringRestore, \* a snapshot of T is allowed before the marker
  RetentionIgnoresMarker, \* after the marker, V's retention still treats T as holding S
  \* ---- added in review (r0.1) ----
  ClaimUntilHead,    \* r0.2 OPTION 1: the claim is dropped when T's HEAD is published (today's fork semantics)
  NoIntent           \* uploaded batch fids are not rooted by an intent before the HEAD CAS

Idx  == 1..NIdx
VF   == [i \in Idx |-> <<"V", i>>]          \* the source fid of index i
TF   == [i \in Idx |-> <<"T", i>>]          \* T's own fid for index i
Fids == {VF[i] : i \in Idx} \cup {TF[i] : i \in Idx}
\* r0.1: "no HEAD yet" is a function too (TLC cannot compare a function with a string)
NoHead == [i \in Idx |-> <<"N", i>>]

VARIABLES
  store,    \* Fids -> "none" | "pres" | "del"
  vlive,    \* V still exists (its HEAD roots V fids)
  slive,    \* S exists
  claim,    \* the restore's claim on S
  thead,    \* T's HEAD: Idx -> fid, or "none" before the fork
  step,     \* restore step: "c" claimed | "f" forked | "own" | "done"
  up,       \* T fids uploaded durably but not yet in HEAD (current batch)
  marker,   \* restore/INDEPENDENT exists (remote)
  tsnap,    \* a snapshot of T (its fid set), {} if none
  gclist,   \* fids put on a GC list (rule 8 check)
  localdone,\* local "restore done" flag of the current owner (ResumeLocal mutant)
  crashes, gcs

vars == <<store, vlive, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>

THeadFids == IF thead = NoHead THEN {} ELSE {thead[i] : i \in Idx}
VRefs     == {i \in Idx : thead # NoHead /\ thead[i][1] = "V"}
\* roots: V's HEAD (while V lives), S (while it lives), T's HEAD, T's snapshot,
\* uploaded-but-unpublished batch fids (intent roots them), S's fids while claimed
SFids     == {VF[i] : i \in Idx}
Roots     == (IF vlive THEN SFids ELSE {})
             \cup (IF slive THEN SFids ELSE {})
             \cup THeadFids \cup tsnap \cup (IF NoIntent THEN {} ELSE up)
\* S may be deleted by retention when nothing holds it
SHeld     == claim \/ (RetentionIgnoresMarker /\ thead # NoHead)

Init ==
  /\ store = [f \in Fids |-> IF f[1] = "V" THEN "pres" ELSE "none"]
  /\ vlive = TRUE /\ slive = TRUE /\ claim = FALSE /\ thead = NoHead
  /\ step = "start" /\ up = {} /\ marker = FALSE /\ tsnap = {} /\ gclist = {}
  /\ localdone = FALSE /\ crashes = 0 /\ gcs = 0

\* step 0: S is sealed (given) and claimed
Claim == step = "start" /\ slive /\ claim' = TRUE /\ step' = "c"
         /\ UNCHANGED <<store, vlive, slive, thead, up, marker, tsnap, gclist, localdone, crashes, gcs>>

\* step 1: fork (T HEAD names S's fids, all V-owned)
\* r0.1: two steps. Between them T exists without a HEAD, so only the claim keeps S (and
\* through S its fids) alive; that window is what the claim is for.
ForkBegin == step = "c" /\ step' = "fk"
        /\ claim' = (IF DropClaimAtFork THEN FALSE ELSE claim)
        /\ UNCHANGED <<store, vlive, slive, thead, up, marker, tsnap, gclist, localdone, crashes, gcs>>
ForkEnd == step = "fk" /\ thead' = VF /\ step' = "own"
        /\ claim' = (IF ClaimUntilHead THEN FALSE ELSE claim)
        /\ UNCHANGED <<store, vlive, slive, up, marker, tsnap, gclist, localdone, crashes, gcs>>

\* step 3: one batch: upload own fids for up to Batch V-owned indexes (intent roots them)
BatchSet == {B \in SUBSET VRefs : B # {} /\ Cardinality(B) <= Batch}
Upload(B) == step = "own" /\ up = {} /\ ~ CasBeforeUpload
             /\ (\A i \in B : store[VF[i]] = "pres")        \* reads its local copy (filled from V)
             /\ store' = [f \in Fids |-> IF f \in {TF[j] : j \in B} THEN "pres" ELSE store[f]]
             /\ up' = {TF[j] : j \in B}
             /\ UNCHANGED <<vlive, slive, claim, thead, step, marker, tsnap, gclist, localdone, crashes, gcs>>
\* the batch HEAD CAS: swap exactly the uploaded indexes
Cas == step = "own" /\ up # {}
       /\ thead' = [i \in Idx |-> IF TF[i] \in up THEN TF[i] ELSE thead[i]]
       /\ gclist' = (IF ListForeign THEN gclist \cup {VF[i] : i \in {j \in Idx : TF[j] \in up}} ELSE gclist)
       /\ up' = {}
       /\ localdone' = (localdone \/ \A i \in Idx : (thead[i][1] = "V") => (TF[i] \in up))
       /\ UNCHANGED <<store, vlive, slive, claim, step, marker, tsnap, crashes, gcs>>
\* mutant: CAS a batch whose upload is not durable (the T fid is still "none")
CasEarly(B) == CasBeforeUpload /\ step = "own" /\ up = {}
       /\ thead' = [i \in Idx |-> IF i \in B THEN TF[i] ELSE thead[i]]
       /\ UNCHANGED <<store, vlive, slive, claim, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>
UploadLate == CasBeforeUpload /\ step = "own"
       /\ \E i \in Idx : thead[i] = TF[i] /\ store[TF[i]] = "none"
             /\ store' = [store EXCEPT ![TF[i]] = "pres"]
       /\ UNCHANGED <<vlive, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>

\* step 4: marker after the last V ref is gone (and the final CAS is done), then drop the claim
Marker == step = "own" /\ ~ marker /\ up = {}
          /\ (VRefs = {} \/ MarkerEarly \/ (MarkerBeforeCas /\ Cardinality(VRefs) <= Batch))
          /\ marker' = TRUE /\ step' = "done" /\ localdone' = TRUE
          /\ UNCHANGED <<store, vlive, slive, claim, thead, up, tsnap, gclist, crashes, gcs>>
DropClaim == marker /\ claim /\ claim' = FALSE
          /\ UNCHANGED <<store, vlive, slive, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>

\* a snapshot of T: refused before the marker (unless the mutant allows it)
SnapT == tsnap = {} /\ thead # NoHead /\ (marker \/ SnapDuringRestore)
         /\ tsnap' = THeadFids
         /\ UNCHANGED <<store, vlive, slive, claim, thead, step, up, marker, gclist, localdone, crashes, gcs>>

\* crash of the restore process; the new owner resumes from remote facts (thead, marker)
Crash == crashes < MaxCrash /\ step \in {"fk", "own", "done"}
         /\ up' = {}                         \* an unpublished batch is abandoned; its fids are collectable
         /\ step' = (IF ResumeLocal /\ localdone THEN "done"
                    ELSE IF marker THEN "done" ELSE IF thead = NoHead THEN "fk" ELSE "own")
         /\ crashes' = crashes + 1
         /\ UNCHANGED <<store, vlive, slive, claim, thead, marker, tsnap, gclist, localdone, gcs>>

\* user deletes V; retention deletes S when nothing holds it
DeleteV == vlive /\ vlive' = FALSE
           /\ UNCHANGED <<store, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>
DeleteS == slive /\ ~ SHeld /\ slive' = FALSE
           /\ UNCHANGED <<store, vlive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>

\* GC (reduced machine E): deletes present fids no root reaches, and fids put on the list
Gc == gcs < MaxGc
      /\ store' = [f \in Fids |-> IF store[f] = "pres" /\ (f \notin Roots \/ f \in gclist) THEN "del" ELSE store[f]]
      /\ gcs' = gcs + 1
      /\ UNCHANGED <<vlive, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes>>

Next == Claim \/ ForkBegin \/ ForkEnd \/ Cas \/ Marker \/ DropClaim \/ SnapT \/ Crash \/ DeleteV \/ DeleteS \/ Gc \/ UploadLate
        \/ (\E B \in BatchSet : Upload(B) \/ CasEarly(B))

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* INV-RESTORE-NO-GAP: every index of T's HEAD resolves to a present fid
NoGap == thead = NoHead \/ \A i \in Idx : store[thead[i]] = "pres"
\* INV-RESTORE-INDEPENDENT: after the marker, T roots no V fid (HEAD or snapshot)
Independent == marker => (VRefs = {} /\ \A f \in tsnap : f[1] = "T")
\* INV-RESTORE-NO-FOREIGN-LIST: a restore never lists a V fid
NoForeignList == \A f \in gclist : f[1] = "T"
\* INV-RESTORE-CLAIM-UNTIL-INDEPENDENT
ClaimUntilIndependent == (thead # NoHead /\ ~ marker) => claim
\* r0.2 option 1: the claim covers exactly the window in which T exists without a HEAD
ClaimUntilHead_Inv == (step = "fk") => claim
\* lifecycle independence: after the marker and the claim drop, nothing of T holds S
LifecycleFree == (marker /\ ~ claim) => ~ SHeld
\* INV-RESTORE-STATE-REMOTE: "done" only when the remote marker exists
StateRemote == step = "done" => marker

TypeOK == /\ step \in {"start", "c", "fk", "own", "done"} /\ up \subseteq Fids /\ tsnap \subseteq Fids
\* witnesses (r0.1): each is an invariant expected to be VIOLATED, proving the state is reachable
W_Done        == ~ (marker /\ ~ claim /\ ~ slive /\ ~ vlive /\ \A i \in Idx : store[VF[i]] = "del")
W_CrashResume == ~ (marker /\ crashes = MaxCrash)
=============================================================================
