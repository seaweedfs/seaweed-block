------------------------------ MODULE RestoreIndependence ------------------------------
(***************************************************************************)
(* Independent restore of a snapshot into a new volume.                    *)
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
(* Reuse: the fid/owner/root vocabulary of the chunk GC model; this        *)
(* spec models only what the restore adds, not the superseded list/grace.  *)
(* NOCLAIM: with NoClaim = TRUE claims are removed;                        *)
(* a snapshot delete never looks at a fork. Safety of the ForkBegin..      *)
(* ForkEnd window comes from two time rules on one clock: (D) ForkEnd only *)
(* while now < readAt + D, else ForkFail; (G) a deleted S stays a GC root  *)
(* while now < delAt + G. With NoClaim = FALSE nothing below changes the   *)
(* old model (Tick, ForkFail, ForkNotFound disabled; new variables fixed). *)
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
  NoIntent,          \* uploaded batch fids are not rooted by an intent before the HEAD CAS
  \* ---- NOCLAIM variant; old cfgs: NoClaim = FALSE, D = G = MaxTick = 0, the three below TRUE ----
  NoClaim,           \* TRUE: no claim ever holds S; DeleteS needs only slive; time rules D and G apply
  D,                 \* fork deadline: ForkEnd only while now < readAt + D (read only when NoClaim)
  G,                 \* grace: a deleted S stays a root while now < delAt + G (read only when NoClaim)
  MaxTick,           \* bound of the clock (Tick is enabled only when NoClaim)
  ForkChecksDeadline,\* design TRUE; mutant FALSE = ForkEnd at any time
  DeletedRooted,     \* design TRUE; mutant FALSE = a deleted S stops being a root at once
  ResumeRereads      \* design TRUE; mutant FALSE = a resume in the fork window reuses the old read (fresh deadline, S not re-read)

Idx  == 1..NIdx
VF   == [i \in Idx |-> <<"V", i>>]          \* the source fid of index i
TF   == [i \in Idx |-> <<"T", i>>]          \* T's own fid for index i
Fids == {VF[i] : i \in Idx} \cup {TF[i] : i \in Idx}
\* r0.1: "no HEAD yet" is a function too (TLC cannot compare a function with a string)
NoHead == [i \in Idx |-> <<"N", i>>]
None == -1                                   \* nc: "no tick" encoding for readAt / delAt

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
  crashes, gcs,
  \* ---- nc (all fixed at their Init value when NoClaim = FALSE) ----
  now,      \* the one clock
  readAt,   \* tick at which the current fork attempt read S (None if no attempt in progress)
  delAt,    \* tick at which S was deleted (None while S lives)
  sdelfk,   \* history: S was deleted while step = "fk" (for the witness)
  ffail     \* history: a fork attempt failed on its deadline (for the witness)

tvars == <<now, readAt, delAt, sdelfk, ffail>>
vars == <<store, vlive, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs, now, readAt, delAt, sdelfk, ffail>>

THeadFids == IF thead = NoHead THEN {} ELSE {thead[i] : i \in Idx}
VRefs     == {i \in Idx : thead # NoHead /\ thead[i][1] = "V"}
\* roots: V's HEAD (while V lives), S (while it lives), T's HEAD, T's snapshot,
\* uploaded-but-unpublished batch fids (intent roots them), S's fids while claimed
SFids     == {VF[i] : i \in Idx}
\* nc rule G: a deleted S stays a root for G ticks (FALSE whenever NoClaim = FALSE)
SGrace    == NoClaim /\ DeletedRooted /\ ~ slive /\ delAt # None /\ now < delAt + G
Roots     == (IF vlive THEN SFids ELSE {})
             \cup (IF (slive \/ SGrace) THEN SFids ELSE {})
             \cup THeadFids \cup tsnap \cup (IF NoIntent THEN {} ELSE up)
\* S may be deleted by retention when nothing holds it
SHeld     == claim \/ (RetentionIgnoresMarker /\ thead # NoHead)
\* nc: with NoClaim the snapshot delete never looks at a fork
SDeletable == slive /\ (NoClaim \/ ~ SHeld)
\* nc rule D (TRUE whenever NoClaim = FALSE)
DeadlineOK == (~ NoClaim) \/ (~ ForkChecksDeadline) \/ (now < readAt + D)

Init ==
  /\ store = [f \in Fids |-> IF f[1] = "V" THEN "pres" ELSE "none"]
  /\ vlive = TRUE /\ slive = TRUE /\ claim = FALSE /\ thead = NoHead
  /\ step = "start" /\ up = {} /\ marker = FALSE /\ tsnap = {} /\ gclist = {}
  /\ localdone = FALSE /\ crashes = 0 /\ gcs = 0
  /\ now = 0 /\ readAt = None /\ delAt = None /\ sdelfk = FALSE /\ ffail = FALSE

\* step 0: S is sealed (given) and claimed
\* nc: the step is kept (S found, step counter) but takes no claim
Claim == step = "start" /\ slive /\ claim' = (~ NoClaim) /\ step' = "c"
         /\ UNCHANGED <<store, vlive, slive, thead, up, marker, tsnap, gclist, localdone, crashes, gcs>>
         /\ UNCHANGED tvars

\* step 1: fork (T HEAD names S's fids, all V-owned)
\* r0.1: two steps. Between them T exists without a HEAD, so only the claim keeps S (and
\* through S its fids) alive; that window is what the claim is for.
\* nc: ForkBegin reads S (requires S live) and records readAt; ForkEnd only within the deadline
ForkBegin == step = "c" /\ step' = "fk"
        /\ (NoClaim => slive)
        /\ claim' = (IF DropClaimAtFork THEN FALSE ELSE claim)
        /\ readAt' = (IF NoClaim THEN now ELSE readAt)
        /\ UNCHANGED <<store, vlive, slive, thead, up, marker, tsnap, gclist, localdone, crashes, gcs>>
        /\ UNCHANGED <<now, delAt, sdelfk, ffail>>
ForkEnd == step = "fk" /\ DeadlineOK /\ thead' = VF /\ step' = "own"
        /\ claim' = (IF ClaimUntilHead THEN FALSE ELSE claim)
        /\ UNCHANGED <<store, vlive, slive, up, marker, tsnap, gclist, localdone, crashes, gcs>>
        /\ UNCHANGED tvars
\* nc: past the deadline the fork fails cleanly: T stays without a HEAD, a retry is a new ForkBegin
ForkFail == NoClaim /\ step = "fk" /\ now >= readAt + D
        /\ step' = "c" /\ readAt' = None /\ ffail' = TRUE
        /\ UNCHANGED <<store, vlive, slive, claim, thead, up, marker, tsnap, gclist, localdone, crashes, gcs>>
        /\ UNCHANGED <<now, delAt, sdelfk>>
\* nc: a (re)try that re-reads S and finds it gone ends with "not found" (terminal)
ForkNotFound == NoClaim /\ step = "c" /\ ~ slive /\ step' = "nf"
        /\ UNCHANGED <<store, vlive, slive, claim, thead, up, marker, tsnap, gclist, localdone, crashes, gcs>>
        /\ UNCHANGED tvars

\* step 3: one batch: upload own fids for up to Batch V-owned indexes (intent roots them)
BatchSet == {B \in SUBSET VRefs : B # {} /\ Cardinality(B) <= Batch}
Upload(B) == step = "own" /\ up = {} /\ ~ CasBeforeUpload
             /\ (\A i \in B : store[VF[i]] = "pres")        \* reads its local copy (filled from V)
             /\ store' = [f \in Fids |-> IF f \in {TF[j] : j \in B} THEN "pres" ELSE store[f]]
             /\ up' = {TF[j] : j \in B}
             /\ UNCHANGED <<vlive, slive, claim, thead, step, marker, tsnap, gclist, localdone, crashes, gcs>>
             /\ UNCHANGED tvars
\* the batch HEAD CAS: swap exactly the uploaded indexes
Cas == step = "own" /\ up # {}
       /\ thead' = [i \in Idx |-> IF TF[i] \in up THEN TF[i] ELSE thead[i]]
       /\ gclist' = (IF ListForeign THEN gclist \cup {VF[i] : i \in {j \in Idx : TF[j] \in up}} ELSE gclist)
       /\ up' = {}
       /\ localdone' = (localdone \/ \A i \in Idx : (thead[i][1] = "V") => (TF[i] \in up))
       /\ UNCHANGED <<store, vlive, slive, claim, step, marker, tsnap, crashes, gcs>>
       /\ UNCHANGED tvars
\* mutant: CAS a batch whose upload is not durable (the T fid is still "none")
CasEarly(B) == CasBeforeUpload /\ step = "own" /\ up = {}
       /\ thead' = [i \in Idx |-> IF i \in B THEN TF[i] ELSE thead[i]]
       /\ UNCHANGED <<store, vlive, slive, claim, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>
       /\ UNCHANGED tvars
UploadLate == CasBeforeUpload /\ step = "own"
       /\ \E i \in Idx : thead[i] = TF[i] /\ store[TF[i]] = "none"
             /\ store' = [store EXCEPT ![TF[i]] = "pres"]
       /\ UNCHANGED <<vlive, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>
       /\ UNCHANGED tvars

\* step 4: marker after the last V ref is gone (and the final CAS is done), then drop the claim
Marker == step = "own" /\ ~ marker /\ up = {}
          /\ (VRefs = {} \/ MarkerEarly \/ (MarkerBeforeCas /\ Cardinality(VRefs) <= Batch))
          /\ marker' = TRUE /\ step' = "done" /\ localdone' = TRUE
          /\ UNCHANGED <<store, vlive, slive, claim, thead, up, tsnap, gclist, crashes, gcs>>
          /\ UNCHANGED tvars
DropClaim == marker /\ claim /\ claim' = FALSE
          /\ UNCHANGED <<store, vlive, slive, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>
          /\ UNCHANGED tvars

\* a snapshot of T: refused before the marker (unless the mutant allows it)
SnapT == tsnap = {} /\ thead # NoHead /\ (marker \/ SnapDuringRestore)
         /\ tsnap' = THeadFids
         /\ UNCHANGED <<store, vlive, slive, claim, thead, step, up, marker, gclist, localdone, crashes, gcs>>
         /\ UNCHANGED tvars

\* crash of the restore process; the new owner resumes from remote facts (thead, marker)
\* nc: a resume in the fork window (no HEAD) is a retry that re-reads S (back to "c", new
\* ForkBegin requires S live); mutant ~ResumeRereads: stays "fk" with a fresh deadline (readAt = now)
\* and the content read before the crash
Crash == crashes < MaxCrash /\ step \in {"fk", "own", "done"}
         /\ up' = {}                         \* an unpublished batch is abandoned; its fids are collectable
         /\ step' = (IF ResumeLocal /\ localdone THEN "done"
                    ELSE IF marker THEN "done"
                    ELSE IF thead = NoHead THEN (IF (NoClaim /\ ResumeRereads) THEN "c" ELSE "fk")
                    ELSE "own")
         /\ readAt' = (IF (NoClaim /\ thead = NoHead) THEN (IF ResumeRereads THEN None ELSE now) ELSE readAt)
         /\ crashes' = crashes + 1
         /\ UNCHANGED <<store, vlive, slive, claim, thead, marker, tsnap, gclist, localdone, gcs>>
         /\ UNCHANGED <<now, delAt, sdelfk, ffail>>

\* user deletes V; retention deletes S when nothing holds it
DeleteV == vlive /\ vlive' = FALSE
           /\ UNCHANGED <<store, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>
           /\ UNCHANGED tvars
\* nc: whenever S is live (SDeletable = slive); records delAt and whether a fork was in its window
DeleteS == SDeletable /\ slive' = FALSE
           /\ delAt' = (IF NoClaim THEN now ELSE delAt)
           /\ sdelfk' = (IF (NoClaim /\ step = "fk") THEN TRUE ELSE sdelfk)
           /\ UNCHANGED <<store, vlive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>
           /\ UNCHANGED <<now, readAt, ffail>>

\* GC (reduced machine E): deletes present fids no root reaches, and fids put on the list
Gc == gcs < MaxGc
      /\ store' = [f \in Fids |-> IF store[f] = "pres" /\ (f \notin Roots \/ f \in gclist) THEN "del" ELSE store[f]]
      /\ gcs' = gcs + 1
      /\ UNCHANGED <<vlive, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes>>
      /\ UNCHANGED tvars

\* nc: time advances by one tick (never when NoClaim = FALSE)
Tick == NoClaim /\ now < MaxTick /\ now' = now + 1
        /\ UNCHANGED <<store, vlive, slive, claim, thead, step, up, marker, tsnap, gclist, localdone, crashes, gcs>>
        /\ UNCHANGED <<readAt, delAt, sdelfk, ffail>>

Next == Claim \/ ForkBegin \/ ForkEnd \/ Cas \/ Marker \/ DropClaim \/ SnapT \/ Crash \/ DeleteV \/ DeleteS \/ Gc \/ UploadLate
        \/ (\E B \in BatchSet : Upload(B) \/ CasEarly(B))
        \/ ForkFail \/ ForkNotFound \/ Tick

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
\* nc replacement of LifecycleFree: nothing ever holds S; S is deletable at every moment it lives
LifecycleFreeNC == NoClaim => (~ claim /\ (SDeletable = slive))
\* nc frame check for old cfgs: with NoClaim = FALSE the new variables never move
OldFrame == (~ NoClaim) => (now = 0 /\ readAt = None /\ delAt = None /\ ~ sdelfk /\ ~ ffail)
\* INV-RESTORE-STATE-REMOTE: "done" only when the remote marker exists
StateRemote == step = "done" => marker

TypeOK == /\ step \in {"start", "c", "fk", "own", "done", "nf"} /\ up \subseteq Fids /\ tsnap \subseteq Fids
          /\ now \in 0..MaxTick /\ readAt \in {None} \cup (0..MaxTick) /\ delAt \in {None} \cup (0..MaxTick)
          /\ sdelfk \in BOOLEAN /\ ffail \in BOOLEAN
\* witnesses (r0.1): each is an invariant expected to be VIOLATED, proving the state is reachable
W_Done        == ~ (marker /\ ~ claim /\ ~ slive /\ ~ vlive /\ \A i \in Idx : store[VF[i]] = "del")
W_CrashResume == ~ (marker /\ crashes = MaxCrash)
\* nc witnesses: S deleted inside the fork window and the restore still completes, every block present,
\* every V fid physically gone
W_nc_DelInFork == ~ (sdelfk /\ marker /\ step = "done" /\ ~ vlive /\ ~ slive
                     /\ (\A i \in Idx : store[VF[i]] = "del")
                     /\ (\A i \in Idx : store[thead[i]] = "pres"))
\* nc witness: a fork failed on its deadline and a retry found S gone
W_nc_FailNotFound == ~ (ffail /\ step = "nf")
=============================================================================
