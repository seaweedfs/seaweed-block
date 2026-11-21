---------------------------- MODULE ForkDelete ----------------------------
(***************************************************************************)
(* Fork / restore versus snapshot delete with NO lock between them.        *)
(*                                                                         *)
(* Volume V has one published snapshot S naming the chunks 1..NC. A fork   *)
(* creates a new volume T whose HEAD will name S's chunks. A delete of S   *)
(* never waits for, and is never refused because of, a fork. Safety comes  *)
(* from two time rules only:                                               *)
(*  (F) the fork reads S at its local time readAt and publishes T's HEAD   *)
(*      only if its local clock is still < readAt + D (checked right       *)
(*      before publishing: one atomic step, or, with PublishAtomic=FALSE,  *)
(*      a check step and a later publish step at most P ticks apart);      *)
(*  (G) the delete is stamped delAt on the catalog's local clock; GC keeps *)
(*      S's chunks rooted while ITS local clock is < delAt + G.            *)
(* GC is one atomic step deleting every chunk no root names. Roots: V's    *)
(* HEAD (V may overwrite chunks), S while not deleted, S's deleted record  *)
(* until the grace has passed, T's HEAD once published.                    *)
(*                                                                         *)
(* Time: real time `now` ticks by 1. Actor a reads now + bias[a]; biases   *)
(* are fixed at start in -E..E with pairwise difference <= E. Clocks do    *)
(* not step and run at the same rate.                                      *)
(*                                                                         *)
(* Mutant switches are CONSTANTS; the design value is noted on each.       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
  NC,           \* chunks 1..NC, all named by S (and by V's HEAD at start)
  D,            \* operation deadline of the fork (on the fork's clock)
  G,            \* GC grace after a delete (delAt on the catalog clock, compared on the GC clock)
  E,            \* pairwise clock skew bound
  P,            \* max ticks between the deadline check and the publish (PublishAtomic=FALSE only)
  MaxNow,       \* real-time bound
  MaxAttempts,  \* fork attempts (1 = no retry)
  MaxReads,     \* reads of T after its publish (witness only)
  VCanOverwrite,          \* [TRUE]  V may stop naming chunks (nondeterministic action)
  \* ---- design switches (design value in brackets) ----
  PublishChecksDeadline,  \* [TRUE]  publish only if local clock < readAt + D
  DeletedRecordRooted,    \* [TRUE]  deleted S roots its chunks until the grace has passed
  PublishAtomic,          \* [TRUE]  check + publish in one step; FALSE: two steps <= P ticks apart
  RetryAfterDelete        \* [TRUE]  a retry re-reads S (fails if deleted); FALSE: reuses old chunk set

ASSUME NC \in Nat /\ D \in Nat /\ G \in Nat /\ E \in Nat /\ P \in Nat /\ MaxNow \in Nat

Chunks  == 1..NC
SChunks == Chunks
Actors  == {"F", "C", "GC"}           \* fork process, catalog (stamps delete), garbage collector
NoTime  == -1000
FStates == {"idle", "read", "created", "checked", "published", "failed", "crashed", "notfound"}

VARIABLES
  now,       \* real time
  bias,      \* actor -> clock bias (fixed after Init)
  vNames,    \* chunks V's HEAD still names
  sDeleted,  \* S has been deleted
  delAt,     \* catalog-local time stamped on the delete (NoTime before)
  exists,    \* chunk -> still stored
  fst,       \* fork state
  attempts,  \* fork attempts started
  readAt,    \* fork-local time of the current attempt's read
  readSet,   \* chunk set held by the fork (from the last real read of S)
  haveRead,  \* some attempt has read S successfully
  checkAt,   \* real time of the deadline check (two-step publish only)
  tPub,      \* T's HEAD is published
  tHead,     \* chunks named by T's HEAD
  nReads,    \* reads of T done
  \* ---- history (witnesses / trace readability) ----
  pubAfterDel,    \* T's HEAD was published after S was deleted
  lateReadOK,     \* T read all its chunks after the grace passed, with V no longer naming them
  deadlineLoss,   \* a fork attempt failed cleanly on the deadline
  retryNotFound,  \* a retry got "snapshot not found"
  lastAct         \* name of the last action (for traces)

forkVars == <<fst, attempts, readAt, readSet, haveRead, checkAt>>
tVars    == <<tPub, tHead, nReads>>
histVars == <<pubAfterDel, lateReadOK, deadlineLoss, retryNotFound>>
vars     == <<now, bias, vNames, sDeleted, delAt, exists, forkVars, tVars, histVars, lastAct>>

LC(a) == now + bias[a]                \* local clock of actor a

TypeOK ==
  /\ now \in 0..MaxNow
  /\ bias \in [Actors -> -E..E]
  /\ vNames \subseteq Chunks
  /\ sDeleted \in BOOLEAN
  /\ delAt \in Int
  /\ exists \in [Chunks -> BOOLEAN]
  /\ fst \in FStates
  /\ attempts \in 0..MaxAttempts
  /\ readAt \in Int
  /\ readSet \subseteq Chunks
  /\ haveRead \in BOOLEAN
  /\ checkAt \in Int
  /\ tPub \in BOOLEAN
  /\ tHead \subseteq Chunks
  /\ nReads \in 0..MaxReads
  /\ pubAfterDel \in BOOLEAN
  /\ lateReadOK \in BOOLEAN
  /\ deadlineLoss \in BOOLEAN
  /\ retryNotFound \in BOOLEAN
  /\ lastAct \in STRING

Init ==
  /\ now = 0
  /\ bias \in {b \in [Actors -> -E..E] : (\A x, y \in Actors : b[x] - b[y] <= E)}
  /\ vNames = Chunks
  /\ sDeleted = FALSE
  /\ delAt = NoTime
  /\ exists = [c \in Chunks |-> TRUE]
  /\ fst = "idle"
  /\ attempts = 0
  /\ readAt = NoTime
  /\ readSet = {}
  /\ haveRead = FALSE
  /\ checkAt = NoTime
  /\ tPub = FALSE
  /\ tHead = {}
  /\ nReads = 0
  /\ pubAfterDel = FALSE
  /\ lateReadOK = FALSE
  /\ deadlineLoss = FALSE
  /\ retryNotFound = FALSE
  /\ lastAct = "Init"

(* ============================ time ============================ *)

\* Real time advances. With a two-step publish, at most P ticks pass between check and publish.
Tick ==
  /\ now < MaxNow
  /\ (PublishAtomic \/ fst # "checked" \/ now < checkAt + P)
  /\ now' = now + 1
  /\ lastAct' = "Tick"
  /\ UNCHANGED <<bias, vNames, sDeleted, delAt, exists, forkVars, tVars, histVars>>

(* ============================ fork ============================ *)

\* Start an attempt (first try, or retry after a clean failure or a crash).
ReadSnapshot ==
  /\ fst \in {"idle", "failed", "crashed"}
  /\ attempts < MaxAttempts
  /\ attempts' = attempts + 1
  /\ lastAct' = "ReadSnapshot"
  /\ (IF ~RetryAfterDelete /\ haveRead
        THEN \* mutant: reuse the chunk set of an earlier attempt, no re-read of S
             /\ fst' = "read"
             /\ readAt' = LC("F")
             /\ UNCHANGED <<readSet, haveRead, retryNotFound>>
        ELSE (IF sDeleted
                THEN /\ fst' = "notfound"            \* "snapshot not found": T never gets a HEAD
                     /\ retryNotFound' = (retryNotFound \/ attempts > 0)
                     /\ UNCHANGED <<readAt, readSet, haveRead>>
                ELSE /\ fst' = "read"
                     /\ readAt' = LC("F")
                     /\ readSet' = SChunks
                     /\ haveRead' = TRUE
                     /\ UNCHANGED retryNotFound))
  /\ UNCHANGED <<now, bias, vNames, sDeleted, delAt, exists, checkAt, tVars,
                 pubAfterDel, lateReadOK, deadlineLoss>>

\* Create T's record (no HEAD yet, no effect on roots).
CreateTarget ==
  /\ fst = "read"
  /\ fst' = "created"
  /\ lastAct' = "CreateTarget"
  /\ UNCHANGED <<now, bias, vNames, sDeleted, delAt, exists,
                 attempts, readAt, readSet, haveRead, checkAt, tVars, histVars>>

DeadlineOK == (~PublishChecksDeadline) \/ (LC("F") < readAt + D)

\* Publish T's HEAD = the chunk set held by the fork.
DoPublish ==
  /\ fst' = "published"
  /\ tPub' = TRUE
  /\ tHead' = readSet
  /\ pubAfterDel' = (pubAfterDel \/ sDeleted)

\* (F) atomic: deadline check and publish in one step.
PublishAtomicStep ==
  /\ PublishAtomic
  /\ fst = "created"
  /\ (IF DeadlineOK
        THEN /\ DoPublish
             /\ lastAct' = "Publish"
             /\ UNCHANGED deadlineLoss
        ELSE /\ fst' = "failed"
             /\ deadlineLoss' = TRUE
             /\ lastAct' = "PublishDeadlineFail"
             /\ UNCHANGED <<tPub, tHead, pubAfterDel>>)
  /\ UNCHANGED <<now, bias, vNames, sDeleted, delAt, exists,
                 attempts, readAt, readSet, haveRead, checkAt, nReads,
                 lateReadOK, retryNotFound>>

\* (F) two-step, step 1: the deadline check.
CheckDeadline ==
  /\ ~PublishAtomic
  /\ fst = "created"
  /\ (IF DeadlineOK
        THEN /\ fst' = "checked"
             /\ checkAt' = now
             /\ lastAct' = "CheckDeadlineOK"
             /\ UNCHANGED deadlineLoss
        ELSE /\ fst' = "failed"
             /\ deadlineLoss' = TRUE
             /\ lastAct' = "CheckDeadlineFail"
             /\ UNCHANGED checkAt)
  /\ UNCHANGED <<now, bias, vNames, sDeleted, delAt, exists,
                 attempts, readAt, readSet, haveRead, tVars,
                 pubAfterDel, lateReadOK, retryNotFound>>

\* (F) two-step, step 2: the publish, no new check (Tick bounds it to <= P ticks after the check).
PublishAfterCheck ==
  /\ fst = "checked"
  /\ DoPublish
  /\ lastAct' = "PublishAfterCheck"
  /\ UNCHANGED <<now, bias, vNames, sDeleted, delAt, exists,
                 attempts, readAt, readSet, haveRead, checkAt, nReads,
                 lateReadOK, deadlineLoss, retryNotFound>>

\* The fork crashes in the middle of an attempt; it may later retry or never continue.
Crash ==
  /\ fst \in {"read", "created", "checked"}
  /\ fst' = "crashed"
  /\ lastAct' = "Crash"
  /\ UNCHANGED <<now, bias, vNames, sDeleted, delAt, exists,
                 attempts, readAt, readSet, haveRead, checkAt, tVars, histVars>>

(* ========================== catalog / V ========================== *)

\* (G) delete S at any time: stamped on the catalog clock; S stops being a live root.
DeleteS ==
  /\ ~sDeleted
  /\ sDeleted' = TRUE
  /\ delAt' = LC("C")
  /\ lastAct' = "DeleteS"
  /\ UNCHANGED <<now, bias, vNames, exists, forkVars, tVars, histVars>>

\* V's HEAD stops naming chunk c (V overwrote it).
VOverwrite(c) ==
  /\ VCanOverwrite
  /\ c \in vNames
  /\ vNames' = vNames \ {c}
  /\ lastAct' = "VOverwrite"
  /\ UNCHANGED <<now, bias, sDeleted, delAt, exists, forkVars, tVars, histVars>>

(* ============================== GC ============================== *)

Rooted(c) ==
  \/ c \in vNames
  \/ ~sDeleted
  \/ (DeletedRecordRooted /\ LC("GC") < delAt + G)
  \/ (tPub /\ c \in tHead)

\* One step: delete every stored chunk no root names.
GC ==
  /\ (\E c \in Chunks : exists[c] /\ ~Rooted(c))
  /\ exists' = [c \in Chunks |-> exists[c] /\ Rooted(c)]
  /\ lastAct' = "GC"
  /\ UNCHANGED <<now, bias, vNames, sDeleted, delAt, forkVars, tVars, histVars>>

(* ============================ readers of T ============================ *)

TRead ==
  /\ tPub
  /\ nReads < MaxReads
  /\ nReads' = nReads + 1
  /\ lateReadOK' = (lateReadOK \/
                    (/\ sDeleted
                     /\ vNames \cap tHead = {}
                     /\ LC("GC") >= delAt + G
                     /\ (\A c \in tHead : exists[c])))
  /\ lastAct' = "TRead"
  /\ UNCHANGED <<now, bias, vNames, sDeleted, delAt, exists, forkVars, tPub, tHead,
                 pubAfterDel, deadlineLoss, retryNotFound>>

Next ==
  \/ Tick
  \/ ReadSnapshot
  \/ CreateTarget
  \/ PublishAtomicStep
  \/ CheckDeadline
  \/ PublishAfterCheck
  \/ Crash
  \/ DeleteS
  \/ (\E c \in Chunks : VOverwrite(c))
  \/ GC
  \/ TRead

Spec == Init /\ [][Next]_vars

(* ============================ properties ============================ *)

\* Whenever T's HEAD is published, every chunk it names exists (checked in every state).
NoGap == tPub => (\A c \in tHead : exists[c])

(* Witnesses: each is expected VIOLATED (= the behaviour is reachable). *)
\* Benefit: a fork that read S before the delete publishes after the delete and T reads fine
\* after the grace has passed (only T's HEAD roots the chunks then).
WitNoBenefit == ~(pubAfterDel /\ lateReadOK)
\* A fork loses cleanly because the deadline passed.
WitNoDeadlineLoss == ~deadlineLoss
\* A retry gets "snapshot not found".
WitNoRetryNotFound == ~retryNotFound
\* GC deletes some chunk at all (shows GC is not vacuous in a safe configuration).
WitNoGCDelete == \A c \in Chunks : exists[c]
=============================================================================
