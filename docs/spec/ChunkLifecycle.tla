--------------------------- MODULE ChunkLifecycle ---------------------------
(***************************************************************************)
(* sw-block chunk-layer lifecycle: HEAD, intents, snapshots (name record + *)
(* attempt path), fork claims, tombstones, GC and volume delete, as PR     *)
(* #152 had it at dfbf7273 (volume delete WITH the snapshot cascade, and   *)
(* a commit point NOT bound to the deletion incarnation).                  *)
(*                                                                         *)
(* One atomic step = one remote call (one filer RPC), unless a comment     *)
(* says the step merges a read with the conditional write it guards.       *)
(* HISTORICAL: this is the pre-fix design; it is kept because it produces  *)
(* the counterexamples. ChunkLifecycleFixed.tla is the current model.      *)
(* path:line references in comments are at dfbf7273 (see README.md).      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    SnapCallers,  \* same-request snapshot callers of name "N" (model values); each is also its attempt id
    Deleters,     \* volume-delete callers of "P" (model values)
    ForkOn,       \* one fork caller: F <- P/M
    DelSnapOn,    \* explicit DeleteSnapshot callers, one per name
    CreateOn,     \* one blockmaster CreateVolume("P") caller
    MaxHostOps,   \* budget of host write / detach / attach steps
    MaxRuns,      \* runs per delete caller (an operator rerun after outcome_unknown)
    LostReply,    \* an Admit may land with its reply lost
    PendingOn,    \* a snapshot publish may time out and land later (CreateEntry cannot be cancelled)
    SweepOn       \* GC copy sweep: AbortSnapshot / AbortFork tombstones

NoAtt   == "-"
Vols    == {"P", "F"}
Names   == {"M", "N"}
MP      == <<"M", "m0">>                         \* the pre-existing snapshot P/M (fork parent)
AttPaths == {MP} \cup {<<"N", s>> : s \in SnapCallers}
Chunks  == {"c1", "c2", "c3"}
ChunkOf(i) == IF i = 2 THEN "c2" ELSE "c3"
ForkIds    == IF ForkOn THEN {"fork"} ELSE {}
DelSnapIds == IF DelSnapOn THEN {"xM", "xN"} ELSE {}   \* one explicit DeleteSnapshot caller per name
CreateIds  == IF CreateOn THEN {"create"} ELSE {}
ReaderIds  == SnapCallers \cup ForkIds
DnCallers  == SnapCallers \cup Deleters
GaveBack   == {"refused_claimed", "refused_snapshots", "in_use", "givenback_by_other"}
EmptySnap  == [st |-> "none", ch |-> {}, claims |-> {}, exp |-> FALSE]

(* --algorithm ChunkLifecycle
variables
    \* HEAD per volume. P starts attached (held) with chunk c1; F does not exist.
    head = [v \in Vols |-> IF v = "P" THEN [st |-> "held", gen |-> 1, ch |-> {"c1"}]
                                     ELSE [st |-> "none", gen |-> 0, ch |-> {}]],
    volume = [v \in Vols |-> IF v = "P" THEN "live" ELSE "none"],   \* VOLUME entry
    volCreates = [v \in Vols |-> IF v = "P" THEN 1 ELSE 0],
    names = [n \in Names |-> IF n = "M" THEN "m0" ELSE NoAtt],        \* snapnames/<n> -> attempt
    snaps = [p \in AttPaths |-> IF p = MP
                                THEN [st |-> "active", ch |-> {"c1"}, claims |-> {}, exp |-> FALSE]
                                ELSE EmptySnap],                       \* snaps/<n>/<attempt>
    intents = {},
    pending = {},            \* uncancellable snapshot creates still in flight
    stored = {"c1"},         \* chunks physically present
    nextChunk = 2,
    hostOps = 0,
    resv = [st |-> "none", toks |-> {}],     \* blockmaster deletion reservation of P
    reads = [r \in ReaderIds |-> {}],        \* grace abstraction: keys an in-flight op has read
    dnErr = [p \in DnCallers |-> FALSE],
    \* ---- history / outcome variables (never read by the protocol) ----
    markedBy = [p \in AttPaths |-> "none"],
    badMark = FALSE,         \* a published snapshot marked deleting while P's delete was not committed
    badCommit = FALSE,       \* commit to purging while a fork past its gate holds a live claim
    badCommitAny = FALSE,    \* commit to purging while any live claim exists (literal form)
    acked = {},              \* snapshot paths some caller was told succeeded
    idReuse = FALSE,         \* blockmaster created a reserved id
    sres = [s \in SnapCallers |-> "none"],
    fres = "none",
    dres = [d \in Deleters |-> "none"],
    dtoks = [d \in Deleters |-> {}];

define
    Gone == volume["P"] = "deleted" \/ head["P"].st \in {"deleting", "purging", "deleted"}
    Named(p) == names[p[1]] = p[2]
    \* retention.go liveClaims: a claim is live while its fork target has no HEAD
    LiveClaim(p) == \E c \in snaps[p].claims : head["F"].st = "none"
    Committed == head["P"].st \in {"purging", "deleted"}
    SnapRoot(p) == snaps[p].st = "active" \/ (snaps[p].st = "deleting" /\ (Named(p) \/ ~snaps[p].exp))
    AllRoots == UNION {head[v].ch : v \in Vols}
                \cup UNION {snaps[p].ch : p \in {q \in AttPaths : SnapRoot(q)}}
                \cup UNION {i.keys : i \in intents}
                \cup UNION {reads[r] : r \in ReaderIds}
    \* a fork that has passed its volumeGone gate (catalog.go:1209) and still needs its claim
    PastForkGate == {"f_vol", "f_intent", "f_head", "f_dropintent"}
    CommitUnsafe == \E f \in ForkIds : pc[f] \in PastForkGate /\ LiveClaim(MP)
    CommitAnyClaim == \E p \in AttPaths : LiveClaim(p)
end define;

\* catalog.go deleteNameForVolume (volume_delete.go:454-492): settle one name for a volume delete.
procedure DeleteName(dn, why)
variables datt = NoAtt, dtries = 0;
begin
dn_resolve:                                   \* resolveSnap
    if names[dn] = NoAtt then
        return;
    else
        datt := names[dn];
        if snaps[<<dn, names[dn]>>].st = "none" then goto dn_tomb; else goto dn_mark; end if;
    end if;
dn_tomb:                                      \* CreateEntry(aborted tombstone), exclusive
    if snaps[<<dn, datt>>].st = "none" then
        snaps[<<dn, datt>>] := [EmptySnap EXCEPT !.st = "aborted"];
    end if;
    goto dn_resolve;
dn_mark:                                      \* markDeleting (retention.go:448-503)
    if snaps[<<dn, datt>>].st # "active" then
        goto dn_free;                         \* markSettled
    elsif LiveClaim(<<dn, datt>>) then        \* markClaimed: retry, then claimedErr
        if dtries < 2 then dtries := dtries + 1; goto dn_resolve;
        else dnErr[self] := TRUE; return; end if;
    elsif snaps[<<dn, datt>>].claims # {} then
        snaps[<<dn, datt>>].claims := {};     \* every claim dead: clear, decide again
        goto dn_mark;
    else
        snaps[<<dn, datt>>].st := "deleting";
        markedBy[<<dn, datt>>] := why;
        badMark := badMark \/ ~Committed;
    end if;
dn_free:                                      \* freeName: DeleteEntry(snapnames/<n>) iff it names datt
    if names[dn] = datt then names[dn] := NoAtt; end if;
    return;
end procedure;

\* ---------------- volume server of P (chunk_layer.go capture/detach, master attach) ----------------
process Host = "host"
begin
h_loop:
    while hostOps < MaxHostOps do
        either      \* write + capture: upload under an intent, then CASHead {gen}
            await head["P"].st = "held" /\ nextChunk <= 3;
            stored := stored \cup {ChunkOf(nextChunk)};
            head["P"] := [head["P"] EXCEPT !.ch = {ChunkOf(nextChunk)}, !.gen = @ + 1];
            nextChunk := nextChunk + 1;
        or          \* clean detach: final cut, HEAD published detached
            await head["P"].st = "held";
            head["P"] := [head["P"] EXCEPT !.st = "detached", !.gen = @ + 1];
        or          \* attach: needs a master assignment (refused while reserved), CASHead from detached
            await head["P"].st = "detached" /\ resv.st = "none";
            head["P"] := [head["P"] EXCEPT !.st = "held", !.gen = @ + 1];
        end either;
        hostOps := hostOps + 1;
    end while;
end process;

\* ---------------- GC, copy sweep, and the filer applying late creates ----------------
process GC = "gc"
begin
g_loop:
    while TRUE do
        either      \* one mark + sweep (gc.go:108-300), atomic
            await stored \ AllRoots # {};
            stored := stored \cap AllRoots;
        or          \* an unnamed deleting attempt passes grace (gc_copy.go:177-183)
            with p \in {q \in AttPaths : snaps[q].st = "deleting" /\ ~Named(q) /\ ~snaps[q].exp} do
                snaps[p].exp := TRUE;
            end with;
        or          \* a create whose caller gave up lands (or is refused: the path exists)
            with x \in pending do
                if snaps[x.path].st = "none" then
                    snaps[x.path] := [st |-> "active", ch |-> x.ch, claims |-> {}, exp |-> FALSE];
                end if;
                pending := pending \ {x};
            end with;
        or          \* SweepCopies -> AbortSnapshot (catalog.go:707): tombstone, then drop the intent
            await SweepOn;
            with i \in {j \in intents : j.vol = "P"} do
                if snaps[<<"N", i.att>>].st = "none" then
                    snaps[<<"N", i.att>>] := [EmptySnap EXCEPT !.st = "aborted"];
                end if;
                intents := intents \ {i};
            end with;
        or          \* SweepCopies -> AbortFork (catalog.go:1418): HEAD tombstone of the target
            await SweepOn;
            with i \in {j \in intents : j.vol = "F"} do
                if head["F"].st = "none" then
                    head["F"] := [st |-> "aborted", gen |-> 1, ch |-> {}];
                end if;
                intents := intents \ {i};
            end with;
        end either;
    end while;
end process;

\* ---------------- snapshot callers of P/N, same request id ----------------
process Snap \in SnapCallers
variables myCh = {}, myAtt = NoAtt, outc = "none";
begin
s_cut:                                        \* chunk_control.go:477 CutVolume -> CASHead
    if head["P"].st = "held" then
        myCh := head["P"].ch;
        reads[self] := head["P"].ch;
        head["P"].gen := head["P"].gen + 1;
    else
        sres[self] := "closing"; goto s_done;
    end if;
s_replay:                                     \* catalog.go:824 SnapshotReplay (984-998)
    if names["N"] # NoAtt /\ snaps[<<"N", names["N"]>>].st \in {"active", "deleting"} then
        acked := acked \cup {<<"N", names["N"]>>};
        sres[self] := "replayed"; goto s_done;
    elsif names["N"] # NoAtt /\ snaps[<<"N", names["N"]>>].st = "aborted" then
        sres[self] := "exists"; goto s_done;
    end if;
s_claim:                                      \* catalog.go:846 claimName (937-959)
    if names["N"] = NoAtt then
        names["N"] := self; myAtt := self;
    else
        myAtt := names["N"];
    end if;
s_gone:                                       \* catalog.go:853-861 volumeGone, then the back-out
    if Gone then
        call DeleteName("N", "backout");
        goto s_refused;
    end if;
s_intent:                                     \* catalog.go:891 putIntent
    intents := intents \cup {[vol |-> "P", own |-> self, att |-> myAtt, keys |-> myCh]};
s_create:                                     \* catalog.go:903 createResolved: exclusive create at the attempt path
    either
        if snaps[<<"N", myAtt>>].st = "none" then
            snaps[<<"N", myAtt>>] := [st |-> "active", ch |-> myCh, claims |-> {}, exp |-> FALSE];
            outc := "created";
        elsif snaps[<<"N", myAtt>>].st \in {"active", "deleting"} then
            outc := "created";                \* extAttempt matches: createdNow (catalog.go:781)
        else
            outc := "exists";                 \* tombstone (catalog.go:920-924)
        end if;
    or
        await PendingOn;                      \* publish ctx expired, outcome unknown (catalog.go:928-930)
        pending := pending \cup {[path |-> <<"N", myAtt>>, ch |-> myCh]};
        outc := "unknown";
    end either;
s_drop:                                       \* catalog.go:911/923/926 dropIntent unless unsettled
    if outc # "unknown" then
        intents := {i \in intents : i.own # self};
    end if;
    if outc = "created" then acked := acked \cup {<<"N", myAtt>>}; end if;
    sres[self] := outc;
    goto s_done;
s_refused:
    sres[self] := "volume_deleted";
s_done:
    reads[self] := {};
end process;

\* ---------------- fork F <- P/M ----------------
process Fork \in ForkIds
variables fch = {};
begin
f_target:                                     \* catalog.go:1174-1195
    if volume["F"] = "deleted" then fres := "volume_deleted"; goto f_done; end if;
f_claim:                                      \* catalog.go:1201 claimForkParent (1317-1355)
    if names["M"] = NoAtt \/ snaps[MP].st \in {"none", "aborted"} then
        fres := "not_found"; goto f_done;
    elsif snaps[MP].st = "deleting" then
        fres := "snapshot_deleting"; goto f_done;
    else
        snaps[MP].claims := snaps[MP].claims \cup {"F"};
        fch := snaps[MP].ch;
        reads[self] := snaps[MP].ch;
    end if;
f_gone:                                       \* catalog.go:1209 volumeGone(source)
    if Gone then goto f_dropgone; end if;
f_vol:                                        \* catalog.go:1251 exclusive VOLUME create
    if volume["F"] = "none" then
        volume["F"] := "live";
        volCreates["F"] := volCreates["F"] + 1;
    end if;
f_intent:                                     \* catalog.go:1270 putIntent in the target
    intents := intents \cup {[vol |-> "F", own |-> self, att |-> NoAtt, keys |-> fch]};
f_head:                                       \* catalog.go:1285 exclusive HEAD create: gen 1, epoch 0
    if head["F"].st = "none" then
        head["F"] := [st |-> "active", gen |-> 1, ch |-> fch];
        fres := "ok";
    else
        fres := "lost";
    end if;
f_dropintent:
    intents := {i \in intents : i.own # self};
f_dropclaim:                                  \* catalog.go:1309 dropForkClaim -> removeForkClaim (1367-1408)
    snaps[MP].claims := snaps[MP].claims \ {"F"};
    goto f_done;
f_dropgone:                                   \* catalog.go:1212
    snaps[MP].claims := snaps[MP].claims \ {"F"};
    fres := "volume_deleted";
f_done:
    reads[self] := {};
end process;

\* ---------------- explicit DeleteSnapshot (catalog.go:1052-1101) ----------------
process DelSnap \in DelSnapIds
variables xn = "M", xatt = NoAtt;
begin
x_pick:
    xn := IF self = "xM" THEN "M" ELSE "N";
x_resolve:
    if names[xn] = NoAtt then
        goto x_done;
    else
        xatt := names[xn];
        if snaps[<<xn, names[xn]>>].st = "none" then goto x_tomb;
        elsif snaps[<<xn, names[xn]>>].st = "active" then goto x_mark;
        else goto x_free;
        end if;
    end if;
x_tomb:                                       \* catalog.go:1075 tombstone the unpublished attempt
    if snaps[<<xn, xatt>>].st = "none" then
        snaps[<<xn, xatt>>] := [EmptySnap EXCEPT !.st = "aborted"];
    end if;
    goto x_resolve;
x_mark:                                       \* catalog.go:1082 markDeleting
    if snaps[<<xn, xatt>>].st # "active" then
        goto x_free;
    elsif LiveClaim(<<xn, xatt>>) then
        goto x_done;                          \* snapshot_claimed
    elsif snaps[<<xn, xatt>>].claims # {} then
        snaps[<<xn, xatt>>].claims := {};
        goto x_mark;
    else
        snaps[<<xn, xatt>>].st := "deleting";
        markedBy[<<xn, xatt>>] := "explicit";
    end if;
x_free:                                       \* catalog.go:1094 DeleteEntry(snapnames) iff it names xatt
    if names[xn] = xatt then names[xn] := NoAtt; end if;
x_done:
    skip;
end process;

\* ---------------- blockmaster CreateVolume("P") (services.go:52-70) ----------------
process Create \in CreateIds
variables saw = FALSE;
begin
c_check:                                      \* services.go:62-64 Reserved() under workspaceMu, then Unlock
    saw := resv.st # "none";
c_create:                                     \* services.go:69 Volumes.CreateVolume, lock NOT held
    if ~saw then idReuse := idReuse \/ resv.st # "none"; end if;
end process;

\* ---------------- volume delete of P (volume_delete.go:105-238 at dfbf7273) ----------------
process Del \in Deleters
variables run = 1, tok = <<>>, h0 = "";
begin
d_read:                                       \* :111-122 ReadVolume, ReadHead; :80-85 a fresh random token per run
    h0 := head["P"].st;
    tok := <<self, run>>;
    dtoks[self] := dtoks[self] \cup {<<self, run>>};
d_admit:                                      \* :126 Admit -> services.go:129 MarkVolumeDeleting
    if head["P"].st = "held" then             \* live authority line: refused, nothing recorded
        dres[self] := "not_admitted"; goto d_end;
    else
        resv := [st |-> IF resv.st = "committed" THEN "committed" ELSE "admitted",
                 toks |-> resv.toks \cup {tok}];
        either skip;
        or await LostReply; dres[self] := "unknown"; goto d_end;
        end either;
    end if;
d_branch:                                     \* :146
    if h0 \in {"purging", "deleted"} then goto d_admcommit; end if;
d_mark:                                       \* :147 markHeadDeleting (278-305)
    if head["P"].st \in {"deleting", "purging", "deleted"} then
        skip;                                 \* joins whatever mark is there
    elsif head["P"].st = "held" then
        dres[self] := "in_use"; goto d_abort;
    else
        head["P"] := [head["P"] EXCEPT !.st = "deleting", !.gen = @ + 1];
    end if;
d_claims:                                     \* :158 anyLiveClaim (398-424)
    if \E p \in AttPaths : Named(p) /\ snaps[p].st = "active" /\ LiveClaim(p) then
        goto d_revert;
    else
        goto d_commit;
    end if;
d_revert:                                     \* :164 revertHeadDeleting (311-338)
    if head["P"].st # "deleting" then
        if head["P"].st \in {"held", "detached"} then
            dres[self] := "refused_claimed"; goto d_abort;
        else
            dres[self] := "unknown"; goto d_end;   \* another delete committed (:169-172)
        end if;
    else
        head["P"] := [head["P"] EXCEPT !.st = "detached", !.gen = @ + 1];
        dres[self] := "refused_claimed"; goto d_abort;
    end if;
d_commit:                                     \* :179 commitHeadPurging (343-368): any deleting HEAD
    if head["P"].st \in {"purging", "deleted"} then
        skip;
    elsif head["P"].st = "deleting" then
        head["P"] := [head["P"] EXCEPT !.st = "purging", !.gen = @ + 1];
        badCommit := badCommit \/ CommitUnsafe;
        badCommitAny := badCommitAny \/ CommitAnyClaim;
    else
        dres[self] := "givenback_by_other"; goto d_abort;
    end if;
d_admcommit:                                  \* :191 Admission.Commit (deletion_reservations.go:122)
    resv.st := "committed";
d_cascade_m:                                  \* :198-213 listNames, deleteNameForVolume per name (name order)
    if names["M"] # NoAtt then call DeleteName("M", "cascade"); end if;
d_cascade_n:
    if dnErr[self] then dres[self] := "error"; goto d_end;
    elsif names["N"] # NoAtt then call DeleteName("N", "cascade");
    end if;
d_finish:                                     \* :228 finishHeadDeleted (371-394)
    if dnErr[self] then dres[self] := "error"; goto d_end;
    elsif head["P"].st = "purging" then
        head["P"] := [st |-> "deleted", gen |-> head["P"].gen + 1, ch |-> {}];
    elsif head["P"].st # "deleted" then
        dres[self] := "error"; goto d_end;
    end if;
d_volmark:                                    \* :232 markVolumeDeleted
    volume["P"] := "deleted";
    dres[self] := "completed";
    goto d_end;
d_abort:                                      \* :137 Admission.Abort (deletion_reservations.go:134-151)
    if resv.st = "committed" then
        dres[self] := "unknown";
    elsif resv.toks \ {tok} = {} then
        resv := [st |-> "none", toks |-> {}];
    else
        resv.toks := resv.toks \ {tok};
    end if;
d_end:                                        \* the operator reruns an outcome_unknown
    if dres[self] = "unknown" /\ run < MaxRuns then
        run := run + 1;
        goto d_read;
    end if;
end process;

end algorithm; *)
\* BEGIN TRANSLATION
CONSTANT defaultInitValue
VARIABLES head, volume, volCreates, names, snaps, intents, pending, stored, 
          nextChunk, hostOps, resv, reads, dnErr, markedBy, badMark, 
          badCommit, badCommitAny, acked, idReuse, sres, fres, dres, dtoks, 
          pc, stack

(* define statement *)
Gone == volume["P"] = "deleted" \/ head["P"].st \in {"deleting", "purging", "deleted"}
Named(p) == names[p[1]] = p[2]

LiveClaim(p) == \E c \in snaps[p].claims : head["F"].st = "none"
Committed == head["P"].st \in {"purging", "deleted"}
SnapRoot(p) == snaps[p].st = "active" \/ (snaps[p].st = "deleting" /\ (Named(p) \/ ~snaps[p].exp))
AllRoots == UNION {head[v].ch : v \in Vols}
            \cup UNION {snaps[p].ch : p \in {q \in AttPaths : SnapRoot(q)}}
            \cup UNION {i.keys : i \in intents}
            \cup UNION {reads[r] : r \in ReaderIds}

PastForkGate == {"f_vol", "f_intent", "f_head", "f_dropintent"}
CommitUnsafe == \E f \in ForkIds : pc[f] \in PastForkGate /\ LiveClaim(MP)
CommitAnyClaim == \E p \in AttPaths : LiveClaim(p)

VARIABLES dn, why, datt, dtries, myCh, myAtt, outc, fch, xn, xatt, saw, run, 
          tok, h0

vars == << head, volume, volCreates, names, snaps, intents, pending, stored, 
           nextChunk, hostOps, resv, reads, dnErr, markedBy, badMark, 
           badCommit, badCommitAny, acked, idReuse, sres, fres, dres, dtoks, 
           pc, stack, dn, why, datt, dtries, myCh, myAtt, outc, fch, xn, xatt, 
           saw, run, tok, h0 >>

ProcSet == {"host"} \cup {"gc"} \cup (SnapCallers) \cup (ForkIds) \cup (DelSnapIds) \cup (CreateIds) \cup (Deleters)

Init == (* Global variables *)
        /\ head = [v \in Vols |-> IF v = "P" THEN [st |-> "held", gen |-> 1, ch |-> {"c1"}]
                                            ELSE [st |-> "none", gen |-> 0, ch |-> {}]]
        /\ volume = [v \in Vols |-> IF v = "P" THEN "live" ELSE "none"]
        /\ volCreates = [v \in Vols |-> IF v = "P" THEN 1 ELSE 0]
        /\ names = [n \in Names |-> IF n = "M" THEN "m0" ELSE NoAtt]
        /\ snaps = [p \in AttPaths |-> IF p = MP
                                       THEN [st |-> "active", ch |-> {"c1"}, claims |-> {}, exp |-> FALSE]
                                       ELSE EmptySnap]
        /\ intents = {}
        /\ pending = {}
        /\ stored = {"c1"}
        /\ nextChunk = 2
        /\ hostOps = 0
        /\ resv = [st |-> "none", toks |-> {}]
        /\ reads = [r \in ReaderIds |-> {}]
        /\ dnErr = [p \in DnCallers |-> FALSE]
        /\ markedBy = [p \in AttPaths |-> "none"]
        /\ badMark = FALSE
        /\ badCommit = FALSE
        /\ badCommitAny = FALSE
        /\ acked = {}
        /\ idReuse = FALSE
        /\ sres = [s \in SnapCallers |-> "none"]
        /\ fres = "none"
        /\ dres = [d \in Deleters |-> "none"]
        /\ dtoks = [d \in Deleters |-> {}]
        (* Procedure DeleteName *)
        /\ dn = [ self \in ProcSet |-> defaultInitValue]
        /\ why = [ self \in ProcSet |-> defaultInitValue]
        /\ datt = [ self \in ProcSet |-> NoAtt]
        /\ dtries = [ self \in ProcSet |-> 0]
        (* Process Snap *)
        /\ myCh = [self \in SnapCallers |-> {}]
        /\ myAtt = [self \in SnapCallers |-> NoAtt]
        /\ outc = [self \in SnapCallers |-> "none"]
        (* Process Fork *)
        /\ fch = [self \in ForkIds |-> {}]
        (* Process DelSnap *)
        /\ xn = [self \in DelSnapIds |-> "M"]
        /\ xatt = [self \in DelSnapIds |-> NoAtt]
        (* Process Create *)
        /\ saw = [self \in CreateIds |-> FALSE]
        (* Process Del *)
        /\ run = [self \in Deleters |-> 1]
        /\ tok = [self \in Deleters |-> <<>>]
        /\ h0 = [self \in Deleters |-> ""]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self = "host" -> "h_loop"
                                        [] self = "gc" -> "g_loop"
                                        [] self \in SnapCallers -> "s_cut"
                                        [] self \in ForkIds -> "f_target"
                                        [] self \in DelSnapIds -> "x_pick"
                                        [] self \in CreateIds -> "c_check"
                                        [] self \in Deleters -> "d_read"]

dn_resolve(self) == /\ pc[self] = "dn_resolve"
                    /\ IF names[dn[self]] = NoAtt
                          THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                               /\ datt' = [datt EXCEPT ![self] = Head(stack[self]).datt]
                               /\ dtries' = [dtries EXCEPT ![self] = Head(stack[self]).dtries]
                               /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                               /\ why' = [why EXCEPT ![self] = Head(stack[self]).why]
                               /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                          ELSE /\ datt' = [datt EXCEPT ![self] = names[dn[self]]]
                               /\ IF snaps[<<dn[self], names[dn[self]]>>].st = "none"
                                     THEN /\ pc' = [pc EXCEPT ![self] = "dn_tomb"]
                                     ELSE /\ pc' = [pc EXCEPT ![self] = "dn_mark"]
                               /\ UNCHANGED << stack, dn, why, dtries >>
                    /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                    intents, pending, stored, nextChunk, 
                                    hostOps, resv, reads, dnErr, markedBy, 
                                    badMark, badCommit, badCommitAny, acked, 
                                    idReuse, sres, fres, dres, dtoks, myCh, 
                                    myAtt, outc, fch, xn, xatt, saw, run, tok, 
                                    h0 >>

dn_tomb(self) == /\ pc[self] = "dn_tomb"
                 /\ IF snaps[<<dn[self], datt[self]>>].st = "none"
                       THEN /\ snaps' = [snaps EXCEPT ![<<dn[self], datt[self]>>] = [EmptySnap EXCEPT !.st = "aborted"]]
                       ELSE /\ TRUE
                            /\ snaps' = snaps
                 /\ pc' = [pc EXCEPT ![self] = "dn_resolve"]
                 /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                 pending, stored, nextChunk, hostOps, resv, 
                                 reads, dnErr, markedBy, badMark, badCommit, 
                                 badCommitAny, acked, idReuse, sres, fres, 
                                 dres, dtoks, stack, dn, why, datt, dtries, 
                                 myCh, myAtt, outc, fch, xn, xatt, saw, run, 
                                 tok, h0 >>

dn_mark(self) == /\ pc[self] = "dn_mark"
                 /\ IF snaps[<<dn[self], datt[self]>>].st # "active"
                       THEN /\ pc' = [pc EXCEPT ![self] = "dn_free"]
                            /\ UNCHANGED << snaps, dnErr, markedBy, badMark, 
                                            stack, dn, why, datt, dtries >>
                       ELSE /\ IF LiveClaim(<<dn[self], datt[self]>>)
                                  THEN /\ IF dtries[self] < 2
                                             THEN /\ dtries' = [dtries EXCEPT ![self] = dtries[self] + 1]
                                                  /\ pc' = [pc EXCEPT ![self] = "dn_resolve"]
                                                  /\ UNCHANGED << dnErr, stack, 
                                                                  dn, why, 
                                                                  datt >>
                                             ELSE /\ dnErr' = [dnErr EXCEPT ![self] = TRUE]
                                                  /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                                  /\ datt' = [datt EXCEPT ![self] = Head(stack[self]).datt]
                                                  /\ dtries' = [dtries EXCEPT ![self] = Head(stack[self]).dtries]
                                                  /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                                                  /\ why' = [why EXCEPT ![self] = Head(stack[self]).why]
                                                  /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                       /\ UNCHANGED << snaps, markedBy, 
                                                       badMark >>
                                  ELSE /\ IF snaps[<<dn[self], datt[self]>>].claims # {}
                                             THEN /\ snaps' = [snaps EXCEPT ![<<dn[self], datt[self]>>].claims = {}]
                                                  /\ pc' = [pc EXCEPT ![self] = "dn_mark"]
                                                  /\ UNCHANGED << markedBy, 
                                                                  badMark >>
                                             ELSE /\ snaps' = [snaps EXCEPT ![<<dn[self], datt[self]>>].st = "deleting"]
                                                  /\ markedBy' = [markedBy EXCEPT ![<<dn[self], datt[self]>>] = why[self]]
                                                  /\ badMark' = (badMark \/ ~Committed)
                                                  /\ pc' = [pc EXCEPT ![self] = "dn_free"]
                                       /\ UNCHANGED << dnErr, stack, dn, why, 
                                                       datt, dtries >>
                 /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                 pending, stored, nextChunk, hostOps, resv, 
                                 reads, badCommit, badCommitAny, acked, 
                                 idReuse, sres, fres, dres, dtoks, myCh, myAtt, 
                                 outc, fch, xn, xatt, saw, run, tok, h0 >>

dn_free(self) == /\ pc[self] = "dn_free"
                 /\ IF names[dn[self]] = datt[self]
                       THEN /\ names' = [names EXCEPT ![dn[self]] = NoAtt]
                       ELSE /\ TRUE
                            /\ names' = names
                 /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                 /\ datt' = [datt EXCEPT ![self] = Head(stack[self]).datt]
                 /\ dtries' = [dtries EXCEPT ![self] = Head(stack[self]).dtries]
                 /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                 /\ why' = [why EXCEPT ![self] = Head(stack[self]).why]
                 /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                 /\ UNCHANGED << head, volume, volCreates, snaps, intents, 
                                 pending, stored, nextChunk, hostOps, resv, 
                                 reads, dnErr, markedBy, badMark, badCommit, 
                                 badCommitAny, acked, idReuse, sres, fres, 
                                 dres, dtoks, myCh, myAtt, outc, fch, xn, xatt, 
                                 saw, run, tok, h0 >>

DeleteName(self) == dn_resolve(self) \/ dn_tomb(self) \/ dn_mark(self)
                       \/ dn_free(self)

h_loop == /\ pc["host"] = "h_loop"
          /\ IF hostOps < MaxHostOps
                THEN /\ \/ /\ head["P"].st = "held" /\ nextChunk <= 3
                           /\ stored' = (stored \cup {ChunkOf(nextChunk)})
                           /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.ch = {ChunkOf(nextChunk)}, !.gen = @ + 1]]
                           /\ nextChunk' = nextChunk + 1
                        \/ /\ head["P"].st = "held"
                           /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.st = "detached", !.gen = @ + 1]]
                           /\ UNCHANGED <<stored, nextChunk>>
                        \/ /\ head["P"].st = "detached" /\ resv.st = "none"
                           /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.st = "held", !.gen = @ + 1]]
                           /\ UNCHANGED <<stored, nextChunk>>
                     /\ hostOps' = hostOps + 1
                     /\ pc' = [pc EXCEPT !["host"] = "h_loop"]
                ELSE /\ pc' = [pc EXCEPT !["host"] = "Done"]
                     /\ UNCHANGED << head, stored, nextChunk, hostOps >>
          /\ UNCHANGED << volume, volCreates, names, snaps, intents, pending, 
                          resv, reads, dnErr, markedBy, badMark, badCommit, 
                          badCommitAny, acked, idReuse, sres, fres, dres, 
                          dtoks, stack, dn, why, datt, dtries, myCh, myAtt, 
                          outc, fch, xn, xatt, saw, run, tok, h0 >>

Host == h_loop

g_loop == /\ pc["gc"] = "g_loop"
          /\ \/ /\ stored \ AllRoots # {}
                /\ stored' = (stored \cap AllRoots)
                /\ UNCHANGED <<head, snaps, intents, pending>>
             \/ /\ \E p \in {q \in AttPaths : snaps[q].st = "deleting" /\ ~Named(q) /\ ~snaps[q].exp}:
                     snaps' = [snaps EXCEPT ![p].exp = TRUE]
                /\ UNCHANGED <<head, intents, pending, stored>>
             \/ /\ \E x \in pending:
                     /\ IF snaps[x.path].st = "none"
                           THEN /\ snaps' = [snaps EXCEPT ![x.path] = [st |-> "active", ch |-> x.ch, claims |-> {}, exp |-> FALSE]]
                           ELSE /\ TRUE
                                /\ snaps' = snaps
                     /\ pending' = pending \ {x}
                /\ UNCHANGED <<head, intents, stored>>
             \/ /\ SweepOn
                /\ \E i \in {j \in intents : j.vol = "P"}:
                     /\ IF snaps[<<"N", i.att>>].st = "none"
                           THEN /\ snaps' = [snaps EXCEPT ![<<"N", i.att>>] = [EmptySnap EXCEPT !.st = "aborted"]]
                           ELSE /\ TRUE
                                /\ snaps' = snaps
                     /\ intents' = intents \ {i}
                /\ UNCHANGED <<head, pending, stored>>
             \/ /\ SweepOn
                /\ \E i \in {j \in intents : j.vol = "F"}:
                     /\ IF head["F"].st = "none"
                           THEN /\ head' = [head EXCEPT !["F"] = [st |-> "aborted", gen |-> 1, ch |-> {}]]
                           ELSE /\ TRUE
                                /\ head' = head
                     /\ intents' = intents \ {i}
                /\ UNCHANGED <<snaps, pending, stored>>
          /\ pc' = [pc EXCEPT !["gc"] = "g_loop"]
          /\ UNCHANGED << volume, volCreates, names, nextChunk, hostOps, resv, 
                          reads, dnErr, markedBy, badMark, badCommit, 
                          badCommitAny, acked, idReuse, sres, fres, dres, 
                          dtoks, stack, dn, why, datt, dtries, myCh, myAtt, 
                          outc, fch, xn, xatt, saw, run, tok, h0 >>

GC == g_loop

s_cut(self) == /\ pc[self] = "s_cut"
               /\ IF head["P"].st = "held"
                     THEN /\ myCh' = [myCh EXCEPT ![self] = head["P"].ch]
                          /\ reads' = [reads EXCEPT ![self] = head["P"].ch]
                          /\ head' = [head EXCEPT !["P"].gen = head["P"].gen + 1]
                          /\ pc' = [pc EXCEPT ![self] = "s_replay"]
                          /\ sres' = sres
                     ELSE /\ sres' = [sres EXCEPT ![self] = "closing"]
                          /\ pc' = [pc EXCEPT ![self] = "s_done"]
                          /\ UNCHANGED << head, reads, myCh >>
               /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                               pending, stored, nextChunk, hostOps, resv, 
                               dnErr, markedBy, badMark, badCommit, 
                               badCommitAny, acked, idReuse, fres, dres, dtoks, 
                               stack, dn, why, datt, dtries, myAtt, outc, fch, 
                               xn, xatt, saw, run, tok, h0 >>

s_replay(self) == /\ pc[self] = "s_replay"
                  /\ IF names["N"] # NoAtt /\ snaps[<<"N", names["N"]>>].st \in {"active", "deleting"}
                        THEN /\ acked' = (acked \cup {<<"N", names["N"]>>})
                             /\ sres' = [sres EXCEPT ![self] = "replayed"]
                             /\ pc' = [pc EXCEPT ![self] = "s_done"]
                        ELSE /\ IF names["N"] # NoAtt /\ snaps[<<"N", names["N"]>>].st = "aborted"
                                   THEN /\ sres' = [sres EXCEPT ![self] = "exists"]
                                        /\ pc' = [pc EXCEPT ![self] = "s_done"]
                                   ELSE /\ pc' = [pc EXCEPT ![self] = "s_claim"]
                                        /\ sres' = sres
                             /\ acked' = acked
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  intents, pending, stored, nextChunk, hostOps, 
                                  resv, reads, dnErr, markedBy, badMark, 
                                  badCommit, badCommitAny, idReuse, fres, dres, 
                                  dtoks, stack, dn, why, datt, dtries, myCh, 
                                  myAtt, outc, fch, xn, xatt, saw, run, tok, 
                                  h0 >>

s_claim(self) == /\ pc[self] = "s_claim"
                 /\ IF names["N"] = NoAtt
                       THEN /\ names' = [names EXCEPT !["N"] = self]
                            /\ myAtt' = [myAtt EXCEPT ![self] = self]
                       ELSE /\ myAtt' = [myAtt EXCEPT ![self] = names["N"]]
                            /\ names' = names
                 /\ pc' = [pc EXCEPT ![self] = "s_gone"]
                 /\ UNCHANGED << head, volume, volCreates, snaps, intents, 
                                 pending, stored, nextChunk, hostOps, resv, 
                                 reads, dnErr, markedBy, badMark, badCommit, 
                                 badCommitAny, acked, idReuse, sres, fres, 
                                 dres, dtoks, stack, dn, why, datt, dtries, 
                                 myCh, outc, fch, xn, xatt, saw, run, tok, h0 >>

s_gone(self) == /\ pc[self] = "s_gone"
                /\ IF Gone
                      THEN /\ /\ dn' = [dn EXCEPT ![self] = "N"]
                              /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DeleteName",
                                                                       pc        |->  "s_refused",
                                                                       datt      |->  datt[self],
                                                                       dtries    |->  dtries[self],
                                                                       dn        |->  dn[self],
                                                                       why       |->  why[self] ] >>
                                                                   \o stack[self]]
                              /\ why' = [why EXCEPT ![self] = "backout"]
                           /\ datt' = [datt EXCEPT ![self] = NoAtt]
                           /\ dtries' = [dtries EXCEPT ![self] = 0]
                           /\ pc' = [pc EXCEPT ![self] = "dn_resolve"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "s_intent"]
                           /\ UNCHANGED << stack, dn, why, datt, dtries >>
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, dnErr, markedBy, badMark, 
                                badCommit, badCommitAny, acked, idReuse, sres, 
                                fres, dres, dtoks, myCh, myAtt, outc, fch, xn, 
                                xatt, saw, run, tok, h0 >>

s_intent(self) == /\ pc[self] = "s_intent"
                  /\ intents' = (intents \cup {[vol |-> "P", own |-> self, att |-> myAtt[self], keys |-> myCh[self]]})
                  /\ pc' = [pc EXCEPT ![self] = "s_create"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, dnErr, markedBy, badMark, badCommit, 
                                  badCommitAny, acked, idReuse, sres, fres, 
                                  dres, dtoks, stack, dn, why, datt, dtries, 
                                  myCh, myAtt, outc, fch, xn, xatt, saw, run, 
                                  tok, h0 >>

s_create(self) == /\ pc[self] = "s_create"
                  /\ \/ /\ IF snaps[<<"N", myAtt[self]>>].st = "none"
                              THEN /\ snaps' = [snaps EXCEPT ![<<"N", myAtt[self]>>] = [st |-> "active", ch |-> myCh[self], claims |-> {}, exp |-> FALSE]]
                                   /\ outc' = [outc EXCEPT ![self] = "created"]
                              ELSE /\ IF snaps[<<"N", myAtt[self]>>].st \in {"active", "deleting"}
                                         THEN /\ outc' = [outc EXCEPT ![self] = "created"]
                                         ELSE /\ outc' = [outc EXCEPT ![self] = "exists"]
                                   /\ snaps' = snaps
                        /\ UNCHANGED pending
                     \/ /\ PendingOn
                        /\ pending' = (pending \cup {[path |-> <<"N", myAtt[self]>>, ch |-> myCh[self]]})
                        /\ outc' = [outc EXCEPT ![self] = "unknown"]
                        /\ snaps' = snaps
                  /\ pc' = [pc EXCEPT ![self] = "s_drop"]
                  /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                  stored, nextChunk, hostOps, resv, reads, 
                                  dnErr, markedBy, badMark, badCommit, 
                                  badCommitAny, acked, idReuse, sres, fres, 
                                  dres, dtoks, stack, dn, why, datt, dtries, 
                                  myCh, myAtt, fch, xn, xatt, saw, run, tok, 
                                  h0 >>

s_drop(self) == /\ pc[self] = "s_drop"
                /\ IF outc[self] # "unknown"
                      THEN /\ intents' = {i \in intents : i.own # self}
                      ELSE /\ TRUE
                           /\ UNCHANGED intents
                /\ IF outc[self] = "created"
                      THEN /\ acked' = (acked \cup {<<"N", myAtt[self]>>})
                      ELSE /\ TRUE
                           /\ acked' = acked
                /\ sres' = [sres EXCEPT ![self] = outc[self]]
                /\ pc' = [pc EXCEPT ![self] = "s_done"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, dnErr, markedBy, badMark, badCommit, 
                                badCommitAny, idReuse, fres, dres, dtoks, 
                                stack, dn, why, datt, dtries, myCh, myAtt, 
                                outc, fch, xn, xatt, saw, run, tok, h0 >>

s_refused(self) == /\ pc[self] = "s_refused"
                   /\ sres' = [sres EXCEPT ![self] = "volume_deleted"]
                   /\ pc' = [pc EXCEPT ![self] = "s_done"]
                   /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                   intents, pending, stored, nextChunk, 
                                   hostOps, resv, reads, dnErr, markedBy, 
                                   badMark, badCommit, badCommitAny, acked, 
                                   idReuse, fres, dres, dtoks, stack, dn, why, 
                                   datt, dtries, myCh, myAtt, outc, fch, xn, 
                                   xatt, saw, run, tok, h0 >>

s_done(self) == /\ pc[self] = "s_done"
                /\ reads' = [reads EXCEPT ![self] = {}]
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, dnErr, markedBy, badMark, badCommit, 
                                badCommitAny, acked, idReuse, sres, fres, dres, 
                                dtoks, stack, dn, why, datt, dtries, myCh, 
                                myAtt, outc, fch, xn, xatt, saw, run, tok, h0 >>

Snap(self) == s_cut(self) \/ s_replay(self) \/ s_claim(self)
                 \/ s_gone(self) \/ s_intent(self) \/ s_create(self)
                 \/ s_drop(self) \/ s_refused(self) \/ s_done(self)

f_target(self) == /\ pc[self] = "f_target"
                  /\ IF volume["F"] = "deleted"
                        THEN /\ fres' = "volume_deleted"
                             /\ pc' = [pc EXCEPT ![self] = "f_done"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "f_claim"]
                             /\ fres' = fres
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  intents, pending, stored, nextChunk, hostOps, 
                                  resv, reads, dnErr, markedBy, badMark, 
                                  badCommit, badCommitAny, acked, idReuse, 
                                  sres, dres, dtoks, stack, dn, why, datt, 
                                  dtries, myCh, myAtt, outc, fch, xn, xatt, 
                                  saw, run, tok, h0 >>

f_claim(self) == /\ pc[self] = "f_claim"
                 /\ IF names["M"] = NoAtt \/ snaps[MP].st \in {"none", "aborted"}
                       THEN /\ fres' = "not_found"
                            /\ pc' = [pc EXCEPT ![self] = "f_done"]
                            /\ UNCHANGED << snaps, reads, fch >>
                       ELSE /\ IF snaps[MP].st = "deleting"
                                  THEN /\ fres' = "snapshot_deleting"
                                       /\ pc' = [pc EXCEPT ![self] = "f_done"]
                                       /\ UNCHANGED << snaps, reads, fch >>
                                  ELSE /\ snaps' = [snaps EXCEPT ![MP].claims = snaps[MP].claims \cup {"F"}]
                                       /\ fch' = [fch EXCEPT ![self] = snaps'[MP].ch]
                                       /\ reads' = [reads EXCEPT ![self] = snaps'[MP].ch]
                                       /\ pc' = [pc EXCEPT ![self] = "f_gone"]
                                       /\ fres' = fres
                 /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                 pending, stored, nextChunk, hostOps, resv, 
                                 dnErr, markedBy, badMark, badCommit, 
                                 badCommitAny, acked, idReuse, sres, dres, 
                                 dtoks, stack, dn, why, datt, dtries, myCh, 
                                 myAtt, outc, xn, xatt, saw, run, tok, h0 >>

f_gone(self) == /\ pc[self] = "f_gone"
                /\ IF Gone
                      THEN /\ pc' = [pc EXCEPT ![self] = "f_dropgone"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "f_vol"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, dnErr, markedBy, badMark, 
                                badCommit, badCommitAny, acked, idReuse, sres, 
                                fres, dres, dtoks, stack, dn, why, datt, 
                                dtries, myCh, myAtt, outc, fch, xn, xatt, saw, 
                                run, tok, h0 >>

f_vol(self) == /\ pc[self] = "f_vol"
               /\ IF volume["F"] = "none"
                     THEN /\ volume' = [volume EXCEPT !["F"] = "live"]
                          /\ volCreates' = [volCreates EXCEPT !["F"] = volCreates["F"] + 1]
                     ELSE /\ TRUE
                          /\ UNCHANGED << volume, volCreates >>
               /\ pc' = [pc EXCEPT ![self] = "f_intent"]
               /\ UNCHANGED << head, names, snaps, intents, pending, stored, 
                               nextChunk, hostOps, resv, reads, dnErr, 
                               markedBy, badMark, badCommit, badCommitAny, 
                               acked, idReuse, sres, fres, dres, dtoks, stack, 
                               dn, why, datt, dtries, myCh, myAtt, outc, fch, 
                               xn, xatt, saw, run, tok, h0 >>

f_intent(self) == /\ pc[self] = "f_intent"
                  /\ intents' = (intents \cup {[vol |-> "F", own |-> self, att |-> NoAtt, keys |-> fch[self]]})
                  /\ pc' = [pc EXCEPT ![self] = "f_head"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, dnErr, markedBy, badMark, badCommit, 
                                  badCommitAny, acked, idReuse, sres, fres, 
                                  dres, dtoks, stack, dn, why, datt, dtries, 
                                  myCh, myAtt, outc, fch, xn, xatt, saw, run, 
                                  tok, h0 >>

f_head(self) == /\ pc[self] = "f_head"
                /\ IF head["F"].st = "none"
                      THEN /\ head' = [head EXCEPT !["F"] = [st |-> "active", gen |-> 1, ch |-> fch[self]]]
                           /\ fres' = "ok"
                      ELSE /\ fres' = "lost"
                           /\ head' = head
                /\ pc' = [pc EXCEPT ![self] = "f_dropintent"]
                /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, dnErr, markedBy, badMark, badCommit, 
                                badCommitAny, acked, idReuse, sres, dres, 
                                dtoks, stack, dn, why, datt, dtries, myCh, 
                                myAtt, outc, fch, xn, xatt, saw, run, tok, h0 >>

f_dropintent(self) == /\ pc[self] = "f_dropintent"
                      /\ intents' = {i \in intents : i.own # self}
                      /\ pc' = [pc EXCEPT ![self] = "f_dropclaim"]
                      /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                      pending, stored, nextChunk, hostOps, 
                                      resv, reads, dnErr, markedBy, badMark, 
                                      badCommit, badCommitAny, acked, idReuse, 
                                      sres, fres, dres, dtoks, stack, dn, why, 
                                      datt, dtries, myCh, myAtt, outc, fch, xn, 
                                      xatt, saw, run, tok, h0 >>

f_dropclaim(self) == /\ pc[self] = "f_dropclaim"
                     /\ snaps' = [snaps EXCEPT ![MP].claims = snaps[MP].claims \ {"F"}]
                     /\ pc' = [pc EXCEPT ![self] = "f_done"]
                     /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                     pending, stored, nextChunk, hostOps, resv, 
                                     reads, dnErr, markedBy, badMark, 
                                     badCommit, badCommitAny, acked, idReuse, 
                                     sres, fres, dres, dtoks, stack, dn, why, 
                                     datt, dtries, myCh, myAtt, outc, fch, xn, 
                                     xatt, saw, run, tok, h0 >>

f_dropgone(self) == /\ pc[self] = "f_dropgone"
                    /\ snaps' = [snaps EXCEPT ![MP].claims = snaps[MP].claims \ {"F"}]
                    /\ fres' = "volume_deleted"
                    /\ pc' = [pc EXCEPT ![self] = "f_done"]
                    /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                    pending, stored, nextChunk, hostOps, resv, 
                                    reads, dnErr, markedBy, badMark, badCommit, 
                                    badCommitAny, acked, idReuse, sres, dres, 
                                    dtoks, stack, dn, why, datt, dtries, myCh, 
                                    myAtt, outc, fch, xn, xatt, saw, run, tok, 
                                    h0 >>

f_done(self) == /\ pc[self] = "f_done"
                /\ reads' = [reads EXCEPT ![self] = {}]
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, dnErr, markedBy, badMark, badCommit, 
                                badCommitAny, acked, idReuse, sres, fres, dres, 
                                dtoks, stack, dn, why, datt, dtries, myCh, 
                                myAtt, outc, fch, xn, xatt, saw, run, tok, h0 >>

Fork(self) == f_target(self) \/ f_claim(self) \/ f_gone(self)
                 \/ f_vol(self) \/ f_intent(self) \/ f_head(self)
                 \/ f_dropintent(self) \/ f_dropclaim(self)
                 \/ f_dropgone(self) \/ f_done(self)

x_pick(self) == /\ pc[self] = "x_pick"
                /\ xn' = [xn EXCEPT ![self] = IF self = "xM" THEN "M" ELSE "N"]
                /\ pc' = [pc EXCEPT ![self] = "x_resolve"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, dnErr, markedBy, badMark, 
                                badCommit, badCommitAny, acked, idReuse, sres, 
                                fres, dres, dtoks, stack, dn, why, datt, 
                                dtries, myCh, myAtt, outc, fch, xatt, saw, run, 
                                tok, h0 >>

x_resolve(self) == /\ pc[self] = "x_resolve"
                   /\ IF names[xn[self]] = NoAtt
                         THEN /\ pc' = [pc EXCEPT ![self] = "x_done"]
                              /\ xatt' = xatt
                         ELSE /\ xatt' = [xatt EXCEPT ![self] = names[xn[self]]]
                              /\ IF snaps[<<xn[self], names[xn[self]]>>].st = "none"
                                    THEN /\ pc' = [pc EXCEPT ![self] = "x_tomb"]
                                    ELSE /\ IF snaps[<<xn[self], names[xn[self]]>>].st = "active"
                                               THEN /\ pc' = [pc EXCEPT ![self] = "x_mark"]
                                               ELSE /\ pc' = [pc EXCEPT ![self] = "x_free"]
                   /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                   intents, pending, stored, nextChunk, 
                                   hostOps, resv, reads, dnErr, markedBy, 
                                   badMark, badCommit, badCommitAny, acked, 
                                   idReuse, sres, fres, dres, dtoks, stack, dn, 
                                   why, datt, dtries, myCh, myAtt, outc, fch, 
                                   xn, saw, run, tok, h0 >>

x_tomb(self) == /\ pc[self] = "x_tomb"
                /\ IF snaps[<<xn[self], xatt[self]>>].st = "none"
                      THEN /\ snaps' = [snaps EXCEPT ![<<xn[self], xatt[self]>>] = [EmptySnap EXCEPT !.st = "aborted"]]
                      ELSE /\ TRUE
                           /\ snaps' = snaps
                /\ pc' = [pc EXCEPT ![self] = "x_resolve"]
                /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, dnErr, markedBy, badMark, badCommit, 
                                badCommitAny, acked, idReuse, sres, fres, dres, 
                                dtoks, stack, dn, why, datt, dtries, myCh, 
                                myAtt, outc, fch, xn, xatt, saw, run, tok, h0 >>

x_mark(self) == /\ pc[self] = "x_mark"
                /\ IF snaps[<<xn[self], xatt[self]>>].st # "active"
                      THEN /\ pc' = [pc EXCEPT ![self] = "x_free"]
                           /\ UNCHANGED << snaps, markedBy >>
                      ELSE /\ IF LiveClaim(<<xn[self], xatt[self]>>)
                                 THEN /\ pc' = [pc EXCEPT ![self] = "x_done"]
                                      /\ UNCHANGED << snaps, markedBy >>
                                 ELSE /\ IF snaps[<<xn[self], xatt[self]>>].claims # {}
                                            THEN /\ snaps' = [snaps EXCEPT ![<<xn[self], xatt[self]>>].claims = {}]
                                                 /\ pc' = [pc EXCEPT ![self] = "x_mark"]
                                                 /\ UNCHANGED markedBy
                                            ELSE /\ snaps' = [snaps EXCEPT ![<<xn[self], xatt[self]>>].st = "deleting"]
                                                 /\ markedBy' = [markedBy EXCEPT ![<<xn[self], xatt[self]>>] = "explicit"]
                                                 /\ pc' = [pc EXCEPT ![self] = "x_free"]
                /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, dnErr, badMark, badCommit, badCommitAny, 
                                acked, idReuse, sres, fres, dres, dtoks, stack, 
                                dn, why, datt, dtries, myCh, myAtt, outc, fch, 
                                xn, xatt, saw, run, tok, h0 >>

x_free(self) == /\ pc[self] = "x_free"
                /\ IF names[xn[self]] = xatt[self]
                      THEN /\ names' = [names EXCEPT ![xn[self]] = NoAtt]
                      ELSE /\ TRUE
                           /\ names' = names
                /\ pc' = [pc EXCEPT ![self] = "x_done"]
                /\ UNCHANGED << head, volume, volCreates, snaps, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, dnErr, markedBy, badMark, badCommit, 
                                badCommitAny, acked, idReuse, sres, fres, dres, 
                                dtoks, stack, dn, why, datt, dtries, myCh, 
                                myAtt, outc, fch, xn, xatt, saw, run, tok, h0 >>

x_done(self) == /\ pc[self] = "x_done"
                /\ TRUE
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, dnErr, markedBy, badMark, 
                                badCommit, badCommitAny, acked, idReuse, sres, 
                                fres, dres, dtoks, stack, dn, why, datt, 
                                dtries, myCh, myAtt, outc, fch, xn, xatt, saw, 
                                run, tok, h0 >>

DelSnap(self) == x_pick(self) \/ x_resolve(self) \/ x_tomb(self)
                    \/ x_mark(self) \/ x_free(self) \/ x_done(self)

c_check(self) == /\ pc[self] = "c_check"
                 /\ saw' = [saw EXCEPT ![self] = resv.st # "none"]
                 /\ pc' = [pc EXCEPT ![self] = "c_create"]
                 /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                 intents, pending, stored, nextChunk, hostOps, 
                                 resv, reads, dnErr, markedBy, badMark, 
                                 badCommit, badCommitAny, acked, idReuse, sres, 
                                 fres, dres, dtoks, stack, dn, why, datt, 
                                 dtries, myCh, myAtt, outc, fch, xn, xatt, run, 
                                 tok, h0 >>

c_create(self) == /\ pc[self] = "c_create"
                  /\ IF ~saw[self]
                        THEN /\ idReuse' = (idReuse \/ resv.st # "none")
                        ELSE /\ TRUE
                             /\ UNCHANGED idReuse
                  /\ pc' = [pc EXCEPT ![self] = "Done"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  intents, pending, stored, nextChunk, hostOps, 
                                  resv, reads, dnErr, markedBy, badMark, 
                                  badCommit, badCommitAny, acked, sres, fres, 
                                  dres, dtoks, stack, dn, why, datt, dtries, 
                                  myCh, myAtt, outc, fch, xn, xatt, saw, run, 
                                  tok, h0 >>

Create(self) == c_check(self) \/ c_create(self)

d_read(self) == /\ pc[self] = "d_read"
                /\ h0' = [h0 EXCEPT ![self] = head["P"].st]
                /\ tok' = [tok EXCEPT ![self] = <<self, run[self]>>]
                /\ dtoks' = [dtoks EXCEPT ![self] = dtoks[self] \cup {<<self, run[self]>>}]
                /\ pc' = [pc EXCEPT ![self] = "d_admit"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, dnErr, markedBy, badMark, 
                                badCommit, badCommitAny, acked, idReuse, sres, 
                                fres, dres, stack, dn, why, datt, dtries, myCh, 
                                myAtt, outc, fch, xn, xatt, saw, run >>

d_admit(self) == /\ pc[self] = "d_admit"
                 /\ IF head["P"].st = "held"
                       THEN /\ dres' = [dres EXCEPT ![self] = "not_admitted"]
                            /\ pc' = [pc EXCEPT ![self] = "d_end"]
                            /\ resv' = resv
                       ELSE /\ resv' = [st |-> IF resv.st = "committed" THEN "committed" ELSE "admitted",
                                        toks |-> resv.toks \cup {tok[self]}]
                            /\ \/ /\ TRUE
                                  /\ pc' = [pc EXCEPT ![self] = "d_branch"]
                                  /\ dres' = dres
                               \/ /\ LostReply
                                  /\ dres' = [dres EXCEPT ![self] = "unknown"]
                                  /\ pc' = [pc EXCEPT ![self] = "d_end"]
                 /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                 intents, pending, stored, nextChunk, hostOps, 
                                 reads, dnErr, markedBy, badMark, badCommit, 
                                 badCommitAny, acked, idReuse, sres, fres, 
                                 dtoks, stack, dn, why, datt, dtries, myCh, 
                                 myAtt, outc, fch, xn, xatt, saw, run, tok, h0 >>

d_branch(self) == /\ pc[self] = "d_branch"
                  /\ IF h0[self] \in {"purging", "deleted"}
                        THEN /\ pc' = [pc EXCEPT ![self] = "d_admcommit"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "d_mark"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  intents, pending, stored, nextChunk, hostOps, 
                                  resv, reads, dnErr, markedBy, badMark, 
                                  badCommit, badCommitAny, acked, idReuse, 
                                  sres, fres, dres, dtoks, stack, dn, why, 
                                  datt, dtries, myCh, myAtt, outc, fch, xn, 
                                  xatt, saw, run, tok, h0 >>

d_mark(self) == /\ pc[self] = "d_mark"
                /\ IF head["P"].st \in {"deleting", "purging", "deleted"}
                      THEN /\ TRUE
                           /\ pc' = [pc EXCEPT ![self] = "d_claims"]
                           /\ UNCHANGED << head, dres >>
                      ELSE /\ IF head["P"].st = "held"
                                 THEN /\ dres' = [dres EXCEPT ![self] = "in_use"]
                                      /\ pc' = [pc EXCEPT ![self] = "d_abort"]
                                      /\ head' = head
                                 ELSE /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.st = "deleting", !.gen = @ + 1]]
                                      /\ pc' = [pc EXCEPT ![self] = "d_claims"]
                                      /\ dres' = dres
                /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, dnErr, markedBy, badMark, badCommit, 
                                badCommitAny, acked, idReuse, sres, fres, 
                                dtoks, stack, dn, why, datt, dtries, myCh, 
                                myAtt, outc, fch, xn, xatt, saw, run, tok, h0 >>

d_claims(self) == /\ pc[self] = "d_claims"
                  /\ IF \E p \in AttPaths : Named(p) /\ snaps[p].st = "active" /\ LiveClaim(p)
                        THEN /\ pc' = [pc EXCEPT ![self] = "d_revert"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "d_commit"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  intents, pending, stored, nextChunk, hostOps, 
                                  resv, reads, dnErr, markedBy, badMark, 
                                  badCommit, badCommitAny, acked, idReuse, 
                                  sres, fres, dres, dtoks, stack, dn, why, 
                                  datt, dtries, myCh, myAtt, outc, fch, xn, 
                                  xatt, saw, run, tok, h0 >>

d_revert(self) == /\ pc[self] = "d_revert"
                  /\ IF head["P"].st # "deleting"
                        THEN /\ IF head["P"].st \in {"held", "detached"}
                                   THEN /\ dres' = [dres EXCEPT ![self] = "refused_claimed"]
                                        /\ pc' = [pc EXCEPT ![self] = "d_abort"]
                                   ELSE /\ dres' = [dres EXCEPT ![self] = "unknown"]
                                        /\ pc' = [pc EXCEPT ![self] = "d_end"]
                             /\ head' = head
                        ELSE /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.st = "detached", !.gen = @ + 1]]
                             /\ dres' = [dres EXCEPT ![self] = "refused_claimed"]
                             /\ pc' = [pc EXCEPT ![self] = "d_abort"]
                  /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, dnErr, markedBy, badMark, badCommit, 
                                  badCommitAny, acked, idReuse, sres, fres, 
                                  dtoks, stack, dn, why, datt, dtries, myCh, 
                                  myAtt, outc, fch, xn, xatt, saw, run, tok, 
                                  h0 >>

d_commit(self) == /\ pc[self] = "d_commit"
                  /\ IF head["P"].st \in {"purging", "deleted"}
                        THEN /\ TRUE
                             /\ pc' = [pc EXCEPT ![self] = "d_admcommit"]
                             /\ UNCHANGED << head, badCommit, badCommitAny, 
                                             dres >>
                        ELSE /\ IF head["P"].st = "deleting"
                                   THEN /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.st = "purging", !.gen = @ + 1]]
                                        /\ badCommit' = (badCommit \/ CommitUnsafe)
                                        /\ badCommitAny' = (badCommitAny \/ CommitAnyClaim)
                                        /\ pc' = [pc EXCEPT ![self] = "d_admcommit"]
                                        /\ dres' = dres
                                   ELSE /\ dres' = [dres EXCEPT ![self] = "givenback_by_other"]
                                        /\ pc' = [pc EXCEPT ![self] = "d_abort"]
                                        /\ UNCHANGED << head, badCommit, 
                                                        badCommitAny >>
                  /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, dnErr, markedBy, badMark, acked, 
                                  idReuse, sres, fres, dtoks, stack, dn, why, 
                                  datt, dtries, myCh, myAtt, outc, fch, xn, 
                                  xatt, saw, run, tok, h0 >>

d_admcommit(self) == /\ pc[self] = "d_admcommit"
                     /\ resv' = [resv EXCEPT !.st = "committed"]
                     /\ pc' = [pc EXCEPT ![self] = "d_cascade_m"]
                     /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                     intents, pending, stored, nextChunk, 
                                     hostOps, reads, dnErr, markedBy, badMark, 
                                     badCommit, badCommitAny, acked, idReuse, 
                                     sres, fres, dres, dtoks, stack, dn, why, 
                                     datt, dtries, myCh, myAtt, outc, fch, xn, 
                                     xatt, saw, run, tok, h0 >>

d_cascade_m(self) == /\ pc[self] = "d_cascade_m"
                     /\ IF names["M"] # NoAtt
                           THEN /\ /\ dn' = [dn EXCEPT ![self] = "M"]
                                   /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DeleteName",
                                                                            pc        |->  "d_cascade_n",
                                                                            datt      |->  datt[self],
                                                                            dtries    |->  dtries[self],
                                                                            dn        |->  dn[self],
                                                                            why       |->  why[self] ] >>
                                                                        \o stack[self]]
                                   /\ why' = [why EXCEPT ![self] = "cascade"]
                                /\ datt' = [datt EXCEPT ![self] = NoAtt]
                                /\ dtries' = [dtries EXCEPT ![self] = 0]
                                /\ pc' = [pc EXCEPT ![self] = "dn_resolve"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "d_cascade_n"]
                                /\ UNCHANGED << stack, dn, why, datt, dtries >>
                     /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                     intents, pending, stored, nextChunk, 
                                     hostOps, resv, reads, dnErr, markedBy, 
                                     badMark, badCommit, badCommitAny, acked, 
                                     idReuse, sres, fres, dres, dtoks, myCh, 
                                     myAtt, outc, fch, xn, xatt, saw, run, tok, 
                                     h0 >>

d_cascade_n(self) == /\ pc[self] = "d_cascade_n"
                     /\ IF dnErr[self]
                           THEN /\ dres' = [dres EXCEPT ![self] = "error"]
                                /\ pc' = [pc EXCEPT ![self] = "d_end"]
                                /\ UNCHANGED << stack, dn, why, datt, dtries >>
                           ELSE /\ IF names["N"] # NoAtt
                                      THEN /\ /\ dn' = [dn EXCEPT ![self] = "N"]
                                              /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DeleteName",
                                                                                       pc        |->  "d_finish",
                                                                                       datt      |->  datt[self],
                                                                                       dtries    |->  dtries[self],
                                                                                       dn        |->  dn[self],
                                                                                       why       |->  why[self] ] >>
                                                                                   \o stack[self]]
                                              /\ why' = [why EXCEPT ![self] = "cascade"]
                                           /\ datt' = [datt EXCEPT ![self] = NoAtt]
                                           /\ dtries' = [dtries EXCEPT ![self] = 0]
                                           /\ pc' = [pc EXCEPT ![self] = "dn_resolve"]
                                      ELSE /\ pc' = [pc EXCEPT ![self] = "d_finish"]
                                           /\ UNCHANGED << stack, dn, why, 
                                                           datt, dtries >>
                                /\ dres' = dres
                     /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                     intents, pending, stored, nextChunk, 
                                     hostOps, resv, reads, dnErr, markedBy, 
                                     badMark, badCommit, badCommitAny, acked, 
                                     idReuse, sres, fres, dtoks, myCh, myAtt, 
                                     outc, fch, xn, xatt, saw, run, tok, h0 >>

d_finish(self) == /\ pc[self] = "d_finish"
                  /\ IF dnErr[self]
                        THEN /\ dres' = [dres EXCEPT ![self] = "error"]
                             /\ pc' = [pc EXCEPT ![self] = "d_end"]
                             /\ head' = head
                        ELSE /\ IF head["P"].st = "purging"
                                   THEN /\ head' = [head EXCEPT !["P"] = [st |-> "deleted", gen |-> head["P"].gen + 1, ch |-> {}]]
                                        /\ pc' = [pc EXCEPT ![self] = "d_volmark"]
                                        /\ dres' = dres
                                   ELSE /\ IF head["P"].st # "deleted"
                                              THEN /\ dres' = [dres EXCEPT ![self] = "error"]
                                                   /\ pc' = [pc EXCEPT ![self] = "d_end"]
                                              ELSE /\ pc' = [pc EXCEPT ![self] = "d_volmark"]
                                                   /\ dres' = dres
                                        /\ head' = head
                  /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, dnErr, markedBy, badMark, badCommit, 
                                  badCommitAny, acked, idReuse, sres, fres, 
                                  dtoks, stack, dn, why, datt, dtries, myCh, 
                                  myAtt, outc, fch, xn, xatt, saw, run, tok, 
                                  h0 >>

d_volmark(self) == /\ pc[self] = "d_volmark"
                   /\ volume' = [volume EXCEPT !["P"] = "deleted"]
                   /\ dres' = [dres EXCEPT ![self] = "completed"]
                   /\ pc' = [pc EXCEPT ![self] = "d_end"]
                   /\ UNCHANGED << head, volCreates, names, snaps, intents, 
                                   pending, stored, nextChunk, hostOps, resv, 
                                   reads, dnErr, markedBy, badMark, badCommit, 
                                   badCommitAny, acked, idReuse, sres, fres, 
                                   dtoks, stack, dn, why, datt, dtries, myCh, 
                                   myAtt, outc, fch, xn, xatt, saw, run, tok, 
                                   h0 >>

d_abort(self) == /\ pc[self] = "d_abort"
                 /\ IF resv.st = "committed"
                       THEN /\ dres' = [dres EXCEPT ![self] = "unknown"]
                            /\ resv' = resv
                       ELSE /\ IF resv.toks \ {tok[self]} = {}
                                  THEN /\ resv' = [st |-> "none", toks |-> {}]
                                  ELSE /\ resv' = [resv EXCEPT !.toks = resv.toks \ {tok[self]}]
                            /\ dres' = dres
                 /\ pc' = [pc EXCEPT ![self] = "d_end"]
                 /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                 intents, pending, stored, nextChunk, hostOps, 
                                 reads, dnErr, markedBy, badMark, badCommit, 
                                 badCommitAny, acked, idReuse, sres, fres, 
                                 dtoks, stack, dn, why, datt, dtries, myCh, 
                                 myAtt, outc, fch, xn, xatt, saw, run, tok, h0 >>

d_end(self) == /\ pc[self] = "d_end"
               /\ IF dres[self] = "unknown" /\ run[self] < MaxRuns
                     THEN /\ run' = [run EXCEPT ![self] = run[self] + 1]
                          /\ pc' = [pc EXCEPT ![self] = "d_read"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ run' = run
               /\ UNCHANGED << head, volume, volCreates, names, snaps, intents, 
                               pending, stored, nextChunk, hostOps, resv, 
                               reads, dnErr, markedBy, badMark, badCommit, 
                               badCommitAny, acked, idReuse, sres, fres, dres, 
                               dtoks, stack, dn, why, datt, dtries, myCh, 
                               myAtt, outc, fch, xn, xatt, saw, tok, h0 >>

Del(self) == d_read(self) \/ d_admit(self) \/ d_branch(self)
                \/ d_mark(self) \/ d_claims(self) \/ d_revert(self)
                \/ d_commit(self) \/ d_admcommit(self) \/ d_cascade_m(self)
                \/ d_cascade_n(self) \/ d_finish(self) \/ d_volmark(self)
                \/ d_abort(self) \/ d_end(self)

Next == Host \/ GC
           \/ (\E self \in ProcSet: DeleteName(self))
           \/ (\E self \in SnapCallers: Snap(self))
           \/ (\E self \in ForkIds: Fork(self))
           \/ (\E self \in DelSnapIds: DelSnap(self))
           \/ (\E self \in CreateIds: Create(self))
           \/ (\E self \in Deleters: Del(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
    /\ \A v \in Vols : head[v].st \in {"none", "held", "detached", "active", "deleting", "purging", "deleted", "aborted"}
    /\ stored \subseteq Chunks
    /\ resv.st \in {"none", "admitted", "committed"}
    /\ \A p \in AttPaths : snaps[p].st \in {"none", "active", "deleting", "aborted"}

\* GC never deletes a chunk reachable from a live HEAD, a non-deleted snapshot or an intent.
GCSafe ==
    (UNION {head[v].ch : v \in Vols}
       \cup UNION {snaps[p].ch : p \in {q \in AttPaths : snaps[q].st = "active"}}
       \cup UNION {i.keys : i \in intents}) \subseteq stored

\* (P1, short form) a published snapshot is marked deleting only by an explicit
\* DeleteSnapshot, or by the volume delete once it has committed (dfbf727 cascades).
NoProvisionalSnapMark == ~badMark

\* (P1, the end state found in review) a snapshot a caller was told succeeded is not lost to
\* the volume delete's cleanup while the volume is usable again (delete given back).
AckedKeptAfterGiveBack ==
    \A p \in acked : markedBy[p] \in {"backout", "cascade"} => head["P"].st \notin {"held", "detached"}

\* (P1, data) its chunks survive GC unless explicitly deleted or the volume committed.
AckedChunksSurvive ==
    \A p \in acked : markedBy[p] = "explicit" \/ Committed \/ snaps[p].ch \subseteq stored

\* (P2) volume delete never commits purging while a fork that passed its source
\* check (so it will not back out) holds a live claim.
CommitWithoutLiveClaim == ~badCommit
\* literal form: no live claim at all at the commit (a fork that claims after the
\* delete's check and then backs out on the mark violates this; benign)
CommitWithoutAnyClaim == ~badCommitAny

\* volume ids are never reused (catalog VOLUME, and blockmaster records of a reserved id).
NoIdReuse == (\A v \in Vols : volCreates[v] <= 1) /\ ~idReuse

\* (P2, CLI token) a delete that reports a give-back holds no reservation token.
GiveBackReleases ==
    \A d \in Deleters : (pc[d] = "Done" /\ dres[d] \in GaveBack) => dtoks[d] \cap resv.toks = {}

\* No HEAD moves out of purging or deleted; tombstones and deleted VOLUMEs are permanent.
HeadMonotone ==
    [][ /\ (head["P"].st \in {"purging", "deleted"} => head'["P"].st \in {"purging", "deleted"})
        /\ (head["P"].st = "deleted" => head'["P"].st = "deleted")
        /\ (head["F"].st = "aborted" => head'["F"].st = "aborted")
        /\ (volume["P"] = "deleted" => volume'["P"] = "deleted")
        /\ \A p \in AttPaths : snaps[p].st = "aborted" => snaps'[p].st = "aborted" ]_vars

\* ---- reachability witnesses: each is EXPECTED to be violated (proves the
\* model is not vacuous); run with cfg/*_reach*.cfg ----
W_NeverDeleted == head["P"].st # "deleted"
W_NoForkThenDelete == ~(head["F"].st = "active" /\ head["P"].st = "deleted")
W_NoAckThenDelete == ~(acked # {} /\ head["P"].st = "deleted")
W_NoRefusalThenDelete == ~(head["P"].st = "deleted" /\ \E d \in Deleters : dres[d] \in GaveBack)
W_NoGCReclaim == \A c \in {"c1"} \cup {ChunkOf(i) : i \in 2..(nextChunk - 1)} : c \in stored

Symm == Permutations(SnapCallers) \cup Permutations(Deleters)
=============================================================================
