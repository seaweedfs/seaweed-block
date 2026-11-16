------------------------- MODULE ChunkLifecycleFixed -------------------------
(***************************************************************************)
(* sw-block chunk-layer lifecycle with the FIX for PR #152:                *)
(*  - volume delete never touches snapshots: it REFUSES with               *)
(*    volume_has_snapshots while any snapshot name record exists           *)
(*    (published, being deleted, or an attempt in flight);                 *)
(*  - mark HEAD deleting first (Snapshot/Fork then refuse), check under    *)
(*    that deletion incarnation (the HEAD gen the mark produced/joined),   *)
(*    and commit purging only if the incarnation is unchanged; a revert is *)
(*    bound the same way;                                                  *)
(*  - plus the side fixes in the same commit: the delete token is the      *)
(*    operation's request id, CreateVolume keeps workspaceMu.              *)
(* Pinned to 765e7226 (PR #152 branch; merged with main 5c29382 as         *)
(* 19e2c531). Same environment as ChunkLifecycle.tla.                      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    SnapCallers, Deleters, ForkOn, DelSnapOn, CreateOn,
    MaxHostOps, MaxRuns, LostReply, PendingOn, SweepOn

NoAtt   == "-"
Vols    == {"P", "F"}
Names   == {"M", "N"}
MP      == <<"M", "m0">>
AttPaths == {MP} \cup {<<"N", s>> : s \in SnapCallers}
Chunks  == {"c1", "c2", "c3"}
ChunkOf(i) == IF i = 2 THEN "c2" ELSE "c3"
ForkIds    == IF ForkOn THEN {"fork"} ELSE {}
DelSnapIds == IF DelSnapOn THEN {"xM", "xN"} ELSE {}   \* one explicit DeleteSnapshot caller per name
CreateIds  == IF CreateOn THEN {"create"} ELSE {}
ReaderIds  == SnapCallers \cup ForkIds
GaveBack   == {"refused_snapshots", "in_use", "givenback_by_other"}
EmptySnap  == [st |-> "none", ch |-> {}, claims |-> {}, exp |-> FALSE]
MaxIncarnations == 5                          \* volume_delete.go:149

(* --algorithm ChunkLifecycleFixed
variables
    head = [v \in Vols |-> IF v = "P" THEN [st |-> "held", gen |-> 1, ch |-> {"c1"}]
                                     ELSE [st |-> "none", gen |-> 0, ch |-> {}]],
    volume = [v \in Vols |-> IF v = "P" THEN "live" ELSE "none"],
    volCreates = [v \in Vols |-> IF v = "P" THEN 1 ELSE 0],
    names = [n \in Names |-> IF n = "M" THEN "m0" ELSE NoAtt],
    snaps = [p \in AttPaths |-> IF p = MP
                                THEN [st |-> "active", ch |-> {"c1"}, claims |-> {}, exp |-> FALSE]
                                ELSE EmptySnap],
    intents = {},
    pending = {},
    stored = {"c1"},
    nextChunk = 2,
    hostOps = 0,
    resv = [st |-> "none", toks |-> {}],
    reads = [r \in ReaderIds |-> {}],
    snapAtt = [s \in SnapCallers |-> NoAtt],  \* copy of each caller's attempt, for CommitClean
    \* ---- history / outcome variables ----
    markedBy = [p \in AttPaths |-> "none"],
    badCommit = FALSE,       \* commit while a snapshot, an attempt or a fork claim could still matter
    badPub = FALSE,          \* a snapshot published (attempt path none -> active) after P's delete committed
    acked = {},
    idReuse = FALSE,
    sres = [s \in SnapCallers |-> "none"],
    fres = "none",
    dres = [d \in Deleters |-> "none"],
    dtoks = [d \in Deleters |-> {}];

define
    Gone == volume["P"] = "deleted" \/ head["P"].st \in {"deleting", "purging", "deleted"}
    Named(p) == names[p[1]] = p[2]
    LiveClaim(p) == \E c \in snaps[p].claims : head["F"].st = "none"
    Committed == head["P"].st \in {"purging", "deleted"}
    SnapRoot(p) == snaps[p].st = "active" \/ (snaps[p].st = "deleting" /\ (Named(p) \/ ~snaps[p].exp))
    AllRoots == UNION {head[v].ch : v \in Vols}
                \cup UNION {snaps[p].ch : p \in {q \in AttPaths : SnapRoot(q)}}
                \cup UNION {i.keys : i \in intents}
                \cup UNION {reads[r] : r \in ReaderIds}
    \* a snapshot caller that passed its volumeGone gate and may still publish
    PastSnapGate == {"s_intent", "s_create"}
    \* the refusal rule: at the commit there must be no fork claim at all, no
    \* snapshot that is published or being deleted under its name, no attempt
    \* past its gate that can still publish (its path not yet tombstoned), and
    \* no uncancellable create that can still land
    CommitUnsafe ==
        \/ \E p \in AttPaths : snaps[p].claims # {} /\ LiveClaim(p)
        \/ \E p \in AttPaths : snaps[p].st = "active"
        \/ \E p \in AttPaths : Named(p) /\ snaps[p].st = "deleting"
        \/ \E s \in SnapCallers : pc[s] \in PastSnapGate /\ snaps[<<"N", snapAtt[s]>>].st = "none"
        \/ \E x \in pending : snaps[x.path].st = "none"
end define;

\* ---------------- volume server of P ----------------
process Host = "host"
begin
h_loop:
    while hostOps < MaxHostOps do
        either
            await head["P"].st = "held" /\ nextChunk <= 3;
            stored := stored \cup {ChunkOf(nextChunk)};
            head["P"] := [head["P"] EXCEPT !.ch = {ChunkOf(nextChunk)}, !.gen = @ + 1];
            nextChunk := nextChunk + 1;
        or
            await head["P"].st = "held";
            head["P"] := [head["P"] EXCEPT !.st = "detached", !.gen = @ + 1];
        or
            await head["P"].st = "detached" /\ resv.st = "none";
            head["P"] := [head["P"] EXCEPT !.st = "held", !.gen = @ + 1];
        end either;
        hostOps := hostOps + 1;
    end while;
end process;

\* ---------------- GC, copy sweep, late creates ----------------
process GC = "gc"
begin
g_loop:
    while TRUE do
        either
            await stored \ AllRoots # {};
            stored := stored \cap AllRoots;
        or
            with p \in {q \in AttPaths : snaps[q].st = "deleting" /\ ~Named(q) /\ ~snaps[q].exp} do
                snaps[p].exp := TRUE;
            end with;
        or
            with x \in pending do
                if snaps[x.path].st = "none" then
                    snaps[x.path] := [st |-> "active", ch |-> x.ch, claims |-> {}, exp |-> FALSE];
                    badPub := badPub \/ Committed;
                end if;
                pending := pending \ {x};
            end with;
        or
            await SweepOn;
            with i \in {j \in intents : j.vol = "P"} do
                if snaps[<<"N", i.att>>].st = "none" then
                    snaps[<<"N", i.att>>] := [EmptySnap EXCEPT !.st = "aborted"];
                end if;
                intents := intents \ {i};
            end with;
        or
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

\* ---------------- snapshot callers of P/N ----------------
process Snap \in SnapCallers
variables myCh = {}, myAtt = NoAtt, outc = "none";
begin
s_cut:
    if head["P"].st = "held" then
        myCh := head["P"].ch;
        reads[self] := head["P"].ch;
        head["P"].gen := head["P"].gen + 1;
    else
        sres[self] := "closing"; goto s_done;
    end if;
s_replay:                                     \* catalog.go:824 SnapshotReplay
    if names["N"] # NoAtt /\ snaps[<<"N", names["N"]>>].st \in {"active", "deleting"} then
        acked := acked \cup {<<"N", names["N"]>>};
        sres[self] := "replayed"; goto s_done;
    elsif names["N"] # NoAtt /\ snaps[<<"N", names["N"]>>].st = "aborted" then
        sres[self] := "exists"; goto s_done;
    end if;
s_claim:                                      \* catalog.go:843 claimName
    if names["N"] = NoAtt then
        names["N"] := self; myAtt := self; snapAtt[self] := self;
    else
        myAtt := names["N"]; snapAtt[self] := names["N"];
    end if;
s_gone:                                       \* catalog.go:854-858: refuse, leave the name alone
    if Gone then
        sres[self] := "volume_deleted"; goto s_done;
    end if;
s_intent:                                     \* catalog.go:888 putIntent
    intents := intents \cup {[vol |-> "P", own |-> self, att |-> myAtt, keys |-> myCh]};
s_create:                                     \* catalog.go:900 createResolved
    either
        if snaps[<<"N", myAtt>>].st = "none" then
            snaps[<<"N", myAtt>>] := [st |-> "active", ch |-> myCh, claims |-> {}, exp |-> FALSE];
            badPub := badPub \/ Committed;
            outc := "created";
        elsif snaps[<<"N", myAtt>>].st \in {"active", "deleting"} then
            outc := "created";
        else
            outc := "exists";
        end if;
    or
        await PendingOn;
        pending := pending \cup {[path |-> <<"N", myAtt>>, ch |-> myCh]};
        outc := "unknown";
    end either;
s_drop:
    if outc # "unknown" then
        intents := {i \in intents : i.own # self};
    end if;
    if outc = "created" then
        acked := acked \cup {<<"N", myAtt>>};
    end if;
    sres[self] := outc;
s_done:
    reads[self] := {};
end process;

\* ---------------- fork F <- P/M (unchanged by the fix) ----------------
process Fork \in ForkIds
variables fch = {};
begin
f_target:
    if volume["F"] = "deleted" then fres := "volume_deleted"; goto f_done; end if;
f_claim:                                      \* catalog.go:1314 claimForkParent
    if names["M"] = NoAtt \/ snaps[MP].st \in {"none", "aborted"} then
        fres := "not_found"; goto f_done;
    elsif snaps[MP].st = "deleting" then
        fres := "snapshot_deleting"; goto f_done;
    else
        snaps[MP].claims := snaps[MP].claims \cup {"F"};
        fch := snaps[MP].ch;
        reads[self] := snaps[MP].ch;
    end if;
f_gone:                                       \* catalog.go:1206 volumeGone(source)
    if Gone then goto f_dropgone; end if;
f_vol:
    if volume["F"] = "none" then
        volume["F"] := "live";
        volCreates["F"] := volCreates["F"] + 1;
    end if;
f_intent:
    intents := intents \cup {[vol |-> "F", own |-> self, att |-> NoAtt, keys |-> fch]};
f_head:                                       \* catalog.go:1282 exclusive HEAD create
    if head["F"].st = "none" then
        head["F"] := [st |-> "active", gen |-> 1, ch |-> fch];
        fres := "ok";
    else
        fres := "lost";
    end if;
f_dropintent:
    intents := {i \in intents : i.own # self};
f_dropclaim:                                  \* catalog.go:1364 removeForkClaim
    snaps[MP].claims := snaps[MP].claims \ {"F"};
    goto f_done;
f_dropgone:
    snaps[MP].claims := snaps[MP].claims \ {"F"};
    fres := "volume_deleted";
f_done:
    reads[self] := {};
end process;

\* ---------------- explicit DeleteSnapshot (catalog.go:1049-1098) ----------------
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
x_tomb:
    if snaps[<<xn, xatt>>].st = "none" then
        snaps[<<xn, xatt>>] := [EmptySnap EXCEPT !.st = "aborted"];
    end if;
    goto x_resolve;
x_mark:                                       \* retention.go markDeleting (unchanged)
    if snaps[<<xn, xatt>>].st # "active" then
        goto x_free;
    elsif LiveClaim(<<xn, xatt>>) then
        goto x_done;
    elsif snaps[<<xn, xatt>>].claims # {} then
        snaps[<<xn, xatt>>].claims := {};
        goto x_mark;
    else
        snaps[<<xn, xatt>>].st := "deleting";
        markedBy[<<xn, xatt>>] := "explicit";
    end if;
x_free:
    if names[xn] = xatt then names[xn] := NoAtt; end if;
x_done:
    skip;
end process;

\* ---------------- blockmaster CreateVolume, workspaceMu held through the create ----------------
process Create \in CreateIds
begin
c_create:                                     \* services.go:64-73 at 765e7226
    if resv.st = "none" then idReuse := idReuse \/ resv.st # "none"; end if;
end process;

\* ---------------- volume delete of P (volume_delete.go:104-224 at 765e7226) ----------------
process Del \in Deleters
variables run = 1, tok = <<>>, hst = "", mygen = 0, inc = 0;
begin
d_read:                                       \* :110-121; chunk_volume_delete.go: token = --request-id
    hst := head["P"].st;
    inc := 0;
    tok := self;
    dtoks[self] := {self};
d_admit:                                      \* :125 Admit
    if head["P"].st = "held" then
        dres[self] := "not_admitted"; goto d_end;
    else
        resv := [st |-> IF resv.st = "committed" THEN "committed" ELSE "admitted",
                 toks |-> resv.toks \cup {tok}];
        either skip;
        or await LostReply; dres[self] := "unknown"; goto d_end;
        end either;
    end if;
d_loop:                                       \* :148 for incarnation ...; h.State not purging/deleted
    if hst \in {"purging", "deleted"} then goto d_admcommit;
    elsif inc = MaxIncarnations then dres[self] := "error"; goto d_end;
    end if;
d_mark:                                       \* :152 markHeadDeleting (265-305): produce or join an incarnation
    if head["P"].st \in {"deleting", "purging", "deleted"} then
        mygen := head["P"].gen;
    elsif head["P"].st = "held" then
        dres[self] := "in_use"; goto d_abort;
    else
        mygen := head["P"].gen + 1;           \* the gen this write produces (volume_delete.go:292)
        head["P"] := [head["P"] EXCEPT !.st = "deleting", !.gen = @ + 1];
    end if;
d_check:                                      \* :163 snapshotsBlock (396-406): one listing of snapnames
    if \E n \in Names : names[n] # NoAtt then goto d_revert; else goto d_commit; end if;
d_revert:                                     \* :169 revertHeadDeleting(gen) (310-339)
    if head["P"].st \in {"purging", "deleted"} then
        dres[self] := "unknown"; goto d_end;          \* incarnationCommitted
    elsif head["P"].st # "deleting" then
        dres[self] := "refused_snapshots"; goto d_abort;  \* incarnationGivenBack
    elsif head["P"].gen # mygen then
        goto d_reread;                                \* incarnationMoved
    else
        head["P"] := [head["P"] EXCEPT !.st = "detached", !.gen = @ + 1];
        dres[self] := "refused_snapshots"; goto d_abort;
    end if;
d_commit:                                     \* :189 commitHeadPurging(gen) (343-366)
    if head["P"].st \in {"purging", "deleted"} then
        goto d_admcommit;                             \* incarnationDone
    elsif head["P"].st # "deleting" then
        dres[self] := "givenback_by_other"; goto d_abort;
    elsif head["P"].gen # mygen then
        goto d_reread;                                \* moved: validate again
    else
        head["P"] := [head["P"] EXCEPT !.st = "purging", !.gen = @ + 1];
        badCommit := badCommit \/ CommitUnsafe;
        goto d_admcommit;
    end if;
d_reread:                                     \* :176-178 / :199-201 ReadHead, next incarnation
    hst := head["P"].st;
    inc := inc + 1;
    goto d_loop;
d_admcommit:                                  \* :207 Admission.Commit
    resv.st := "committed";
d_finish:                                     \* :214 finishHeadDeleted
    if head["P"].st = "purging" then
        head["P"] := [st |-> "deleted", gen |-> head["P"].gen + 1, ch |-> {}];
    elsif head["P"].st # "deleted" then
        dres[self] := "error"; goto d_end;
    end if;
d_volmark:                                    \* :218 markVolumeDeleted
    volume["P"] := "deleted";
    dres[self] := "completed";
    goto d_end;
d_abort:                                      \* :136 Admission.Abort
    if resv.st = "committed" then
        dres[self] := "unknown";
    elsif resv.toks \ {tok} = {} then
        resv := [st |-> "none", toks |-> {}];
    else
        resv.toks := resv.toks \ {tok};
    end if;
d_end:
    if dres[self] = "unknown" /\ run < MaxRuns then
        run := run + 1;
        goto d_read;
    end if;
end process;

end algorithm; *)
\* BEGIN TRANSLATION
VARIABLES head, volume, volCreates, names, snaps, intents, pending, stored, 
          nextChunk, hostOps, resv, reads, snapAtt, markedBy, badCommit, 
          badPub, acked, idReuse, sres, fres, dres, dtoks, pc

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

PastSnapGate == {"s_intent", "s_create"}




CommitUnsafe ==
    \/ \E p \in AttPaths : snaps[p].claims # {} /\ LiveClaim(p)
    \/ \E p \in AttPaths : snaps[p].st = "active"
    \/ \E p \in AttPaths : Named(p) /\ snaps[p].st = "deleting"
    \/ \E s \in SnapCallers : pc[s] \in PastSnapGate /\ snaps[<<"N", snapAtt[s]>>].st = "none"
    \/ \E x \in pending : snaps[x.path].st = "none"

VARIABLES myCh, myAtt, outc, fch, xn, xatt, run, tok, hst, mygen, inc

vars == << head, volume, volCreates, names, snaps, intents, pending, stored, 
           nextChunk, hostOps, resv, reads, snapAtt, markedBy, badCommit, 
           badPub, acked, idReuse, sres, fres, dres, dtoks, pc, myCh, myAtt, 
           outc, fch, xn, xatt, run, tok, hst, mygen, inc >>

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
        /\ snapAtt = [s \in SnapCallers |-> NoAtt]
        /\ markedBy = [p \in AttPaths |-> "none"]
        /\ badCommit = FALSE
        /\ badPub = FALSE
        /\ acked = {}
        /\ idReuse = FALSE
        /\ sres = [s \in SnapCallers |-> "none"]
        /\ fres = "none"
        /\ dres = [d \in Deleters |-> "none"]
        /\ dtoks = [d \in Deleters |-> {}]
        (* Process Snap *)
        /\ myCh = [self \in SnapCallers |-> {}]
        /\ myAtt = [self \in SnapCallers |-> NoAtt]
        /\ outc = [self \in SnapCallers |-> "none"]
        (* Process Fork *)
        /\ fch = [self \in ForkIds |-> {}]
        (* Process DelSnap *)
        /\ xn = [self \in DelSnapIds |-> "M"]
        /\ xatt = [self \in DelSnapIds |-> NoAtt]
        (* Process Del *)
        /\ run = [self \in Deleters |-> 1]
        /\ tok = [self \in Deleters |-> <<>>]
        /\ hst = [self \in Deleters |-> ""]
        /\ mygen = [self \in Deleters |-> 0]
        /\ inc = [self \in Deleters |-> 0]
        /\ pc = [self \in ProcSet |-> CASE self = "host" -> "h_loop"
                                        [] self = "gc" -> "g_loop"
                                        [] self \in SnapCallers -> "s_cut"
                                        [] self \in ForkIds -> "f_target"
                                        [] self \in DelSnapIds -> "x_pick"
                                        [] self \in CreateIds -> "c_create"
                                        [] self \in Deleters -> "d_read"]

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
                          resv, reads, snapAtt, markedBy, badCommit, badPub, 
                          acked, idReuse, sres, fres, dres, dtoks, myCh, myAtt, 
                          outc, fch, xn, xatt, run, tok, hst, mygen, inc >>

Host == h_loop

g_loop == /\ pc["gc"] = "g_loop"
          /\ \/ /\ stored \ AllRoots # {}
                /\ stored' = (stored \cap AllRoots)
                /\ UNCHANGED <<head, snaps, intents, pending, badPub>>
             \/ /\ \E p \in {q \in AttPaths : snaps[q].st = "deleting" /\ ~Named(q) /\ ~snaps[q].exp}:
                     snaps' = [snaps EXCEPT ![p].exp = TRUE]
                /\ UNCHANGED <<head, intents, pending, stored, badPub>>
             \/ /\ \E x \in pending:
                     /\ IF snaps[x.path].st = "none"
                           THEN /\ snaps' = [snaps EXCEPT ![x.path] = [st |-> "active", ch |-> x.ch, claims |-> {}, exp |-> FALSE]]
                                /\ badPub' = (badPub \/ Committed)
                           ELSE /\ TRUE
                                /\ UNCHANGED << snaps, badPub >>
                     /\ pending' = pending \ {x}
                /\ UNCHANGED <<head, intents, stored>>
             \/ /\ SweepOn
                /\ \E i \in {j \in intents : j.vol = "P"}:
                     /\ IF snaps[<<"N", i.att>>].st = "none"
                           THEN /\ snaps' = [snaps EXCEPT ![<<"N", i.att>>] = [EmptySnap EXCEPT !.st = "aborted"]]
                           ELSE /\ TRUE
                                /\ snaps' = snaps
                     /\ intents' = intents \ {i}
                /\ UNCHANGED <<head, pending, stored, badPub>>
             \/ /\ SweepOn
                /\ \E i \in {j \in intents : j.vol = "F"}:
                     /\ IF head["F"].st = "none"
                           THEN /\ head' = [head EXCEPT !["F"] = [st |-> "aborted", gen |-> 1, ch |-> {}]]
                           ELSE /\ TRUE
                                /\ head' = head
                     /\ intents' = intents \ {i}
                /\ UNCHANGED <<snaps, pending, stored, badPub>>
          /\ pc' = [pc EXCEPT !["gc"] = "g_loop"]
          /\ UNCHANGED << volume, volCreates, names, nextChunk, hostOps, resv, 
                          reads, snapAtt, markedBy, badCommit, acked, idReuse, 
                          sres, fres, dres, dtoks, myCh, myAtt, outc, fch, xn, 
                          xatt, run, tok, hst, mygen, inc >>

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
                               snapAtt, markedBy, badCommit, badPub, acked, 
                               idReuse, fres, dres, dtoks, myAtt, outc, fch, 
                               xn, xatt, run, tok, hst, mygen, inc >>

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
                                  resv, reads, snapAtt, markedBy, badCommit, 
                                  badPub, idReuse, fres, dres, dtoks, myCh, 
                                  myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                  mygen, inc >>

s_claim(self) == /\ pc[self] = "s_claim"
                 /\ IF names["N"] = NoAtt
                       THEN /\ names' = [names EXCEPT !["N"] = self]
                            /\ myAtt' = [myAtt EXCEPT ![self] = self]
                            /\ snapAtt' = [snapAtt EXCEPT ![self] = self]
                       ELSE /\ myAtt' = [myAtt EXCEPT ![self] = names["N"]]
                            /\ snapAtt' = [snapAtt EXCEPT ![self] = names["N"]]
                            /\ names' = names
                 /\ pc' = [pc EXCEPT ![self] = "s_gone"]
                 /\ UNCHANGED << head, volume, volCreates, snaps, intents, 
                                 pending, stored, nextChunk, hostOps, resv, 
                                 reads, markedBy, badCommit, badPub, acked, 
                                 idReuse, sres, fres, dres, dtoks, myCh, outc, 
                                 fch, xn, xatt, run, tok, hst, mygen, inc >>

s_gone(self) == /\ pc[self] = "s_gone"
                /\ IF Gone
                      THEN /\ sres' = [sres EXCEPT ![self] = "volume_deleted"]
                           /\ pc' = [pc EXCEPT ![self] = "s_done"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "s_intent"]
                           /\ sres' = sres
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, snapAtt, markedBy, badCommit, 
                                badPub, acked, idReuse, fres, dres, dtoks, 
                                myCh, myAtt, outc, fch, xn, xatt, run, tok, 
                                hst, mygen, inc >>

s_intent(self) == /\ pc[self] = "s_intent"
                  /\ intents' = (intents \cup {[vol |-> "P", own |-> self, att |-> myAtt[self], keys |-> myCh[self]]})
                  /\ pc' = [pc EXCEPT ![self] = "s_create"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, snapAtt, markedBy, badCommit, badPub, 
                                  acked, idReuse, sres, fres, dres, dtoks, 
                                  myCh, myAtt, outc, fch, xn, xatt, run, tok, 
                                  hst, mygen, inc >>

s_create(self) == /\ pc[self] = "s_create"
                  /\ \/ /\ IF snaps[<<"N", myAtt[self]>>].st = "none"
                              THEN /\ snaps' = [snaps EXCEPT ![<<"N", myAtt[self]>>] = [st |-> "active", ch |-> myCh[self], claims |-> {}, exp |-> FALSE]]
                                   /\ badPub' = (badPub \/ Committed)
                                   /\ outc' = [outc EXCEPT ![self] = "created"]
                              ELSE /\ IF snaps[<<"N", myAtt[self]>>].st \in {"active", "deleting"}
                                         THEN /\ outc' = [outc EXCEPT ![self] = "created"]
                                         ELSE /\ outc' = [outc EXCEPT ![self] = "exists"]
                                   /\ UNCHANGED << snaps, badPub >>
                        /\ UNCHANGED pending
                     \/ /\ PendingOn
                        /\ pending' = (pending \cup {[path |-> <<"N", myAtt[self]>>, ch |-> myCh[self]]})
                        /\ outc' = [outc EXCEPT ![self] = "unknown"]
                        /\ UNCHANGED <<snaps, badPub>>
                  /\ pc' = [pc EXCEPT ![self] = "s_drop"]
                  /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                  stored, nextChunk, hostOps, resv, reads, 
                                  snapAtt, markedBy, badCommit, acked, idReuse, 
                                  sres, fres, dres, dtoks, myCh, myAtt, fch, 
                                  xn, xatt, run, tok, hst, mygen, inc >>

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
                                reads, snapAtt, markedBy, badCommit, badPub, 
                                idReuse, fres, dres, dtoks, myCh, myAtt, outc, 
                                fch, xn, xatt, run, tok, hst, mygen, inc >>

s_done(self) == /\ pc[self] = "s_done"
                /\ reads' = [reads EXCEPT ![self] = {}]
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, snapAtt, markedBy, badCommit, badPub, 
                                acked, idReuse, sres, fres, dres, dtoks, myCh, 
                                myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                mygen, inc >>

Snap(self) == s_cut(self) \/ s_replay(self) \/ s_claim(self)
                 \/ s_gone(self) \/ s_intent(self) \/ s_create(self)
                 \/ s_drop(self) \/ s_done(self)

f_target(self) == /\ pc[self] = "f_target"
                  /\ IF volume["F"] = "deleted"
                        THEN /\ fres' = "volume_deleted"
                             /\ pc' = [pc EXCEPT ![self] = "f_done"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "f_claim"]
                             /\ fres' = fres
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  intents, pending, stored, nextChunk, hostOps, 
                                  resv, reads, snapAtt, markedBy, badCommit, 
                                  badPub, acked, idReuse, sres, dres, dtoks, 
                                  myCh, myAtt, outc, fch, xn, xatt, run, tok, 
                                  hst, mygen, inc >>

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
                                 snapAtt, markedBy, badCommit, badPub, acked, 
                                 idReuse, sres, dres, dtoks, myCh, myAtt, outc, 
                                 xn, xatt, run, tok, hst, mygen, inc >>

f_gone(self) == /\ pc[self] = "f_gone"
                /\ IF Gone
                      THEN /\ pc' = [pc EXCEPT ![self] = "f_dropgone"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "f_vol"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, snapAtt, markedBy, badCommit, 
                                badPub, acked, idReuse, sres, fres, dres, 
                                dtoks, myCh, myAtt, outc, fch, xn, xatt, run, 
                                tok, hst, mygen, inc >>

f_vol(self) == /\ pc[self] = "f_vol"
               /\ IF volume["F"] = "none"
                     THEN /\ volume' = [volume EXCEPT !["F"] = "live"]
                          /\ volCreates' = [volCreates EXCEPT !["F"] = volCreates["F"] + 1]
                     ELSE /\ TRUE
                          /\ UNCHANGED << volume, volCreates >>
               /\ pc' = [pc EXCEPT ![self] = "f_intent"]
               /\ UNCHANGED << head, names, snaps, intents, pending, stored, 
                               nextChunk, hostOps, resv, reads, snapAtt, 
                               markedBy, badCommit, badPub, acked, idReuse, 
                               sres, fres, dres, dtoks, myCh, myAtt, outc, fch, 
                               xn, xatt, run, tok, hst, mygen, inc >>

f_intent(self) == /\ pc[self] = "f_intent"
                  /\ intents' = (intents \cup {[vol |-> "F", own |-> self, att |-> NoAtt, keys |-> fch[self]]})
                  /\ pc' = [pc EXCEPT ![self] = "f_head"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, snapAtt, markedBy, badCommit, badPub, 
                                  acked, idReuse, sres, fres, dres, dtoks, 
                                  myCh, myAtt, outc, fch, xn, xatt, run, tok, 
                                  hst, mygen, inc >>

f_head(self) == /\ pc[self] = "f_head"
                /\ IF head["F"].st = "none"
                      THEN /\ head' = [head EXCEPT !["F"] = [st |-> "active", gen |-> 1, ch |-> fch[self]]]
                           /\ fres' = "ok"
                      ELSE /\ fres' = "lost"
                           /\ head' = head
                /\ pc' = [pc EXCEPT ![self] = "f_dropintent"]
                /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, snapAtt, markedBy, badCommit, badPub, 
                                acked, idReuse, sres, dres, dtoks, myCh, myAtt, 
                                outc, fch, xn, xatt, run, tok, hst, mygen, inc >>

f_dropintent(self) == /\ pc[self] = "f_dropintent"
                      /\ intents' = {i \in intents : i.own # self}
                      /\ pc' = [pc EXCEPT ![self] = "f_dropclaim"]
                      /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                      pending, stored, nextChunk, hostOps, 
                                      resv, reads, snapAtt, markedBy, 
                                      badCommit, badPub, acked, idReuse, sres, 
                                      fres, dres, dtoks, myCh, myAtt, outc, 
                                      fch, xn, xatt, run, tok, hst, mygen, inc >>

f_dropclaim(self) == /\ pc[self] = "f_dropclaim"
                     /\ snaps' = [snaps EXCEPT ![MP].claims = snaps[MP].claims \ {"F"}]
                     /\ pc' = [pc EXCEPT ![self] = "f_done"]
                     /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                     pending, stored, nextChunk, hostOps, resv, 
                                     reads, snapAtt, markedBy, badCommit, 
                                     badPub, acked, idReuse, sres, fres, dres, 
                                     dtoks, myCh, myAtt, outc, fch, xn, xatt, 
                                     run, tok, hst, mygen, inc >>

f_dropgone(self) == /\ pc[self] = "f_dropgone"
                    /\ snaps' = [snaps EXCEPT ![MP].claims = snaps[MP].claims \ {"F"}]
                    /\ fres' = "volume_deleted"
                    /\ pc' = [pc EXCEPT ![self] = "f_done"]
                    /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                    pending, stored, nextChunk, hostOps, resv, 
                                    reads, snapAtt, markedBy, badCommit, 
                                    badPub, acked, idReuse, sres, dres, dtoks, 
                                    myCh, myAtt, outc, fch, xn, xatt, run, tok, 
                                    hst, mygen, inc >>

f_done(self) == /\ pc[self] = "f_done"
                /\ reads' = [reads EXCEPT ![self] = {}]
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, snapAtt, markedBy, badCommit, badPub, 
                                acked, idReuse, sres, fres, dres, dtoks, myCh, 
                                myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                mygen, inc >>

Fork(self) == f_target(self) \/ f_claim(self) \/ f_gone(self)
                 \/ f_vol(self) \/ f_intent(self) \/ f_head(self)
                 \/ f_dropintent(self) \/ f_dropclaim(self)
                 \/ f_dropgone(self) \/ f_done(self)

x_pick(self) == /\ pc[self] = "x_pick"
                /\ xn' = [xn EXCEPT ![self] = IF self = "xM" THEN "M" ELSE "N"]
                /\ pc' = [pc EXCEPT ![self] = "x_resolve"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, snapAtt, markedBy, badCommit, 
                                badPub, acked, idReuse, sres, fres, dres, 
                                dtoks, myCh, myAtt, outc, fch, xatt, run, tok, 
                                hst, mygen, inc >>

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
                                   hostOps, resv, reads, snapAtt, markedBy, 
                                   badCommit, badPub, acked, idReuse, sres, 
                                   fres, dres, dtoks, myCh, myAtt, outc, fch, 
                                   xn, run, tok, hst, mygen, inc >>

x_tomb(self) == /\ pc[self] = "x_tomb"
                /\ IF snaps[<<xn[self], xatt[self]>>].st = "none"
                      THEN /\ snaps' = [snaps EXCEPT ![<<xn[self], xatt[self]>>] = [EmptySnap EXCEPT !.st = "aborted"]]
                      ELSE /\ TRUE
                           /\ snaps' = snaps
                /\ pc' = [pc EXCEPT ![self] = "x_resolve"]
                /\ UNCHANGED << head, volume, volCreates, names, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, snapAtt, markedBy, badCommit, badPub, 
                                acked, idReuse, sres, fres, dres, dtoks, myCh, 
                                myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                mygen, inc >>

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
                                reads, snapAtt, badCommit, badPub, acked, 
                                idReuse, sres, fres, dres, dtoks, myCh, myAtt, 
                                outc, fch, xn, xatt, run, tok, hst, mygen, inc >>

x_free(self) == /\ pc[self] = "x_free"
                /\ IF names[xn[self]] = xatt[self]
                      THEN /\ names' = [names EXCEPT ![xn[self]] = NoAtt]
                      ELSE /\ TRUE
                           /\ names' = names
                /\ pc' = [pc EXCEPT ![self] = "x_done"]
                /\ UNCHANGED << head, volume, volCreates, snaps, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, snapAtt, markedBy, badCommit, badPub, 
                                acked, idReuse, sres, fres, dres, dtoks, myCh, 
                                myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                mygen, inc >>

x_done(self) == /\ pc[self] = "x_done"
                /\ TRUE
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, snapAtt, markedBy, badCommit, 
                                badPub, acked, idReuse, sres, fres, dres, 
                                dtoks, myCh, myAtt, outc, fch, xn, xatt, run, 
                                tok, hst, mygen, inc >>

DelSnap(self) == x_pick(self) \/ x_resolve(self) \/ x_tomb(self)
                    \/ x_mark(self) \/ x_free(self) \/ x_done(self)

c_create(self) == /\ pc[self] = "c_create"
                  /\ IF resv.st = "none"
                        THEN /\ idReuse' = (idReuse \/ resv.st # "none")
                        ELSE /\ TRUE
                             /\ UNCHANGED idReuse
                  /\ pc' = [pc EXCEPT ![self] = "Done"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  intents, pending, stored, nextChunk, hostOps, 
                                  resv, reads, snapAtt, markedBy, badCommit, 
                                  badPub, acked, sres, fres, dres, dtoks, myCh, 
                                  myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                  mygen, inc >>

Create(self) == c_create(self)

d_read(self) == /\ pc[self] = "d_read"
                /\ hst' = [hst EXCEPT ![self] = head["P"].st]
                /\ inc' = [inc EXCEPT ![self] = 0]
                /\ tok' = [tok EXCEPT ![self] = self]
                /\ dtoks' = [dtoks EXCEPT ![self] = {self}]
                /\ pc' = [pc EXCEPT ![self] = "d_admit"]
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, snapAtt, markedBy, badCommit, 
                                badPub, acked, idReuse, sres, fres, dres, myCh, 
                                myAtt, outc, fch, xn, xatt, run, mygen >>

d_admit(self) == /\ pc[self] = "d_admit"
                 /\ IF head["P"].st = "held"
                       THEN /\ dres' = [dres EXCEPT ![self] = "not_admitted"]
                            /\ pc' = [pc EXCEPT ![self] = "d_end"]
                            /\ resv' = resv
                       ELSE /\ resv' = [st |-> IF resv.st = "committed" THEN "committed" ELSE "admitted",
                                        toks |-> resv.toks \cup {tok[self]}]
                            /\ \/ /\ TRUE
                                  /\ pc' = [pc EXCEPT ![self] = "d_loop"]
                                  /\ dres' = dres
                               \/ /\ LostReply
                                  /\ dres' = [dres EXCEPT ![self] = "unknown"]
                                  /\ pc' = [pc EXCEPT ![self] = "d_end"]
                 /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                 intents, pending, stored, nextChunk, hostOps, 
                                 reads, snapAtt, markedBy, badCommit, badPub, 
                                 acked, idReuse, sres, fres, dtoks, myCh, 
                                 myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                 mygen, inc >>

d_loop(self) == /\ pc[self] = "d_loop"
                /\ IF hst[self] \in {"purging", "deleted"}
                      THEN /\ pc' = [pc EXCEPT ![self] = "d_admcommit"]
                           /\ dres' = dres
                      ELSE /\ IF inc[self] = MaxIncarnations
                                 THEN /\ dres' = [dres EXCEPT ![self] = "error"]
                                      /\ pc' = [pc EXCEPT ![self] = "d_end"]
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "d_mark"]
                                      /\ dres' = dres
                /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                intents, pending, stored, nextChunk, hostOps, 
                                resv, reads, snapAtt, markedBy, badCommit, 
                                badPub, acked, idReuse, sres, fres, dtoks, 
                                myCh, myAtt, outc, fch, xn, xatt, run, tok, 
                                hst, mygen, inc >>

d_mark(self) == /\ pc[self] = "d_mark"
                /\ IF head["P"].st \in {"deleting", "purging", "deleted"}
                      THEN /\ mygen' = [mygen EXCEPT ![self] = head["P"].gen]
                           /\ pc' = [pc EXCEPT ![self] = "d_check"]
                           /\ UNCHANGED << head, dres >>
                      ELSE /\ IF head["P"].st = "held"
                                 THEN /\ dres' = [dres EXCEPT ![self] = "in_use"]
                                      /\ pc' = [pc EXCEPT ![self] = "d_abort"]
                                      /\ UNCHANGED << head, mygen >>
                                 ELSE /\ mygen' = [mygen EXCEPT ![self] = head["P"].gen + 1]
                                      /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.st = "deleting", !.gen = @ + 1]]
                                      /\ pc' = [pc EXCEPT ![self] = "d_check"]
                                      /\ dres' = dres
                /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                pending, stored, nextChunk, hostOps, resv, 
                                reads, snapAtt, markedBy, badCommit, badPub, 
                                acked, idReuse, sres, fres, dtoks, myCh, myAtt, 
                                outc, fch, xn, xatt, run, tok, hst, inc >>

d_check(self) == /\ pc[self] = "d_check"
                 /\ IF \E n \in Names : names[n] # NoAtt
                       THEN /\ pc' = [pc EXCEPT ![self] = "d_revert"]
                       ELSE /\ pc' = [pc EXCEPT ![self] = "d_commit"]
                 /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                 intents, pending, stored, nextChunk, hostOps, 
                                 resv, reads, snapAtt, markedBy, badCommit, 
                                 badPub, acked, idReuse, sres, fres, dres, 
                                 dtoks, myCh, myAtt, outc, fch, xn, xatt, run, 
                                 tok, hst, mygen, inc >>

d_revert(self) == /\ pc[self] = "d_revert"
                  /\ IF head["P"].st \in {"purging", "deleted"}
                        THEN /\ dres' = [dres EXCEPT ![self] = "unknown"]
                             /\ pc' = [pc EXCEPT ![self] = "d_end"]
                             /\ head' = head
                        ELSE /\ IF head["P"].st # "deleting"
                                   THEN /\ dres' = [dres EXCEPT ![self] = "refused_snapshots"]
                                        /\ pc' = [pc EXCEPT ![self] = "d_abort"]
                                        /\ head' = head
                                   ELSE /\ IF head["P"].gen # mygen[self]
                                              THEN /\ pc' = [pc EXCEPT ![self] = "d_reread"]
                                                   /\ UNCHANGED << head, dres >>
                                              ELSE /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.st = "detached", !.gen = @ + 1]]
                                                   /\ dres' = [dres EXCEPT ![self] = "refused_snapshots"]
                                                   /\ pc' = [pc EXCEPT ![self] = "d_abort"]
                  /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, snapAtt, markedBy, badCommit, badPub, 
                                  acked, idReuse, sres, fres, dtoks, myCh, 
                                  myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                  mygen, inc >>

d_commit(self) == /\ pc[self] = "d_commit"
                  /\ IF head["P"].st \in {"purging", "deleted"}
                        THEN /\ pc' = [pc EXCEPT ![self] = "d_admcommit"]
                             /\ UNCHANGED << head, badCommit, dres >>
                        ELSE /\ IF head["P"].st # "deleting"
                                   THEN /\ dres' = [dres EXCEPT ![self] = "givenback_by_other"]
                                        /\ pc' = [pc EXCEPT ![self] = "d_abort"]
                                        /\ UNCHANGED << head, badCommit >>
                                   ELSE /\ IF head["P"].gen # mygen[self]
                                              THEN /\ pc' = [pc EXCEPT ![self] = "d_reread"]
                                                   /\ UNCHANGED << head, 
                                                                   badCommit >>
                                              ELSE /\ head' = [head EXCEPT !["P"] = [head["P"] EXCEPT !.st = "purging", !.gen = @ + 1]]
                                                   /\ badCommit' = (badCommit \/ CommitUnsafe)
                                                   /\ pc' = [pc EXCEPT ![self] = "d_admcommit"]
                                        /\ dres' = dres
                  /\ UNCHANGED << volume, volCreates, names, snaps, intents, 
                                  pending, stored, nextChunk, hostOps, resv, 
                                  reads, snapAtt, markedBy, badPub, acked, 
                                  idReuse, sres, fres, dtoks, myCh, myAtt, 
                                  outc, fch, xn, xatt, run, tok, hst, mygen, 
                                  inc >>

d_reread(self) == /\ pc[self] = "d_reread"
                  /\ hst' = [hst EXCEPT ![self] = head["P"].st]
                  /\ inc' = [inc EXCEPT ![self] = inc[self] + 1]
                  /\ pc' = [pc EXCEPT ![self] = "d_loop"]
                  /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                  intents, pending, stored, nextChunk, hostOps, 
                                  resv, reads, snapAtt, markedBy, badCommit, 
                                  badPub, acked, idReuse, sres, fres, dres, 
                                  dtoks, myCh, myAtt, outc, fch, xn, xatt, run, 
                                  tok, mygen >>

d_admcommit(self) == /\ pc[self] = "d_admcommit"
                     /\ resv' = [resv EXCEPT !.st = "committed"]
                     /\ pc' = [pc EXCEPT ![self] = "d_finish"]
                     /\ UNCHANGED << head, volume, volCreates, names, snaps, 
                                     intents, pending, stored, nextChunk, 
                                     hostOps, reads, snapAtt, markedBy, 
                                     badCommit, badPub, acked, idReuse, sres, 
                                     fres, dres, dtoks, myCh, myAtt, outc, fch, 
                                     xn, xatt, run, tok, hst, mygen, inc >>

d_finish(self) == /\ pc[self] = "d_finish"
                  /\ IF head["P"].st = "purging"
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
                                  reads, snapAtt, markedBy, badCommit, badPub, 
                                  acked, idReuse, sres, fres, dtoks, myCh, 
                                  myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                  mygen, inc >>

d_volmark(self) == /\ pc[self] = "d_volmark"
                   /\ volume' = [volume EXCEPT !["P"] = "deleted"]
                   /\ dres' = [dres EXCEPT ![self] = "completed"]
                   /\ pc' = [pc EXCEPT ![self] = "d_end"]
                   /\ UNCHANGED << head, volCreates, names, snaps, intents, 
                                   pending, stored, nextChunk, hostOps, resv, 
                                   reads, snapAtt, markedBy, badCommit, badPub, 
                                   acked, idReuse, sres, fres, dtoks, myCh, 
                                   myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                   mygen, inc >>

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
                                 reads, snapAtt, markedBy, badCommit, badPub, 
                                 acked, idReuse, sres, fres, dtoks, myCh, 
                                 myAtt, outc, fch, xn, xatt, run, tok, hst, 
                                 mygen, inc >>

d_end(self) == /\ pc[self] = "d_end"
               /\ IF dres[self] = "unknown" /\ run[self] < MaxRuns
                     THEN /\ run' = [run EXCEPT ![self] = run[self] + 1]
                          /\ pc' = [pc EXCEPT ![self] = "d_read"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ run' = run
               /\ UNCHANGED << head, volume, volCreates, names, snaps, intents, 
                               pending, stored, nextChunk, hostOps, resv, 
                               reads, snapAtt, markedBy, badCommit, badPub, 
                               acked, idReuse, sres, fres, dres, dtoks, myCh, 
                               myAtt, outc, fch, xn, xatt, tok, hst, mygen, 
                               inc >>

Del(self) == d_read(self) \/ d_admit(self) \/ d_loop(self) \/ d_mark(self)
                \/ d_check(self) \/ d_revert(self) \/ d_commit(self)
                \/ d_reread(self) \/ d_admcommit(self) \/ d_finish(self)
                \/ d_volmark(self) \/ d_abort(self) \/ d_end(self)

Next == Host \/ GC
           \/ (\E self \in SnapCallers: Snap(self))
           \/ (\E self \in ForkIds: Fork(self))
           \/ (\E self \in DelSnapIds: DelSnap(self))
           \/ (\E self \in CreateIds: Create(self))
           \/ (\E self \in Deleters: Del(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

-----------------------------------------------------------------------------
TypeOK ==
    /\ \A v \in Vols : head[v].st \in {"none", "held", "detached", "active", "deleting", "purging", "deleted", "aborted"}
    /\ stored \subseteq Chunks
    /\ resv.st \in {"none", "admitted", "committed"}
    /\ \A p \in AttPaths : snaps[p].st \in {"none", "active", "deleting", "aborted"}

GCSafe ==
    (UNION {head[v].ch : v \in Vols}
       \cup UNION {snaps[p].ch : p \in {q \in AttPaths : snaps[q].st = "active"}}
       \cup UNION {i.keys : i \in intents}) \subseteq stored

\* a published snapshot is marked deleting only by an explicit DeleteSnapshot of it
SnapOnlyExplicitDelete == \A p \in AttPaths : snaps[p].st = "deleting" => markedBy[p] = "explicit"

\* an acked snapshot keeps its chunks unless it was explicitly deleted
AckedChunksSurvive == \A p \in acked : markedBy[p] = "explicit" \/ snaps[p].ch \subseteq stored

\* volume delete never commits while a fork claim, snapshot or in-flight attempt exists
CommitClean == ~badCommit
\* and no snapshot of P is published after the commit (a caller may still
\* receive the reply of a publication that happened before it)
NoPublishAfterCommit == ~badPub

NoIdReuse == (\A v \in Vols : volCreates[v] <= 1) /\ ~idReuse

GiveBackReleases ==
    \A d \in Deleters : (pc[d] = "Done" /\ dres[d] \in GaveBack) => dtoks[d] \cap resv.toks = {}

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

\* N3: a snapshot attempt refused at its gate leaves an unpublished name record that
\* blocks a later delete with volume_has_snapshots (expected to be violated)
W_NoHiddenNameBlock == ~(/\ \E s \in SnapCallers : sres[s] = "volume_deleted"
                         /\ names["M"] = NoAtt /\ names["N"] # NoAtt
                         /\ snaps[<<"N", names["N"]>>].st = "none"
                         /\ \E d \in Deleters : dres[d] = "refused_snapshots" /\ pc[d] = "Done"
                         /\ head["P"].st \in {"held", "detached"})

Symm == Permutations(SnapCallers) \cup Permutations(Deleters)
=============================================================================
