-------------------------- MODULE WorkspaceTakeover --------------------------
(***************************************************************************)
(* S2: epoch, fencing and takeover of one sw-block workspace volume moving *)
(* from host A (epoch 1) to host B, by a CLEAN move (detach, revoke, ACK,  *)
(* assign) or a FORCED move (lease expired, assign at a higher epoch).     *)
(*                                                                         *)
(* Plain TLA+. One action = one remote call or one local decision. A       *)
(* discrete global clock `now` is the blockmaster's (and the real) time;   *)
(* each host's clock may lag it by at most Drift ticks (bounded rate       *)
(* drift). Messages live in a set: delayed, reordered or lost.             *)
(* Code at seaweed-block main 5c293821; mapping in README.md (S2).        *)
(*                                                                         *)
(* #205, durable lease horizon (PR #209): lease.exp is the PUBLISHED       *)
(* horizon (the blockmaster's in-memory copy of the durable record), dhz   *)
(* the horizon on DISK, deadline the latest grant (memory only). A renewal *)
(* of a live lease writes nothing: grant = min(now + TTL, lease.exp). An   *)
(* extension is an ordinary serialized writer that raises the horizon to   *)
(* trigger + TTL + HSlack; its write may fail without landing or fail      *)
(* after landing (unknown outcome). Any failed write latches `uncertain`:  *)
(* renewals and other writes are refused until a restart reloads the disk. *)
(* Nondecreasing `now` (Tick/SlowTick only advance it) is an ASSUMPTION of *)
(* this model, not a property of the deployment's clocks.                  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Kinds,          \* move kinds the operator may use: subset of {"clean", "forced"}
    MaxT,           \* clock bound
    TTL,            \* workspace lease TTL (ticks)
    ITTL,           \* GC intent_ttl (ticks)
    MaxLSN,         \* VM writes per host bound (as LSN)
    MaxCuts,        \* cuts per host bound
    Drift,          \* max ticks a host clock may fall behind the master clock
    MaxRestarts,    \* blockmaster restarts
    LeaseOn,        \* --workspace-lease guard on the hosts (TRUE for workspace volumes)
    \* ---- guards (TRUE = as in the code); RED variants turn one off ----
    LsnGuard,       \* CASHead refuses next.remote_lsn < current            chunk_remote.go:404
    StoreEpochGuard,\* CASHead refuses next.epoch < expected.epoch          chunk_remote.go:391
    HostFence,      \* layer fences on a HEAD of a newer epoch (supersededBy) chunk_layer.go:916,972,1032
    LayerLsnCheck,  \* layer refuses a cut below HEAD.remote_lsn (lsn_behind) chunk_layer.go:800
    SeedFromObserved,\* B seeds its WAL from the HEAD its takeover CAS protects  chunk_restore.go:38-43
    GCEpochRule,    \* GC expires an intent only when the remote HEAD epoch > intent epoch gc.go:221
    WaitLeaseExpiry,\* forced move waits for the lease to expire            authority/workspace.go:314-318
    Clamp,          \* #205: a renewal's grant is clamped to the published horizon (FALSE: unsafe memory-only renewal)
    ExtFail         \* #205: an extension write may fail, landed or not (unknown outcome -> uncertain)

Hosts == {"A", "B"}
NoRec == [gen |-> 0, epoch |-> 0, holder |-> "-", st |-> "-", dgen |-> 0, lsn |-> 0, img |-> <<"-", 0>>]
Head0 == [gen |-> 1, epoch |-> 1, holder |-> "A", st |-> "active", dgen |-> 0, lsn |-> 0, img |-> <<"A", 0>>]
Other(h) == IF h = "A" THEN "B" ELSE "A"
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
\* #205 horizon geometry (workspace_lease_view.go): an extension writes H = trigger + TTL + HSlack; a renewal
\* triggers one when the published horizon is closer than TTL + Margin. Code: TTL/2 and TTL/4; at least one tick here.
HSlack == Max(1, TTL \div 2)
Margin == Max(1, TTL \div 4)

VARIABLES
    now, lag,            \* clocks
    head, stored, intents,   \* remote store: HEAD, chunk bodies, intents
    msgs,                \* in-flight messages
    asg, lease, ack, journal, maxEpoch,   \* blockmaster published state (= the authority store unless uncertain)
    deadline, restarts,  \* blockmaster in-memory lease deadline (the latest grant; reset on restart)
    dhz, uncertain, extReq, \* #205: horizon on disk; a write outcome is unknown; pending extension trigger time (-1: none)
    mv,                  \* operator's move in progress
    host,                \* per-host state
    cut,                 \* per-host chunk-layer cut in progress
    bpc, bobs,           \* B's attach sequence and the HEAD it observed
    hist                 \* history (never read by the protocol)

vars == <<now, lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch,
          deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bpc, bobs, hist>>

HNow(h) == now - lag[h]

\* the volume server admits I/O and cuts (WorkspaceLeaseGuard.permits, workspace_lease.go:57-65)
Permits(h) == /\ host[h].epoch > 0
              /\ ~host[h].fenced
              /\ (LeaseOn => host[h].until > HNow(h))
Serving(h) == Permits(h) /\ host[h].ready /\ ~host[h].stopIO /\ ~host[h].detached

\* chunk_layer.go:1032 supersededBy
SupersededBy(h, cur) == cur.epoch > host[h].epoch \/ (cur.epoch = host[h].epoch /\ cur.holder # h /\ cur.gen # 0)
Fences(h, cur) == HostFence /\ SupersededBy(h, cur)

\* CASHead (chunk_remote.go:388-425): conditional on {gen, epoch}; refuses lower epoch / lower LSN
CasOK(base, next) == /\ head.gen = base.gen /\ head.epoch = base.epoch
                     /\ (StoreEpochGuard => next.epoch >= base.epoch)
                     /\ (LsnGuard => next.lsn >= head.lsn)

Init ==
    /\ now = 0 /\ lag = [h \in Hosts |-> 0]
    /\ head = Head0 /\ stored = {<<"A", 0>>} /\ intents = {}
    /\ msgs = {}
    /\ asg = [holder |-> "A", epoch |-> 1]
    \* A's first grant: horizon now + TTL + HSlack on disk, grant (deadline, A's validity) now + TTL
    /\ lease = [holder |-> "A", epoch |-> 1, exp |-> TTL + HSlack, revoking |-> FALSE, none |-> FALSE]
    /\ ack = [gen |-> 0, holder |-> "-", epoch |-> 0]
    /\ journal = [kind |-> "-", gen |-> 0, lsn |-> 0, newEpoch |-> 0]
    /\ maxEpoch = 1
    /\ deadline = TTL /\ restarts = 0
    /\ dhz = TTL + HSlack /\ uncertain = FALSE /\ extReq = -1
    /\ mv = [st |-> "none", kind |-> "-", h |-> NoRec]
    /\ host = [h \in Hosts |-> IF h = "A"
                 THEN [epoch |-> 1, until |-> TTL, fenced |-> FALSE, revoked |-> FALSE, stopIO |-> FALSE,
                       ready |-> TRUE, detached |-> FALSE, lsn |-> 0, lh |-> Head0, renewing |-> FALSE]
                 ELSE [epoch |-> 0, until |-> 0, fenced |-> FALSE, revoked |-> FALSE, stopIO |-> FALSE,
                       ready |-> FALSE, detached |-> FALSE, lsn |-> 0, lh |-> NoRec, renewing |-> FALSE]]
    /\ cut = [h \in Hosts |-> [pc |-> "idle", n |-> 0, base |-> NoRec, lsn |-> 0, img |-> <<"-", 0>>, detach |-> FALSE]]
    /\ bpc = "wait" /\ bobs = NoRec
    /\ hist = [takeover |-> FALSE, aCasAfter |-> FALSE, late |-> {}, detHead |-> NoRec, bFirst |-> NoRec,
               badExpire |-> FALSE, acked |-> [h \in Hosts |-> 0], bSeed |-> -1, steps |-> <<>>,
               aStopT |-> -1, bServeT |-> -1]

-----------------------------------------------------------------------------
(* clocks and the VM *)

Tick == /\ now < MaxT /\ now' = now + 1
        /\ UNCHANGED <<lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bpc, bobs, hist>>
\* the master clock advances, A's does not (bounded drift)
SlowTick == /\ now < MaxT /\ lag["A"] < Drift
            /\ now' = now + 1 /\ lag' = [lag EXCEPT !["A"] = @ + 1]
            /\ UNCHANGED <<head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bpc, bobs, hist>>

\* a VM write accepted by a serving host (WAL LSN + 1)
VMWrite(h) == /\ Serving(h) /\ host[h].lsn < MaxLSN
              /\ host' = [host EXCEPT ![h].lsn = @ + 1]
              /\ hist' = [hist EXCEPT !.acked[h] = host[h].lsn + 1,
                                      !.bServeT = IF h = "B" /\ hist.bServeT < 0 THEN now ELSE hist.bServeT]
              /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, cut, bpc, bobs>>

LoseMsg == \E m \in msgs : /\ msgs' = msgs \ {m}
                           /\ host' = IF m.t = "renew" \/ m.t = "lease" THEN [host EXCEPT ![m.h].renewing = FALSE] ELSE host
                           /\ UNCHANGED <<now, lag, head, stored, intents, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, cut, bpc, bobs, hist>>

-----------------------------------------------------------------------------
(* workspace lease (core/host/volume/workspace_lease.go, core/authority/workspace_lease_view.go) *)

\* Refresh: validity anchored to the request START (workspace_lease.go:68-127)
SendRenew(h) ==
    /\ LeaseOn /\ host[h].epoch > 0 /\ ~host[h].fenced /\ ~host[h].renewing
    /\ IF host[h].until > 0 /\ HNow(h) >= host[h].until
       THEN /\ host' = [host EXCEPT ![h].fenced = TRUE]          \* expired: self-fence (:73-80)
            /\ msgs' = msgs
       ELSE /\ host' = [host EXCEPT ![h].renewing = TRUE]
            /\ msgs' = msgs \cup {[t |-> "renew", h |-> h, e |-> host[h].epoch, s |-> HNow(h)]}
    /\ UNCHANGED <<now, lag, head, stored, intents, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, cut, bpc, bobs, hist>>

\* Publisher.RenewWorkspaceLease (#205, workspace_lease_view.go), reply validFor = expiry - now (workspace_service.go leaseFact).
\*  - Revoking: the drain reply, expiry min(horizon, deadline); never extends (allowed while uncertain, as before).
\*  - No lease yet: the FIRST grant, a synchronous durable write (refused while uncertain): horizon now + TTL + HSlack.
\*  - Live lease: memory only. grant = min(now + TTL, horizon) (Clamp); refused while uncertain or when it leaves no
\*    validity; deadline = max(deadline, grant); triggers an extension when the horizon is closer than TTL + Margin.
MasterRenew == \E m \in msgs :
    /\ m.t = "renew"
    /\ LET okLine == asg.holder = m.h /\ asg.epoch = m.e
           Reply(ok, v, rv) == [t |-> "lease", h |-> m.h, s |-> m.s, ok |-> ok, valid |-> v, revoking |-> rv]
           grant == IF Clamp THEN Min(now + TTL, lease.exp) ELSE now + TTL
       IN IF okLine /\ ~lease.none /\ lease.revoking
          THEN /\ msgs' = (msgs \ {m}) \cup {Reply(TRUE, Min(lease.exp, deadline) - now, TRUE)}
               /\ UNCHANGED <<lease, deadline, dhz, extReq>>
          ELSE IF okLine /\ lease.none /\ ~uncertain
          THEN /\ lease' = [holder |-> m.h, epoch |-> m.e, exp |-> now + TTL + HSlack, revoking |-> FALSE, none |-> FALSE]
               /\ dhz' = now + TTL + HSlack
               /\ deadline' = now + TTL
               /\ msgs' = (msgs \ {m}) \cup {Reply(TRUE, TTL, FALSE)}
               /\ extReq' = extReq
          ELSE IF okLine /\ ~lease.none /\ now < lease.exp /\ now < deadline /\ ~uncertain /\ grant > now
          THEN /\ deadline' = Max(deadline, grant)
               /\ extReq' = IF lease.exp < now + TTL + Margin /\ extReq < 0 THEN now ELSE extReq
               /\ msgs' = (msgs \ {m}) \cup {Reply(TRUE, grant - now, FALSE)}
               /\ UNCHANGED <<lease, dhz>>
          ELSE /\ msgs' = (msgs \ {m}) \cup {Reply(FALSE, 0, FALSE)}
               /\ UNCHANGED <<lease, deadline, dhz, extReq>>
    /\ UNCHANGED <<now, lag, head, stored, intents, asg, ack, journal, maxEpoch, restarts, uncertain, mv, host, cut, bpc, bobs, hist>>

\* the extender (#205, workspace_lease_view.go extendWorkspaceLease): an ordinary writer serialized with every other
\* write; it rechecks the lease and writes H = trigger + TTL + HSlack. Success publishes H. A failed write latches
\* uncertain whether or not it landed on disk (ExtFail). A failed recheck is a known no-write.
MasterExtend ==
    /\ extReq >= 0
    /\ LET H == extReq + TTL + HSlack
           ok == /\ ~lease.none /\ ~lease.revoking /\ ~uncertain
                 /\ lease.holder = asg.holder /\ lease.epoch = asg.epoch /\ H > lease.exp
       IN IF ok
          THEN \/ /\ lease' = [lease EXCEPT !.exp = H] /\ dhz' = H /\ uncertain' = uncertain
               \/ /\ ExtFail /\ lease' = lease /\ dhz' = dhz /\ uncertain' = TRUE     \* failed, nothing landed
               \/ /\ ExtFail /\ lease' = lease /\ dhz' = H /\ uncertain' = TRUE       \* landed, reply lost
          ELSE UNCHANGED <<lease, dhz, uncertain>>
    /\ extReq' = -1
    /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, ack, journal, maxEpoch, deadline, restarts, mv, host, cut, bpc, bobs, hist>>

\* the host applies a reply (workspace_lease.go:88-127): the OLD deadline is checked first
HostLease == \E m \in msgs :
    /\ m.t = "lease"
    /\ LET h == m.h
           t == HNow(h)
           expiredOld == host[h].until > 0 /\ t >= host[h].until
           fence == expiredOld \/ (m.ok /\ t >= m.s + m.valid) \/ m.revoking
       IN host' = [host EXCEPT ![h].renewing = FALSE,
                               ![h].fenced = @ \/ fence,
                               ![h].revoked = @ \/ m.revoking,
                               ![h].until = IF m.ok /\ ~fence /\ ~host[h].fenced THEN m.s + m.valid ELSE @]
    /\ msgs' = msgs \ {m}
    /\ hist' = [hist EXCEPT !.aStopT = IF m.h = "A" /\ hist.aStopT < 0 /\ m.revoking THEN now ELSE hist.aStopT]
    /\ UNCHANGED <<now, lag, head, stored, intents, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, cut, bpc, bobs>>

\* drain then clean ACK with the last committed HEAD gen, only while the lease is valid (:129-160)
SendAck ==
    /\ host["A"].revoked /\ cut["A"].pc = "idle"
    /\ host["A"].until > HNow("A") /\ host["A"].lh.gen # 0
    /\ ~\E m \in msgs : m.t = "ack"
    /\ msgs' = msgs \cup {[t |-> "ack", h |-> "A", e |-> host["A"].epoch, gen |-> host["A"].lh.gen]}
    /\ UNCHANGED <<now, lag, head, stored, intents, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bpc, bobs, hist>>

\* Publisher.AckWorkspaceRevocation (authority/workspace.go:215-235): a durable write (refused while uncertain)
MasterAck == \E m \in msgs :
    /\ m.t = "ack"
    /\ msgs' = msgs \ {m}
    /\ IF asg.holder = m.h /\ asg.epoch = m.e /\ m.gen # 0 /\ ~lease.none /\ lease.revoking
          /\ now < lease.exp /\ now < deadline /\ ~uncertain
       THEN ack' = [gen |-> m.gen, holder |-> m.h, epoch |-> m.e]
       ELSE ack' = ack
    /\ UNCHANGED <<now, lag, head, stored, intents, asg, lease, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bpc, bobs, hist>>

\* operator: BeginWorkspaceRevocation (authority/workspace.go:200-213): a durable write (refused while uncertain)
Revoke == /\ "clean" \in Kinds /\ ~lease.none /\ ~lease.revoking /\ lease.holder = asg.holder /\ asg.holder = "A"
          /\ ~uncertain
          /\ lease' = [lease EXCEPT !.revoking = TRUE]
          /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bpc, bobs, hist>>

\* operator / orchestrator: stop guest I/O on A before the final cut
StopGuest == /\ "clean" \in Kinds /\ ~host["A"].stopIO
             /\ host' = [host EXCEPT !["A"].stopIO = TRUE]
             /\ hist' = [hist EXCEPT !.aStopT = IF hist.aStopT < 0 THEN now ELSE hist.aStopT]
             /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, cut, bpc, bobs>>

\* blockmaster restart: the published state is reloaded from disk (so the published horizon becomes dhz), the
\* in-memory deadline conservatively covers a full TTL again (authority.go reloadFromStore), uncertainty and any
\* pending extension are gone
MasterRestart == /\ restarts < MaxRestarts
                 /\ restarts' = restarts + 1
                 /\ deadline' = now + TTL
                 /\ lease' = IF lease.none THEN lease ELSE [lease EXCEPT !.exp = dhz]
                 /\ uncertain' = FALSE /\ extReq' = -1
                 /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, ack, journal, maxEpoch, dhz, mv, host, cut, bpc, bobs, hist>>

-----------------------------------------------------------------------------
(* the move (workspace_service.go:100-145, authority/workspace.go:157-198, 287-335) *)

\* the HEAD is read before the publisher lock (workspace_service.go:104-109)
MoveRead == /\ mv.st = "none" /\ asg.holder = "A"
            /\ \E k \in Kinds : mv' = [st |-> "read", kind |-> k, h |-> head]
            /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, host, cut, bpc, bobs, hist>>

\* a durable write (refused while uncertain, authority.go apply); the forced barrier reads the published horizon
\* and the in-memory deadline
MovePrepare ==
    /\ mv.st = "read"
    /\ LET q == mv.h
           headOK == q.gen # 0 /\ q.epoch = asg.epoch /\ q.holder = asg.holder
           cleanOK == q.st = "detached" /\ ~lease.none /\ lease.revoking /\ ack.gen = q.gen /\ ack.epoch = asg.epoch
           forcedOK == ~WaitLeaseExpiry \/ (now >= lease.exp /\ now >= deadline)
           ok == headOK /\ ~lease.none /\ ~uncertain /\ IF mv.kind = "clean" THEN cleanOK ELSE forcedOK
       IN IF ok
          THEN /\ maxEpoch' = maxEpoch + 1
                /\ journal' = [kind |-> mv.kind, gen |-> q.gen, lsn |-> q.lsn, newEpoch |-> maxEpoch + 1]
                /\ asg' = [holder |-> "B", epoch |-> maxEpoch + 1]
                /\ lease' = [lease EXCEPT !.none = TRUE]
                /\ ack' = [gen |-> 0, holder |-> "-", epoch |-> 0]
                /\ msgs' = msgs \cup {[t |-> "assign", h |-> "B", e |-> maxEpoch + 1]}
                /\ mv' = [mv EXCEPT !.st = "done"]
          ELSE /\ mv' = [st |-> "none", kind |-> "-", h |-> NoRec]       \* refused; may be retried
                /\ UNCHANGED <<maxEpoch, journal, asg, lease, ack, msgs>>
    /\ UNCHANGED <<now, lag, head, stored, intents, deadline, restarts, dhz, uncertain, extReq, host, cut, bpc, bobs, hist>>

-----------------------------------------------------------------------------
(* B: VOLUME hook -> attach -> seed -> takeover CAS -> serve
   (provider_chunk.go:229-317, chunk_restore.go:13-80) *)

Step(s) == hist' = [hist EXCEPT !.steps = Append(hist.steps, s)]

BAssign == \E m \in msgs :
    /\ m.t = "assign" /\ bpc = "wait"
    /\ msgs' = msgs \ {m}
    /\ host' = [host EXCEPT !["B"].epoch = m.e]
    /\ bpc' = "hook"
    /\ UNCHANGED <<now, lag, head, stored, intents, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, cut, bobs, hist>>

BHook == /\ bpc = "hook" /\ bpc' = "attach" /\ Step("hook")       \* EnsureVolume / attach check (provider_chunk.go:240-247)
         /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bobs>>

BAttach == /\ bpc = "attach"                                    \* ClaimChunkHead: GetHead (chunk_restore.go:30-36)
           /\ IF head.epoch > host["B"].epoch
              THEN /\ bpc' = "failed" /\ bobs' = bobs /\ hist' = hist
              ELSE /\ bpc' = "seed" /\ bobs' = head /\ Step("attach")
           /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut>>

BSeed == /\ bpc = "seed"                                        \* PrepareRestore -> SeedChunkFrontier (provider_chunk.go:286-307)
         /\ LET L == IF SeedFromObserved THEN bobs.lsn ELSE journal.lsn
            IN /\ host' = [host EXCEPT !["B"].lsn = L]
               /\ hist' = [hist EXCEPT !.steps = Append(hist.steps, "seed"), !.bSeed = L]
         /\ bpc' = "claim"
         /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, cut, bobs>>

BClaim == /\ bpc = "claim"                                      \* the takeover CAS (chunk_restore.go:55-79)
          /\ LET next == [bobs EXCEPT !.gen = bobs.gen + 1, !.epoch = host["B"].epoch, !.holder = "B",
                                       !.st = "active", !.dgen = 0]
             IN IF CasOK(bobs, next)
                THEN /\ head' = next
                     /\ host' = [host EXCEPT !["B"].lh = next, !["B"].ready = TRUE]
                     /\ bpc' = "claimed"
                     /\ hist' = [hist EXCEPT !.steps = Append(hist.steps, "claim"), !.takeover = TRUE, !.bFirst = next]
                ELSE /\ bpc' = "attach"                         \* not installed: the attach is retried
                     /\ UNCHANGED <<head, host, hist>>
          /\ UNCHANGED <<now, lag, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, cut, bobs>>

-----------------------------------------------------------------------------
(* the chunk layer's cut on host h (chunk_layer.go:680-988) *)

CutStart(h, det) ==
    /\ cut[h].pc = "idle" /\ cut[h].n < MaxCuts
    /\ host[h].ready /\ Permits(h) /\ ~host[h].detached               \* admit (:599-619, :695)
    /\ ~SupersededBy(h, host[h].lh) \/ ~HostFence
    /\ IF det THEN h = "A" /\ host[h].stopIO /\ "clean" \in Kinds
              ELSE host[h].lsn > host[h].lh.lsn \/ (~LayerLsnCheck /\ host[h].lsn # host[h].lh.lsn)
    /\ LayerLsnCheck => host[h].lsn >= host[h].lh.lsn                  \* lsn_behind (:800)
    /\ cut' = [cut EXCEPT ![h] = [pc |-> "intent", n |-> @.n, base |-> host[h].lh, lsn |-> host[h].lsn,
                                  img |-> <<h, @.n + 1>>, detach |-> det]]
    /\ UNCHANGED <<now, lag, head, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, bpc, bobs, hist>>

CutIntent(h) ==                                                         \* PutIntent before any upload (:853, :1489)
    /\ cut[h].pc = "intent"
    /\ intents' = intents \cup {[own |-> h, n |-> cut[h].n + 1, epoch |-> host[h].epoch, key |-> cut[h].img, created |-> now]}
    /\ cut' = [cut EXCEPT ![h].pc = "upload"]
    /\ UNCHANGED <<now, lag, head, stored, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, bpc, bobs, hist>>

CutUpload(h) ==
    /\ cut[h].pc = "upload"
    /\ stored' = stored \cup {cut[h].img}
    /\ hist' = [hist EXCEPT !.late = IF h = "A" /\ hist.takeover THEN @ \cup {cut[h].img} ELSE @]
    /\ cut' = [cut EXCEPT ![h].pc = "admit"]
    /\ UNCHANGED <<now, lag, head, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, bpc, bobs>>

DropOwn(h) == {i \in intents : ~(i.own = h /\ i.n = cut[h].n + 1)}

CutAdmit(h) ==                                                          \* admission re-checked right before the CAS (:911-923)
    /\ cut[h].pc = "admit"
    /\ IF Permits(h) /\ (~HostFence \/ ~SupersededBy(h, host[h].lh))
       THEN /\ cut' = [cut EXCEPT ![h].pc = "cas"] /\ intents' = intents
       ELSE /\ cut' = [cut EXCEPT ![h].pc = "idle", ![h].n = @ + 1] /\ intents' = DropOwn(h)
    /\ UNCHANGED <<now, lag, head, stored, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, bpc, bobs, hist>>

CutCAS(h) ==                                                            \* CASHead (:926), then settle by re-read (:957-987)
    /\ cut[h].pc = "cas"
    /\ LET b == cut[h].base
           det == cut[h].detach
           next == [gen |-> b.gen + 1, epoch |-> host[h].epoch, holder |-> h,
                    st |-> IF det THEN "detached" ELSE "active", dgen |-> IF det THEN b.gen + 1 ELSE 0,
                    lsn |-> cut[h].lsn, img |-> cut[h].img]
       IN IF CasOK(b, next)
          THEN /\ head' = next
               /\ host' = [host EXCEPT ![h].lh = next, ![h].detached = det]
               /\ hist' = [hist EXCEPT !.aCasAfter = @ \/ (h = "A" /\ hist.takeover),
                                       !.detHead = IF det THEN next ELSE @]
          ELSE /\ head' = head
               /\ host' = IF Fences(h, head) THEN [host EXCEPT ![h].fenced = TRUE, ![h].lh = head]
                                             ELSE [host EXCEPT ![h].lh = head]
               /\ hist' = hist
    /\ cut' = [cut EXCEPT ![h].pc = "drop"]
    /\ UNCHANGED <<now, lag, stored, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, bpc, bobs>>

CutDrop(h) ==                                                           \* DeleteIntent after settling (:1378-1421)
    /\ cut[h].pc = "drop"
    /\ intents' = DropOwn(h)
    /\ cut' = [cut EXCEPT ![h].pc = "idle", ![h].n = @ + 1]
    /\ UNCHANGED <<now, lag, head, stored, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, bpc, bobs, hist>>

-----------------------------------------------------------------------------
(* GC (core/frontend/chunkstore/gc.go:108-300) *)

\* capture-intent expiry (gc.go:214-222): age > intent_ttl, and
\* issued > intent epoch, HEAD epoch > intent epoch, HEAD epoch <= issued
GCExpire == \E i \in intents :
    /\ now - i.created > ITTL
    /\ GCEpochRule => (maxEpoch > i.epoch /\ head.epoch > i.epoch /\ head.epoch <= maxEpoch)
    /\ intents' = intents \ {i}
    /\ hist' = [hist EXCEPT !.badExpire = @ \/ (i.epoch = head.epoch /\ i.own = head.holder)]
    /\ UNCHANGED <<now, lag, head, stored, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bpc, bobs>>

\* mark + sweep, atomic: roots are HEAD and live intents (grace abstracted, as in ChunkLifecycle)
GCCollect == LET roots == {head.img} \cup {i.key : i \in intents}
             IN /\ stored \ roots # {}
                /\ stored' = stored \cap roots
                /\ UNCHANGED <<now, lag, head, intents, msgs, asg, lease, ack, journal, maxEpoch, deadline, restarts, dhz, uncertain, extReq, mv, host, cut, bpc, bobs, hist>>

-----------------------------------------------------------------------------
Next ==
    \/ Tick \/ SlowTick \/ LoseMsg
    \/ \E h \in Hosts : VMWrite(h) \/ SendRenew(h) \/ CutStart(h, FALSE) \/ CutStart(h, TRUE)
                        \/ CutIntent(h) \/ CutUpload(h) \/ CutAdmit(h) \/ CutCAS(h) \/ CutDrop(h)
    \/ MasterRenew \/ MasterExtend \/ HostLease \/ SendAck \/ MasterAck \/ Revoke \/ StopGuest \/ MasterRestart
    \/ MoveRead \/ MovePrepare
    \/ BAssign \/ BHook \/ BAttach \/ BSeed \/ BClaim
    \/ GCExpire \/ GCCollect

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

\* at most one host accepts VM writes at any instant (real/master time)
NoDualServing == ~(Serving("A") /\ Serving("B"))

\* after B's takeover CAS no CASHead by A succeeds, and nothing A uploaded after it is reachable
NoOldCasAfterTakeover == ~hist.aCasAfter
NoLateChunkReachable == head.img \notin hist.late

\* CLEAN move loses nothing: B's first HEAD is A's detached image, and A acked nothing after it
CleanNoLoss == (journal.kind = "clean" /\ hist.bFirst # NoRec) =>
                 /\ hist.bFirst.img = hist.detHead.img /\ hist.bFirst.lsn = hist.detHead.lsn
                 /\ hist.acked["A"] = hist.detHead.lsn

\* FORCED move: what is lost is A's journaled window (exact form, and the bound)
ForcedLossExact == (journal.kind = "forced" /\ hist.bFirst # NoRec) =>
                     hist.bFirst.lsn = journal.lsn /\ hist.bFirst.gen = journal.gen + 1
ForcedLossBounded == (journal.kind = "forced" /\ hist.bFirst # NoRec) => hist.bFirst.lsn >= journal.lsn

\* GC: HEAD's chunk is stored; no intent of the current HEAD holder is expired
GCSafe == head.img \in stored
NoLiveIntentExpired == ~hist.badExpire

\* B serves only after VOLUME hook -> attach -> seed -> takeover CAS, in that order, and serves from its seed
AttachOrder ==
    /\ Len(hist.steps) > 0 => hist.steps[1] = "hook"
    /\ \A i \in 2..Len(hist.steps) :
          /\ hist.steps[i] # "hook"
          /\ hist.steps[i] = "attach" => hist.steps[i - 1] \in {"hook", "seed"}   \* a refused claim retries the attach
          /\ hist.steps[i] = "seed" => hist.steps[i - 1] = "attach"
          /\ hist.steps[i] = "claim" => hist.steps[i - 1] = "seed"
    /\ host["B"].ready => hist.takeover
SeedMatchesTakeover == hist.bFirst # NoRec => hist.bSeed = hist.bFirst.lsn

\* HEAD epoch and remote_lsn never decrease
HeadMonotone == [][head'.epoch >= head.epoch /\ head'.lsn >= head.lsn]_vars

\* #205 (S1, scoped to the active lease): the current holder's validity never exceeds the published horizon, and the
\* published horizon never exceeds the disk; they are equal unless a write outcome is unknown
HostWithinHorizon == ~lease.none =>
                       \A h \in Hosts : (h = lease.holder /\ host[h].epoch = lease.epoch) => host[h].until <= lease.exp
DiskCoversMemory == ~lease.none => (lease.exp <= dhz /\ (~uncertain => lease.exp = dhz))

\* reachability witnesses (each is EXPECTED to be violated)
W_NoCleanServe  == ~(journal.kind = "clean" /\ Serving("B") /\ hist.acked["A"] > 0)
W_NoForcedServe == ~(journal.kind = "forced" /\ Serving("B") /\ hist.acked["A"] > 0)
W_NoBCut        == ~(head.holder = "B" /\ hist.bFirst # NoRec /\ head.gen > hist.bFirst.gen)
W_NoRestartMove == ~(restarts > 0 /\ hist.takeover)
W_NoExtension   == ~(~lease.none /\ lease.epoch = 1 /\ lease.exp > TTL + HSlack)   \* an extension raised A's horizon
W_NoExtLanded   == ~(~lease.none /\ dhz > lease.exp)                               \* a failed extension landed on disk
W_NoUncertain   == ~uncertain

TypeOK == /\ now \in 0..MaxT /\ head.gen \in Nat /\ stored \subseteq (Hosts \X (0..MaxCuts))
          /\ uncertain \in BOOLEAN /\ extReq \in -1..MaxT
=============================================================================
