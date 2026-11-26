------------------------- MODULE MaintenanceOwner -------------------------
EXTENDS Naturals, TLC
\* Kick (2026-11-25): a snapshot deleted through the holder kicks one pass
\* (Armed -> Due; kicks coalesce). A pass the volume refuses as busy (a cut
\* session open: the cleaning never runs during one) keeps the kick pending
\* and re-arms it soon; only a pass that is not refused serves the kick.
\* LoseKickOnBusy = TRUE: a busy pass drops the kick (must violate NoLostKick).
CONSTANT AllowOverlap, ReleaseBeforeJoin, AllowStaleRearm, LoseKickOnBusy
VARIABLES state, due, workers, released, owner, current, arms, closing, kicked, session, lostKick
vars == <<state,due,workers,released,owner,current,arms,closing,kicked,session,lostKick>>
Init == /\ state = "pending" /\ due = FALSE /\ workers = 0 /\ released = FALSE
        /\ owner = 1 /\ current = 1 /\ arms = 0 /\ closing = FALSE
        /\ kicked = FALSE /\ session = FALSE /\ lostKick = FALSE
Attach == /\ state = "pending" /\ state' = "armed" /\ arms' = arms+1
          /\ UNCHANGED <<due,workers,released,owner,current,closing,kicked,session,lostKick>>
Tick == /\ state = "armed" /\ due' = TRUE
        /\ UNCHANGED <<state,workers,released,owner,current,arms,closing,kicked,session,lostKick>>
Kick == /\ state = "armed" /\ due' = TRUE /\ kicked' = TRUE
        /\ UNCHANGED <<state,workers,released,owner,current,arms,closing,session,lostKick>>
Start == /\ state = "armed" /\ due /\ (workers = 0 \/ AllowOverlap)
         /\ workers < 2 /\ workers' = workers+1 /\ due' = FALSE
         /\ UNCHANGED <<state,released,owner,current,arms,closing,kicked,session,lostKick>>
\* A pass ends; it was refused as busy iff a cut session was open. A served
\* (not busy) pass clears the kick; a busy pass keeps it and re-arms soon.
Finish == /\ workers > 0 /\ workers' = workers-1
          /\ IF session
               THEN IF LoseKickOnBusy
                      THEN /\ kicked' = FALSE /\ lostKick' = (lostKick \/ kicked) /\ due' = due
                      ELSE /\ kicked' = kicked /\ lostKick' = lostKick
                           /\ due' = (due \/ (kicked /\ state = "armed"))
               ELSE /\ kicked' = FALSE /\ lostKick' = lostKick /\ due' = due
          /\ UNCHANGED <<state,released,owner,current,arms,closing,session>>
OpenSession == /\ ~session /\ session' = TRUE
               /\ UNCHANGED <<state,due,workers,released,owner,current,arms,closing,kicked,lostKick>>
CloseSession == /\ session /\ session' = FALSE
                /\ UNCHANGED <<state,due,workers,released,owner,current,arms,closing,kicked,lostKick>>
Stop == /\ state = "armed" /\ state' = "draining" /\ due' = FALSE
        /\ closing' \in BOOLEAN /\ UNCHANGED <<workers,released,owner,current,arms,kicked,session,lostKick>>
Join == /\ state = "draining" /\ (workers = 0 \/ ReleaseBeforeJoin)
        /\ state' = IF closing THEN "closed" ELSE "idle"
        /\ released' = TRUE /\ UNCHANGED <<due,workers,owner,current,arms,closing,kicked,session,lostKick>>
Rearm == /\ state = "idle" /\ arms < 3 /\ (owner = current \/ AllowStaleRearm)
         /\ state' = "armed" /\ released' = FALSE /\ arms' = arms+1
         /\ UNCHANGED <<due,workers,owner,current,closing,kicked,session,lostKick>>
ReplaceHandle == /\ state = "idle" /\ workers = 0 /\ current = 1
                 /\ current' = 2
                 /\ UNCHANGED <<state,due,workers,released,owner,arms,closing,kicked,session,lostKick>>
AttachCurrent == /\ state = "idle" /\ workers = 0 /\ owner # current /\ arms < 3
                 /\ owner' = current /\ state' = "armed" /\ released' = FALSE
                 /\ arms' = arms+1 /\ UNCHANGED <<due,workers,current,closing,kicked,session,lostKick>>
Next == Attach \/ Tick \/ Kick \/ Start \/ Finish \/ OpenSession \/ CloseSession
        \/ Stop \/ Join \/ Rearm \/ ReplaceHandle \/ AttachCurrent
Spec == Init /\ [][Next]_vars
OneOwner == workers <= 1
JoinedBeforeRelease == released => workers = 0
NoCatchupQueue == due \in BOOLEAN
CurrentHandleAtAdmission == workers > 0 => owner = current
\* A kick is never dropped by a busy pass: it stays until a pass serves it.
NoLostKick == ~lostKick
=============================================================================
