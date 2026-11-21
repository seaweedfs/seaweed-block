------------------------- MODULE MaintenanceOwner -------------------------
EXTENDS Naturals, TLC
CONSTANT AllowOverlap, ReleaseBeforeJoin, AllowStaleRearm
VARIABLES state, due, workers, released, owner, current, arms, closing
vars == <<state,due,workers,released,owner,current,arms,closing>>
Init == /\ state = "pending" /\ due = FALSE /\ workers = 0 /\ released = FALSE
        /\ owner = 1 /\ current = 1 /\ arms = 0 /\ closing = FALSE
Attach == /\ state = "pending" /\ state' = "armed" /\ arms' = arms+1
          /\ UNCHANGED <<due,workers,released,owner,current,closing>>
Tick == /\ state = "armed" /\ due' = TRUE
        /\ UNCHANGED <<state,workers,released,owner,current,arms,closing>>
Start == /\ state = "armed" /\ due /\ (workers = 0 \/ AllowOverlap)
         /\ workers < 2 /\ workers' = workers+1 /\ due' = FALSE
         /\ UNCHANGED <<state,released,owner,current,arms,closing>>
Finish == /\ workers > 0 /\ workers' = workers-1
          /\ UNCHANGED <<state,due,released,owner,current,arms,closing>>
Stop == /\ state = "armed" /\ state' = "draining" /\ due' = FALSE
        /\ closing' \in BOOLEAN /\ UNCHANGED <<workers,released,owner,current,arms>>
Join == /\ state = "draining" /\ (workers = 0 \/ ReleaseBeforeJoin)
        /\ state' = IF closing THEN "closed" ELSE "idle"
        /\ released' = TRUE /\ UNCHANGED <<due,workers,owner,current,arms,closing>>
Rearm == /\ state = "idle" /\ arms < 3 /\ (owner = current \/ AllowStaleRearm)
         /\ state' = "armed" /\ released' = FALSE /\ arms' = arms+1
         /\ UNCHANGED <<due,workers,owner,current,closing>>
ReplaceHandle == /\ state = "idle" /\ workers = 0 /\ current = 1
                 /\ current' = 2
                 /\ UNCHANGED <<state,due,workers,released,owner,arms,closing>>
AttachCurrent == /\ state = "idle" /\ workers = 0 /\ owner # current /\ arms < 3
                 /\ owner' = current /\ state' = "armed" /\ released' = FALSE
                 /\ arms' = arms+1 /\ UNCHANGED <<due,workers,current,closing>>
Next == Attach \/ Tick \/ Start \/ Finish \/ Stop \/ Join \/ Rearm \/ ReplaceHandle \/ AttachCurrent
Spec == Init /\ [][Next]_vars
OneOwner == workers <= 1
JoinedBeforeRelease == released => workers = 0
NoCatchupQueue == due \in BOOLEAN
CurrentHandleAtAdmission == workers > 0 => owner = current
=============================================================================
