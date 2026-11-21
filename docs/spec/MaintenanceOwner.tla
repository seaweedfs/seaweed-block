------------------------- MODULE MaintenanceOwner -------------------------
EXTENDS Naturals, TLC
CONSTANT AllowOverlap, ReleaseBeforeJoin
VARIABLES state, due, workers, released
vars == <<state, due, workers, released>>
Init == /\ state = "pending" /\ due = FALSE /\ workers = 0 /\ released = FALSE
Attach == /\ state = "pending" /\ state' = "armed" /\ UNCHANGED <<due, workers, released>>
Tick == /\ state = "armed" /\ due' = TRUE /\ UNCHANGED <<state, workers, released>>
Start == /\ state = "armed" /\ due /\ (workers = 0 \/ AllowOverlap)
         /\ workers < 2 /\ workers' = workers + 1 /\ due' = FALSE
         /\ UNCHANGED <<state, released>>
Finish == /\ workers > 0 /\ workers' = workers - 1 /\ UNCHANGED <<state, due, released>>
Stop == /\ state = "armed" /\ state' = "draining" /\ due' = FALSE
        /\ UNCHANGED <<workers, released>>
Join == /\ state = "draining" /\ (workers = 0 \/ ReleaseBeforeJoin)
        /\ state' = "closed" /\ released' = TRUE /\ UNCHANGED <<due, workers>>
Next == Attach \/ Tick \/ Start \/ Finish \/ Stop \/ Join
Spec == Init /\ [][Next]_vars
OneOwner == workers <= 1
JoinedBeforeRelease == released => workers = 0
NoCatchupQueue == due \in BOOLEAN
=============================================================================
