--------------------------- MODULE HeadReopenBelt ---------------------------
\* One same-owner reopen, one server-held old CAS, one new acknowledged image.
\* The single serialization point evaluates a CAS guard at application time.
EXTENDS Naturals, TLC
CONSTANT ClaimWrites
VARIABLES gen, image, pending, sent, reopened, late, acked, published
vars == <<gen, image, pending, sent, reopened, late, acked, published>>
Init == /\ gen = 1 /\ image = 0 /\ pending = FALSE /\ sent = FALSE
        /\ reopened = FALSE /\ late = FALSE /\ acked = FALSE /\ published = FALSE
SendOld == /\ ~sent /\ ~reopened /\ sent' = TRUE /\ pending' = TRUE
           /\ UNCHANGED <<gen, image, reopened, late, acked, published>>
Reopen == /\ sent /\ ~reopened /\ reopened' = TRUE
          /\ gen' = IF ClaimWrites THEN gen + 1 ELSE gen
          /\ UNCHANGED <<image, pending, sent, late, acked, published>>
ApplyOld == /\ pending /\ pending' = FALSE
            /\ gen' = IF gen = 1 THEN 2 ELSE gen
            /\ image' = IF gen = 1 THEN 1 ELSE image
            /\ late' = (late \/ (gen = 1 /\ reopened))
            /\ UNCHANGED <<sent, reopened, acked, published>>
AckNew == /\ reopened /\ ~acked /\ acked' = TRUE
          /\ UNCHANGED <<gen, image, pending, sent, reopened, late, published>>
PublishNew == /\ acked /\ ~published /\ published' = TRUE
              /\ gen' = gen + 1 /\ image' = 2
              /\ UNCHANGED <<pending, sent, reopened, late, acked>>
Next == SendOld \/ Reopen \/ ApplyOld \/ AckNew \/ PublishNew
Spec == Init /\ [][Next]_vars
OldCASCannotLandAfterReopen == ~late
PublishedAcknowledgedImage == published => image = 2
=============================================================================
