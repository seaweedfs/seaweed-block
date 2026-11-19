----------------------------- MODULE WALReclaim -----------------------------
(***************************************************************************)
(* Liveness of v3 WAL reclaim under write pressure (claude05, 2026-11-19). *)
(* Abstracts core/storage at seaweed-block main 39f11d6a:                  *)
(*   head    = WAL head LSN (records appended)                             *)
(*   wack    = durable WAL frontier; today advanced only by Sync           *)
(*             (walstore_v3.go:829)                                        *)
(*   flushed = records the flusher has written to the extent               *)
(*   ckpt    = checkpoint = reuse floor, clamped to                        *)
(*             min(flushed, wack, session pin)                             *)
(*             (walstore_map_checkpoint_v3.go:57-62)                       *)
(* A writer may append only while head - ckpt < Cap (the hard watermark).  *)
(* A blocked writer waits INSIDE the capture read lock                     *)
(* (walstore.go:829-832, admission at :850). A cut takes the capture       *)
(* write lock; Go's RWMutex makes a pending writer block new readers.      *)
(*                                                                         *)
(* InternalOn: the fix under review (codex03 design r0) - under pressure   *)
(* the store makes its own WAL durable (wack := head) with no guest Sync.  *)
(* Mut:                                                                    *)
(*   "none"          the design                                            *)
(*   "noClamp"       checkpoint ignores wack (reclaims non-durable WAL)    *)
(*   "syncTakesCapture" the internal sync needs the capture read lock      *)
(*                   (finding F2)                                          *)
(* PinFair: a catch-up session pin is eventually released (finding F3).    *)
(* The guest's Sync has NO fairness: a guest may never flush.              *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Cap, MaxLSN, MaxCuts, MaxPins, InternalOn, Mut, PinOn, PinFair

VARIABLES head, wack, flushed, ckpt, inside, cutPending, cuts, pin, pins, gsync

vars == <<head, wack, flushed, ckpt, inside, cutPending, cuts, pin, pins, gsync>>

Min(a, b) == IF a < b THEN a ELSE b
NoPin == MaxLSN + 1
Full == head - ckpt >= Cap

Init == /\ head = 0 /\ wack = 0 /\ flushed = 0 /\ ckpt = 0
        /\ inside = FALSE /\ cutPending = FALSE /\ cuts = 0 /\ pin = NoPin /\ pins = 0 /\ gsync = FALSE

\* A writer that finds the ring full enters the admission wait holding the
\* capture read lock; it can enter only if no cut is pending.
EnterWait == /\ head < MaxLSN /\ Full /\ ~inside /\ ~cutPending
             /\ inside' = TRUE
             /\ UNCHANGED <<head, wack, flushed, ckpt, cutPending, cuts, pin, pins, gsync>>

\* Append: a fresh writer needs the read lock (blocked by a pending cut); a
\* writer already inside the wait holds it.
Append == /\ head < MaxLSN /\ ~Full /\ (inside \/ ~cutPending)
          /\ head' = head + 1 /\ inside' = FALSE
          /\ UNCHANGED <<wack, flushed, ckpt, cutPending, cuts, pin, pins, gsync>>

\* Guest Flush/Sync: may happen or never (no fairness).
GuestSync == /\ wack < head
             /\ wack' = head /\ gsync' = TRUE
             /\ UNCHANGED <<head, flushed, ckpt, inside, cutPending, cuts, pin, pins>>

\* The fix: internal sync on pressure. In the mutant it also needs the
\* capture read lock, so it is blocked while a cut is pending.
InternalSync == /\ InternalOn /\ wack < head /\ Full
                /\ (Mut = "syncTakesCapture" => ~cutPending)
                /\ wack' = head
                /\ UNCHANGED <<head, flushed, ckpt, inside, cutPending, cuts, pin, pins, gsync>>

Flush == /\ flushed < head
         /\ flushed' = flushed + 1
         /\ UNCHANGED <<head, wack, ckpt, inside, cutPending, cuts, pin, pins, gsync>>

Target == IF Mut = "noClamp" THEN Min(flushed, pin) ELSE Min(Min(flushed, wack), pin)

Checkpoint == /\ Target > ckpt
              /\ ckpt' = Target
              /\ UNCHANGED <<head, wack, flushed, inside, cutPending, cuts, pin, pins, gsync>>

\* A cut requests the capture write lock; it completes once no reader holds
\* the lock (the waiting writer is a reader).
CutStart == /\ cuts < MaxCuts /\ ~cutPending
            /\ cutPending' = TRUE /\ cuts' = cuts + 1
            /\ UNCHANGED <<head, wack, flushed, ckpt, inside, pin, pins, gsync>>
CutFinish == /\ cutPending /\ ~inside
             /\ cutPending' = FALSE
             /\ UNCHANGED <<head, wack, flushed, ckpt, inside, cuts, pin, pins, gsync>>

\* An RF2 catch-up session pins the WAL at the current checkpoint, later
\* releases it.
PinSet == /\ PinOn /\ pin = NoPin /\ pins < MaxPins
          /\ pin' = ckpt /\ pins' = pins + 1
          /\ UNCHANGED <<head, wack, flushed, ckpt, inside, cutPending, cuts, gsync>>
PinRelease == /\ pin # NoPin
              /\ pin' = NoPin
              /\ UNCHANGED <<head, wack, flushed, ckpt, inside, cutPending, cuts, pins, gsync>>

Next == \/ EnterWait \/ Append \/ GuestSync \/ InternalSync \/ Flush \/ Checkpoint
        \/ CutStart \/ CutFinish \/ PinSet \/ PinRelease

Fairness == /\ WF_vars(Append) /\ WF_vars(EnterWait) /\ WF_vars(Flush) /\ WF_vars(Checkpoint)
            /\ WF_vars(InternalSync) /\ WF_vars(CutFinish)
            /\ (PinFair => WF_vars(PinRelease))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---- safety ----
TypeOK == /\ head \in 0..MaxLSN /\ wack \in 0..MaxLSN /\ flushed \in 0..MaxLSN
          /\ ckpt \in 0..MaxLSN /\ inside \in BOOLEAN /\ cutPending \in BOOLEAN
          /\ cuts \in 0..MaxCuts /\ pin \in 0..(MaxLSN + 1) /\ pins \in 0..MaxPins /\ gsync \in BOOLEAN
I1 == wack <= head                    \* durable frontier never ahead of the head
I2 == ckpt <= wack                    \* never reclaim WAL that is not durable
I3 == ckpt <= flushed                 \* never reclaim a record not yet in the extent
IRing == head - ckpt <= Cap           \* the ring never overflows
IPin == pin # NoPin => ckpt >= pin    \* a pin never moves the floor backwards (sanity)

\* ---- witness: all writes complete with NO guest Sync (must be violated = reachable when InternalOn) ----
W_NeedsGuestSync == ~(head = MaxLSN /\ ~gsync)

\* ---- liveness ----
AllWritten == <>(head = MaxLSN)                       \* no write stalls forever
NoStuckWaiter == [](inside => <>(~inside))            \* a waiting writer is always admitted
=============================================================================
