package volume

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MasterWaitLimit is how long a workspace chunk primary waits for its grant
// while the master answers: counted from the start of the current contact
// streak, or from the master's barrier time when that is later.
const MasterWaitLimit = 30 * time.Second

// masterWaitLogEvery repeats the wait's log line while it lasts.
const masterWaitLogEvery = 10 * time.Minute

// MasterWait is why a workspace primary is not yet granted, for /status.
// Reason is unreachable (no reply), credentials (the master refused the
// node's credentials), barrier (Until is the master's barrier time),
// no_assignment, refused, moved or deleted. Until is zero while the wait
// has no bound.
type MasterWait struct {
	Reason string
	Detail string // diagnostic text of a typed final refusal; never used as a guard
	Since  time.Time
	Until  time.Time
}

// masterWait observes the results of the process's calls to blockmaster
// (heartbeat, assignment stream, lease renewal). It holds observations
// only: no decision branches on it except the startup wait's end.
type masterWait struct {
	mu          sync.Mutex
	now         func() time.Time
	contact     time.Time // start of the current contact streak; zero while none
	last        time.Time // the latest reply
	down        time.Time // first failure without a reply since the last contact
	creds       time.Time // first credentials refusal since the last accepted call
	ident       time.Time // first identity refusal since the last accepted call
	identAt     time.Time // the latest identity refusal
	identOf     string    // its kind: unregistered or wrong_host
	start       time.Time // when the startup wait began (after the local open); zero before
	window      time.Time // the first reply at or after start in the current run of replies
	barrier     time.Time // T: request start + retry_after of the latest BARRIER refusal
	final       string    // "moved" or "deleted": a permanent refusal
	finalDetail string
	logged      string // the reason last logged
	loggedAt    time.Time
	logf        func(string, ...any)
}

func newMasterWait(now func() time.Time, logf func(string, ...any)) *masterWait {
	if now == nil {
		now = time.Now
	}
	if logf == nil {
		logf = log.Printf
	}
	return &masterWait{now: now, logf: logf}
}

// masterCallClass sorts a call result: a reply produced by blockmaster's
// handler (any status but the transport codes) is contact; Unavailable,
// DeadlineExceeded and Canceled are no reply; Unauthenticated and
// PermissionDenied are a credentials refusal.
func masterCallClass(err error) (contact, credentials bool) {
	if err == nil {
		return true, false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false, false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return false, false
	case codes.Unauthenticated, codes.PermissionDenied:
		return false, true
	}
	return true, false
}

// call records one call's result. An identity refusal (the master's typed
// IDENTITY detail: this server id is not registered, or calls from another
// host) is not contact: like refused credentials it has no bound, because a
// restart cannot fix a registration. It lasts until an identity-checked call
// (a lease renewal, an assignment delivered) is accepted, or until
// identityDecay passes with replies and no new identity refusal (the
// refused stream retries within seconds; a fixed registration may bring no
// assignment to accept). The heartbeat does not check identity, so its
// answers alone do not end it sooner.
func (w *masterWait) call(err error) {
	contact, credentials := masterCallClass(err)
	identity := ""
	r := leaseRefusalDetail(err)
	if r != nil && r.GetKind() == control.WorkspaceLeaseRefusal_IDENTITY {
		contact, identity = false, r.GetIdentity()
		if identity == "" {
			identity = "unknown"
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	if r != nil && status.Code(err) == codes.FailedPrecondition {
		switch r.GetKind() {
		case control.WorkspaceLeaseRefusal_MOVED:
			w.final, w.finalDetail = "moved", status.Convert(err).Message()
		case control.WorkspaceLeaseRefusal_DELETED:
			w.final, w.finalDetail = "deleted", status.Convert(err).Message()
		}
	}
	switch {
	case identity != "":
		w.endStreakLocked()
		w.down, w.creds = time.Time{}, time.Time{}
		if w.ident.IsZero() {
			w.ident = now
		}
		w.identAt, w.identOf = now, identity
	case contact:
		if !w.ident.IsZero() {
			if now.Sub(w.identAt) < identityDecay {
				return // still refused: a heartbeat answer does not end it
			}
			w.ident, w.identOf = time.Time{}, ""
		}
		w.replyLocked(now)
		w.down, w.creds = time.Time{}, time.Time{}
	case credentials:
		w.endStreakLocked()
		w.down, w.ident = time.Time{}, time.Time{}
		if w.creds.IsZero() {
			w.creds = now
		}
	default:
		w.endStreakLocked()
		w.creds, w.ident = time.Time{}, time.Time{}
		if w.down.IsZero() {
			w.down = now
		}
	}
}

// identityDecay ends an identity refusal that is not repeated: replies keep
// coming and no call was refused on identity for this long.
const identityDecay = 30 * time.Second

// replyLocked records an accepted reply at now: it starts a run of replies
// when none is running, and opens the wait's window at the first reply at
// or after the wait's start. Caller holds mu.
func (w *masterWait) replyLocked(now time.Time) {
	if w.contact.IsZero() {
		w.contact = now
	}
	w.last = now
	if w.window.IsZero() && !w.start.IsZero() && !now.Before(w.start) {
		w.window = now
	}
}

// endStreakLocked ends the run of replies: the window closes with it, and the
// next reply opens a new one. Caller holds mu.
func (w *masterWait) endStreakLocked() {
	w.contact, w.window = time.Time{}, time.Time{}
}

// identityAccepted ends an identity refusal: an identity-checked call (a
// lease renewal, an assignment delivered) was accepted. The run of replies
// starts again there, as after any refusal that had no bound.
func (w *masterWait) identityAccepted() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ident.IsZero() {
		return
	}
	w.ident, w.identOf = time.Time{}, ""
	w.endStreakLocked()
	w.replyLocked(w.now())
}

// renewal records a lease renewal started at start: the call itself, and
// the typed kind of a refusal (status detail only, never its text).
func (w *masterWait) renewal(start time.Time, err error) {
	w.call(err)
	if err == nil {
		w.identityAccepted()
	}
	r := leaseRefusalDetail(err)
	if r == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	switch r.GetKind() {
	case control.WorkspaceLeaseRefusal_BARRIER:
		if t := start.Add(time.Duration(r.GetRetryAfterNs())); t.After(w.barrier) {
			w.barrier = t
		}
	}
}

// leaseRefusalDetail returns the typed refusal of a renewal error, or nil
// (OTHER) when it carries none.
func leaseRefusalDetail(err error) *control.WorkspaceLeaseRefusal {
	st, ok := status.FromError(err)
	if err == nil || !ok {
		return nil
	}
	for _, d := range st.Details() {
		if r, ok := d.(*control.WorkspaceLeaseRefusal); ok {
			return r
		}
	}
	return nil
}

// deadlineLocked is the one bound of the startup wait: max(the window, the
// master's barrier time) + MasterWaitLimit, where the window is the first
// reply at or after the wait's start (local open and recovery count toward
// nothing). Zero (no bound) before the wait starts, while there is no window
// yet (no reply since the start, or the master gives no reply), and while
// the node's credentials or identity are refused. Caller holds mu.
func (w *masterWait) deadlineLocked() time.Time {
	if w.window.IsZero() || !w.ident.IsZero() || !w.creds.IsZero() {
		return time.Time{}
	}
	from := w.window
	if w.barrier.After(from) {
		from = w.barrier
	}
	return from.Add(MasterWaitLimit)
}

// begin records the startup wait's start (the latest wait's, for a later
// recovery): the window opens at the first reply at or after it.
func (w *masterWait) begin(start time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if start.Equal(w.start) {
		return
	}
	w.start, w.window = start, time.Time{}
	if !w.contact.IsZero() && !w.contact.Before(start) {
		w.window = w.contact
	}
}

// state returns the startup wait's bound (zero: none) and a permanent
// refusal ("" when none).
func (w *masterWait) state() (time.Time, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deadlineLocked(), w.final
}

// snapshot names the reason for a process that holds no grant, and logs it
// on a change and every masterWaitLogEvery while it lasts.
func (w *masterWait) snapshot(assigned bool) MasterWait {
	w.mu.Lock()
	now := w.now()
	var out MasterWait
	switch {
	case w.final != "":
		out = MasterWait{Reason: w.final, Detail: w.finalDetail, Since: now}
	case !w.creds.IsZero():
		out = MasterWait{Reason: "credentials", Since: w.creds}
	case !w.ident.IsZero():
		out = MasterWait{Reason: "identity refused: " + w.identOf, Since: w.ident}
	case w.contact.IsZero():
		out = MasterWait{Reason: "unreachable", Since: w.down}
		if w.down.IsZero() {
			out.Since = now
		}
	case w.barrier.After(now):
		out = MasterWait{Reason: "barrier", Since: w.contact, Until: w.barrier}
	case w.window.IsZero():
		out = MasterWait{Reason: "no_assignment", Since: w.contact}
		if assigned {
			out.Reason = "refused"
		}
	case !assigned:
		out = MasterWait{Reason: "no_assignment", Since: w.contact, Until: w.deadlineLocked()}
	default:
		out = MasterWait{Reason: "refused", Since: w.contact, Until: w.deadlineLocked()}
	}
	logIt := out.Reason != w.logged || now.Sub(w.loggedAt) >= masterWaitLogEvery
	if logIt {
		w.logged, w.loggedAt = out.Reason, now
	}
	logf := w.logf
	w.mu.Unlock()
	if logIt {
		if out.Until.IsZero() {
			logf("blockvolume: waiting for the master: %s since %s (no bound)", out.Reason, out.Since.UTC().Format(time.RFC3339))
		} else {
			logf("blockvolume: waiting for the master: %s since %s until %s", out.Reason, out.Since.UTC().Format(time.RFC3339), out.Until.UTC().Format(time.RFC3339))
		}
	}
	return out
}

// MasterWaitState is the workspace startup wait's bound (zero while the
// master gives no reply or refuses the node's credentials: no bound) and a
// permanent refusal of the lease ("moved" or "deleted"; "" when none).
func (h *Host) MasterWaitState() (time.Time, string) { return h.mw.state() }

// MasterWaitDetail is diagnostic only; exit classification uses the typed kind.
func (h *Host) MasterWaitDetail() string {
	h.mw.mu.Lock()
	defer h.mw.mu.Unlock()
	return h.mw.finalDetail
}

// MasterWaitStatus is why a workspace primary is not serving under a
// grant, or nil when it holds one (or is not a workspace primary).
func (h *Host) MasterWaitStatus() *MasterWait {
	if !h.cfg.WorkspaceLease {
		return nil
	}
	if g := h.view.workspaceGuard.Load(); g != nil && g.Held() {
		return nil
	}
	w := h.mw.snapshot(h.realAdpt.Projection().Epoch > 0)
	return &w
}

// barrierTime is the master's barrier time T from the latest BARRIER refusal
// (zero when none): the lease guard sleeps until just past it.
func (w *masterWait) barrierTime() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.barrier
}

// MasterWaitBound is the startup wait's one bound for a wait that started at
// start (zero: no bound) and a permanent refusal of the lease ("moved" or
// "deleted"; "" when none). /status shows the same bound.
func (h *Host) MasterWaitBound(start time.Time) (time.Time, string) {
	h.mw.begin(start)
	return h.mw.state()
}
