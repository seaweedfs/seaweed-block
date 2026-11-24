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
	Since  time.Time
	Until  time.Time
}

// masterWait observes the results of the process's calls to blockmaster
// (heartbeat, assignment stream, lease renewal). It holds observations
// only: no decision branches on it except the startup wait's end.
type masterWait struct {
	mu       sync.Mutex
	now      func() time.Time
	contact  time.Time // start of the current contact streak; zero while none
	last     time.Time // the latest reply
	down     time.Time // first failure without a reply since the last contact
	creds    time.Time // first credentials refusal since the last accepted call
	barrier  time.Time // T: request start + retry_after of the latest BARRIER refusal
	final    string    // "moved" or "deleted": a permanent refusal
	logged   string    // the reason last logged
	loggedAt time.Time
	logf     func(string, ...any)
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

// call records one call's result.
func (w *masterWait) call(err error) {
	contact, credentials := masterCallClass(err)
	w.mu.Lock()
	now := w.now()
	switch {
	case contact:
		if w.contact.IsZero() {
			w.contact = now
		}
		w.last = now
		w.down, w.creds = time.Time{}, time.Time{}
	case credentials:
		w.contact, w.down = time.Time{}, time.Time{}
		if w.creds.IsZero() {
			w.creds = now
		}
	default:
		w.contact, w.creds = time.Time{}, time.Time{}
		if w.down.IsZero() {
			w.down = now
		}
	}
	w.mu.Unlock()
}

// renewal records a lease renewal started at start: the call itself, and
// the typed kind of a refusal (status detail only, never its text).
func (w *masterWait) renewal(start time.Time, err error) {
	w.call(err)
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
	case control.WorkspaceLeaseRefusal_MOVED:
		w.final = "moved"
	case control.WorkspaceLeaseRefusal_DELETED:
		w.final = "deleted"
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

// deadlineLocked is max(contact streak start, barrier T) + MasterWaitLimit,
// or zero while there is no contact (no bound). Caller holds mu.
func (w *masterWait) deadlineLocked() time.Time {
	if w.contact.IsZero() {
		return time.Time{}
	}
	from := w.contact
	if w.barrier.After(from) {
		from = w.barrier
	}
	return from.Add(MasterWaitLimit)
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
		out = MasterWait{Reason: w.final, Since: now}
	case !w.creds.IsZero():
		out = MasterWait{Reason: "credentials", Since: w.creds}
	case w.contact.IsZero():
		out = MasterWait{Reason: "unreachable", Since: w.down}
		if w.down.IsZero() {
			out.Since = now
		}
	case w.barrier.After(now):
		out = MasterWait{Reason: "barrier", Since: w.contact, Until: w.barrier}
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

// MasterWaitContact is the startup wait's input: the first reply of the
// current run of replies (zero while the master gives no reply or refuses
// the node's credentials or identity), the latest reply, the master's
// barrier time, and a permanent refusal ("moved" or "deleted"; "" if none).
func (h *Host) MasterWaitContact() (streakStart, lastReply, barrier time.Time, final string) {
	w := h.mw
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.contact, w.last, w.barrier, w.final
}
