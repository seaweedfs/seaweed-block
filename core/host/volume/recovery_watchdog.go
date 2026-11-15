package volume

import (
	"sync"
	"time"
)

// Recovery watchdog (#128 rule 6).
//
// A replica that holds the PRIMARY assignment but whose local recovery
// has not produced the engine's promotion-ready fact within the recover
// timeout reports an explicit unhealthy fact (Eligible=false plus a typed
// reason) instead of staying silently not-ready, so the master fails over
// at once to a peer that passes the promotion evidence gate, or, with no
// qualifying peer, promotes nobody and surfaces the reason.
//
//   - Scope: only a primary-assigned replica is flagged. A supporting
//     replica in a long catch-up is not; its catch-up state (elapsed,
//     lag) is visible in /status instead.
//   - The timeout runs from the primary assignment in this incarnation
//     (a new primary epoch restarts it).
//   - recovery_timeout latches until a fresh promotion-ready fact clears
//     it (Eligible=true, reason empty). An epoch that already moved
//     stays moved: the master decides that, not the volume.
//   - It watches recovery after a primary assignment only: once the
//     replica has been promotion-ready in that assignment, the watchdog
//     is disarmed until the next primary epoch (a running primary that
//     later degrades is not a boot-recovery stall).
//   - role_admission_failed reports the G1 role-admission latch of a
//     primary-assigned replica, for as long as the latch holds.
//
// wal_corrupt and io_error need typed failures out of the durable
// recovery path; they are a tracked follow-up.

const (
	UnhealthyRecoveryTimeout = "recovery_timeout"
	UnhealthyRoleAdmission   = "role_admission_failed"

	// DefaultRecoverTimeout matches blockmaster's default freshness
	// window.
	DefaultRecoverTimeout = 30 * time.Second
)

// A nil *recoveryWatchdog (a Host built without New, as some tests do)
// is inert: no reason is ever reported.
type recoveryWatchdog struct {
	mu      sync.Mutex
	timeout time.Duration
	now     func() time.Time

	primarySince    time.Time // zero unless this replica holds the primary assignment
	primaryEpoch    uint64
	supportingSince time.Time // zero unless admitted as a supporting replica
	supportingEpoch uint64
	timedOut        bool // recovery_timeout latched until a ready fact
	readySeen       bool // promotion-ready once in this primary assignment: disarmed
}

func newRecoveryWatchdog(timeout time.Duration) *recoveryWatchdog {
	if timeout <= 0 {
		timeout = DefaultRecoverTimeout
	}
	return &recoveryWatchdog{timeout: timeout, now: time.Now}
}

// primaryAssigned records a primary assignment for this replica. A new
// epoch restarts the timeout.
func (w *recoveryWatchdog) primaryAssigned(epoch uint64) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.primarySince.IsZero() || epoch != w.primaryEpoch {
		w.primarySince, w.primaryEpoch, w.timedOut, w.readySeen = w.now(), epoch, false, false
	}
	w.supportingSince, w.supportingEpoch = time.Time{}, 0
}

// notPrimary records that another replica holds the primary assignment.
func (w *recoveryWatchdog) notPrimary() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.primarySince, w.primaryEpoch, w.timedOut, w.readySeen = time.Time{}, 0, false, false
}

// supportingAdmitted records this replica's admission as a supporting
// replica at epoch; a new epoch restarts its catch-up clock.
func (w *recoveryWatchdog) supportingAdmitted(epoch uint64) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.primarySince, w.primaryEpoch, w.timedOut, w.readySeen = time.Time{}, 0, false, false
	if w.supportingSince.IsZero() || epoch != w.supportingEpoch {
		w.supportingSince, w.supportingEpoch = w.now(), epoch
	}
}

// reason returns the slot's explicit unhealthy reason given whether it
// is promotion-ready now and whether the G1 latch holds. fired is true
// on the call that latched recovery_timeout.
func (w *recoveryWatchdog) reason(ready, roleAdmissionFailed bool) (reason string, fired bool) {
	if w == nil {
		return "", false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.primarySince.IsZero() {
		return "", false
	}
	if ready {
		w.timedOut, w.readySeen = false, true
		return "", false
	}
	if roleAdmissionFailed {
		return UnhealthyRoleAdmission, false
	}
	if w.readySeen {
		return "", false
	}
	if !w.timedOut && w.now().Sub(w.primarySince) >= w.timeout {
		w.timedOut, fired = true, true
	}
	if w.timedOut {
		return UnhealthyRecoveryTimeout, fired
	}
	return "", false
}

// catchUpSince is when this replica was admitted as a supporting replica
// at its current epoch; zero when it is not supporting.
func (w *recoveryWatchdog) catchUpSince() time.Time {
	if w == nil {
		return time.Time{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.supportingSince
}

// UnhealthyReason is the slot's explicit unhealthy reason now (#128
// rule 6), the same value its heartbeat reports; empty when healthy.
func (h *Host) UnhealthyReason() string {
	return h.unhealthyReason(h.readyForPrimaryFact())
}

func (h *Host) unhealthyReason(ready bool) string {
	if h == nil || h.wd == nil {
		return ""
	}
	failed, _ := h.RoleAdmissionFailure()
	reason, fired := h.wd.reason(ready, failed)
	if fired {
		h.log.Printf("blockvolume: volume %s primary assignment epoch=%d not promotion-ready after %s: reporting unhealthy (%s)",
			h.cfg.VolumeID, h.wd.epoch(), h.wd.timeout, UnhealthyRecoveryTimeout)
	}
	return reason
}

func (w *recoveryWatchdog) epoch() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.primaryEpoch
}

// CatchUpSince is when this replica was admitted as a supporting replica
// at its current epoch; zero when it is not supporting.
func (h *Host) CatchUpSince() time.Time {
	if h == nil || h.wd == nil {
		return time.Time{}
	}
	return h.wd.catchUpSince()
}
