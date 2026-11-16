package volume

import (
	"errors"
	"io"
	"io/fs"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
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
//     is disarmed until the next primary epoch or a new process
//     incarnation (its state is per process). A running primary that
//     later degrades is not a boot-recovery stall: it goes through the
//     normal controller/gate failover.
//   - role_admission_failed reports the G1 role-admission latch of a
//     primary-assigned replica, for as long as the latch holds, whatever
//     an older engine-ready fact says.
//   - Only a ready fact for the watched primary epoch clears or disarms:
//     an engine still ready at an older epoch (its new assignment not
//     admitted) does not.
//
//   - wal_corrupt / io_error (#133): when local durable recovery failed
//     with a classifiable error (LocalRecoveryHealthReason), a
//     primary-assigned replica reports that typed reason at once instead
//     of waiting out the timeout. An unclassified failure reports nothing
//     early; recovery_timeout still covers it.

const (
	UnhealthyRecoveryTimeout = "recovery_timeout"
	UnhealthyRoleAdmission   = "role_admission_failed"
	UnhealthyWALCorrupt      = "wal_corrupt"
	UnhealthyIOError         = "io_error"
	UnhealthyDataChecksum    = "data_checksum"

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
// is promotion-ready now, at which epoch that readiness is, and whether
// the G1 latch holds. Readiness counts only for the watched primary
// epoch, compared under the lock, so an assignment that advanced while
// the fact was read is not cleared by it. fired is true on the call that
// latched recovery_timeout.
func (w *recoveryWatchdog) reason(ready bool, readyEpoch uint64, roleAdmissionFailed bool, localFailure string) (reason string, fired bool) {
	if w == nil {
		return "", false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.primarySince.IsZero() {
		return "", false
	}
	if roleAdmissionFailed {
		return UnhealthyRoleAdmission, false
	}
	if localFailure != "" {
		return localFailure, false
	}
	if ready && readyEpoch == w.primaryEpoch {
		w.timedOut, w.readySeen = false, true
		return "", false
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
	fact, ready := h.promotionReady()
	return h.unhealthyReason(fact, ready)
}

// unhealthyReason evaluates the watchdog with the promotion fact (its
// epoch binds readiness to the watched assignment) and the G1 latch.
func (h *Host) unhealthyReason(fact engine.PromotionReadyFact, ready bool) string {
	if h == nil || h.wd == nil {
		return ""
	}
	failed, _ := h.RoleAdmissionFailure()
	local, _ := h.localRecoveryReason.Load().(string)
	reason, fired := h.wd.reason(ready, fact.Epoch, failed, local)
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

// LocalRecoveryHealthReason classifies a local durable open, replay or runtime
// failure into a typed unhealthy reason (#133). It returns "" for an
// error it cannot classify (the watchdog's timeout still covers that).
//
//   - data_checksum: checked extent or manifest bytes differ.
//   - wal_corrupt: the substrate found a WAL/extent integrity fault
//     (storage.RecoveryFailure kind WALIntegrity, or ErrWALIntegrityFault).
//   - io_error: the local storage could not be opened or read at startup:
//     kind SubstrateIO, any fs.PathError, io.ErrUnexpectedEOF or EIO. This
//     is deliberately broad. A path error such as ENOENT, EACCES or EISDIR
//     is io_error too: it says the volume's local storage is unusable, NOT
//     that the physical disk is damaged. The underlying error is kept in
//     the readiness evidence and the log line, for diagnosis.
func LocalRecoveryHealthReason(err error) string {
	if err == nil {
		return ""
	}
	// A checksum mismatch is EIO too; preserve its specific classification
	// before broader wrapper and substrate-IO handling.
	if errors.Is(err, storage.ErrDataChecksum) {
		return UnhealthyDataChecksum
	}
	var rf *storage.RecoveryFailure
	if errors.As(err, &rf) {
		switch rf.Kind {
		case storage.StorageRecoveryFailureWALIntegrity:
			return UnhealthyWALCorrupt
		case storage.StorageRecoveryFailureSubstrateIO:
			return UnhealthyIOError
		}
	}
	if errors.Is(err, storage.ErrWALIntegrityFault) {
		return UnhealthyWALCorrupt
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.EIO) {
		return UnhealthyIOError
	}
	return ""
}

// ReportLocalRecoveryFailure records why local durable recovery failed,
// classified by LocalRecoveryHealthReason, and returns the reason. A
// primary-assigned replica then reports it at once (#133). It holds until
// ClearLocalReadinessBlock (a later clean recovery).
func (h *Host) ReportLocalRecoveryFailure(err error) string {
	reason := LocalRecoveryHealthReason(err)
	h.localRecoveryReason.Store(reason)
	if reason != "" {
		h.log.Printf("blockvolume: volume %s local durable storage failed (%s): %v; a primary assignment reports it as unhealthy", h.cfg.VolumeID, reason, err)
	}
	return reason
}

// ReportDataChecksumFailure is the durable backend runtime-fault callback.
// The caller releases its IO lock first. The block prevents future local
// readiness publication; the typed reason also overrides an older ready fact.
func (h *Host) ReportDataChecksumFailure(err error) {
	if !errors.Is(err, storage.ErrDataChecksum) {
		return
	}
	h.BlockLocalReadiness("data checksum failure: " + err.Error())
	h.ReportLocalRecoveryFailure(err)
}
