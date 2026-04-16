package storage

import (
	"errors"
	"sync/atomic"
	"time"
)

// errVolumeClosed is returned by walAdmission.Acquire if the store
// closes while the caller is waiting.
var errVolumeClosed = errors.New("storage: volume closed during admission wait")

// walAdmission controls write admission based on WAL pressure
// watermarks. Limits concurrent writers via a counting semaphore
// and gates new admission when WAL usage exceeds configurable
// thresholds.
//
// Watermark behavior (faithful to V2 weed/storage/blockvol/wal_admission.go):
//
//   - below soft watermark: writes pass through immediately
//   - between soft and hard: writes admitted with a small delay
//     scaled by where in the soft band the pressure sits — gives
//     the flusher headroom and desynchronizes concurrent writers
//   - above hard watermark: new writes block until pressure drops
//     OR the deadline expires (errWALFull)
//
// One deadline governs the whole Acquire call. Time spent waiting
// for the hard watermark to clear reduces the budget available
// for semaphore acquisition.
type walAdmission struct {
	sem      chan struct{} // counting semaphore for concurrent appenders
	walUsed  func() float64
	notifyFn func() // wakes flusher
	softMark float64
	hardMark float64
	closedFn func() bool

	sleepFn func(time.Duration) // replaceable for tests

	softPressureWaitNs atomic.Int64
	hardPressureWaitNs atomic.Int64
}

type walAdmissionConfig struct {
	MaxConcurrent int
	SoftWatermark float64
	HardWatermark float64
	WALUsedFn     func() float64
	NotifyFn      func()
	ClosedFn      func() bool
}

// newWALAdmission constructs an admission controller. Caller must
// set every field of cfg — there are no defaults. Behavior mirrors
// V2's weed/storage/blockvol/wal_admission.go NewWALAdmission:
// passing a zero watermark or nil NotifyFn/ClosedFn is a bug, not
// a shortcut.
func newWALAdmission(cfg walAdmissionConfig) *walAdmission {
	return &walAdmission{
		sem:      make(chan struct{}, cfg.MaxConcurrent),
		walUsed:  cfg.WALUsedFn,
		notifyFn: cfg.NotifyFn,
		softMark: cfg.SoftWatermark,
		hardMark: cfg.HardWatermark,
		closedFn: cfg.ClosedFn,
		sleepFn:  time.Sleep,
	}
}

// PressureState returns "hard", "soft", or "normal" based on current WAL usage.
func (a *walAdmission) PressureState() string {
	used := a.walUsed()
	if used >= a.hardMark {
		return "hard"
	}
	if used >= a.softMark {
		return "soft"
	}
	return "normal"
}

func (a *walAdmission) SoftPressureWaitNs() int64 { return a.softPressureWaitNs.Load() }
func (a *walAdmission) HardPressureWaitNs() int64 { return a.hardPressureWaitNs.Load() }
func (a *walAdmission) SoftMark() float64         { return a.softMark }
func (a *walAdmission) HardMark() float64         { return a.hardMark }

// Acquire blocks until a write slot is available or the deadline
// expires. Returns errWALFull on timeout, errVolumeClosed if the
// store closes during the wait.
func (a *walAdmission) Acquire(timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	pressure := a.walUsed()

	// Hard watermark: wait for flusher to drain before competing
	// for the semaphore.
	if pressure >= a.hardMark {
		a.notifyFn()
		hardStart := time.Now()
		for a.walUsed() >= a.hardMark {
			if a.closedFn() {
				a.hardPressureWaitNs.Add(time.Since(hardStart).Nanoseconds())
				return errVolumeClosed
			}
			a.notifyFn()
			select {
			case <-deadline.C:
				a.hardPressureWaitNs.Add(time.Since(hardStart).Nanoseconds())
				return errWALFull
			default:
			}
			a.sleepFn(2 * time.Millisecond)
		}
		a.hardPressureWaitNs.Add(time.Since(hardStart).Nanoseconds())
		// Pressure dropped — fall through to semaphore acquisition.
	} else if pressure >= a.softMark {
		// Soft watermark: small delay to desynchronize writer herd.
		a.notifyFn()
		scale := (pressure - a.softMark) / (a.hardMark - a.softMark)
		if scale > 1 {
			scale = 1
		}
		// Scale: softMark→0ms, hardMark→5ms.
		delay := time.Duration(scale * 5 * float64(time.Millisecond))
		if delay > 0 {
			softStart := time.Now()
			a.sleepFn(delay)
			a.softPressureWaitNs.Add(time.Since(softStart).Nanoseconds())
		}
	}

	// Acquire semaphore slot using the same deadline.
	select {
	case a.sem <- struct{}{}:
		return nil
	default:
	}
	closeTick := time.NewTicker(5 * time.Millisecond)
	defer closeTick.Stop()
	for {
		select {
		case a.sem <- struct{}{}:
			return nil
		case <-deadline.C:
			return errWALFull
		case <-closeTick.C:
			if a.closedFn() {
				return errVolumeClosed
			}
		}
	}
}

// Release returns a write slot to the semaphore.
func (a *walAdmission) Release() { <-a.sem }
