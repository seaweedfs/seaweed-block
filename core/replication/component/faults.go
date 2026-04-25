package component

import (
	"fmt"
	"sync/atomic"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Substrate-wrap helpers for fault injection at the executor layer.
// Use with Cluster.WithPrimaryStorageWrap(NewXxxWrap(...)).
//
// All wraps embed `storage.LogicalStorage` and override one or more
// methods. The embedded interface forwards everything else
// transparently — only the overridden behavior changes.
//
// KNOWN WRAP-PATTERN LIMITATION: substrate-specific extension methods
// (e.g. walstore's `CheckpointLSN`) are MASKED by the wrap because
// embedding only exposes the LogicalStorage interface methods. The
// catch-up sender's `recovery_mode` label probe duck-types
// CheckpointLSN to distinguish walstore from smartwal — under a
// wrap, that probe always sees state_convergence regardless of the
// underlying substrate. Scenarios that need wrap + accurate mode
// label simultaneously cannot rely on AssertSawRecoveryMode for the
// walstore label. Forward-carry: refactor mode-label detection
// behind a substrate-reported `RecoveryMode()` method that wraps
// can forward explicitly.

// NewRecycledScanWrap returns a PrimaryStorageWrap that always
// returns ErrWALRecycled from ScanLBAs. Use to test the rebuild-
// escalation path without driving a real walstore checkpoint dance.
//
// Authoring:
//
//	c := component.NewCluster(t, component.Walstore).
//	    WithPrimaryStorageWrap(component.NewRecycledScanWrap()).
//	    Start()
//	result := c.CatchUpReplica(0)
//	// result.FailReason contains "WAL recycled" → engine maps to Rebuild
func NewRecycledScanWrap() PrimaryStorageWrap {
	return func(inner storage.LogicalStorage) storage.LogicalStorage {
		return &recycledScanWrap{LogicalStorage: inner}
	}
}

type recycledScanWrap struct{ storage.LogicalStorage }

func (r *recycledScanWrap) ScanLBAs(uint64, func(storage.RecoveryEntry) error) error {
	return storage.ErrWALRecycled
}

// NewSeverDuringScanWrap returns a PrimaryStorageWrap that emits
// `afterNEntries` entries successfully, then returns the given error
// from ScanLBAs (defaults to a generic "stream error" if nil).
//
// Use to test mid-stream drop / partial-progress scenarios:
//   - INV-REPL-CATCHUP-LASTSENT-MONOTONIC: lastSent reflects only
//     the entries successfully emitted before the drop
//   - INV-REPL-CATCHUP-DEADLINE-PER-CALL-SCOPE: the per-call
//     deadline is cleared even on mid-stream failure
//
// Authoring:
//
//	c := component.NewCluster(t, component.Walstore).
//	    WithPrimaryStorageWrap(component.NewSeverDuringScanWrap(5, nil)).
//	    Start()
//	c.PrimaryWriteN(20)
//	result := c.CatchUpReplica(0)
//	// result.AchievedLSN reflects entries 1..5 (at most), not 1..20
func NewSeverDuringScanWrap(afterNEntries int, severErr error) PrimaryStorageWrap {
	if severErr == nil {
		severErr = fmt.Errorf("component: substrate severed mid-scan after %d entries", afterNEntries)
	}
	return func(inner storage.LogicalStorage) storage.LogicalStorage {
		return &severDuringScanWrap{
			LogicalStorage: inner,
			afterN:         int32(afterNEntries),
			err:            severErr,
		}
	}
}

type severDuringScanWrap struct {
	storage.LogicalStorage
	afterN int32
	err    error
}

func (s *severDuringScanWrap) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	var emitted atomic.Int32
	intercept := func(e storage.RecoveryEntry) error {
		if emitted.Load() >= s.afterN {
			return s.err
		}
		if err := fn(e); err != nil {
			return err
		}
		emitted.Add(1)
		return nil
	}
	if err := s.LogicalStorage.ScanLBAs(fromLSN, intercept); err != nil {
		return err
	}
	// If the substrate exhausted before the intercept fired the sever,
	// don't fail closed — the caller asked to sever AT entry N+1, but
	// the substrate had ≤ N entries. That's a "no fault to inject"
	// case; return nil so scenarios are robust to substrate-emit
	// counts smaller than the sever threshold.
	return nil
}

// NewObservedScanWrap returns a wrap that counts ScanLBAs entries
// emitted and exposes the count via the returned counter pointer.
// Use for observability assertions ("did the sender emit N entries
// before the barrier?").
//
// Authoring:
//
//	count := new(atomic.Int32)
//	c := component.NewCluster(t, component.Walstore).
//	    WithPrimaryStorageWrap(component.NewObservedScanWrap(count)).
//	    Start()
//	c.PrimaryWriteN(10)
//	c.CatchUpReplica(0)
//	if count.Load() != 10 { t.Errorf("emit count = %d, want 10", count.Load()) }
func NewObservedScanWrap(counter *atomic.Int32) PrimaryStorageWrap {
	return func(inner storage.LogicalStorage) storage.LogicalStorage {
		return &observedScanWrap{LogicalStorage: inner, counter: counter}
	}
}

type observedScanWrap struct {
	storage.LogicalStorage
	counter *atomic.Int32
}

func (o *observedScanWrap) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	intercept := func(e storage.RecoveryEntry) error {
		if err := fn(e); err != nil {
			return err
		}
		o.counter.Add(1)
		return nil
	}
	return o.LogicalStorage.ScanLBAs(fromLSN, intercept)
}
