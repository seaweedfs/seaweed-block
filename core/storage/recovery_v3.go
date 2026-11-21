package storage

import (
	"errors"
	"fmt"
)

// Impl-3 recovery decisions (format bump v3, DESIGN r15 sections 6.5 and
// 6.6; plan row 5). These are the pure parts: the scan start, the replay
// floor F, the contiguous prefix R with its end E, the R < W* decision for
// serving and installing descriptors, and the exact byte ranges of the drop
// fence. The record scan itself (nonce, cursor, lap and CRC trust order) is
// plan row 4; this file consumes its trusted records.

// ErrInstallSuffixDamaged: an installing volume's acknowledged install-log
// suffix is missing or corrupt (R < W*). The volume stays not ready, the
// install cannot be resumed, and only a covering superseding install (new
// BaseLSN >= W) may proceed (DESIGN 6.5, 8.3).
var ErrInstallSuffixDamaged = errors.New("storage: v3 install-log suffix damaged; resume refused")

// ErrBaseIncomplete: the descriptor is installing; the local image is not
// ready (DESIGN 8.3). It wraps ErrInstallSuffixDamaged and a WALIntegrity
// cause when the suffix is damaged.
var ErrBaseIncomplete = errors.New("storage: v3 BASE install incomplete; not ready")

// scanStartV3 is where recovery starts reading (DESIGN 6.5 step 2): the
// retained tail, or max(BaseCursor, retainedTail) when a BASE state is set
// (G2: after installed the tail may have moved past BaseCursor).
func scanStartV3(d descriptorV3) uint64 {
	if d.BaseState == BaseNone {
		return d.RetainedTail
	}
	return max(d.BaseCursor, d.RetainedTail)
}

// replayFloorV3 is F (DESIGN 6.5 step 1): LSNs at or below it need not be
// in this WAL.
func replayFloorV3(d descriptorV3) uint64 {
	if d.BaseState == BaseNone {
		return d.CheckpointLSN
	}
	return max(d.CheckpointLSN, d.BaseLSN)
}

// trustedRecordV3 is one record of the trusted chain, in cursor order, as
// the row-4 scan returns it: its first and last LSN (a batch covers a
// range; padding carries none) and the logical cursor just past it.
type trustedRecordV3 struct {
	FirstLSN uint64 // 0 for padding
	LastLSN  uint64
	End      uint64 // logical cursor after this record
	Padding  bool
}

// contiguousPrefixV3 computes R, the contiguous LSN prefix above F, and E,
// the logical cursor just past the last record that belongs to it (or to
// the already-covered region at or below F). Records above R are dropped by
// recovery. start is the scan start, used when no record qualifies. LSNs
// in a trusted chain only increase; a decrease is a contradiction and is
// refused.
func contiguousPrefixV3(start, F uint64, recs []trustedRecordV3) (R, E uint64, err error) {
	R, E = F, start
	var prev uint64
	for _, r := range recs {
		if r.Padding {
			// The loop returns at the first gap, so any padding seen here
			// lies inside the kept run: E moves past it.
			E = r.End
			continue
		}
		if r.FirstLSN == 0 || r.LastLSN < r.FirstLSN {
			return 0, 0, fmt.Errorf("storage: v3 recovery: malformed record span %d..%d", r.FirstLSN, r.LastLSN)
		}
		if prev != 0 && r.FirstLSN <= prev {
			return 0, 0, fmt.Errorf("storage: v3 recovery: LSN %d after %d in one trusted chain", r.FirstLSN, prev)
		}
		prev = r.LastLSN
		switch {
		case r.LastLSN <= R:
			E = r.End // at or below the floor or already counted: part of the kept run
		case r.FirstLSN <= R+1:
			R, E = r.LastLSN, r.End
		default:
			return R, E, nil // a gap: everything from here on is dropped
		}
	}
	return R, E, nil
}

// recoveryOutcomeV3 is what recovery does with the scan (DESIGN 6.5).
type recoveryOutcomeV3 struct {
	Start, F, R, E, WStar uint64
	// Serve: replay records <= R, drop the rest, fence above E, publish
	// retainedHead = E, admit. Installing: not ready (ErrBaseIncomplete);
	// Damaged says whether resume is refused.
	Serve      bool
	Installing bool
	Damaged    bool
}

// decideRecoveryV3 applies DESIGN 6.5 steps 3 to 7. maxDurable is the
// highest DurableLSN among the trusted records. It returns an error only
// for a serving descriptor with R < W* (WALIntegrity, wal_corrupt); the
// installing outcomes are reported in the result, damaged or not, and the
// caller wraps them in ErrBaseIncomplete.
func decideRecoveryV3(d descriptorV3, recs []trustedRecordV3, maxDurable uint64) (recoveryOutcomeV3, error) {
	o := recoveryOutcomeV3{Start: scanStartV3(d), F: replayFloorV3(d)}
	R, E, err := contiguousPrefixV3(o.Start, o.F, recs)
	if err != nil {
		ierr := NewWALIntegrityFailure(err, err.Error())
		if d.BaseState == BaseInstalling {
			// Installing keeps its outer not-ready outcome (DESIGN 8.3).
			o.Installing, o.Damaged = true, true
			return o, fmt.Errorf("%w: %w: %w", ErrBaseIncomplete, ErrInstallSuffixDamaged, ierr)
		}
		return o, ierr
	}
	o.R, o.E = R, E
	o.WStar = max(d.W, maxDurable)
	if d.BaseState == BaseInstalling {
		o.Installing = true
		o.Damaged = R < o.WStar
		return o, nil
	}
	if R < o.WStar {
		return o, NewWALIntegrityFailure(nil, fmt.Sprintf(
			"v3 recovery: synced LSN missing or corrupt: contiguous prefix R=%d below W*=%d (F=%d, start=%d)",
			R, o.WStar, o.F, o.Start))
	}
	o.Serve = true
	return o, nil
}

// installingErrorV3 is the typed not-ready error for an installing outcome.
func installingErrorV3(o recoveryOutcomeV3) error {
	if !o.Damaged {
		return ErrBaseIncomplete
	}
	return fmt.Errorf("%w: %w (R=%d < W*=%d): %w", ErrBaseIncomplete, ErrInstallSuffixDamaged, o.R, o.WStar,
		NewWALIntegrityFailure(nil, "acknowledged install-log record missing or corrupt"))
}

// fenceRangeV3 is one physical byte range of the WAL region to zero,
// relative to the region start (the caller adds the WAL offset).
type fenceRangeV3 struct{ Off, Len uint64 }

// fenceRangesV3 returns the exact physical byte ranges of the drop fence
// (DESIGN 6.6, r14): the logical interval [E, floor+walSize) mapped modulo
// the ring, split at the physical wrap into at most two ranges. Nothing
// outside it is touched: not [floor, E), and no rounding to pages. With
// sameNonce false (a reset between its two publications) the interval is
// empty, as it is when E >= floor+walSize.
func fenceRangesV3(E, floor, walSize uint64, sameNonce bool) ([]fenceRangeV3, error) {
	if walSize == 0 {
		return nil, fmt.Errorf("storage: v3 fence: zero WAL size")
	}
	if !sameNonce {
		return nil, nil
	}
	if E < floor {
		return nil, fmt.Errorf("storage: v3 fence: head %d below the reuse floor %d", E, floor)
	}
	// Interval-length arithmetic: floor+walSize is never formed, so a
	// history ending at the last representable cursor still recovers.
	// The logical end may exceed 2^64-2; only the physical bytes matter.
	used := E - floor
	if used >= walSize {
		return nil, nil
	}
	n := walSize - used // at most walSize
	start := E % walSize
	if start+n <= walSize {
		return []fenceRangeV3{{Off: start, Len: n}}, nil
	}
	first := walSize - start
	return []fenceRangeV3{{Off: start, Len: first}, {Off: 0, Len: n - first}}, nil
}
