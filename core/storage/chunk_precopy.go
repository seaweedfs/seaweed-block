package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrPrecopyUnknown      = errors.New("storage: pre-copy cost unknown")
	ErrPrecopyNotConverged = errors.New("storage: pre-copy not converged")
)

const (
	precopyAttempts   = 5
	precopyDuration   = 60 * time.Second
	precopyAllowance  = 1200 * time.Millisecond
	precopyMargin     = 100 * time.Millisecond
	precopyDrainBound = 100 * time.Millisecond
)

type precopySample struct {
	at    time.Time
	fixed time.Duration
	rate  float64
}

type PrecopyAttempt struct {
	Attempt     int           `json:"attempt"`
	Drain       time.Duration `json:"drainNs"`
	Barrier     time.Duration `json:"barrierNs"`
	Pause       time.Duration `json:"pauseNs"`
	TailBytes   uint64        `json:"tailBytes"`
	RateSamples int           `json:"rateSamples"`
	Estimate    time.Duration `json:"estimateNs"`
	Reason      string        `json:"reason"`
	Frozen      bool          `json:"frozen"`
	Error       string        `json:"error,omitempty"`
}

// PrecopyReport records admission estimates, not a worst-case latency proof.
type PrecopyReport struct {
	Rounds                []PrecopyAttempt `json:"rounds,omitempty"`
	Drain                 time.Duration    `json:"drainNs"`
	DrainTimedOut         bool             `json:"drainTimedOut"`
	Attempts              int              `json:"attempts"`
	TailBytes             uint64           `json:"tailBytes"`
	Fixed                 time.Duration    `json:"fixedNs"`
	Estimate              time.Duration    `json:"estimateNs"`
	SampleAge             time.Duration    `json:"sampleAgeNs"`
	BytesPerSecond        float64          `json:"bytesPerSecond"`
	RateSamples           int              `json:"rateSamples"`
	Reason                string           `json:"reason"`
	Frozen                bool             `json:"frozen"`
	FreezeElapsed         time.Duration    `json:"freezeElapsedNs"`
	ExtraCASAttempts      uint64           `json:"extraCASAttempts"`
	CASFailureTimeout     time.Duration    `json:"casFailureTimeoutNs"`
	DeclaredWorst         time.Duration    `json:"declaredWorstNs"`
	LatencyGateFailed     bool             `json:"latencyGateFailed"`
	BarrierBeforeFreeze   time.Duration    `json:"barrierBeforeFreezeNs"`
	TentativePauseTotal   time.Duration    `json:"tentativePauseTotalNs"`
	LongestTentativePause time.Duration    `json:"longestTentativePauseNs"`
}

// Finish records the whole freeze, including any owner-driven settlement.
// D_cas is failure allowance only; it never enters successful admission.
func (r *PrecopyReport) Finish(elapsed time.Duration, extraCAS uint64, failed bool) {
	r.FreezeElapsed, r.ExtraCASAttempts = elapsed, extraCAS
	r.CASFailureTimeout = chunkHeadOperationTimeout
	r.DeclaredWorst = r.Estimate + time.Duration(extraCAS)*chunkHeadOperationTimeout
	r.LatencyGateFailed = failed || elapsed > precopyAllowance
}

// PrecopyDetach runs rolling cuts before the final detached cut. stopIO is
// called at the capture barrier only when the measured tail fits. It must
// atomically refuse new frontend I/O and return true only if all previously
// admitted I/O has drained, without blocking or calling the store. False leaves
// admission unchanged. The owner settles any error after a successful stop.
// capMu owns the callback and samples; no owner lock is taken while waiting
// for capMu. The callback must never wait for an I/O holding a backend read lock.
func (l *chunkLayerImpl) PrecopyDetach(ctx context.Context, stopIO func() bool) (ChunkCutResult, error) {
	return l.precopyDetach(ctx, nil, stopIO)
}

// prepare closes frontend admission and drains before the store barrier. Its
// release reopens only that tentative admission; it must not undo a host fence.
func (l *chunkLayerImpl) PrecopyDetachWithAdmission(ctx context.Context, prepare func(context.Context) (func(), error), stopIO func() bool) (ChunkCutResult, error) {
	return l.precopyDetach(ctx, prepare, stopIO)
}

func (l *chunkLayerImpl) precopyDetach(ctx context.Context, prepare func(context.Context) (func(), error), stopIO func() bool) (ChunkCutResult, error) {
	if stopIO == nil {
		return ChunkCutResult{}, fmt.Errorf("%w: missing I/O stop", ErrPrecopyUnknown)
	}
	ctx, cancel := context.WithTimeout(ctx, precopyDuration)
	defer cancel()
	if err := l.lockPrecopy(ctx); err != nil {
		return ChunkCutResult{}, err
	}
	defer l.capMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ChunkCutResult{}, err
	}
	if r, ok := l.storedDetach(); ok {
		r.Precopy.Frozen = true
		return r, nil
	}
	ctx, countCAS := WithChunkCASAccounting(ctx)
	var report PrecopyReport
	var frozenAt time.Time
	var casAtFreeze uint64
	var pauseTotal, longestPause time.Duration
	var drain time.Duration
	var drainErr error
	var rounds []PrecopyAttempt
	if prepare != nil {
		l.precopyPrepare = func(ctx context.Context) (func(), error) {
			dc, cancel := context.WithTimeout(ctx, precopyDrainBound)
			defer cancel()
			start := time.Now()
			release, err := prepare(dc)
			drain = time.Since(start)
			drainErr = err
			return release, err
		}
	}

	l.precopyPause = func(pause time.Duration) {
		pauseTotal += pause
		if pause > longestPause {
			longestPause = pause
		}
	}
	l.precopyFreeze = func(tail uint64, barrierStart time.Time) bool {
		report = l.precopyEstimate(tail, report.Attempts)
		report.Drain = drain
		report.DrainTimedOut = errors.Is(drainErr, context.DeadlineExceeded)
		if drainErr != nil {
			report.Reason = "drain_refused"
			if report.DrainTimedOut {
				report.Reason = "drain_timeout"
			}
			return false
		}
		spent := time.Since(barrierStart)
		if spent > report.Fixed {
			report.Estimate += spent - report.Fixed
		}
		if report.Reason == "fit" && report.Estimate > precopyAllowance {
			report.Reason = "barrier_over_budget"
		}
		if report.Reason != "fit" {
			return false
		}
		if !stopIO() {
			report.Reason = "admitted_io"
			return false
		}
		report.Frozen = true
		report.BarrierBeforeFreeze = time.Since(barrierStart)
		frozenAt, casAtFreeze = barrierStart, countCAS()
		return true
	}
	defer func() { l.precopyFreeze = nil; l.precopyPause = nil; l.precopyPrepare = nil }()
	for attempt := 1; attempt <= precopyAttempts; attempt++ {
		report = PrecopyReport{Attempts: attempt, Rounds: rounds}
		drain, drainErr = 0, nil
		if err := ctx.Err(); err != nil {
			return ChunkCutResult{Precopy: report}, err
		}
		pauseBefore := pauseTotal
		r, err := l.captureLocked(ctx, false)
		a := PrecopyAttempt{
			Attempt: attempt, Drain: drain, Pause: pauseTotal - pauseBefore,
			TailBytes: report.TailBytes, RateSamples: report.RateSamples,
			Estimate: report.Estimate, Reason: report.Reason, Frozen: report.Frozen,
		}
		a.Barrier = max(time.Duration(0), a.Pause-a.Drain)
		if a.Reason == "" {
			a.Reason = "before_barrier"
		}
		if err != nil {
			a.Error = err.Error()
		}
		rounds = append(rounds, a)
		report.Rounds = rounds
		report.TentativePauseTotal, report.LongestTentativePause = pauseTotal, longestPause
		r.Precopy = report
		if errors.Is(err, errChunkLayerDetached) {
			if previous, ok := l.storedDetach(); ok {
				previous.Precopy = report
				previous.Precopy.Frozen = true
				previous.Precopy.Reason = "previous_detach_applied"
				return previous, nil
			}
		}
		if report.Frozen {
			attempts := countCAS() - casAtFreeze
			extra := uint64(0)
			if attempts > 0 {
				extra = attempts - 1
			}
			r.Precopy.Finish(time.Since(frozenAt), extra, err != nil)
			return r, err
		}
		if err != nil {
			// An unresolved publication or lost authority is never another rolling point.
			if errors.Is(err, errChunkCASOutcomeUnknown) || errors.Is(err, errChunkLayerFenced) || errors.Is(err, errChunkLayerClosed) || ctx.Err() != nil || !(errors.Is(err, errCutCOWAborted) || errors.Is(err, errChunkRemoteUnavailable) || errors.Is(err, errChunkHeadPrecondition)) {
				return r, err
			}
		}
	}
	cause := ErrPrecopyNotConverged
	if report.Reason == "samples_unknown" {
		cause = ErrPrecopyUnknown
	}
	return ChunkCutResult{Precopy: report}, fmt.Errorf("%w: reason=%s attempts=%d tail=%d rate=%.0f age=%s fixed=%s estimate=%s allowance=%s", cause, report.Reason, report.Attempts, report.TailBytes, report.BytesPerSecond, report.SampleAge, report.Fixed, report.Estimate, precopyAllowance)
}

func (l *chunkLayerImpl) precopyEstimate(tail uint64, attempts int) PrecopyReport {
	r := PrecopyReport{Attempts: attempts, TailBytes: tail, Reason: "samples_unknown"}
	now := time.Now()
	var rates []float64
	for _, s := range l.precopySamples {
		age := now.Sub(s.at)
		if age > precopyDuration {
			continue
		}
		if age > r.SampleAge {
			r.SampleAge = age
		}
		// With at most 32 observations the maximum is a conservative empirical
		// p99 envelope of barrier-to-settlement non-payload work. Serving
		// preparation and host-budget waits are outside the freeze. The stop
		// is already drained; metadata and CAS/read-back remain included.
		if s.fixed > r.Fixed {
			r.Fixed = s.fixed
		}
		if s.rate > 0 {
			rates = append(rates, s.rate)
		}
	}
	r.RateSamples = len(rates)
	if r.Fixed <= 0 {
		return r
	}
	r.Estimate = r.Fixed + precopyMargin
	if tail != 0 {
		if len(rates) < 3 {
			return r
		}
		minRate := rates[len(rates)-3]
		for _, rate := range rates[len(rates)-2:] {
			if rate < minRate {
				minRate = rate
			}
		}
		r.BytesPerSecond = minRate / 2
		seconds := float64(tail) / r.BytesPerSecond
		if seconds > precopyDuration.Seconds() {
			r.Reason = "tail_too_large"
			r.Estimate = precopyDuration
			return r
		}
		r.Estimate += time.Duration(seconds * float64(time.Second))
	}
	r.Reason = "tail_too_large"
	if r.Estimate <= precopyAllowance {
		r.Reason = "fit"
	}
	return r
}

func (l *chunkLayerImpl) samplePrecopy(st chunkCutStats, fixedNs int64) {
	// A no-op root read is not a measurement of final CAS + confirmation.
	if st.CASNs <= 0 {
		return
	}
	s := precopySample{at: time.Now(), fixed: time.Duration(max(int64(1), fixedNs))}
	if st.UploadedBytes > 0 && st.PayloadNs > 0 {
		s.rate = float64(st.UploadedBytes) / time.Duration(st.PayloadNs).Seconds()
	}
	l.precopySamples = append(l.precopySamples, s)
	if len(l.precopySamples) > 32 {
		l.precopySamples = l.precopySamples[len(l.precopySamples)-32:]
	}
}

// A queued caller's cooperative budget starts before pipeline ownership.
// No background lock waiter can outlive cancellation or capture a later cut.
func (l *chunkLayerImpl) lockPrecopy(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.capMu.TryLock() {
		return nil
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if l.capMu.TryLock() {
				return nil
			}
		}
	}
}
