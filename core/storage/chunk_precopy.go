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
	precopyAttempts    = 5
	precopyDuration    = 60 * time.Second
	precopyAllowance   = 1200 * time.Millisecond
	precopyMargin      = 100 * time.Millisecond
	precopyDrainBound  = 100 * time.Millisecond
	precopyFreezeLimit = 5 * time.Second
	precopyFixedWindow = 5
	precopyFixedMaxAge = 15 * time.Minute
)

// PrecopyFixedParts are disjoint terms of the same retained fixed-cost sample.
type PrecopyFixedParts struct {
	Candidate      time.Duration `json:"candidateNs"`
	WindowSetup    time.Duration `json:"windowSetupNs"`
	PublishPrepare time.Duration `json:"publishPrepareNs"`
	Manifest       time.Duration `json:"manifestNs"`
	CommitState    time.Duration `json:"commitStateNs"`
	IntentCleanup  time.Duration `json:"intentCleanupNs"`
	NonPayloadCopy time.Duration `json:"nonPayloadCopyNs"`

	Drain        time.Duration `json:"drainNs"`
	StoreLock    time.Duration `json:"storeLockNs"`
	DataSync     time.Duration `json:"dataSyncNs"`
	HeadCAS      time.Duration `json:"headCASNs"`
	HeadReadback time.Duration `json:"headReadbackNs"`
	MapPersist   time.Duration `json:"mapPersistNs"`
	Other        time.Duration `json:"otherNs"`
}

func precopyFixedParts(st chunkCutStats, fixed time.Duration) PrecopyFixedParts {
	p := PrecopyFixedParts{Candidate: time.Duration(st.CandidateNs), WindowSetup: time.Duration(st.WindowSetupNs), PublishPrepare: time.Duration(st.PublishPrepareNs), Manifest: time.Duration(st.ManifestNs), CommitState: time.Duration(st.CommitStateNs), IntentCleanup: time.Duration(st.IntentCleanupNs), NonPayloadCopy: time.Duration(st.NonPayloadCopyNs), Drain: time.Duration(st.DrainNs), StoreLock: time.Duration(max(int64(0), st.LockWaitNs-st.DrainNs)), DataSync: time.Duration(st.SyncNs), HeadCAS: time.Duration(st.CASWriteNs), HeadReadback: time.Duration(st.HeadReadbackNs), MapPersist: time.Duration(st.MapPersistNs)}
	p.Other = max(time.Duration(0), fixed-p.measured())
	return p
}

type precopySample struct {
	at    time.Time
	fixed time.Duration
	parts PrecopyFixedParts
	rate  float64
}

type PrecopyAttempt struct {
	BarrierParts   PrecopyFixedParts `json:"barrierParts"`
	BarrierOverage time.Duration     `json:"barrierOverageNs"`
	// Attempt is the physical round ordinal, including measurement rounds.
	Attempt          int               `json:"attempt"`
	Drain            time.Duration     `json:"drainNs"`
	Barrier          time.Duration     `json:"barrierNs"`
	Pause            time.Duration     `json:"pauseNs"`
	Fixed            time.Duration     `json:"fixedNs"`
	FixedParts       PrecopyFixedParts `json:"fixedParts"`
	TailEstimate     time.Duration     `json:"tailEstimateNs"`
	DeclaredFreeze   time.Duration     `json:"declaredFreezeNs"`
	PreviousEstimate time.Duration     `json:"previousEstimateNs"`
	TailBytes        uint64            `json:"tailBytes"`
	RateSamples      int               `json:"rateSamples"`
	Estimate         time.Duration     `json:"estimateNs"`
	Reason           string            `json:"reason"`
	Frozen           bool              `json:"frozen"`
	Error            string            `json:"error,omitempty"`
}

// PrecopyReport records admission estimates, not a worst-case latency proof.
type PrecopyReport struct {
	// Live barrier prefix is diagnostic, not additive to the selected forecast.
	BarrierParts          PrecopyFixedParts `json:"barrierParts"`
	BarrierOverage        time.Duration     `json:"barrierOverageNs"`
	GuestFrozen           bool              `json:"guestFrozen,omitempty"`
	GuestFreezeElapsed    time.Duration     `json:"guestFreezeElapsedNs,omitempty"`
	RemainingFreeze       time.Duration     `json:"remainingFreezeNs,omitempty"`
	OverBudget            bool              `json:"overBudget,omitempty"`
	Rounds                []PrecopyAttempt  `json:"rounds,omitempty"`
	Drain                 time.Duration     `json:"drainNs"`
	DrainTimedOut         bool              `json:"drainTimedOut"`
	Attempts              int               `json:"attempts"`
	MeasurementRounds     int               `json:"measurementRounds"`
	SamplesAtStart        int               `json:"samplesAtStart"`
	TailBytes             uint64            `json:"tailBytes"`
	FixedParts            PrecopyFixedParts `json:"fixedParts"`
	TailEstimate          time.Duration     `json:"tailEstimateNs"`
	DeclaredFreeze        time.Duration     `json:"declaredFreezeNs"`
	PreviousEstimate      time.Duration     `json:"previousEstimateNs"`
	Fixed                 time.Duration     `json:"fixedNs"`
	Estimate              time.Duration     `json:"estimateNs"`
	SampleAge             time.Duration     `json:"sampleAgeNs"`
	BytesPerSecond        float64           `json:"bytesPerSecond"`
	RateSamples           int               `json:"rateSamples"`
	Reason                string            `json:"reason"`
	Frozen                bool              `json:"frozen"`
	FreezeElapsed         time.Duration     `json:"freezeElapsedNs"`
	ExtraCASAttempts      uint64            `json:"extraCASAttempts"`
	CASFailureTimeout     time.Duration     `json:"casFailureTimeoutNs"`
	DeclaredWorst         time.Duration     `json:"declaredWorstNs"`
	LatencyGateFailed     bool              `json:"latencyGateFailed"`
	BarrierBeforeFreeze   time.Duration     `json:"barrierBeforeFreezeNs"`
	TentativePauseTotal   time.Duration     `json:"tentativePauseTotalNs"`
	LongestTentativePause time.Duration     `json:"longestTentativePauseNs"`
}

// Finish records the whole freeze, including any owner-driven settlement.
// D_cas is failure allowance only; it never enters successful admission.
func (r *PrecopyReport) Finish(elapsed time.Duration, extraCAS uint64, failed bool) {
	r.FreezeElapsed, r.ExtraCASAttempts = elapsed, extraCAS
	r.CASFailureTimeout = chunkHeadOperationTimeout
	r.DeclaredWorst = r.Estimate + time.Duration(extraCAS)*chunkHeadOperationTimeout
	limit := precopyAllowance
	if r.GuestFrozen {
		r.DeclaredWorst = r.DeclaredFreeze + time.Duration(extraCAS)*chunkHeadOperationTimeout
		limit = precopyFreezeLimit
	} else if r.Reason == "no_progress" || r.Reason == "final_attempt" {
		limit = min(r.DeclaredFreeze, precopyFreezeLimit)
	}
	r.LatencyGateFailed = failed || elapsed > limit
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
	guestFreeze, externallyFrozen := FrozenDetachFromContext(ctx)

	if err := ctx.Err(); err != nil {
		return ChunkCutResult{}, err
	}
	if r, ok := l.storedDetach(); ok {
		r.Precopy.Frozen = true
		return r, nil
	}
	l.stMu.Lock()
	pending := l.pending != nil
	l.stMu.Unlock()
	if externallyFrozen && !pending {
		if err := l.admit(); err != nil {
			return ChunkCutResult{}, err
		}
		tail := l.candidateTail()
		r := l.precopyEstimate(tail.Bytes, 0)
		if !r.admitFrozen(guestFreeze) {
			return ChunkCutResult{Precopy: r}, frozenRefusal(r)
		}
	}
	ctx, countCAS := WithChunkCASAccounting(ctx)
	var report PrecopyReport
	var frozenAt time.Time
	var casAtFreeze uint64
	var pauseTotal, longestPause time.Duration
	var drain time.Duration
	var drainErr error
	var rounds []PrecopyAttempt
	samplesAtStart := l.precopyEstimate(0, 0).RateSamples
	warmAttempts, measurementRounds := 0, 0
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
	l.precopyFreeze = func(tail uint64, barrierStart time.Time, current chunkCutStats) bool {
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
		report.BarrierParts = precopyFixedParts(current, spent)
		if spent > report.Fixed {
			extra := spent - report.Fixed
			report.BarrierOverage = extra
			report.Estimate += extra
			report.FixedParts.Other += extra
			report.Fixed = spent
		}
		if report.Reason == "fit" && report.Estimate > precopyAllowance {
			report.Reason = "barrier_over_budget"
		}
		var previous *PrecopyAttempt
		if len(rounds) > 0 {
			previous = &rounds[len(rounds)-1]
		}
		admitted := false
		if externallyFrozen {
			admitted = report.admitFrozen(guestFreeze)
		} else {
			admitted = report.admitPrecopy(previous, len(rounds)+1 == precopyAttempts)
		}
		if !admitted {
			return false
		}
		if !externallyFrozen {
			report.DeclaredFreeze = report.Estimate
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
	maxAttempts := precopyAttempts
	if externallyFrozen {
		maxAttempts = 1
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Measurement rounds still consume the same five-round/60s budget.
		// Count an admission attempt only with a warm rate window at entry.
		if l.precopyEstimate(0, 0).RateSamples >= 3 {
			warmAttempts++
		} else {
			measurementRounds++
		}
		report = PrecopyReport{Attempts: warmAttempts, Rounds: rounds, SamplesAtStart: samplesAtStart, MeasurementRounds: measurementRounds}
		drain, drainErr = 0, nil
		if err := ctx.Err(); err != nil {
			return ChunkCutResult{Precopy: report}, err
		}
		pauseBefore := pauseTotal
		r, err := l.captureLocked(ctx, false)
		report.SamplesAtStart, report.MeasurementRounds = samplesAtStart, measurementRounds
		a := PrecopyAttempt{
			Attempt: attempt, Drain: drain, Pause: pauseTotal - pauseBefore, BarrierParts: report.BarrierParts, BarrierOverage: report.BarrierOverage,
			TailBytes: report.TailBytes, RateSamples: report.RateSamples,
			Estimate: report.Estimate, Reason: report.Reason, Frozen: report.Frozen,
			Fixed: report.Fixed, FixedParts: report.FixedParts, TailEstimate: report.TailEstimate, DeclaredFreeze: report.DeclaredFreeze, PreviousEstimate: report.PreviousEstimate,
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
		if errors.Is(err, ErrFrozenTailTooLarge) {
			return r, errors.Join(err, frozenRefusal(report))
		}
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
			elapsed := time.Since(frozenAt)
			if externallyFrozen {
				elapsed = time.Since(guestFreeze.StartedAt)
			}
			r.Precopy.Finish(elapsed, extra, err != nil)
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
	// Fixed work uses up to the last five fresh observations, independent of the
	// latest-three-rate window. Parts always belong to the selected observation.
	fixedCount := 0
	for i := len(l.precopySamples) - 1; i >= 0; i-- {
		sample := l.precopySamples[i]
		age := now.Sub(sample.at)
		if age < 0 {
			continue
		}
		if age <= precopyDuration && sample.rate > 0 {
			rates = append(rates, sample.rate)
		}
		if age >= precopyFixedMaxAge || fixedCount >= precopyFixedWindow {
			continue
		}
		fixedCount++
		r.SampleAge = max(r.SampleAge, age)
		if sample.fixed > r.Fixed {
			r.Fixed = sample.fixed
			r.FixedParts = precopyFixedPartsFromSample(sample)
		}
	}
	// Rates were visited newest first; only the latest three are used below.
	r.RateSamples = len(rates)
	if len(rates) > 3 {
		rates = rates[:3]
	}
	if fixedCount == 0 || r.Fixed <= 0 {
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
			r.TailEstimate = precopyDuration
			return r
		}
		r.TailEstimate = time.Duration(seconds * float64(time.Second))
		r.Estimate += r.TailEstimate
	}
	r.Reason = "tail_too_large"
	if r.Estimate <= precopyAllowance {
		r.Reason = "fit"
	}
	return r
}

// Rounds shrink the tail; a plateau or the final round may use the full
// declared freeze budget. The final flag is the physical round, not the number
// of warm admission attempts (measurement rounds consume the same limit).
func (r *PrecopyReport) admitPrecopy(previous *PrecopyAttempt, final bool) bool {
	if r.Reason == "samples_unknown" || r.Estimate <= 0 {
		return false
	}
	if r.Estimate <= precopyAllowance {
		r.Reason = "fit"
		return true
	}
	if r.Estimate > precopyFreezeLimit {
		r.Reason = "freeze_budget_exceeded"
		return false
	}
	if comparablePrecopyAttempt(previous) {
		r.PreviousEstimate = previous.Estimate
		// Division first avoids overflow while retaining nanosecond precision.
		threshold := previous.Estimate - previous.Estimate/10
		if r.Estimate >= threshold {
			r.Reason = "no_progress"
			return true
		}
	}
	if final {
		r.Reason = "final_attempt"
		return true
	}
	return false
}

func precopyFixedPartsFromSample(s precopySample) PrecopyFixedParts {
	p := s.parts
	p.Other = max(time.Duration(0), s.fixed-p.measured())
	return p
}

func (l *chunkLayerImpl) samplePrecopy(st chunkCutStats, fixedNs int64) {
	// A no-op root read is not a measurement of final CAS + confirmation.
	if st.CASNs <= 0 {
		return
	}
	s := precopySample{at: time.Now(), fixed: time.Duration(max(int64(1), fixedNs))}
	s.parts = precopyFixedParts(st, s.fixed)
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

// Only a completed cost estimate is comparable. A drain/before-barrier refusal
// can carry Fixed+margin without a known tail, just like samples_unknown.
func comparablePrecopyAttempt(a *PrecopyAttempt) bool {
	if a == nil || a.Estimate <= 0 || a.Error != "" {
		return false
	}
	switch a.Reason {
	case "fit", "tail_too_large", "barrier_over_budget", "freeze_budget_exceeded", "no_progress", "final_attempt", "admitted_io":
		return true
	default:
		return false
	}
}

func (p PrecopyFixedParts) measured() time.Duration {
	return p.Drain + p.StoreLock + p.DataSync + p.HeadCAS + p.HeadReadback + p.MapPersist + p.Candidate + p.WindowSetup + p.PublishPrepare + p.Manifest + p.CommitState + p.IntentCleanup + p.NonPayloadCopy
}

// Candidate count is conservative and advisory while guest writes continue.
// The same immutable rate observation supplies both the estimate and its rate.
func (l *chunkLayerImpl) candidateTail() *ChunkCandidateTail {
	candidates := l.captureCandidates()
	l.stMu.Lock()
	for idx := range l.dirty {
		candidates[idx] = true
	}
	if l.allDirty {
		for idx := uint64(0); idx < l.numChunks; idx++ {
			if !l.absent[idx] || l.chunkHasLocalLocked(idx) {
				candidates[idx] = true
			}
		}
	}
	l.stMu.Unlock()
	bytes := uint64(len(candidates)) * uint64(l.blockSize) * l.chunkBlocks
	r := l.precopyEstimate(bytes, 0)
	return &ChunkCandidateTail{Bytes: bytes, Estimate: r.TailEstimate, BytesPerSecond: r.BytesPerSecond, RateSamples: r.RateSamples, SampleAge: r.SampleAge, Known: bytes == 0 || r.BytesPerSecond > 0}
}
