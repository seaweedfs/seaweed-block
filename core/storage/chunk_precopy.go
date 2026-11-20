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
	precopyAttempts  = 5
	precopyDuration  = 60 * time.Second
	precopyAllowance = 1200 * time.Millisecond
	precopyMargin    = 100 * time.Millisecond
)

type precopySample struct {
	at    time.Time
	fixed time.Duration
	rate  float64
}

// PrecopyReport records admission estimates, not a worst-case latency proof.
type PrecopyReport struct {
	Attempts                   int
	TailBytes                  uint64
	Fixed, Estimate, SampleAge time.Duration
	BytesPerSecond             float64
	Reason                     string
	Frozen                     bool
}

// PrecopyDetach runs rolling cuts before the final detached cut. stopIO is
// called at the capture barrier only when the measured tail fits. It must
// atomically refuse new frontend I/O and return true only if all previously
// admitted I/O has drained, without blocking or calling the store. False leaves
// admission unchanged. The owner settles any error after a successful stop.
// capMu owns the callback and samples; no owner lock is taken while waiting
// for capMu. The callback must never wait for an I/O holding a backend read lock.
func (l *chunkLayerImpl) PrecopyDetach(ctx context.Context, stopIO func() bool) (ChunkCutResult, error) {
	if stopIO == nil {
		return ChunkCutResult{}, fmt.Errorf("%w: missing I/O stop", ErrPrecopyUnknown)
	}
	l.capMu.Lock()
	defer l.capMu.Unlock()
	if r, ok := l.storedDetach(); ok {
		return r, nil
	}
	ctx, cancel := context.WithTimeout(ctx, precopyDuration)
	defer cancel()
	var report PrecopyReport
	l.precopyFreeze = func(tail uint64) bool {
		report = l.precopyEstimate(tail, report.Attempts)
		if report.Reason != "fit" {
			return false
		}
		if !stopIO() {
			report.Reason = "admitted_io"
			return false
		}
		report.Frozen = true
		return true
	}
	defer func() { l.precopyFreeze = nil }()
	for attempt := 1; attempt <= precopyAttempts; attempt++ {
		report.Attempts = attempt
		if err := ctx.Err(); err != nil {
			return ChunkCutResult{Precopy: report}, err
		}
		r, err := l.captureLocked(ctx, false)
		r.Precopy = report
		if report.Frozen {
			return r, err
		}
		if err != nil {
			// An unresolved publication or lost authority is never another rolling point.
			if errors.Is(err, errChunkCASOutcomeUnknown) || errors.Is(err, errChunkLayerFenced) || errors.Is(err, errChunkLayerClosed) || ctx.Err() != nil {
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
		// p99 envelope. Whole-cut time includes transfer too, so it overcharges,
		// rather than omitting, manifest/read-back/local work. The stop is drained.
		if s.fixed > r.Fixed {
			r.Fixed = s.fixed
		}
		if s.rate > 0 {
			rates = append(rates, s.rate)
		}
	}
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

func (l *chunkLayerImpl) samplePrecopy(st chunkCutStats) {
	s := precopySample{at: time.Now(), fixed: time.Duration(st.TotalNs)}
	if st.UploadedBytes > 0 && st.UploadNs > 0 {
		s.rate = float64(st.UploadedBytes) / time.Duration(st.UploadNs).Seconds()
	}
	l.precopySamples = append(l.precopySamples, s)
	if len(l.precopySamples) > 32 {
		l.precopySamples = l.precopySamples[len(l.precopySamples)-32:]
	}
}
