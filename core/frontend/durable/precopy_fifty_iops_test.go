package durable_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Backend Write+Sync models the guest's 4KiB synchronous command stream. The
// peer reply is held until a separate control probe witnesses closed admission,
// then released after20ms. The first attempt must fit only when its measured
// estimate fits the allowance or the declared small-tail plateau exception;
// host I/O costs are not fixture constants.
// This is a forced schedule, not physical NVMe proof.
func TestPrecopyFiftyIOPSSyncConvergesFirstAttempt(t *testing.T) {
	precopyFiftyIOPS(t, 3, false, 0)
}

func TestPrecopyFiftyIOPSColdCostsKeepAdmissionHonest(t *testing.T) {
	precopyFiftyIOPS(t, 1, true, 0)
}

func TestPrecopyFiftyIOPSSlowMeasuredCostsKeepServing(t *testing.T) {
	precopyFiftyIOPS(t, 3, false, 1300*time.Millisecond)
}

func precopyFiftyIOPS(t *testing.T, samples byte, wantUnknown bool, fixedDelay time.Duration) {
	t.Helper()
	remote := &precopyFixedSampleRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	p := precopyProvider(t, remote, &parkView{stubView: v1View()})
	openRecovered(t, p)
	backend, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	remote.nextDelay.Store(int64(fixedDelay))
	for i := byte(1); i <= samples; i++ {
		if _, err := backend.Write(context.Background(), 4096, block(i)); err != nil {
			t.Fatal(err)
		}
		if err := backend.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
			t.Fatal(err)
		}
	}
	before, err := remote.GetHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	releasePeer := func() { releaseOnce.Do(func() { close(release) }) }
	if err := backend.(*durable.StorageBackend).SetWriteObserver(writeObserverFunc{sync: func(ctx context.Context, _ uint64) error {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	writerDone, probeDone := make(chan struct{}), make(chan struct{})
	var stopped atomic.Bool
	var guestErrors atomic.Int64
	t.Cleanup(func() {
		stopped.Store(true)
		releasePeer()
		cancel()
		for _, done := range []chan struct{}{writerDone, probeDone} {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("writer/probe did not join")
			}
		}
	})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			_, err := backend.Write(ctx, 4096, block(0x5b))
			if err == nil {
				err = backend.Sync(ctx)
			}
			if err != nil {
				if !stopped.Load() {
					guestErrors.Add(1)
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		close(probeDone)
		t.Fatal("synchronous writer not admitted")
	}
	go func() {
		defer close(probeDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			probe, stop := context.WithTimeout(ctx, 5*time.Millisecond)
			_, err := backend.Write(probe, 8192, block(0x6d))
			stop()
			if errors.Is(err, frontend.ErrBackendBusy) && errors.Is(err, context.DeadlineExceeded) {
				timer := time.NewTimer(20 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
					releasePeer()
				case <-ctx.Done():
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	result, _, err := p.DetachVolume(ctx, "v1")
	stopped.Store(true)
	t.Logf("attempts=%d measurement_rounds=%d samples_at_start=%d drain=%s estimate=%s frozen=%t guest_errors=%d", result.Precopy.Attempts, result.Precopy.MeasurementRounds, result.Precopy.SamplesAtStart, result.Precopy.Drain, result.Precopy.Estimate, result.Precopy.Frozen, guestErrors.Load())
	if accountingErr := precopyFiftyRoundAccounting(result.Precopy); accountingErr != nil {
		t.Fatal(accountingErr)
	}
	if wantUnknown {
		if result.Precopy.Frozen {
			// Staging or later rounds may leave an empty tail, which needs
			// only measured fixed cost. A nonempty first tail still needs
			// three upload samples; the one-sample fixture cannot supply them.
			if err != nil || (len(result.Precopy.Rounds) == 1 && result.Precopy.TailBytes != 0) || guestErrors.Load() != 0 || !bytes.Equal(headBlock(t, remote.MemChunkRemote, 1), block(0x5b)) {
				t.Fatalf("cold samples claimed premature fit: report=%+v guest_errors=%d err=%v", result.Precopy, guestErrors.Load(), err)
			}
			return
		}
		if !errors.Is(err, storage.ErrPrecopyUnknown) || guestErrors.Load() != 0 {
			t.Fatalf("unknown cost did not preserve live service: report=%+v guest_errors=%d err=%v", result.Precopy, guestErrors.Load(), err)
		}
		if _, err := backend.Write(ctx, 12288, block(0x7d)); err != nil {
			t.Fatal("unknown-cost refusal stopped the frontend:", err)
		}
		return
	}
	// A loaded host may correctly refuse an expensive tail. Preserve the
	// first-attempt oracle whenever it fits: EVERY earlier attempt must have
	// been refused by both the ordinary allowance and the small-tail plateau rule.
	if fixedDelay > 0 && result.Precopy.Fixed < fixedDelay {
		t.Fatalf("lost slow fixed sample: %+v", result.Precopy)
	}
	const allowance = 1200 * time.Millisecond
	if len(result.Precopy.Rounds) == 0 {
		t.Fatal("no admission attempts")
	}
	if result.Precopy.Rounds[len(result.Precopy.Rounds)-1].Frozen != result.Precopy.Frozen {
		t.Fatal("final freeze disagrees with attempt report")
	}
	for i, round := range result.Precopy.Rounds {
		eligible := round.Estimate <= allowance
		reason := "fit"
		if !eligible && i > 0 && round.TailEstimate <= allowance/4 && round.Estimate >= result.Precopy.Rounds[i-1].Estimate && round.Estimate <= 5*time.Second {
			eligible = true
			reason = "fixed_cost_over_allowance"
		}
		if round.Frozen {
			if !eligible || i != len(result.Precopy.Rounds)-1 || round.Reason != reason || round.DeclaredFreeze != round.Estimate || round.Error != "" {
				t.Fatalf("invalid fitting attempt: %+v", round)
			}
		} else if eligible || (round.Reason != "tail_too_large" && round.Reason != "barrier_over_budget" && round.Reason != "fixed_cost_exceeds_freeze_budget") || round.Error != "" {
			t.Fatalf("refused an eligible attempt or hid a failure: %+v", round)
		}
	}
	if !result.Precopy.Frozen {
		if !errors.Is(err, storage.ErrPrecopyNotConverged) || len(result.Precopy.Rounds) != 5 || result.Precopy.Attempts != 5 || guestErrors.Load() != 0 {
			t.Fatalf("wrong over-budget outcome: report=%+v err=%v", result.Precopy, err)
		}
		// Settle both guest workers before proving the refused owner is still live.
		releasePeer()
		cancel()
		for _, done := range []chan struct{}{writerDone, probeDone} {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("guest worker did not settle")
			}
		}
		liveCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		head, headErr := remote.GetHead(liveCtx)
		if headErr != nil || head.Gen < before.Gen || head.RemoteLSN < before.RemoteLSN || head.Epoch != before.Epoch || head.Holder != before.Holder || head.State != storage.ChunkHeadActive || head.DetachedBy != nil {
			t.Fatalf("refusal lost the active owner or regressed HEAD: %+v %v", head, headErr)
		}
		if _, writeErr := backend.Write(liveCtx, 12288, block(0x7d)); writeErr != nil {
			t.Fatal("refusal stopped service:", writeErr)
		}
		for _, read := range []struct {
			offset int64
			value  byte
		}{{4096, 0x5b}, {12288, 0x7d}} {
			got := make([]byte, 4096)
			n, readErr := backend.Read(liveCtx, read.offset, got)
			if readErr != nil || n != len(got) || !bytes.Equal(got, block(read.value)) {
				t.Fatalf("refused-owner bytes at %d: n=%d err=%v", read.offset, n, readErr)
			}
		}
		return
	}
	if err != nil {
		t.Fatalf("50IOPS cutover: report=%+v err=%v", result.Precopy, err)
	}
	if guestErrors.Load() != 0 {
		t.Fatalf("guest errors before cutover: %d", guestErrors.Load())
	}
	if !bytes.Equal(headBlock(t, remote.MemChunkRemote, 1), block(0x5b)) {
		t.Fatal("final cut lost synchronous writer bytes")
	}
}

// Both outcomes consume the same physical-round budget; admission and
// measurement counters must account for every recorded round, including success.
func precopyFiftyRoundAccounting(report storage.PrecopyReport) error {
	if len(report.Rounds) == 0 || len(report.Rounds) > 5 {
		return fmt.Errorf("physical-round budget: rounds=%d limit=5", len(report.Rounds))
	}
	if report.Attempts+report.MeasurementRounds != len(report.Rounds) {
		return fmt.Errorf("attempt accounting: admission=%d measurement=%d rounds=%d", report.Attempts, report.MeasurementRounds, len(report.Rounds))
	}
	for i, round := range report.Rounds {
		if round.Attempt != i+1 {
			return fmt.Errorf("physical ordinal: round=%d attempt=%d", i+1, round.Attempt)
		}
	}
	return nil
}

func TestPrecopyFiftyIOPSRoundAccounting(t *testing.T) {
	makeReport := func(n int, frozen bool) storage.PrecopyReport {
		report := storage.PrecopyReport{Attempts: n, Frozen: frozen}
		for i := 1; i <= n; i++ {
			report.Rounds = append(report.Rounds, storage.PrecopyAttempt{Attempt: i, Estimate: 1300 * time.Millisecond, Reason: "tail_too_large"})
		}
		if frozen {
			last := &report.Rounds[n-1]
			last.Frozen, last.Estimate, last.Reason = true, 500*time.Millisecond, "fit"
		}
		return report
	}
	for _, frozen := range []bool{false, true} {
		t.Run(fmt.Sprintf("five_rounds_frozen_%t", frozen), func(t *testing.T) {
			if err := precopyFiftyRoundAccounting(makeReport(5, frozen)); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("five_refusals_then_sixth_fit", func(t *testing.T) {
		report := makeReport(6, true)
		if err := precopyFiftyRoundAccounting(report); err == nil {
			t.Fatal("five expensive refusals followed by a sixth fit escaped the physical-round cap")
		}
	})
	t.Run("six_refusals", func(t *testing.T) {
		if err := precopyFiftyRoundAccounting(makeReport(6, false)); err == nil {
			t.Fatal("six refusals escaped the physical-round cap")
		}
	})
	t.Run("measurement_rounds_count_toward_cap", func(t *testing.T) {
		report := makeReport(5, true)
		report.Attempts, report.MeasurementRounds = 3, 2
		if err := precopyFiftyRoundAccounting(report); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("successful_attempt_not_counted", func(t *testing.T) {
		report := makeReport(5, true)
		report.Attempts--
		if err := precopyFiftyRoundAccounting(report); err == nil {
			t.Fatal("successful round absent from accounting")
		}
	})
}
