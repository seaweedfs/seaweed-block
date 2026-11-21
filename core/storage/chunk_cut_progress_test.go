package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// The spill budget, rather than a fixed chunk count, determines when local
// reads must be uploaded before fixing the cut frontier.
func TestChunkStageV3_SpillBudgetTriggersPrecopy(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 5)
	l.cfg.COWSpillLimit = 2 * int64(ctChunkBlocks*ctBlockSize)
	rounds := 0
	l.testAfterStageHook = func(int) {
		rounds++
		// A hot subset smaller than the cap remains writable while staging.
		w.write(t, s, 1, 0xb1)
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if rounds == 0 || rounds > 2 {
		t.Fatalf("precopy rounds=%d, want progress before the barrier", rounds)
	}
	if r.Stats.StagedChunksUsed < 4 {
		t.Fatalf("reused %d unchanged staged chunks", r.Stats.StagedChunksUsed)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// A known-aborted point retains immutable uploaded chunks and their intents.
// Retrying the current frontier must not send unchanged payloads again.
func TestChunkStageV3_AbortedPointReusesUploads(t *testing.T) {
	_, f, l, _ := precopyLayer(t, 1, 5)
	l.cfg.COWSpillLimit = int64(ctChunkBlocks * ctBlockSize)
	ctx, cancel := context.WithCancel(context.Background())
	l.testBeforeBarrierLockHook = cancel
	before := f.stats().head
	_, err := l.Cut(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted cut: %v", err)
	}
	if f.stats().head.Gen != before.Gen {
		t.Fatal("aborted point moved HEAD")
	}
	f.mu.Lock()
	sent := f.putCalls
	f.mu.Unlock()
	if sent == 0 {
		t.Fatal("no uploads before abort")
	}
	l.testBeforeBarrierLockHook = nil
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	resent := f.putCalls - sent
	f.mu.Unlock()
	if resent != 0 {
		t.Fatalf("retried unchanged point uploaded %d chunks", resent)
	}
	if bytes.Equal(f.imageOf(t, r.Head), f.imageOf(t, before)) {
		t.Fatal("retry did not publish the submitted changes")
	}
	assertNoIntentsLeft(t, f)
}

// The changed image is four times the spill cap; only one hot chunk keeps
// changing while the cut runs. No guest Flush is needed for the write stream.
func TestChunkStageV3_FourTimesCapConvergesUnderSteadyWrites(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 8)
	l.cfg.COWSpillLimit = 2 * ctChunkBytes
	stop, done, started := make(chan struct{}), make(chan struct{}), make(chan struct{})
	errs := make(chan error, 1)
	var once sync.Once
	finish := func() { once.Do(func() { close(stop) }) }
	t.Cleanup(func() { finish(); cowWaitSignal(t, done, "steady writer cleanup") })
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for i := 0; ; i++ {
			data := makeBlock(ctBlockSize, byte(0x80+i%64))
			lsn, err := s.Write(1, data)
			if err != nil {
				errs <- err
				return
			}
			w.record(lsn, 1, data)
			if i == 0 {
				close(started)
			}
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	cowWaitSignal(t, started, "steady writer admission")
	result, err := l.Cut(ctTimeout(t))
	finish()
	cowWaitSignal(t, done, "steady writer completion")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if result.Stats.StageRounds == 0 {
		t.Fatal("large dirty image bypassed pre-copy")
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	assertNoIntentsLeft(t, f)
}

// Each of two deadlines interrupts the third upload, after two immutable chunks
// completed. The next pass must reuse those chunks rather than restart the set.
type deadlineProgressRemote struct {
	successful map[string][]byte
	*fakeChunkRemote
	completed int
	calls     int
}

func (r *deadlineProgressRemote) Put(ctx context.Context, key string, b []byte) error {
	r.calls++
	if r.completed == 2 {
		<-ctx.Done()
		return ctx.Err()
	}
	err := r.fakeChunkRemote.Put(ctx, key, b)
	if err == nil {
		r.completed++
		if _, exists := r.successful[key]; exists {
			return errors.New("test: retransmitted unchanged successful key")
		}
		if r.successful == nil {
			r.successful = map[string][]byte{}
		}
		r.successful[key] = append([]byte(nil), b...)
	}
	return err
}
func TestChunkStageV3_MultipleDeadlinesKeepProgress(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 5)
	l.cfg.COWSpillLimit = ctChunkBytes
	l.cfg.UploadConcurrency = 1
	r := &deadlineProgressRemote{fakeChunkRemote: f}
	l.remote = r
	base := f.stats().head.Gen
	previous := 0
	lastSubmitted := s.NextLSN() - 1
	expectedImage := w.imageAt(lastSubmitted)
	var finalHead chunkHead
	for pass := 0; pass < 3; pass++ {
		r.completed = 0
		r.calls = 0
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		result, err := l.Cut(ctx)
		cancel()
		if pass < 2 {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("pass %d did not reach injected deadline: %v", pass, err)
			}
			if f.stats().head.Gen != base {
				t.Fatal("deadline moved HEAD")
			}
			l.stMu.Lock()
			cached := len(l.reusable)
			l.stMu.Unlock()
			if cached <= previous {
				t.Fatalf("pass %d restarted dirty set: cached=%d previous=%d", pass, cached, previous)
			}
			previous = cached
		} else if err != nil || result.Head.Gen != base+1 {
			t.Fatalf("third pass failed to publish: %+v %v", result, err)
		} else {
			finalHead = result.Head
		}
	}
	// Five changed chunks, five successful puts, no retransmission after expiry.
	if len(r.successful) != 5 {
		t.Fatalf("successful upload keys=%d want5", len(r.successful))
	}
	totalBytes := 0
	for _, payload := range r.successful {
		totalBytes += len(payload)
	}
	if totalBytes != 5*ctChunkBytes {
		t.Fatalf("uploaded bytes=%d want%d", totalBytes, 5*ctChunkBytes)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	if finalHead.RemoteLSN != lastSubmitted {
		t.Fatalf("final frontier=%d want last submitted=%d", finalHead.RemoteLSN, lastSubmitted)
	}
	assertImage(t, "final complete submitted image", f.imageOf(t, finalHead), expectedImage)
	if previous != 4 {
		t.Fatalf("reusable chunks=%d", previous)
	}
	assertNoIntentsLeft(t, f)
}
