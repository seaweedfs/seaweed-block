package durable

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

// INV-CUT-PRECOPY-001: a completed pause releases admission, not one
// whole storage operation at a time. Independent LBAs must run concurrently.
func TestPrecopyResumeReleasesAllWaiters(t *testing.T) {
	b := precopyMemoryBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	queued := make(chan struct{}, 3)
	var want, granted []*precopyWaiter
	grantUnlocked := false
	b.testPrecopyGranted = func(w *precopyWaiter) {
		granted = append(granted, w)
		if b.mu.TryLock() {
			b.mu.Unlock()
			grantUnlocked = true
		}
	}
	b.testPrecopyQueued = func() { queued <- struct{}{} }
	entered := make(chan byte, 3)
	release := make(chan struct{})
	var once sync.Once
	unpark := func() { once.Do(func() { close(release) }) }
	if err := b.SetWriteObserver(precopyQueueObserver{observe: func(v byte) {
		entered <- v
		select {
		case <-release:
		case <-ctx.Done():
		}
	}}); err != nil {
		t.Fatal(err)
	}
	resume, err := b.BeginPrecopyPause(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 3)
	t.Cleanup(func() {
		unpark()
		cancel()
		resume()
		joined := make(chan struct{})
		go func() { wg.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("resumed writers did not join")
		}
	})
	for v := byte(1); v <= 3; v++ {
		wg.Add(1)
		go func(v byte) {
			defer wg.Done()
			_, err := b.Write(ctx, int64(v-1)*4096, bytes.Repeat([]byte{v}, 4096))
			results <- err
		}(v)
		select {
		case <-queued:
			b.mu.Lock()
			want = append(want, b.precopy.waiters[len(b.precopy.waiters)-1])
			b.mu.Unlock()
		case <-ctx.Done():
			t.Fatal("write did not queue")
		}
	}
	select {
	case v := <-entered:
		t.Fatalf("paused write %d reached observer", v)
	default:
	}
	resume()
	if grantUnlocked || len(granted) != 3 {
		t.Fatalf("FIFO grants not made together under mu: count=%d unlocked=%v", len(granted), grantUnlocked)
	}
	for i, w := range want {
		if granted[i] != w {
			t.Fatalf("grant %d bypassed arrival order", i)
		}
	}
	seen := map[byte]bool{}
	for i := 0; i < 3; i++ {
		select {
		case v := <-entered:
			seen[v] = true
		case <-ctx.Done():
			t.Fatalf("resume serialized independent I/O: %d/3 entered before any completion", len(seen))
		}
	}
	unpark()
	for i := 0; i < 3; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("resumed write did not finish")
		}
	}
	for v := byte(1); v <= 3; v++ {
		got := make([]byte, 4096)
		if _, err := b.Read(ctx, int64(v-1)*4096, got); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{v}, 4096)) {
			t.Fatalf("resumed LBA %d bytes incorrect: %v", v-1, err)
		}
	}
}

// Real elapsed time, not a changed production budget: an admitted Sync remains
// held past the old pause's five-second clock while a fresh write must succeed.
func TestPrecopyResumedArrivalDoesNotInheritExpiredPause(t *testing.T) {
	b := precopyMemoryBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	queued := make(chan struct{}, 2)
	b.testPrecopyQueued = func() { queued <- struct{}{} }
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unpark := func() { once.Do(func() { close(release) }) }
	if err := b.SetWriteObserver(precopyQueueObserver{sync: func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}); err != nil {
		t.Fatal(err)
	}
	resume, err := b.BeginPrecopyPause(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Capture the existing pause's origin only to wait beyond its public bound.
	b.mu.Lock()
	started := b.precopy.started
	b.mu.Unlock()
	var syncErr error
	t.Cleanup(func() {
		unpark()
		cancel()
		resume()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("held Sync did not join")
		}
	})
	go func() { defer close(done); syncErr = b.Sync(ctx) }()
	select {
	case <-queued:
	case <-ctx.Done():
		t.Fatal("Sync did not queue")
	}
	resume()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("Sync did not resume")
	}
	timer := time.NewTimer(time.Until(started.Add(5*time.Second + 50*time.Millisecond)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal("caller expired before pause-age witness")
	}
	want := bytes.Repeat([]byte{7}, 4096)
	if n, err := b.Write(ctx, 4096, want); err != nil || n != len(want) {
		t.Fatalf("serving volume rejected a fresh arrival after ended pause aged past five seconds: n=%d err=%v", n, err)
	}
	got := make([]byte, 4096)
	if _, err := b.Read(ctx, 4096, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("fresh write bytes: %v", err)
	}
	unpark()
	select {
	case <-done:
		if syncErr != nil {
			t.Fatal(syncErr)
		}
	case <-ctx.Done():
		t.Fatal("Sync did not finish")
	}
}
