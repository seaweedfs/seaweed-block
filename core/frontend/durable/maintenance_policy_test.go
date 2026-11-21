package durable

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestMaintenanceVolumePhasesDoNotAlign(t *testing.T) {
	interval := 15 * time.Minute
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		s := initialCutSchedule(fmt.Sprintf("volume-%d", i), interval, t0, true)
		offset := s.firstDue.Sub(t0)
		if offset <= 0 || offset >= interval {
			t.Fatalf("phase outside interval: %s", offset)
		}
		seen[offset] = true
		if _, due := testPolicy().due(s, s.firstDue.Add(-time.Nanosecond), false); due {
			t.Fatal("published before own phase")
		}
	}
	if len(seen) < 190 {
		t.Fatalf("200 volumes collapsed into %d phases", len(seen))
	}
}

// Virtual quiescence observes the join boundary directly, so a slow storage
// Close cannot accidentally make an omitted scheduler join look correct.
func TestMaintenanceStopJoinsBeforeRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done, release, cancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		finish := func() { once.Do(func() { close(release) }) }
		defer finish()
		p := &DurableProvider{}
		h := &volHandle{chunkStop: cancel, chunkDone: done}
		go func() { <-ctx.Done(); close(cancelled); <-release; close(done) }()
		returned1, returned2 := make(chan struct{}), make(chan struct{})
		go func() { p.stopChunkLoop(h); close(returned1) }()
		<-cancelled
		go func() { p.stopChunkLoop(h); close(returned2) }()
		synctest.Wait()
		for _, returned := range []chan struct{}{returned1, returned2} {
			select {
			case <-returned:
				t.Error("stop released before worker join")
			default:
			}
		}
		finish()
		<-returned1
		<-returned2
	})
}
