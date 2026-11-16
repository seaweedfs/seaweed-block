package iolatency

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHistogramKnownQuantiles(t *testing.T) {
	var h Histogram
	if s := h.Snapshot(); s.Count != 0 || s.P99UpperNanos != 0 {
		t.Fatal(s)
	}
	for i := 0; i < 100; i++ {
		d := time.Duration(3)
		if i >= 99 {
			d = 9
		}
		h.Observe(d)
	}
	s := h.Snapshot()
	if s.Count != 100 || s.SumNanos != 306 || s.P50UpperNanos != 4 || s.P99UpperNanos != 4 || s.Buckets[2] != 99 || s.Buckets[4] != 1 {
		t.Fatal(s)
	}
}
func TestHistogramConcurrentAccounting(t *testing.T) {
	var h Histogram
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				h.Observe(10 * time.Nanosecond)
			}
		}()
	}
	wg.Wait()
	s := h.Snapshot()
	if s.Count != 8000 || s.SumNanos != 80000 {
		t.Fatal(s)
	}
}
func TestRequestKindsAndMissingBackend(t *testing.T) {
	var c Requests
	for _, kind := range []string{"write", "fua", "flush"} {
		ctx, r := c.Begin(context.Background(), kind)
		BackendEnter(ctx)
		BackendExit(ctx)
		r.Finish(kind == "flush")
	}
	_, r := c.Begin(context.Background(), "read")
	if r != nil {
		t.Fatal("read traced")
	}
	r.Finish(false)
	s := c.Snapshot()
	if s.Writes != 2 || s.Flushes != 1 || s.FUAWrites != 1 || s.Failed != 1 || s.RequestTotal.Count != 3 || s.FlushTotal.Count != 1 || s.FUATotal.Count != 1 || s.WriteTotal.Count != 1 || s.ReceiveToBackend.Count != 3 || s.AckDispatch.Count != 3 {
		t.Fatal(s)
	}
	_, r = c.Begin(context.Background(), "flush")
	r.Finish(true)
	s = c.Snapshot()
	if s.Failed != 2 || s.RequestTotal.Count != 4 || s.AckDispatch.Count != 3 {
		t.Fatal("refused request fabricated backend ACK", s)
	}
}
