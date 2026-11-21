// Package iolatency provides bounded, cumulative diagnostic histograms. It
// neither schedules I/O nor participates in durability decisions.
package iolatency

import (
	"context"
	"math/bits"
	"sync/atomic"
	"time"
)

// Histogram uses power-of-two nanosecond buckets. Quantiles are bucket upper
// bounds, not exact samples. Snapshots during traffic are best-effort.
type Histogram struct {
	buckets [64]atomic.Uint64
	sum     atomic.Uint64
}
type Distribution struct {
	Count         uint64     // samples observed
	SumNanos      uint64     // their total nanoseconds
	P50UpperNanos uint64     // upper bound of the bucket holding the median
	P99UpperNanos uint64     // upper bound of the bucket holding the 99th percentile
	Buckets       [64]uint64 // bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns
}

func (h *Histogram) Observe(d time.Duration) {
	n := uint64(0)
	if d > 0 {
		n = uint64(d)
	}
	i := bits.Len64(n)
	if i > 63 {
		i = 63
	}
	h.buckets[i].Add(1)
	h.sum.Add(n)
}
func (h *Histogram) Snapshot() Distribution {
	var s Distribution
	for i := range h.buckets {
		s.Buckets[i] = h.buckets[i].Load()
		s.Count += s.Buckets[i]
	}
	s.SumNanos = h.sum.Load()
	quantile := func(n uint64) uint64 {
		if s.Count == 0 {
			return 0
		}
		target := (s.Count*n + 99) / 100
		var seen uint64
		for i, v := range s.Buckets {
			seen += v
			if seen >= target {
				return uint64(1) << i
			}
		}
		return uint64(1) << 63
	}
	s.P50UpperNanos = quantile(50)
	s.P99UpperNanos = quantile(99)
	return s
}

// Requests is the common frontend-to-backend boundary, shared by ublk and NVMe.
// AckDispatch ends before the handler returns a result, not at guest receipt.
type Requests struct {
	writeTotal, flushTotal, fuaTotal Histogram
	receive, ack, total              Histogram
	writes, flushes, fua, failed     atomic.Uint64
}
type RequestStatus struct {
	WriteTotal       Distribution // end-to-end latency of writes
	FlushTotal       Distribution // end-to-end latency of flushes
	FUATotal         Distribution // end-to-end latency of forced-unit-access writes
	ReceiveToBackend Distribution // from request receipt to the backend call
	AckDispatch      Distribution // from backend completion to the acknowledgement sent
	RequestTotal     Distribution // end-to-end latency of every request
	Writes           uint64       // writes observed
	Flushes          uint64       // flushes observed
	FUAWrites        uint64       // forced-unit-access writes observed
	Failed           uint64       // requests that completed with an error
}

func (c *Requests) Snapshot() RequestStatus {
	return RequestStatus{WriteTotal: c.writeTotal.Snapshot(), FlushTotal: c.flushTotal.Snapshot(), FUATotal: c.fuaTotal.Snapshot(), ReceiveToBackend: c.receive.Snapshot(), AckDispatch: c.ack.Snapshot(), RequestTotal: c.total.Snapshot(), Writes: c.writes.Load(), Flushes: c.flushes.Load(), FUAWrites: c.fua.Load(), Failed: c.failed.Load()}
}

type requestKey struct{}

// Request is owned by one synchronous frontend dispatch. No request IDs, payloads
// or unbounded sample buffers are retained.
type Request struct {
	kind              string
	owner             *Requests
	start, backendEnd time.Time
	entered           bool
}

func (c *Requests) Begin(ctx context.Context, kind string) (context.Context, *Request) {
	switch kind {
	case "write":
		c.writes.Add(1)
	case "flush":
		c.flushes.Add(1)
	case "fua":
		c.writes.Add(1)
		c.fua.Add(1)
	default:
		return ctx, nil
	}
	r := &Request{owner: c, start: time.Now(), kind: kind}
	return context.WithValue(ctx, requestKey{}, r), r
}
func BackendEnter(ctx context.Context) {
	if r, ok := ctx.Value(requestKey{}).(*Request); ok && !r.entered {
		r.owner.receive.Observe(time.Since(r.start))
		r.entered = true
	}
}
func BackendExit(ctx context.Context) {
	if r, ok := ctx.Value(requestKey{}).(*Request); ok {
		r.backendEnd = time.Now()
	}
}

// IncludeReceive includes a lazy provider lookup preceding the delegated Begin.
// Called by the owning synchronous dispatcher before any backend call.
func (r *Request) IncludeReceive(received time.Time) {
	if r != nil && received.Before(r.start) {
		r.start = received
	}
}

func (r *Request) Finish(failed bool) {
	if r == nil {
		return
	}
	end := time.Now()
	r.owner.total.Observe(end.Sub(r.start))
	switch r.kind {
	case "write":
		r.owner.writeTotal.Observe(end.Sub(r.start))
	case "flush":
		r.owner.flushTotal.Observe(end.Sub(r.start))
	case "fua":
		r.owner.fuaTotal.Observe(end.Sub(r.start))
	}
	if !r.backendEnd.IsZero() {
		r.owner.ack.Observe(end.Sub(r.backendEnd))
	}
	if failed {
		r.owner.failed.Add(1)
	}
}
