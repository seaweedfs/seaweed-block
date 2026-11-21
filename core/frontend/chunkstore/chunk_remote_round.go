package chunkstore

// Upload rounds over batch assign. The n objects of one round are split into
// G = min(8, ceil(n/64)) groups (object i goes to group i % G, as slot i / G),
// and each group takes one reservation for all its slots, made when its
// first object is uploaded. So a round costs G assigns instead of n, its
// objects still spread over up to G volumes, and a round with no upload
// makes none.
//
// A slot is frozen once reserved: retrying object i sends the same fid. A
// group's ranges change only for slots no upload has stored yet:
//   - a short grant is topped up by a reservation for the remaining slots;
//   - a refusal before append (read-only or full volume) moves this and the
//     later slots of the group to a fresh range, at most roundMaxReplacements
//     times per group;
//   - a range whose token is past its stated expiry is never sent to; its
//     unsent slots get a fresh range under the same bound.
// A rejected token (401) is terminal. Extra reservations are counted apart
// from the initial ones (AssignCounts).

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	roundGroupSize       = 64
	roundMaxGroups       = 8
	roundMaxReplacements = 3
)

// AssignCounts counts the reservations this remote's upload rounds made.
type AssignCounts struct {
	Initial  int64 // the first reservation of each group
	Short    int64 // top-ups after a short grant
	Capacity int64 // replacements after a refusal before append
	Expired  int64 // replacements of ranges whose token expired before use
}

// Extra is every reservation beyond the initial ones.
func (a AssignCounts) Extra() int64 { return a.Short + a.Capacity + a.Expired }

type assignCounters struct{ initial, short, capacity, expired atomic.Int64 }

// AssignCounts reports the reservations made so far.
func (r *ChunkRemote) AssignCounts() AssignCounts {
	return AssignCounts{
		Initial:  r.assigns.initial.Load(),
		Short:    r.assigns.short.Load(),
		Capacity: r.assigns.capacity.Load(),
		Expired:  r.assigns.expired.Load(),
	}
}

var _ storage.ChunkPutRounds = (*ChunkRemote)(nil)

// BeginPutRound starts a round of n objects of objSize bytes. Without batch
// support in the blob store, the round is one Put per object.
func (r *ChunkRemote) BeginPutRound(n, objSize int) storage.ChunkPutRound {
	rb, ok := r.b.(rangeBlobs)
	if !ok || n <= 0 {
		return plainRound{r}
	}
	g := min(roundMaxGroups, (n+roundGroupSize-1)/roundGroupSize)
	rd := &putRound{r: r, rb: rb, n: n, size: uint64(max(objSize, 0)), groups: make([]*roundGroup, g)}
	for j := range rd.groups {
		// Group j holds objects j, j+g, j+2g, ...
		rd.groups[j] = &roundGroup{slots: (n - j + g - 1) / g}
	}
	return rd
}

type plainRound struct{ r *ChunkRemote }

func (p plainRound) Put(ctx context.Context, _ int, key string, data []byte) error {
	return p.r.Put(ctx, key, data)
}

type putRound struct {
	r      *ChunkRemote
	rb     rangeBlobs
	n      int
	size   uint64
	groups []*roundGroup
}

// roundGroup's ranges cover its slots [0, end) as consecutive spans.
type roundGroup struct {
	slots int

	mu           sync.Mutex
	spans        []roundSpan
	reserving    chan struct{} // non-nil while a reservation is in flight
	replacements int
	next         reserveReason // what the next reservation is for
}

type reserveReason int

const (
	reserveInitial reserveReason = iota
	reserveShort
	reserveCapacity
	reserveExpired
)

type roundSpan struct {
	from int
	rng  blobRange
}

func (g *roundGroup) end() int {
	if len(g.spans) == 0 {
		return 0
	}
	last := g.spans[len(g.spans)-1]
	return last.from + last.rng.count
}

// lookup returns the range and offset of slot s, if reserved. g.mu held.
func (g *roundGroup) lookup(s int) (blobRange, int, bool) {
	for _, sp := range g.spans {
		if s >= sp.from && s < sp.from+sp.rng.count {
			return sp.rng, s - sp.from, true
		}
	}
	return blobRange{}, 0, false
}

// retire drops slots s and later of the span using rng, so they take a fresh
// range; it reports false once the group's replacements are used up. A span
// already replaced (by a concurrent upload) is left as is.
func (g *roundGroup) retire(rng blobRange, s int, why reserveReason) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, sp := range g.spans {
		if sp.rng.base != rng.base {
			continue
		}
		if g.replacements >= roundMaxReplacements {
			return false
		}
		g.replacements++
		g.next = why
		if s <= sp.from {
			g.spans = g.spans[:k]
		} else {
			g.spans[k].rng.count = s - sp.from
			g.spans = g.spans[:k+1]
		}
		return true
	}
	return true
}

// slot returns slot s's range and offset, reserving the group's remaining
// slots when s is not covered yet. Concurrent callers share one reservation;
// waiting follows ctx, and a reply after ctx ends is not used.
func (rd *putRound) slot(ctx context.Context, g *roundGroup, s int) (blobRange, int, error) {
	for {
		g.mu.Lock()
		if rng, off, ok := g.lookup(s); ok {
			g.mu.Unlock()
			return rng, off, nil
		}
		if wait := g.reserving; wait != nil {
			g.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return blobRange{}, 0, ctx.Err()
			}
		}
		from := g.end()
		why := g.next
		done := make(chan struct{})
		g.reserving = done
		g.mu.Unlock()

		rng, err := rd.rb.reserve(ctx, g.slots-from, rd.size)
		if err == nil {
			err = ctx.Err()
		}
		g.mu.Lock()
		g.reserving = nil
		close(done)
		if err == nil {
			g.spans = append(g.spans, roundSpan{from: from, rng: rng})
			g.next = reserveShort // only a short grant leaves slots uncovered now
			switch why {
			case reserveInitial:
				rd.r.assigns.initial.Add(1)
			case reserveShort:
				rd.r.assigns.short.Add(1)
			case reserveCapacity:
				rd.r.assigns.capacity.Add(1)
			case reserveExpired:
				rd.r.assigns.expired.Add(1)
			}
		}
		g.mu.Unlock()
		if err != nil {
			return blobRange{}, 0, err
		}
	}
}

// Put stores object i under key in its group's range and binds key to it.
func (rd *putRound) Put(ctx context.Context, i int, key string, data []byte) error {
	if i < 0 || i >= rd.n {
		return fmt.Errorf("chunkstore: round object %d outside a round of %d", i, rd.n)
	}
	name, err := rd.r.ownKeyName(key)
	if err != nil {
		return err
	}
	if rd.r.knownKey(key) {
		return storage.ErrChunkKeyExists
	}
	g := rd.groups[i%len(rd.groups)]
	s := i / len(rd.groups)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rng, off, err := rd.slot(ctx, g, s)
		if err != nil {
			return err
		}
		if dl, ok := rng.sendDeadline(); ok && !time.Now().Before(dl) {
			// Never sent: the unsent slots take a fresh range.
			if !g.retire(rng, s, reserveExpired) {
				return fmt.Errorf("chunkstore: round object %d: token expired before upload, replacements used up", i)
			}
			continue
		}
		fid, err := rng.fid(off)
		if err != nil {
			return err
		}
		err = rd.rb.putRange(ctx, rng, off, data)
		if errors.Is(err, errRangeRefused) {
			if !g.retire(rng, s, reserveCapacity) {
				return fmt.Errorf("chunkstore: round object %d: %w (replacements used up)", i, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		return rd.r.bindKey(ctx, key, name, fid, data, "")
	}
}
