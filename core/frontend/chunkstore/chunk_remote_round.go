package chunkstore

// Upload rounds over batch assign. The n objects of one round are split into
// G = min(8, ceil(n/64)) groups (object i goes to group i % G, as slot i / G),
// and each group takes one reservation for all its slots, made when its
// first object is uploaded. So a round costs G assigns instead of n, its
// objects still spread over up to G volumes, and a round with no upload
// makes none.
//
// A slot is frozen once an upload was sent for it: until that upload is
// resolved (stored and bound, or definitely not stored), every retry uses
// the same fid, whatever happens to the group's ranges. Ranges change only
// for slots with no upload outstanding:
//   - a short grant is topped up by a reservation for the remaining slots;
//   - a refusal before append (read-only or full volume) moves this and the
//     later slots of the group to a fresh range, at most roundMaxReplacements
//     times per group;
//   - a range whose token is past its stated expiry (less a skew) is never
//     sent to: an upload is sent under that deadline, and a slot frozen on
//     an expired range is resolved by reading its fid back first (the exact
//     bytes: bind it; definitely absent: a fresh range takes it).
// A rejected token (401) and used-up replacements end the object for good
// (storage.ErrChunkPutFinal). A reservation reply that arrives after the
// group's ranges changed is discarded (its ids stay unused). AssignCounts
// counts every reservation attempt, by reason, failed ones included.

import (
	"bytes"
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

// AssignCounts counts the reservation attempts this remote's upload rounds
// made. Each attempt is counted once, by why it was made, whether or not it
// succeeded; Failed counts the ones that returned no usable range.
type AssignCounts struct {
	Initial  int64 // the first attempt of each group
	Short    int64 // top-ups after a short grant
	Capacity int64 // replacements after a refusal before append
	Expired  int64 // replacements of ranges whose token expired before use
	Retry    int64 // attempts after a failed, lost or discarded reply
	Failed   int64 // attempts that returned an error or were discarded
}

// Extra is every attempt beyond the initial ones.
func (a AssignCounts) Extra() int64 { return a.Short + a.Capacity + a.Expired + a.Retry }

type assignCounters struct{ initial, short, capacity, expired, retry, failed atomic.Int64 }

// AssignCounts reports the reservation attempts made so far.
func (r *ChunkRemote) AssignCounts() AssignCounts {
	return AssignCounts{
		Initial:  r.assigns.initial.Load(),
		Short:    r.assigns.short.Load(),
		Capacity: r.assigns.capacity.Load(),
		Expired:  r.assigns.expired.Load(),
		Retry:    r.assigns.retry.Load(),
		Failed:   r.assigns.failed.Load(),
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
		rd.groups[j] = &roundGroup{slots: (n - j + g - 1) / g, pins: map[int]slotPin{}, owners: map[int]chan struct{}{}}
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

type reserveReason int

const (
	reserveInitial reserveReason = iota
	reserveShort
	reserveCapacity
	reserveExpired
	reserveRetry
)

// roundGroup's ranges cover its slots [0, end) as consecutive spans.
type roundGroup struct {
	slots int

	mu           sync.Mutex
	spans        []roundSpan
	ver          int           // bumped by every change to spans other than an append
	reserving    chan struct{} // non-nil while a reservation is in flight
	replacements int
	next         reserveReason // why the next reservation is made
	pins         map[int]slotPin
	owners       map[int]chan struct{} // slot -> held by one Put at a time
}

type roundSpan struct {
	from int
	rng  blobRange
}

// slotPin freezes a slot on the fid an upload was sent to; stored records
// that the upload succeeded (only its binding is outstanding).
type slotPin struct {
	rng    blobRange
	off    int
	stored bool
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

// own makes the caller the only Put working on slot s until release.
func (g *roundGroup) own(ctx context.Context, s int) (release func(), err error) {
	for {
		g.mu.Lock()
		held, busy := g.owners[s]
		if !busy {
			done := make(chan struct{})
			g.owners[s] = done
			g.mu.Unlock()
			return func() {
				g.mu.Lock()
				delete(g.owners, s)
				g.mu.Unlock()
				close(done)
			}, nil
		}
		g.mu.Unlock()
		select {
		case <-held:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (g *roundGroup) pin(s int, p slotPin) {
	g.mu.Lock()
	g.pins[s] = p
	g.mu.Unlock()
}

func (g *roundGroup) unpin(s int) {
	g.mu.Lock()
	delete(g.pins, s)
	g.mu.Unlock()
}

func (g *roundGroup) pinned(s int) (slotPin, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	p, ok := g.pins[s]
	return p, ok
}

// retire drops slots s and later of the span using rng, so they take a fresh
// range (slots with an upload outstanding keep their pinned fid); it reports
// false once the group's replacements are used up. A span already replaced
// (by a concurrent upload) is left as is.
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
		g.ver++
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

func (rd *putRound) count(why reserveReason) {
	switch why {
	case reserveInitial:
		rd.r.assigns.initial.Add(1)
	case reserveShort:
		rd.r.assigns.short.Add(1)
	case reserveCapacity:
		rd.r.assigns.capacity.Add(1)
	case reserveExpired:
		rd.r.assigns.expired.Add(1)
	case reserveRetry:
		rd.r.assigns.retry.Add(1)
	}
}

// slot returns slot s's range and offset, reserving the group's remaining
// slots when s is not covered yet. Concurrent callers share one reservation;
// waiting follows ctx. A reply after ctx ends, or after the group's ranges
// changed while it was in flight, is not used.
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
		from, ver, why := g.end(), g.ver, g.next
		done := make(chan struct{})
		g.reserving = done
		g.mu.Unlock()

		rd.count(why)
		rng, err := rd.rb.reserve(ctx, g.slots-from, rd.size)
		if err == nil {
			err = ctx.Err()
		}
		g.mu.Lock()
		g.reserving = nil
		close(done)
		switch {
		case err != nil:
			rd.r.assigns.failed.Add(1)
			g.next = reserveRetry
		case g.ver != ver || g.end() != from:
			// The ranges changed while this reply was in flight: its ids
			// stay unused, and the uncovered slots are reserved again.
			rd.r.assigns.failed.Add(1)
			g.next = reserveRetry
		default:
			g.spans = append(g.spans, roundSpan{from: from, rng: rng})
			g.next = reserveShort // only a short grant leaves slots uncovered now
		}
		g.mu.Unlock()
		if err != nil {
			return blobRange{}, 0, err
		}
	}
}

// final marks an outcome no retry can change.
func final(err error) error { return errors.Join(storage.ErrChunkPutFinal, err) }

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
	release, err := g.own(ctx, s)
	if err != nil {
		return err
	}
	defer release()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p, frozen := g.pinned(s)
		if !frozen {
			rng, off, err := rd.slot(ctx, g, s)
			if err != nil {
				return err
			}
			p = slotPin{rng: rng, off: off}
		}
		fid, err := p.rng.fid(p.off)
		if err != nil {
			return err
		}
		if p.stored {
			if err := rd.r.bindKey(ctx, key, name, fid, data, ""); err != nil {
				return err
			}
			g.unpin(s)
			return nil
		}
		dl, hasDL := p.rng.sendDeadline()
		if hasDL && !time.Now().Before(dl) {
			if frozen {
				// An earlier upload to fid is unresolved: read it back
				// before giving the fid up.
				got, perr := rd.rb.probe(ctx, p.rng, p.off)
				switch {
				case perr == nil && bytes.Equal(got, data):
					g.pin(s, slotPin{rng: p.rng, off: p.off, stored: true})
					continue
				case perr == nil:
					return final(fmt.Errorf("chunkstore: round object %d: %s holds other bytes", i, fid))
				case !errors.Is(perr, errBlobAbsent):
					return fmt.Errorf("chunkstore: round object %d: earlier upload to %s unresolved: %w", i, fid, perr)
				}
				g.unpin(s)
			}
			if !g.retire(p.rng, s, reserveExpired) {
				return final(fmt.Errorf("chunkstore: round object %d: token expired before upload, replacements used up", i))
			}
			continue
		}
		pctx, cancel := ctx, context.CancelFunc(func() {})
		if hasDL {
			pctx, cancel = context.WithDeadline(ctx, dl)
		}
		g.pin(s, p) // sent: frozen until resolved
		err = rd.rb.putRange(pctx, p.rng, p.off, data)
		cancel()
		switch {
		case err == nil:
			g.pin(s, slotPin{rng: p.rng, off: p.off, stored: true})
			continue
		case errors.Is(err, errRangeRefused):
			g.unpin(s) // definitely not stored
			if !g.retire(p.rng, s, reserveCapacity) {
				return final(fmt.Errorf("chunkstore: round object %d: %w (replacements used up)", i, err))
			}
			continue
		case errors.Is(err, errRangeAuth):
			g.unpin(s) // definitely not stored
			return final(err)
		}
		return err // outcome unknown: the slot stays frozen on fid
	}
}
