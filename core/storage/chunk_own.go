package storage

import (
	"context"
	"sort"
)

// OwnProgress reports one OwnBatch call.
//   - Owned: foreign chunks this call's cut re-uploaded under own keys.
//   - Remaining: chunks HEAD still names with a foreign key after the call.
//   - NotLocal: of those, chunks not local yet (the hydrate walker fills them).
type OwnProgress struct {
	Owned     int
	Remaining int
	NotLocal  int
}

// OwnBatch runs one bounded cut (M6.Restore.Owning>Owning): it picks up to n
// chunks, lowest index first, whose HEAD key is foreign (foreign reports
// whether a key names another volume's object) and that are local (present,
// or every block written locally), and cuts
// with them as candidates: each is uploaded under a new own key under the
// cut's intent and published by the cut's one HEAD CAS, even when its bytes
// equal HEAD's. Absent chunks are skipped (never fetched by the cut). The cut
// is an ordinary cut, so guest writes since the last HEAD go with it. With
// nothing to own it runs no cut. A failed cut leaves HEAD as it was; the next
// call picks again from the current HEAD.
func (l *chunkLayerImpl) OwnBatch(ctx context.Context, n int, foreign func(key string) bool) (OwnProgress, error) {
	l.capMu.Lock()
	defer l.capMu.Unlock()
	l.stMu.Lock()
	var picked []uint64
	for _, idx := range sortedForeign(l.head.Chunks, foreign) {
		if len(picked) < n && l.chunkLocalLocked(idx) {
			picked = append(picked, idx)
		}
	}
	if len(picked) == 0 {
		p := l.ownProgressLocked(foreign)
		l.stMu.Unlock()
		return p.OwnProgress, nil
	}
	l.own = make(map[uint64]bool, len(picked))
	for _, idx := range picked {
		l.own[idx] = true
		// A payload remembered from an earlier failed cut that matched HEAD
		// was never uploaded (no key, data discarded): reusing it would skip
		// the re-own upload. Drop it, so this cut reads and uploads the chunk.
		if sc, ok := l.reusable[idx]; ok && sc.key == "" {
			delete(l.reusable, idx)
		}
	}
	l.stMu.Unlock()
	_, err := l.captureLocked(ctx, false)
	l.stMu.Lock()
	l.own = nil
	p := l.ownProgressLocked(foreign)
	l.stMu.Unlock()
	if err == nil {
		for _, idx := range picked {
			if ref, ok := p.head[idx]; !ok || !foreign(ref.Key) {
				p.Owned++
			}
		}
	}
	return p.OwnProgress, err
}

type ownCount struct {
	OwnProgress
	head map[uint64]chunkRef
}

// ownProgressLocked counts HEAD's foreign chunks. Caller holds stMu.
func (l *chunkLayerImpl) ownProgressLocked(foreign func(string) bool) ownCount {
	c := ownCount{head: l.head.Chunks}
	for _, idx := range sortedForeign(l.head.Chunks, foreign) {
		c.Remaining++
		if !l.chunkLocalLocked(idx) {
			c.NotLocal++
		}
	}
	return c
}

func sortedForeign(chunks map[uint64]chunkRef, foreign func(string) bool) []uint64 {
	var out []uint64
	for idx, ref := range chunks {
		if foreign(ref.Key) {
			out = append(out, idx)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// owning reports whether chunk idx is re-owned by the running OwnBatch cut:
// it is uploaded under a new own key even when its bytes equal HEAD's.
func (l *chunkLayerImpl) owning(idx uint64) bool {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	return l.own[idx]
}

// addOwnCandidatesLocked adds the running OwnBatch's chunks to a cut's
// candidates: a re-own reads the local copy, so an absent chunk is left out.
// Caller holds stMu.
func (l *chunkLayerImpl) addOwnCandidatesLocked(out map[uint64]bool) {
	for idx := range l.own {
		if l.chunkLocalLocked(idx) {
			out[idx] = true
		}
	}
}

// chunkLocalLocked reports whether a re-own can read chunk idx without a
// fetch: it is present, or absent with every block written locally (a guest
// overwrite flushed before the walker reached it). Caller holds stMu.
func (l *chunkLayerImpl) chunkLocalLocked(idx uint64) bool {
	if !l.absent[idx] {
		return true
	}
	first := idx * l.chunkBlocks
	for i := uint64(0); i < l.chunkBlocks; i++ {
		b := first + i
		if b >= uint64(l.s.NumBlocks()) {
			break
		}
		if !l.localBlocks[b] {
			return false
		}
	}
	return true
}

// unchangedFromHead reports whether a captured chunk equals HEAD's and was
// not uploaded: such a chunk keeps its HEAD ref. A re-owned chunk equal to
// HEAD was uploaded by streamCutPayloads under a new own key (c.key set), so
// it is published like a changed chunk.
func unchangedFromHead(c capturedChunk, ref chunkRef, had bool) bool {
	return had && !c.zero && ref.SHA256 == c.sum && c.key == ""
}
