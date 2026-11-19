package storage

import (
	"sort"
	"sync/atomic"
)

// The owned-map file reuses space. A candidate's new pages and root go into
// 4 KiB extents that no live root references, and the file's free tail is
// truncated once the file exceeds 4x the live bytes.
//
// Live roots are the roots the two descriptor slots name and every prepared
// candidate not yet published or superseded (an uncertain candidate stays
// live, because a failed publication latches the publisher). An extent is
// released only after a publication leaves neither slot naming it. New
// pages and the root are fsynced before a publication names them, so a
// crash leaves every slot-named root intact. Roots and pages are addressed
// by offset and SHA-256, so the on-disk format is unchanged.
//
// Map pages are read by offset only at open, before any candidate exists;
// other openers take the same exclusive volume lock.
//
// Lock order: WALStore.mu -> descMu -> liveMu (the publisher's report and
// the definite-abandon path run under descMu), and chunkLSNMapV3.mu ->
// liveMu (a prepare snapshots and registers under it). liveMu is held only
// for map and slice bookkeeping (it may allocate a slice or a map entry),
// never across I/O, sorting or extent allocation,
// and nothing holding liveMu takes another lock.

type mapRootKeyV3 struct{ off, gen uint64 }

type mapRootExtentsV3 struct {
	rootOff, rootLen uint64   // bytes, 4 KiB aligned
	pages            []uint64 // page offsets (every page the root references)
}

// chunkMapLiveV3 is the live set of one owned map.
type chunkMapLiveV3 struct {
	roots       map[mapRootKeyV3]mapRootExtentsV3
	inflight    map[mapRootKeyV3]bool
	truncations atomic.Uint64
	fileBytes   atomic.Uint64 // set at open, after each prepare and truncation
}

// ChunkMapStats reports the owned map file's size, the bytes its live roots
// reference, and how many times its free tail was truncated.
type ChunkMapStats struct {
	FileBytes, LiveBytes, Truncations uint64
}

// registerRootV3 records a slot-named root read at open. Caller holds no lock
// other than possibly m.mu.
func (m *chunkLSNMapV3) registerRootV3(gen, rootOff, rootLen uint64, refs []mapPageRefV3) {
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	m.live.roots[mapRootKeyV3{rootOff, gen}] = rootExtentsV3(rootOff, rootLen, refs)
}

func rootExtentsV3(rootOff, rootLen uint64, refs []mapPageRefV3) mapRootExtentsV3 {
	x := mapRootExtentsV3{rootOff: rootOff, rootLen: rootLen, pages: make([]uint64, len(refs))}
	for i, r := range refs {
		x.pages[i] = r.Offset
	}
	return x
}

// slotsPublishedV3 is the publisher's report after a successful publication
// (under descMu). Roots named by neither slot are released, and so is every
// in-flight candidate the new current generation supersedes (it can never
// be published: a publication needs a strictly higher generation).
func (m *chunkLSNMapV3) slotsPublishedV3(cur, other descriptorV3, otherValid bool) {
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	keep := map[mapRootKeyV3]bool{{cur.MapRootLocator, cur.MapGeneration}: true}
	if otherValid {
		keep[mapRootKeyV3{other.MapRootLocator, other.MapGeneration}] = true
	}
	for k := range m.live.roots {
		if keep[k] {
			delete(m.live.inflight, k)
			continue
		}
		if m.live.inflight[k] && k.gen > cur.MapGeneration {
			continue // a newer candidate, still publishable
		}
		delete(m.live.roots, k)
		delete(m.live.inflight, k)
	}
}

// abandonCandidateV3 releases a prepared candidate that is known never to
// have reached a descriptor slot: its publication was refused before any
// slot I/O (a stale proof, a validation refusal, a latched fault), or its
// caller gave up before publishing. A candidate whose publication I/O
// started stays pinned (uncertain) until reopen.
func (m *chunkLSNMapV3) abandonCandidateV3(c *chunkMapCandidateV3) {
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	k := mapRootKeyV3{c.rootOffset, c.generation}
	if m.live.inflight[k] {
		delete(m.live.inflight, k)
		delete(m.live.roots, k)
	}
}

// abandonV3 is the definite-abandon path for a prepared publication.
func (p *preparedMapPublicationV3) abandonV3() {
	if p != nil && p.candidate != nil && p.candidate.owner != nil {
		p.candidate.owner.abandonCandidateV3(p.candidate)
	}
}

// liveSnapshotV3 copies the live roots' extents under liveMu.
func (m *chunkLSNMapV3) liveSnapshotV3() []mapRootExtentsV3 {
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	out := make([]mapRootExtentsV3, 0, len(m.live.roots))
	for _, x := range m.live.roots {
		out = append(out, x)
	}
	return out
}

// usedPagesV3 returns the sorted, distinct 4 KiB page indexes a live-set
// snapshot references, and the live byte count. It needs no lock.
func usedPagesV3(roots []mapRootExtentsV3) ([]uint64, uint64) {
	set := map[uint64]bool{}
	for _, x := range roots {
		for p := x.rootOff / 4096; p < (x.rootOff+x.rootLen)/4096; p++ {
			set[p] = true
		}
		for _, off := range x.pages {
			set[off/4096] = true
		}
	}
	used := make([]uint64, 0, len(set))
	for p := range set {
		used = append(used, p)
	}
	sort.Slice(used, func(i, j int) bool { return used[i] < used[j] })
	return used, uint64(len(used)) * 4096
}

// extentAllocatorV3 hands out free 4 KiB-aligned runs, lowest first, given
// the pages in use; it grows the file only when no free run fits.
type extentAllocatorV3 struct {
	used map[uint64]bool
	end  uint64 // page index just past the file's last page
	next uint64 // scan start: pages below it were found used or allocated
}

func newExtentAllocatorV3(used []uint64, fileBytes uint64) *extentAllocatorV3 {
	a := &extentAllocatorV3{used: make(map[uint64]bool, len(used)), end: (fileBytes + 4095) / 4096}
	for _, p := range used {
		a.used[p] = true
	}
	return a
}

// alloc reserves n contiguous pages and returns the byte offset.
func (a *extentAllocatorV3) alloc(n uint64) uint64 {
	run, start := uint64(0), uint64(0)
	for p := a.next; p < a.end; p++ {
		if a.used[p] {
			run = 0
			if p == a.next {
				a.next++
			}
			continue
		}
		if run == 0 {
			start = p
		}
		run++
		if run == n {
			for q := start; q < start+n; q++ {
				a.used[q] = true
			}
			if start == a.next {
				a.next = start + n
			}
			return start * 4096
		}
	}
	if run > 0 && start+run == a.end { // a free run at the end grows in place
		for q := start; q < a.end; q++ {
			a.used[q] = true
		}
		need := n - run
		for q := a.end; q < a.end+need; q++ {
			a.used[q] = true
		}
		a.end += need
		return start * 4096
	}
	off := a.end * 4096
	for q := a.end; q < a.end+n; q++ {
		a.used[q] = true
	}
	a.end += n
	return off
}

// Stats reports the map file's size, live bytes and truncations. The live
// bytes are computed on demand from a snapshot.
func (m *chunkLSNMapV3) Stats() ChunkMapStats {
	_, live := usedPagesV3(m.liveSnapshotV3())
	return ChunkMapStats{FileBytes: m.live.fileBytes.Load(), LiveBytes: live, Truncations: m.live.truncations.Load()}
}

// ChunkMapStatsV3 reports the owned map file's bounded-growth counters, or
// the zero value when the store has no owned map. Close clears the map
// under lifecycleMu, so the read holds it shared.
func (s *WALStore) ChunkMapStatsV3() ChunkMapStats {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if s.v3 == nil || s.v3.ownedMap == nil {
		return ChunkMapStats{}
	}
	return s.v3.ownedMap.Stats()
}
