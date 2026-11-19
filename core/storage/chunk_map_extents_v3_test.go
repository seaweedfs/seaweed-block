package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Extent reuse in the owned-map file.
// Map-level schedules on a one-page map (mapDescriptorV3: one chunk), with
// the publisher's slot report driven directly so each schedule is exact.

func extentsMapV3(t *testing.T) (*chunkLSNMapV3, descriptorV3, string) {
	t.Helper()
	d := mapDescriptorV3(t)
	path := filepath.Join(t.TempDir(), "map")
	m, err := createChunkLSNMapV3(path, d, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m, d, path
}

func prepV3(t *testing.T, m *chunkLSNMapV3, g uint64, prior *chunkMapCandidateV3) *chunkMapCandidateV3 {
	t.Helper()
	c, err := m.Prepare(g, 7, []chunkLSNEntryV3{{LSN: 100 + g}}, prior)
	if err != nil {
		t.Fatalf("prepare gen %d: %v", g, err)
	}
	return c
}

func readsV3(t *testing.T, m *chunkLSNMapV3, d descriptorV3, c *chunkMapCandidateV3, what string) {
	t.Helper()
	es, _, err := readChunkLSNMapV3(m.fd, c.descriptor(d), m.chunkBlocks)
	if err != nil {
		t.Fatalf("%s (gen %d at %d) unreadable: %v", what, c.generation, c.rootOffset, err)
	}
	if es[0].LSN != 100+c.generation {
		t.Fatalf("%s (gen %d) reads LSN %d", what, c.generation, es[0].LSN)
	}
}

func publishV3(m *chunkLSNMapV3, d descriptorV3, cur, other *chunkMapCandidateV3) {
	m.slotsPublishedV3(cur.descriptor(d), other.descriptor(d), true)
}

func mapFileSizeV3(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// Every publication leaves the previous generation in the fallback slot. Its
// root and page must stay readable after every later candidate, and the file
// stays within 8 one-page maps. Mutants: a live set without the fallback
// slot (a later candidate overwrites the fallback's extents) and an
// append-only allocator (the file grows by a page and a root per candidate).
func TestChunkMapExtentsV3FallbackRootIsNeverReused(t *testing.T) {
	m, d, path := extentsMapV3(t)
	prev := prepV3(t, m, 1, nil)
	publishV3(m, d, prev, prev)
	for g := uint64(2); g <= 60; g++ {
		c := prepV3(t, m, g, prev)
		publishV3(m, d, c, prev)
		readsV3(t, m, d, c, "current")
		readsV3(t, m, d, prev, "fallback")
		prev = c
	}
	if size := mapFileSizeV3(t, path); size > 8*2*4096 {
		t.Fatalf("map file %d B after 60 generations, bound %d B", size, 8*2*4096)
	}
}

// A candidate prepared after another one that is not yet published (two
// preparers) is newer than the generation the first publication makes
// current, so it stays live and the next candidate must not reuse its
// extents. Mutant: releasing every unpublished candidate at a publication
// overwrites it.
func TestChunkMapExtentsV3NewerInFlightCandidateIsNotReused(t *testing.T) {
	m, d, _ := extentsMapV3(t)
	c1 := prepV3(t, m, 1, nil)
	publishV3(m, d, c1, c1)
	c2 := prepV3(t, m, 2, c1)
	c3 := prepV3(t, m, 3, c1) // a second preparer, not yet published
	publishV3(m, d, c2, c1)
	c4 := prepV3(t, m, 4, c2)
	readsV3(t, m, d, c3, "in-flight gen 3")
	readsV3(t, m, d, c4, "gen 4")
	readsV3(t, m, d, c1, "fallback")
}

// A crash after a candidate was written into reused extents but before any
// publication named it: the reopen of the selected slot and its fallback
// reads both intact, because a candidate only ever takes extents no slot
// names.
func TestChunkMapExtentsV3CrashBeforePublicationOfAReusedExtent(t *testing.T) {
	m, d, path := extentsMapV3(t)
	prev := prepV3(t, m, 1, nil)
	publishV3(m, d, prev, prev)
	var cur *chunkMapCandidateV3
	for g := uint64(2); g <= 10; g++ {
		cur = prepV3(t, m, g, prev)
		publishV3(m, d, cur, prev)
		prev, cur = cur, prev
	}
	cur, prev = prev, cur // cur = gen 10, prev = gen 9
	orphan := prepV3(t, m, 11, cur)
	if orphan.rootOffset+4096 >= uint64(mapFileSizeV3(t, path)) {
		t.Fatalf("setup: gen 11's root at %d did not reuse a freed extent", orphan.rootOffset)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	r, rc, err := openChunkLSNMapV3(path, cur.descriptor(d), 1)
	if err != nil {
		t.Fatalf("reopen of the selected slot after the crash: %v", err)
	}
	defer r.Close()
	if rc.entries[0].LSN != 100+cur.generation {
		t.Fatalf("selected slot reads LSN %d", rc.entries[0].LSN)
	}
	if _, _, err := readChunkLSNMapV3(r.fd, prev.descriptor(d), 1); err != nil {
		t.Fatalf("fallback after the crash: %v", err)
	}
}

// Truncation while the fallback slot's root lies at the file's tail: a burst
// of unpublished candidates raises the high-water mark; the fallback then is
// the highest of them and the current one lands low. The next candidate
// must not truncate below the fallback; once a later publication releases
// it, the free tail is truncated. Mutant: truncating above the lowest live
// page cuts the fallback's root away.
func TestChunkMapExtentsV3TruncationKeepsAFallbackAtTheTail(t *testing.T) {
	m, d, path := extentsMapV3(t)
	c := prepV3(t, m, 1, nil)
	publishV3(m, d, c, c)
	var burst []*chunkMapCandidateV3
	for g := uint64(2); g <= 21; g++ {
		burst = append(burst, prepV3(t, m, g, c)) // unpublished: the file grows
	}
	top := burst[len(burst)-1] // gen 21, the highest extents
	publishV3(m, d, top, c)    // releases gens 2..20
	low := prepV3(t, m, 22, top)
	if low.rootOffset > top.rootOffset {
		t.Fatalf("setup: gen 22 at %d did not land below gen 21 at %d", low.rootOffset, top.rootOffset)
	}
	publishV3(m, d, low, top) // fallback = gen 21 at the tail
	next := prepV3(t, m, 23, low)
	readsV3(t, m, d, top, "fallback at the tail")
	publishV3(m, d, next, low) // releases gen 21
	before := mapFileSizeV3(t, path)
	last := prepV3(t, m, 24, next)
	readsV3(t, m, d, last, "gen 24")
	readsV3(t, m, d, next, "gen 23")
	if after := mapFileSizeV3(t, path); after >= before || m.Stats().Truncations == 0 {
		t.Fatalf("free tail not truncated: %d B -> %d B, truncations %d", before, after, m.Stats().Truncations)
	}
}

// Repeated publications refused before any slot I/O (a stale proof) release
// their candidates at once, so the file and the live set stay bounded.
// Mutant: no definite-abandon path keeps every rejected candidate pinned
// until a later successful publication and the file grows by a root each.
func TestChunkMapExtentsV3RejectedPublicationsStayBounded(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	cp := s.v3.pub.current().CheckpointLSN
	for i := 0; i < 300; i++ {
		prepared, err := s.prepareOwnedMapCheckpointV3(cp)
		if err != nil {
			t.Fatalf("prepare %d: %v", i, err)
		}
		if err := s.v3.pub.invalidateMapProofV3(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err == nil {
			t.Fatal("a stale-proof publication succeeded")
		}
	}
	st := s.ChunkMapStatsV3()
	fi, err := os.Stat(path + ".chunkmap")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > 8*2*4096 || st.LiveBytes > 4*2*4096 {
		t.Fatalf("after 300 rejected publications: file %d B, live %d B", fi.Size(), st.LiveBytes)
	}
}

// damageNewestSlotV3 corrupts the slot with the higher generation so the
// next open selects the fallback slot.
func damageNewestSlotV3(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var gens [2]uint64
	offs := [2]int64{slotAOffsetV3, slotBOffsetV3}
	for i, off := range offs {
		b := make([]byte, slotSizeV3)
		if _, err := f.ReadAt(b, off); err != nil {
			t.Fatal(err)
		}
		if d, err := decodeSlotV3(b, nil); err == nil {
			gens[i] = d.SlotGen
		}
	}
	off := offs[0]
	if gens[1] > gens[0] {
		off = offs[1]
	}
	if _, err := f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, off+64); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// A real two-slot reopen registers BOTH slot roots read from disk, so a
// candidate prepared before the open's first publication (which would
// re-report both slots) cannot take the fallback root's extents. The
// candidates are prepared right after the open, none is published, and the
// store crashes; with the newest slot damaged, the reopen selects the
// fallback slot and its map must validate. Mutant: an open that does not
// register the fallback slot's root lets the candidates overwrite it.
func TestChunkMapExtentsV3ReopenRegistersTheFallbackRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapped")
	s, err := createMappedWALStoreV3ForTest(path, 64, 4096, v3TestWAL, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	for i := uint32(0); i < 10; i++ {
		writeSynced(t, s, i, i)
		if err := s.flusher.flushOnceInternal(false); err != nil {
			t.Fatal(err)
		}
	}
	// One more map publication without the checkpoint's rollover, so the two
	// slots name different map roots at the crash.
	writeSynced(t, s, 12, 12)
	prepared, err := s.prepareOwnedMapCheckpointV3(s.v3.pub.current().CheckpointLSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	cp := r.v3.pub.current().CheckpointLSN
	r.v3.pub.descMu.Lock()
	cur, other, otherValid := r.v3.pub.cur, r.v3.pub.other, r.v3.pub.otherValid
	r.v3.pub.descMu.Unlock()
	if !otherValid || cur.MapGeneration == other.MapGeneration {
		t.Fatalf("setup: slots name map generations %d and %d (fallback valid %v)", cur.MapGeneration, other.MapGeneration, otherValid)
	}
	for i := 0; i < 20; i++ { // unpublished candidates fill every free extent
		if _, err := r.prepareOwnedMapCheckpointV3(cp); err != nil {
			t.Fatal(err)
		}
	}
	crashRecoveryHardeningStore(t, r)
	damageNewestSlotV3(t, path)
	q, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatalf("reopen from the fallback slot: %v", err)
	}
	q.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, q) })
	if _, err := q.Recover(); err != nil {
		t.Fatalf("recover from the fallback slot: %v", err)
	}
}

// A failed fsync of the tail truncation itself (the first fsync of the
// prepare) latches the map: the prepare and every later one fail. The slot
// roots stay readable, because the truncation only removes extents above
// the highest live page.
func TestChunkMapExtentsV3TruncationFaultKeepsTheSlotRoots(t *testing.T) {
	m, d, path := extentsMapV3(t)
	c := prepV3(t, m, 1, nil)
	publishV3(m, d, c, c)
	var top *chunkMapCandidateV3
	for g := uint64(2); g <= 21; g++ {
		top = prepV3(t, m, g, c)
	}
	publishV3(m, d, top, c)
	low := prepV3(t, m, 22, top)
	publishV3(m, d, low, top)
	next := prepV3(t, m, 23, low)
	publishV3(m, d, next, low) // gen 21 released: the tail is free
	before := mapFileSizeV3(t, path)
	orig, calls := m.syncFile, 0
	m.syncFile = func() error {
		calls++
		if calls == 1 {
			return os.ErrInvalid // the truncation's fsync
		}
		return orig()
	}
	if _, err := m.Prepare(24, 7, []chunkLSNEntryV3{{LSN: 124}}, next); err == nil {
		t.Fatal("prepare across a failed truncation fsync succeeded")
	}
	if calls != 1 || mapFileSizeV3(t, path) >= before {
		t.Fatalf("setup: %d fsyncs, file %d B -> %d B; want the truncation's fsync to fail", calls, before, mapFileSizeV3(t, path))
	}
	if got, want := m.Stats().FileBytes, uint64(mapFileSizeV3(t, path)); got != want {
		t.Fatalf("stats after a truncation whose fsync failed: file bytes %d, file is %d B", got, want)
	}
	if _, err := m.Prepare(25, 7, []chunkLSNEntryV3{{LSN: 125}}, next); err == nil || calls != 1 {
		t.Fatalf("the map did not latch: err %v, fsyncs %d", err, calls)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	r, rc, err := openChunkLSNMapV3(path, next.descriptor(d), 1)
	if err != nil {
		t.Fatalf("reopen after the truncation fault: %v", err)
	}
	defer r.Close()
	if rc.entries[0].LSN != 100+next.generation {
		t.Fatalf("selected root reads LSN %d", rc.entries[0].LSN)
	}
	if _, _, err := readChunkLSNMapV3(r.fd, low.descriptor(d), 1); err != nil {
		t.Fatalf("fallback after the truncation fault: %v", err)
	}
}

// A publication whose slot I/O failed leaves an uncertain candidate: the
// publisher latches and the candidate stays pinned until reopen. A later
// admitted prepare must not reuse its extents, and both the uncertain
// candidate and the slot roots stay readable. Mutant: abandoning the
// uncertain candidate like a refused one lets the later prepare overwrite it.
func TestChunkMapExtentsV3UncertainCandidateStaysPinned(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	cp := s.v3.pub.current().CheckpointLSN
	prepared, err := s.prepareOwnedMapCheckpointV3(cp)
	if err != nil {
		t.Fatal(err)
	}
	s.v3.testBeforeSlotWrite = func() error { return os.ErrInvalid }
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err == nil {
		t.Fatal("a publication with a failed slot write succeeded")
	}
	s.v3.testBeforeSlotWrite = nil
	m := s.v3.ownedMap
	unc := prepared.candidate
	for i := 0; i < 5; i++ { // admitted prepares after the failure
		if _, err := m.prepareNextV3(cp, append([]chunkLSNEntryV3(nil), s.v3.mapEntries...), unc); err != nil {
			t.Fatalf("admitted prepare %d: %v", i, err)
		}
	}
	d := s.v3.pub.current()
	if _, _, err := readChunkLSNMapV3(m.fd, unc.descriptor(d), m.chunkBlocks); err != nil {
		t.Fatalf("uncertain candidate overwritten: %v", err)
	}
	if _, _, err := readChunkLSNMapV3(m.fd, d, m.chunkBlocks); err != nil {
		t.Fatalf("selected slot root overwritten: %v", err)
	}
}

// ChunkMapStatsV3 is correct right after a reopen: the file size and the
// live bytes of the two slot roots read from disk, not zero.
func TestChunkMapExtentsV3StatsAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapped")
	s, err := createMappedWALStoreV3ForTest(path, 64, 4096, v3TestWAL, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	for i := uint32(0); i < 5; i++ {
		writeSynced(t, s, i, i)
		if err := s.flusher.flushOnceInternal(false); err != nil {
			t.Fatal(err)
		}
	}
	crashRecoveryHardeningStore(t, s)
	fi, err := os.Stat(path + ".chunkmap")
	if err != nil {
		t.Fatal(err)
	}
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	st := r.ChunkMapStatsV3()
	if st.FileBytes != uint64(fi.Size()) || st.LiveBytes == 0 || st.LiveBytes > st.FileBytes {
		t.Fatalf("stats after reopen: %+v, file %d B", st, fi.Size())
	}
}

// A candidate that is prepared successfully but whose proof went stale
// during its file I/O (a guest write changed the map meanwhile) is
// refused before publication and must be released at once, so a steady
// stream of such refusals keeps the file bounded. Both preparation paths
// are covered: the checkpoint's and prepareMapPublicationV3. Mutant: not
// abandoning after the proof check pins every refused candidate.
func TestChunkMapExtentsV3ProofStalePreparesStayBounded(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	m := s.v3.ownedMap
	orig := m.syncFile
	m.syncFile = func() error { // the proof goes stale during the prepare's I/O
		if err := s.v3.mapProof.mutatedV3(); err != nil {
			return err
		}
		return orig()
	}
	cp := s.v3.pub.current().CheckpointLSN
	for i := 0; i < 150; i++ {
		_, err := s.prepareOwnedMapCheckpointV3(cp)
		var reason mapCoverageRefusal
		if !errors.Is(err, ErrMapCoverageStale) || !errors.As(err, &reason) || reason != "proof_changed_during_prepare" {
			t.Fatalf("checkpoint prepare %d: err = %v, want ErrMapCoverageStale", i, err)
		}
	}
	g := s.v3.pub.current().MapGeneration
	for i := 0; i < 150; i++ {
		g += 1000
		entries := append([]chunkLSNEntryV3(nil), s.v3.mapEntries...)
		_, err := prepareMapPublicationV3(m, s.v3.mapProof, g, cp, entries, nil)
		var reason mapCoverageRefusal
		if !errors.Is(err, ErrMapCoverageStale) || !errors.As(err, &reason) || reason != "proof_changed_during_prepare" {
			t.Fatalf("publication prepare %d: err = %v, want ErrMapCoverageStale", i, err)
		}
	}
	m.syncFile = orig
	fi, err := os.Stat(path + ".chunkmap")
	if err != nil {
		t.Fatal(err)
	}
	if st := s.ChunkMapStatsV3(); fi.Size() > 8*2*4096 || st.LiveBytes > 4*2*4096 {
		t.Fatalf("after 300 proof-stale prepares: file %d B, live %d B", fi.Size(), st.LiveBytes)
	}
}

// A publication whose slot write and fsync both happened, but which a fault
// latched meanwhile refuses at its success boundary, may be named by the
// slot on disk. Its candidate stays pinned: an already admitted preparer
// does not reuse its extents, and after a crash the reopen selects that
// slot and its map validates. Mutant: treating that refusal like one before
// slot I/O abandons the candidate and the preparer overwrites it.
func TestChunkMapExtentsV3DurableButRefusedPublicationStaysPinned(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapped")
	s, err := createMappedWALStoreV3ForTest(path, 64, 4096, v3TestWAL, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	for i := uint32(0); i < 4; i++ {
		writeSynced(t, s, i, i)
		if err := s.flusher.flushOnceInternal(false); err != nil {
			t.Fatal(err)
		}
	}
	writeSynced(t, s, 9, 9)
	cp := s.v3.pub.current().CheckpointLSN
	prepared, err := s.prepareOwnedMapCheckpointV3(cp)
	if err != nil {
		t.Fatal(err)
	}
	orig := s.v3.pub.io.fsync
	s.v3.pub.io.fsync = func() error { // the slot reaches disk, then a fault latches
		err := orig()
		s.latchV3(os.ErrInvalid)
		return err
	}
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err == nil {
		t.Fatal("a publication refused at its success boundary returned no error")
	}
	s.v3.pub.io.fsync = orig
	m := s.v3.ownedMap
	for i := 0; i < 10; i++ { // an already admitted preparer keeps going
		entries := append([]chunkLSNEntryV3(nil), s.v3.mapEntries...)
		entries[0].LSN += uint64(i + 1)
		if _, err := m.prepareNextV3(cp, entries, prepared.candidate); err != nil {
			t.Fatalf("admitted prepare %d: %v", i, err)
		}
	}
	crashRecoveryHardeningStore(t, s)
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatalf("reopen of the durable slot: %v", err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	if g := r.v3.pub.current().MapGeneration; g != prepared.candidate.generation {
		t.Fatalf("reopen selected map generation %d, want the durable one %d", g, prepared.candidate.generation)
	}
}
