package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// Row 3 (DESIGN r15 sections 4, 5, 5.1) controls for the v3 descriptor
// publisher, against a real file through a syncFile leaf that behaves like
// WALStore.syncFile (fileSyncMu, sticky latch on fsync failure).

type descHarnessV3 struct {
	t          *testing.T
	f          *os.File
	fileSyncMu sync.Mutex
	fault      atomic.Pointer[error]
	fsyncErr   atomic.Pointer[error]
	afterFsync func() // test hook inside the leaf, after a successful fsync
	// writeHook, when set, replaces the slot pwrite (to inject a full or
	// partial write followed by an error).
	writeHook func(off int64, b []byte) error
	writes    atomic.Int64
	// afterUnlock, when set, runs in the leaf right after fileSyncMu is
	// released (to let a queued store sync run before the caller resumes).
	afterUnlock func()
	storeMu     sync.Mutex
	pub         *descriptorPublisherV3
}

func newDescHarnessV3(t *testing.T) *descHarnessV3 {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "store.v3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	ref := readSlotVector(t, "slot-ref")
	if _, err := f.WriteAt(ref, slotAOffsetV3); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(make([]byte, slotSizeV3), slotBOffsetV3); err != nil {
		t.Fatal(err)
	}
	d, err := decodeSlotV3(ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &descHarnessV3{t: t, f: f}
	h.pub = newDescriptorPublisherV3(d, slotA, nil, descriptorIOV3{
		writeSlot: func(off int64, b []byte) error {
			h.writes.Add(1)
			if h.writeHook != nil {
				return h.writeHook(off, b)
			}
			_, err := f.WriteAt(b, off)
			return err
		},
		latch: func(err error) { h.fault.CompareAndSwap(nil, &err) },
		fsync: func() error {
			if e := h.fsyncErr.Load(); e != nil {
				return *e
			}
			return f.Sync()
		},
		syncFile: func(write, sync func() error) error {
			err := h.leaf(write, sync)
			if h.afterUnlock != nil {
				h.afterUnlock()
			}
			return err
		},
		sticky: func() error {
			if e := h.fault.Load(); e != nil {
				return *e
			}
			return nil
		},
	})
	return h
}

// leaf is the harness syncFile body: one pwrite+fsync under fileSyncMu,
// refused when a fault is latched, latching an fsync failure.
func (h *descHarnessV3) leaf(write, sync func() error) error {
	h.fileSyncMu.Lock()
	defer h.fileSyncMu.Unlock()
	if e := h.fault.Load(); e != nil {
		return *e
	}
	if write != nil {
		if err := write(); err != nil {
			return err
		}
	}
	if err := sync(); err != nil {
		h.fault.CompareAndSwap(nil, &err)
		return err
	}
	if h.afterFsync != nil {
		h.afterFsync()
	}
	return nil
}

// onDisk selects from the file's two slots, as a reopen would.
func (h *descHarnessV3) onDisk() (descriptorV3, slotIDV3) {
	h.t.Helper()
	a := make([]byte, slotSizeV3)
	b := make([]byte, slotSizeV3)
	if _, err := h.f.ReadAt(a, slotAOffsetV3); err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.f.ReadAt(b, slotBOffsetV3); err != nil {
		h.t.Fatal(err)
	}
	d, id, err := selectSlotV3(a, b, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	return d, id
}

func setW(w uint64) func(*descriptorV3) error {
	return func(d *descriptorV3) error { d.W = w; return nil }
}

func TestDescriptorV3PublishAlternatesSlotsAndReopenSelectsNewest(t *testing.T) {
	h := newDescHarnessV3(t)
	d, err := h.pub.publish(&h.storeMu, nil, setW(9))
	if err != nil {
		t.Fatal(err)
	}
	if d.SlotGen != 2 || d.W != 9 {
		t.Fatalf("published gen %d W %d, want gen 2 W 9", d.SlotGen, d.W)
	}
	if got, id := h.onDisk(); id != slotB || got.SlotGen != 2 || got.W != 9 {
		t.Fatalf("reopen selects slot %d gen %d W %d, want B gen 2 W 9", id, got.SlotGen, got.W)
	}
	if _, err := h.pub.publish(&h.storeMu, nil, setW(11)); err != nil {
		t.Fatal(err)
	}
	if got, id := h.onDisk(); id != slotA || got.SlotGen != 3 || got.W != 11 {
		t.Fatalf("reopen selects slot %d gen %d W %d, want A gen 3 W 11", id, got.SlotGen, got.W)
	}
}

func TestDescriptorV3MergeIsMonotonicAndRefusesForbiddenChanges(t *testing.T) {
	h := newDescHarnessV3(t)
	if _, err := h.pub.publish(&h.storeMu, nil, setW(20)); err != nil {
		t.Fatal(err)
	}
	// A stale request (computed before W reached 20) never lowers W or the
	// checkpoint.
	d, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.W, n.CheckpointLSN = 12, 3
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if d.W != 20 || d.CheckpointLSN != 7 {
		t.Fatalf("stale request regressed W %d / checkpoint %d, want 20 / 7", d.W, d.CheckpointLSN)
	}
	before := h.pub.current()
	refused := []struct {
		name   string
		mutate func(*descriptorV3) error
		want   error
	}{
		{"identity", func(n *descriptorV3) error { n.UUID[0] ^= 1; return nil }, errDescriptorIdentityChange},
		{"geometry", func(n *descriptorV3) error { n.WALSize *= 2; return nil }, errDescriptorIdentityChange},
		{"nonce outside reset", func(n *descriptorV3) error { n.Nonce[0] ^= 1; return nil }, errDescriptorTransition},
		{"map root without a higher generation", func(n *descriptorV3) error { n.MapRootHash[0] ^= 1; return nil }, errDescriptorTransition},
		{"map lineage change", func(n *descriptorV3) error {
			n.MapGeneration++
			n.MapLineage[0] ^= 1
			return nil
		}, errDescriptorTransition},
		{"BASE none -> installed", func(n *descriptorV3) error {
			n.BaseState, n.BaseLSN = BaseInstalled, 20
			n.BaseInstallID[0] = 1
			return nil
		}, errDescriptorTransition},
		{"checkpoint above map coverage", func(n *descriptorV3) error { n.CheckpointLSN = 8; return nil }, errSuperblockInvalid},
	}
	for _, c := range refused {
		t.Run(c.name, func(t *testing.T) {
			if _, err := h.pub.publish(&h.storeMu, nil, c.mutate); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			if after := h.pub.current(); after != before {
				t.Fatal("a refused publication changed the authoritative descriptor")
			}
		})
	}
	// A higher map generation in the same lineage is accepted.
	if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.MapGeneration++
		n.MapRootHash[0] ^= 1
		return nil
	}); err != nil {
		t.Fatalf("higher map generation refused: %v", err)
	}
	// Retained bounds never move back within one nonce.
	if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.RetainedTail, n.RetainedHead = 100, 100
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.RetainedTail = 50
		return nil
	}); !errors.Is(err, errDescriptorTransition) {
		t.Fatalf("tail moved back: err = %v, want errDescriptorTransition", err)
	}
}

func TestDescriptorV3ConcurrentPublishersNeverRegress(t *testing.T) {
	h := newDescHarnessV3(t)
	const n = 40
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(w uint64) {
			defer wg.Done()
			if _, err := h.pub.publish(&h.storeMu, nil, setW(w)); err != nil {
				t.Error(err)
			}
		}(uint64(7 + i))
	}
	wg.Wait()
	cur := h.pub.current()
	if cur.W != 7+n || cur.SlotGen != 1+n {
		t.Fatalf("after %d concurrent publications: W %d gen %d, want W %d gen %d", n, cur.W, cur.SlotGen, 7+n, 1+n)
	}
	if got, _ := h.onDisk(); got != cur {
		t.Fatal("reopen selection differs from the authoritative descriptor")
	}
}

func TestDescriptorV3LockOrderRecorder(t *testing.T) {
	h := newDescHarnessV3(t)
	var events []string
	var mu sync.Mutex
	h.pub.trace = func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }
	snapped := false
	if _, err := h.pub.publish(&h.storeMu, func() { snapped = true }, setW(9)); err != nil {
		t.Fatal(err)
	}
	if !snapped {
		t.Fatal("snapshot did not run under WALStore.mu")
	}
	want := []string{"lock WALStore.mu", "lock descMu", "unlock WALStore.mu", "hold fileSyncMu", "unlock descMu"}
	if len(events) != len(want) {
		t.Fatalf("lock events %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("lock events %v, want %v (WALStore.mu must be released before slot I/O)", events, want)
		}
	}
	// The store lock is not held during slot I/O: another goroutine can
	// take it while the publication sits inside fileSyncMu.
	h.pub.trace = nil
	inLeaf := make(chan struct{})
	release := make(chan struct{})
	h.afterFsync = func() { close(inLeaf); <-release }
	done := make(chan error, 1)
	go func() { _, err := h.pub.publish(&h.storeMu, nil, setW(10)); done <- err }()
	<-inLeaf
	if !h.storeMu.TryLock() {
		t.Fatal("WALStore.mu held across descriptor I/O")
	}
	h.storeMu.Unlock()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDescriptorV3FsyncFailureIsStickyAndPublishesNothing(t *testing.T) {
	h := newDescHarnessV3(t)
	before := h.pub.current()
	boom := errors.New("injected fsync failure")
	h.fsyncErr.Store(&boom)
	if _, err := h.pub.publish(&h.storeMu, nil, setW(9)); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the fsync failure", err)
	}
	if h.pub.current() != before {
		t.Fatal("a failed publication changed the authoritative descriptor")
	}
	h.fsyncErr.Store(nil)
	if _, err := h.pub.publish(&h.storeMu, nil, setW(10)); !errors.Is(err, boom) {
		t.Fatalf("publication after a latched fault: err = %v, want the latched fault", err)
	}
}

func TestDescriptorV3FaultLatchedDuringPublicationRefusesAtSuccessBoundary(t *testing.T) {
	h := newDescHarnessV3(t)
	before := h.pub.current()
	other := errors.New("fault latched by another path")
	h.afterFsync = func() { h.fault.CompareAndSwap(nil, &other) }
	if _, err := h.pub.publish(&h.storeMu, nil, setW(9)); !errors.Is(err, other) {
		t.Fatalf("err = %v, want the concurrently latched fault", err)
	}
	if h.pub.current() != before {
		t.Fatal("a publication refused at the success boundary changed the descriptor")
	}
}

func TestDescriptorV3FullRingRolloverAdvancesTheFloorWithoutAnyChange(t *testing.T) {
	h := newDescHarnessV3(t)
	// A checkpoint publishes tail = head = 100 (a full ring of 100 bytes
	// retired): the fallback slot still pins tail 0.
	if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.RetainedTail, n.RetainedHead = 100, 100
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if floor, ok := h.pub.reuseFloor(); !ok || floor != 0 {
		t.Fatalf("floor after the checkpoint = %d (ok %v), want 0: the fallback pins it", floor, ok)
	}
	beforeW, beforeCkpt := h.pub.current().W, h.pub.current().CheckpointLSN
	published, err := h.pub.rolloverIfPinned()
	if err != nil || !published {
		t.Fatalf("rollover: published %v err %v, want a metadata-only publication", published, err)
	}
	cur := h.pub.current()
	if cur.W != beforeW || cur.CheckpointLSN != beforeCkpt || cur.RetainedTail != 100 {
		t.Fatal("rollover changed W, the checkpoint or the tail")
	}
	if floor, ok := h.pub.reuseFloor(); !ok || floor != 100 {
		t.Fatalf("floor after rollover = %d (ok %v), want 100", floor, ok)
	}
	if again, err := h.pub.rolloverIfPinned(); err != nil || again {
		t.Fatalf("second rollover: published %v err %v, want nothing to do", again, err)
	}
}

func TestDescriptorV3RolloverFsyncFailureKeepsTheFloorPinned(t *testing.T) {
	h := newDescHarnessV3(t)
	if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.RetainedTail, n.RetainedHead = 100, 100
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("injected fsync failure")
	h.fsyncErr.Store(&boom)
	if _, err := h.pub.rolloverIfPinned(); !errors.Is(err, boom) {
		t.Fatalf("rollover err = %v, want the fsync failure", err)
	}
	if floor, _ := h.pub.reuseFloor(); floor != 0 {
		t.Fatalf("floor = %d after a failed rollover, want 0 (still pinned)", floor)
	}
}

func TestDescriptorV3MixedNoncesNeverYieldAReuseFloor(t *testing.T) {
	h := newDescHarnessV3(t)
	old := h.pub.current()
	old.Nonce[0] ^= 0xFF
	h.pub.other, h.pub.otherValid = old, true
	if _, ok := h.pub.reuseFloor(); ok {
		t.Fatal("a reuse floor was computed across different nonces")
	}
	if published, err := h.pub.rolloverIfPinned(); err != nil || published {
		t.Fatalf("rollover across nonces: published %v err %v, want nothing", published, err)
	}
}

func TestDescriptorV3BaseTransitionTable(t *testing.T) {
	id1 := [16]byte{1}
	id2 := [16]byte{2}
	mk := func(state BaseState, lsn uint64, id [16]byte) descriptorV3 {
		return descriptorV3{BaseState: state, BaseLSN: lsn, BaseInstallID: id, CheckpointLSN: 7}
	}
	cases := []struct {
		name     string
		cur, nxt descriptorV3
		ok       bool
	}{
		{"none -> installing", mk(BaseNone, 0, [16]byte{}), mk(BaseInstalling, 50, id1), true},
		{"installing -> installed same ID", mk(BaseInstalling, 50, id1), mk(BaseInstalled, 50, id1), true},
		{"installing -> installed other ID", mk(BaseInstalling, 50, id1), mk(BaseInstalled, 50, id2), false},
		{"installing -> superseding installing", mk(BaseInstalling, 50, id1), mk(BaseInstalling, 60, id2), true},
		{"installing -> installing same ID new BaseLSN", mk(BaseInstalling, 50, id1), mk(BaseInstalling, 60, id1), false},
		{"installing -> none is refused (no abort, r17)", mk(BaseInstalling, 50, id1), mk(BaseNone, 0, [16]byte{}), false},
		{"none -> installed", mk(BaseNone, 0, [16]byte{}), mk(BaseInstalled, 50, id1), false},
		{"installed -> none needs checkpoint past BaseLSN", mk(BaseInstalled, 50, id1), mk(BaseNone, 0, [16]byte{}), false},
		{"installed -> installing is not an 8.5 edge", mk(BaseInstalled, 50, id1), mk(BaseInstalling, 60, id2), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := baseTransitionAllowedV3(&c.cur, &c.nxt)
			if (err == nil) != c.ok {
				t.Fatalf("err = %v, want allowed = %v", err, c.ok)
			}
		})
	}
	cur := mk(BaseInstalled, 50, id1)
	nxt := mk(BaseNone, 0, [16]byte{})
	nxt.CheckpointLSN = 50
	if err := baseTransitionAllowedV3(&cur, &nxt); err != nil {
		t.Fatalf("installed -> none with the checkpoint at BaseLSN refused: %v", err)
	}
	// Retirement also needs both slots' tails at or past BaseCursor (8.4).
	cur.BaseCursor, cur.RetainedTail, nxt.RetainedTail = 100, 100, 99
	if err := baseTransitionAllowedV3(&cur, &nxt); err == nil {
		t.Fatal("installed -> none with the new tail below BaseCursor allowed")
	}
	cur.RetainedTail, nxt.RetainedTail = 99, 100
	if err := baseTransitionAllowedV3(&cur, &nxt); err == nil {
		t.Fatal("installed -> none with the fallback tail below BaseCursor allowed")
	}
}

// codex01 row 3 review P1: a slot write error (full or partial), like an
// fsync error, latches the store, keeps the candidate pinned and refuses
// every further publication, rollover and reuse with no further I/O.
// Reopen (read back and select) is the positive control.
func TestDescriptorV3SlotWriteErrorLatchesAndPinsTheCandidate(t *testing.T) {
	for _, partial := range []bool{true, false} {
		name := "full write then error"
		if partial {
			name = "partial write then error"
		}
		t.Run(name, func(t *testing.T) {
			h := newDescHarnessV3(t)
			// A checkpoint first, so the fallback pins tail 0 and a
			// rollover would otherwise be due.
			if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
				n.RetainedTail, n.RetainedHead = 100, 100
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before := h.pub.current()
			boom := errors.New("injected slot write error")
			h.writeHook = func(off int64, b []byte) error {
				n := len(b)
				if partial {
					n = 1000
				}
				if _, err := h.f.WriteAt(b[:n], off); err != nil {
					t.Fatal(err)
				}
				return boom
			}
			_, err := h.pub.publish(&h.storeMu, nil, setW(30))
			if !errors.Is(err, ErrDescriptorPublishUncertain) || !errors.Is(err, boom) {
				t.Fatalf("err = %v, want ErrDescriptorPublishUncertain wrapping the write error", err)
			}
			if h.pub.current() != before {
				t.Fatal("a failed publication changed the authoritative descriptor")
			}
			if cand, ok := h.pub.uncertainCandidate(); !ok || cand.W != 30 || cand.SlotGen != before.SlotGen+1 {
				t.Fatalf("uncertain candidate not retained: ok %v %+v", ok, cand)
			}
			if h.fault.Load() == nil {
				t.Fatal("the store fault was not latched: other store paths could still acknowledge or reclaim")
			}
			h.writeHook = nil
			writes := h.writes.Load()
			if _, err := h.pub.publish(&h.storeMu, nil, setW(31)); !errors.Is(err, ErrDescriptorPublishUncertain) {
				t.Fatalf("publication after the failure: err = %v, want ErrDescriptorPublishUncertain", err)
			}
			if _, err := h.pub.rolloverIfPinned(); !errors.Is(err, ErrDescriptorPublishUncertain) {
				t.Fatalf("rollover after the failure: err = %v, want ErrDescriptorPublishUncertain", err)
			}
			if _, ok := h.pub.reuseFloor(); ok {
				t.Fatal("reuse allowed while a publication outcome is uncertain")
			}
			if got := h.writes.Load(); got != writes {
				t.Fatalf("%d further slot writes after the failure, want none", got-writes)
			}
			// Reopen resolves it: a torn slot fails its CRC and the
			// previous generation is selected; a complete slot is the new one.
			got, _ := h.onDisk()
			if partial && got != before {
				t.Fatalf("reopen after a torn write selected gen %d W %d, want the previous gen %d", got.SlotGen, got.W, before.SlotGen)
			}
			if !partial && (got.SlotGen != before.SlotGen+1 || got.W != 30) {
				t.Fatalf("reopen after a complete write selected gen %d W %d, want gen %d W 30", got.SlotGen, got.W, before.SlotGen+1)
			}
		})
	}
}

// codex01 row 3 review P2: counters beyond 2^64-2 are refused at
// publication with ErrCounterExhausted, before any write.
func TestDescriptorV3PublicationCounterExhaustion(t *testing.T) {
	const over = counterMaxV3 + 1
	for _, c := range []struct {
		name string
		set  func(*descriptorV3)
	}{
		{"W", func(n *descriptorV3) { n.W = over }},
		{"CheckpointLSN", func(n *descriptorV3) { n.CheckpointLSN = over }},
		{"retainedHead", func(n *descriptorV3) { n.RetainedHead = over }},
		{"mapGeneration", func(n *descriptorV3) { n.MapGeneration = over; n.MapRootHash[0] ^= 1 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newDescHarnessV3(t)
			writes := h.writes.Load()
			if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error { c.set(n); return nil }); !errors.Is(err, ErrCounterExhausted) {
				t.Fatalf("err = %v, want ErrCounterExhausted", err)
			}
			if h.writes.Load() != writes {
				t.Fatal("an exhausted publication wrote a slot")
			}
		})
	}
}

// codex01 row 3 round 2 P1: the store fault for a slot write error is
// latched inside the leaf, before fileSyncMu is released. An independent
// store Sync B queued on fileSyncMu behind A's failing slot write must be
// refused and must not fsync or acknowledge. The harness lets B run right
// after the leaf unlocks and before A's publisher resumes, so a latch taken
// after the unlock would let B succeed.
func TestDescriptorV3QueuedStoreSyncCannotAckAfterSlotWriteError(t *testing.T) {
	for _, partial := range []bool{true, false} {
		name := "full write then error"
		if partial {
			name = "partial write then error"
		}
		t.Run(name, func(t *testing.T) {
			h := newDescHarnessV3(t)
			boom := errors.New("injected slot write error")
			bResult := make(chan error, 1)
			var bFsyncs atomic.Int64
			h.writeHook = func(off int64, b []byte) error {
				n := len(b)
				if partial {
					n = 1000
				}
				if _, err := h.f.WriteAt(b[:n], off); err != nil {
					t.Error(err)
				}
				// B queues on fileSyncMu while A still holds it.
				go func() {
					bResult <- h.leaf(nil, func() error { bFsyncs.Add(1); return nil })
				}()
				return boom
			}
			var bErr error
			h.afterUnlock = func() { bErr = <-bResult }
			if _, err := h.pub.publish(&h.storeMu, nil, setW(30)); !errors.Is(err, ErrDescriptorPublishUncertain) {
				t.Fatalf("A: err = %v, want ErrDescriptorPublishUncertain", err)
			}
			if bErr == nil || bFsyncs.Load() != 0 {
				t.Fatalf("queued store sync B acknowledged after A's slot write failed (err %v, fsyncs %d)", bErr, bFsyncs.Load())
			}
			if !errors.Is(bErr, ErrDescriptorPublishUncertain) {
				t.Fatalf("B refused with %v, want the latched ErrDescriptorPublishUncertain", bErr)
			}
		})
	}
}
