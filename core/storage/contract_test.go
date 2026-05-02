package storage_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// LogicalStorage is re-aliased for brevity inside this _test package.
type LogicalStorage = storage.LogicalStorage

// Contract tests for the LogicalStorage interface. Each test runs
// against every registered implementation. A new implementation slots
// in by adding a constructor to the implementations() table — no test
// duplication needed.
//
// What these tests assert (Phase 07 contract surface):
//
//   - basic Write / Read round-trip
//   - Sync returns the highest written LSN, monotonically non-decreasing
//   - Boundaries (R/S/H) advance correctly
//   - ApplyEntry honors the supplied LSN (replication path)
//   - AdvanceFrontier / AdvanceWALTail behave as documented
//   - Close is idempotent
//
// File-specific recovery tests (Recover, restart-survive) live in
// file_storage_test.go since the in-memory store has nothing to recover.

type implFactory struct {
	name string
	// fresh creates a new empty store. dir is a per-test temp dir the
	// implementation may use (in-memory ignores it).
	fresh func(t *testing.T, dir string, numBlocks uint32, blockSize int) LogicalStorage
}

func implementations() []implFactory {
	return []implFactory{
		{
			name: "BlockStore-mem",
			fresh: func(t *testing.T, dir string, numBlocks uint32, blockSize int) LogicalStorage {
				return storage.NewBlockStore(numBlocks, blockSize)
			},
		},
		{
			name: "WALStore",
			fresh: func(t *testing.T, dir string, numBlocks uint32, blockSize int) LogicalStorage {
				path := filepath.Join(dir, "store.bin")
				s, err := storage.CreateWALStore(path, numBlocks, blockSize)
				if err != nil {
					t.Fatalf("CreateWALStore: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
		{
			name: "MemoryWAL",
			fresh: func(t *testing.T, dir string, numBlocks uint32, blockSize int) LogicalStorage {
				return memorywal.NewStore(numBlocks, blockSize)
			},
		},
	}
}

// runForEachImpl executes fn against every registered implementation as
// a subtest. Use this to write one test body that proves the contract
// holds for both backends.
func runForEachImpl(t *testing.T, fn func(t *testing.T, store LogicalStorage)) {
	t.Helper()
	for _, impl := range implementations() {
		impl := impl
		t.Run(impl.name, func(t *testing.T) {
			dir := t.TempDir()
			store := impl.fresh(t, dir, 16, 4096)
			fn(t, store)
		})
	}
}

func makeBlock(blockSize int, fillByte byte) []byte {
	b := make([]byte, blockSize)
	for i := range b {
		b[i] = fillByte
	}
	return b
}

func TestContract_WriteReadRoundTrip(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		want := makeBlock(store.BlockSize(), 0xAB)
		lsn, err := store.Write(0, want)
		if err != nil {
			t.Fatal(err)
		}
		if lsn == 0 {
			t.Fatal("Write returned LSN 0")
		}
		got, err := store.Read(0)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read returned wrong bytes: got[0]=%02x want[0]=%02x", got[0], want[0])
		}
	})
}

func TestContract_ReadUnwrittenLBAReturnsZeros(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		got, err := store.Read(7)
		if err != nil {
			t.Fatal(err)
		}
		zero := make([]byte, store.BlockSize())
		if !bytes.Equal(got, zero) {
			t.Fatalf("unread LBA should be zeros, got non-zero at index 0: %02x", got[0])
		}
	})
}

func TestContract_WriteAdvancesLSN(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		var lastLSN uint64
		for i := uint32(0); i < 5; i++ {
			lsn, err := store.Write(i, makeBlock(store.BlockSize(), byte(i)))
			if err != nil {
				t.Fatal(err)
			}
			if lsn <= lastLSN {
				t.Fatalf("LSN did not strictly advance: prev=%d got=%d", lastLSN, lsn)
			}
			lastLSN = lsn
		}
	})
}

func TestContract_SyncReturnsHighestWrittenLSN(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		var highest uint64
		for i := uint32(0); i < 3; i++ {
			lsn, err := store.Write(i, makeBlock(store.BlockSize(), 1))
			if err != nil {
				t.Fatal(err)
			}
			highest = lsn
		}
		synced, err := store.Sync()
		if err != nil {
			t.Fatalf("Sync: %v", err)
		}
		if synced != highest {
			t.Fatalf("Sync returned %d, want %d (highest written)", synced, highest)
		}
	})
}

func TestContract_SyncMonotonicNonDecreasing(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		var prev uint64
		for round := 0; round < 3; round++ {
			for i := uint32(0); i < 2; i++ {
				_, err := store.Write(i, makeBlock(store.BlockSize(), byte(round)))
				if err != nil {
					t.Fatal(err)
				}
			}
			synced, err := store.Sync()
			if err != nil {
				t.Fatal(err)
			}
			if synced < prev {
				t.Fatalf("Sync regressed: prev=%d got=%d", prev, synced)
			}
			prev = synced
		}
	})
}

func TestContract_BoundariesReflectWrites(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		R, S, H := store.Boundaries()
		if R != 0 || S != 0 || H != 0 {
			t.Fatalf("fresh store: R=%d S=%d H=%d, all want 0", R, S, H)
		}

		_, err := store.Write(0, makeBlock(store.BlockSize(), 1))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = store.Sync()
		R, _, H = store.Boundaries()
		if R == 0 || H == 0 {
			t.Fatalf("after write+sync: R=%d H=%d, both should be > 0", R, H)
		}
		if R != H {
			t.Fatalf("after sync of single write: R=%d H=%d should be equal", R, H)
		}
	})
}

func TestContract_ApplyEntryUsesSuppliedLSN(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		want := makeBlock(store.BlockSize(), 0xCD)
		// ApplyEntry is the replication path — the LSN comes from the
		// source, not from the local allocator.
		err := store.ApplyEntry(3, want, 100)
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Read(3)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("ApplyEntry data did not round-trip")
		}
		_, _, H := store.Boundaries()
		if H != 100 {
			t.Fatalf("ApplyEntry should advance H to supplied LSN: H=%d, want 100", H)
		}
		// Subsequent local Write must not collide with the applied LSN.
		nextLSN, err := store.Write(4, makeBlock(store.BlockSize(), 0xEF))
		if err != nil {
			t.Fatal(err)
		}
		if nextLSN <= 100 {
			t.Fatalf("Write after ApplyEntry: lsn=%d, should be > 100", nextLSN)
		}
	})
}

func TestContract_AdvanceFrontierBumpsH(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		store.AdvanceFrontier(50)
		_, _, H := store.Boundaries()
		if H != 50 {
			t.Fatalf("AdvanceFrontier(50) should set H=50, got %d", H)
		}
		// Subsequent write must allocate an LSN beyond the advanced frontier.
		lsn, err := store.Write(0, makeBlock(store.BlockSize(), 1))
		if err != nil {
			t.Fatal(err)
		}
		if lsn <= 50 {
			t.Fatalf("Write after AdvanceFrontier(50): lsn=%d should be > 50", lsn)
		}
	})
}

func TestContract_AdvanceWALTailMovesS(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		_, _ = store.Write(0, makeBlock(store.BlockSize(), 1))
		_, _ = store.Write(1, makeBlock(store.BlockSize(), 2))
		store.AdvanceWALTail(20)
		_, S, _ := store.Boundaries()
		if S != 20 {
			t.Fatalf("AdvanceWALTail(20): S=%d, want 20", S)
		}
		// Tail must not regress.
		store.AdvanceWALTail(5)
		_, S, _ = store.Boundaries()
		if S != 20 {
			t.Fatalf("AdvanceWALTail(5) after 20 should not regress: S=%d, want 20", S)
		}
	})
}

func TestContract_AllBlocksReturnsCurrentSnapshot(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		_, _ = store.Write(0, makeBlock(store.BlockSize(), 0x01))
		_, _ = store.Write(2, makeBlock(store.BlockSize(), 0x02))
		_, _ = store.Write(5, makeBlock(store.BlockSize(), 0x05))
		all := store.AllBlocks()
		if len(all) != 3 {
			t.Fatalf("AllBlocks: got %d entries, want 3", len(all))
		}
		if all[0][0] != 0x01 || all[2][0] != 0x02 || all[5][0] != 0x05 {
			t.Fatalf("AllBlocks: wrong content")
		}
	})
}

func TestContract_CloseIsIdempotent(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		if err := store.Close(); err != nil {
			t.Fatalf("first Close: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("second Close (must be idempotent): %v", err)
		}
	})
}

func TestContract_RecoverOnFreshStoreReturnsZero(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		recovered, err := store.Recover()
		if err != nil {
			t.Fatal(err)
		}
		if recovered != 0 {
			t.Fatalf("fresh store Recover: got %d, want 0", recovered)
		}
	})
}

// TestContract_WriteExtentDirect — INV-RECV-BITMAP-CORE (§6.10).
// WriteExtentDirect writes bytes to extent without advancing the
// substrate's LSN frontier (no nextLSN bump, no walHead bump). The
// recovery receiver pairs it with AdvanceFrontier; this test pins the
// "no automatic frontier advance" half of the contract on every
// substrate.
func TestContract_WriteExtentDirect(t *testing.T) {
	runForEachImpl(t, func(t *testing.T, store LogicalStorage) {
		bs := store.BlockSize()
		baseBytes := makeBlock(bs, 0xAA)

		// Pre-condition: fresh substrate, frontier zero.
		_, _, h0 := store.Boundaries()
		if h0 != 0 {
			t.Fatalf("fresh substrate H=%d want 0", h0)
		}

		// WriteExtentDirect must succeed and become readable.
		if err := store.WriteExtentDirect(3, baseBytes); err != nil {
			t.Fatalf("WriteExtentDirect: %v", err)
		}
		got, err := store.Read(3)
		if err != nil {
			t.Fatalf("Read after WriteExtentDirect: %v", err)
		}
		if !bytes.Equal(got, baseBytes) {
			t.Errorf("Read(3) returned %02x... want %02x...", got[:2], baseBytes[:2])
		}

		// Frontier MUST NOT have advanced — BASE has no LSN at this
		// layer; recovery layer is responsible for explicit
		// AdvanceFrontier when the BASE phase completes.
		_, _, h1 := store.Boundaries()
		if h1 != h0 {
			t.Errorf("WriteExtentDirect advanced H from %d to %d (must NOT advance frontier)", h0, h1)
		}
		if next := store.NextLSN(); next != 1 {
			t.Errorf("WriteExtentDirect advanced nextLSN to %d (must stay 1 on fresh substrate)", next)
		}

		// A subsequent ApplyEntry at LSN > 0 must shadow the BASE
		// bytes — this matches the recovery flow where WAL frames
		// at higher LSNs naturally overwrite the BASE snapshot.
		walBytes := makeBlock(bs, 0xBB)
		if err := store.ApplyEntry(3, walBytes, 5); err != nil {
			t.Fatalf("ApplyEntry after WriteExtentDirect: %v", err)
		}
		got2, err := store.Read(3)
		if err != nil {
			t.Fatalf("Read after ApplyEntry: %v", err)
		}
		if !bytes.Equal(got2, walBytes) {
			t.Errorf("Read(3) after ApplyEntry returned %02x... want %02x...", got2[:2], walBytes[:2])
		}

		// Now ApplyEntry HAS advanced the frontier (its normal side
		// effect), so H bumped — this is the receiver's existing
		// side-effect contract for ApplyEntry, unrelated to BASE.
		_, _, h2 := store.Boundaries()
		if h2 < 5 {
			t.Errorf("after ApplyEntry(lsn=5) H=%d want >= 5", h2)
		}
	})
}
