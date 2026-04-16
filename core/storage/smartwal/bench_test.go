package smartwal

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Benchmarks comparing the three LogicalStorage implementations
// across the same workload shapes:
//
//   - BlockStore: in-memory baseline, no fsync, no on-disk format
//   - WALStore:   regular WAL+extent backend, group-committed fsync
//   - SmartWAL:   experimental extent-direct backend, single fsync per Sync
//
// Run with:
//   go test -bench=. -benchmem -run=^$ ./core/storage/smartwal/
//
// The "smart vs regular" question reduces to:
//   1. Are SmartWAL writes faster, slower, or comparable to WALStore?
//   2. Are SmartWAL reads faster after the WALStore flusher has drained?
//   3. Does group commit help WALStore under concurrent writers?
//
// All benchmarks use 4096-byte blocks. Stores are sized for 262144
// LBAs (1 GiB extent) so writes don't all land on the same LBA, and
// both backends get a 64 MiB WAL region (WALStore default; SmartWAL
// sized via CreateStoreWithSlots = 64 MiB / 32 B per slot).

const (
	benchBlockSize    = 4096
	benchNumBlocks    = 262144                 // 1 GiB extent
	benchSmartWALSlots = uint64(64 * 1024 * 1024 / 32) // 64 MiB ring
)

// makeBenchData returns a single fixed-content block — content
// matters less than size for these benchmarks.
func makeBenchData() []byte {
	b := make([]byte, benchBlockSize)
	for i := range b {
		b[i] = 0x42
	}
	return b
}

// --- Setup helpers ---

type backendSetup struct {
	name  string
	build func(b *testing.B) storage.LogicalStorage
}

func backends(b *testing.B) []backendSetup {
	return []backendSetup{
		{
			name: "BlockStore",
			build: func(b *testing.B) storage.LogicalStorage {
				return storage.NewBlockStore(benchNumBlocks, benchBlockSize)
			},
		},
		{
			name: "WALStore",
			build: func(b *testing.B) storage.LogicalStorage {
				dir := b.TempDir()
				path := filepath.Join(dir, "store.bin")
				s, err := storage.CreateWALStore(path, benchNumBlocks, benchBlockSize)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
		{
			name: "SmartWAL",
			build: func(b *testing.B) storage.LogicalStorage {
				dir := b.TempDir()
				path := filepath.Join(dir, "store.bin")
				s, err := CreateStoreWithSlots(path, benchNumBlocks, benchBlockSize, benchSmartWALSlots)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
	}
}

// --- Write-only (no Sync) ---
//
// Measures the raw write path: encode + WAL append (or extent
// pwrite + ring append). NO fsync, NO durability guarantee.

func BenchmarkWriteNoSync(b *testing.B) {
	for _, bk := range backends(b) {
		b.Run(bk.name, func(b *testing.B) {
			s := bk.build(b)
			data := makeBenchData()
			b.ResetTimer()
			b.SetBytes(int64(benchBlockSize))
			for i := 0; i < b.N; i++ {
				lba := uint32(i % benchNumBlocks)
				if _, err := s.Write(lba, data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// --- Write + Sync per op ---
//
// Worst-case durability: every write is followed by Sync. This is
// where group commit and the per-write IOPS differences show up.

func BenchmarkWriteSyncEach(b *testing.B) {
	for _, bk := range backends(b) {
		b.Run(bk.name, func(b *testing.B) {
			s := bk.build(b)
			data := makeBenchData()
			b.ResetTimer()
			b.SetBytes(int64(benchBlockSize))
			for i := 0; i < b.N; i++ {
				lba := uint32(i % benchNumBlocks)
				if _, err := s.Write(lba, data); err != nil {
					b.Fatal(err)
				}
				if _, err := s.Sync(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// --- Write batched, Sync every 64 ---
//
// Realistic mid-load durability: writers buffer for a small batch
// before Sync. WALStore's group commit benefits multiple concurrent
// callers but here we test the single-writer batch case.

func BenchmarkWriteSyncBatch64(b *testing.B) {
	const batch = 64
	for _, bk := range backends(b) {
		b.Run(bk.name, func(b *testing.B) {
			s := bk.build(b)
			data := makeBenchData()
			b.ResetTimer()
			b.SetBytes(int64(benchBlockSize))
			for i := 0; i < b.N; i++ {
				lba := uint32(i % benchNumBlocks)
				if _, err := s.Write(lba, data); err != nil {
					b.Fatal(err)
				}
				if (i+1)%batch == 0 {
					if _, err := s.Sync(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// --- Concurrent write + Sync ---
//
// 16 goroutines each writing-then-Syncing in a loop, sharing one
// store. Group commit on WALStore should batch fsyncs; SmartWAL's
// single-fsync-covers-both should also benefit; BlockStore has no
// fsync so it just measures lock contention.

func BenchmarkWriteSyncConcurrent16(b *testing.B) {
	const writers = 16
	for _, bk := range backends(b) {
		b.Run(bk.name, func(b *testing.B) {
			s := bk.build(b)
			data := makeBenchData()
			b.ResetTimer()
			b.SetBytes(int64(benchBlockSize))
			b.SetParallelism(writers)
			b.RunParallel(func(pb *testing.PB) {
				lba := uint32(0)
				for pb.Next() {
					if _, err := s.Write(lba%benchNumBlocks, data); err != nil {
						b.Fatal(err)
					}
					if _, err := s.Sync(); err != nil {
						b.Fatal(err)
					}
					lba++
				}
			})
		})
	}
}

// --- Sequential read (post-write, pre-flush) ---
//
// Pre-populates the store with one block per LBA, then reads them
// all sequentially. For WALStore: reads check the dirty map first
// → WAL data path. For SmartWAL: extent direct, no dirty-map
// lookup. The difference here is the SmartWAL claim about reads.

func BenchmarkReadSequentialPreFlush(b *testing.B) {
	for _, bk := range backends(b) {
		b.Run(bk.name, func(b *testing.B) {
			s := bk.build(b)
			data := makeBenchData()
			// Populate every LBA we'll read from.
			const populated = 1024
			for lba := uint32(0); lba < populated; lba++ {
				if _, err := s.Write(lba, data); err != nil {
					b.Fatal(err)
				}
			}
			// NOTE: deliberately NO Sync and NO sleep — we want the
			// dirty map to still be hot for WALStore.
			b.ResetTimer()
			b.SetBytes(int64(benchBlockSize))
			for i := 0; i < b.N; i++ {
				lba := uint32(i % populated)
				if _, err := s.Read(lba); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// --- Sequential read (post-flush) ---
//
// Same as above, but waits for the WALStore flusher to drain the
// dirty map (so reads come from the extent). For SmartWAL there's
// no flush concept; reads come from extent either way.
// BlockStore: no flusher; reads come from in-memory map either way.

func BenchmarkReadSequentialPostFlush(b *testing.B) {
	for _, bk := range backends(b) {
		b.Run(bk.name, func(b *testing.B) {
			s := bk.build(b)
			data := makeBenchData()
			const populated = 1024
			for lba := uint32(0); lba < populated; lba++ {
				if _, err := s.Write(lba, data); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := s.Sync(); err != nil {
				b.Fatal(err)
			}

			// For WALStore, wait for the background flusher to drain
			// the dirty map. Up to ~500ms is plenty for 1024 entries
			// on the default 100ms flusher tick. Other backends:
			// no-op (sleep doesn't hurt).
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				type cpr interface{ CheckpointLSN() uint64 }
				if c, ok := s.(cpr); ok {
					if c.CheckpointLSN() >= uint64(populated) {
						break
					}
					time.Sleep(50 * time.Millisecond)
				} else {
					break
				}
			}

			b.ResetTimer()
			b.SetBytes(int64(benchBlockSize))
			for i := 0; i < b.N; i++ {
				lba := uint32(i % populated)
				if _, err := s.Read(lba); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
