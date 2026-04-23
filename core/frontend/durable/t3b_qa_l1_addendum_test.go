// Ownership: QA (L1 addendum per user direction 2026-04-22, scope
// design referenced against V2 test catalog coverage for durable
// data path).
// sw may NOT modify without architect approval via §8B.4.
//
// Purpose: extend T3b L1 coverage beyond the core matrix to close
// three gaps identified against V2 reference:
//
//   1. ConcurrentIO + Sync barrier — V2 group_commit_test parity
//   2. RecoverThenServe — open existing storage + immediate I/O
//   3. LargeSpanningWrite — multi-LBA single-write integrity
//
// All three tests matrix-parameterize via logicalStorageFactories()
// so walstore and smartwal are both exercised.

package durable_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

// --- L1-1: ConcurrentIO_SyncBarrier ---
//
// V2 parity: group_commit_test.go covered concurrent waiters
// batched into one fsync. V3 adapter + GroupCommitter chain must
// preserve the guarantee: every ack'd Write visible after Sync,
// regardless of scheduling order, regardless of impl.
func TestT3_Durable_ConcurrentIO_SyncBarrier(t *testing.T) {
	const workers = 8
	const blockSize = 4096
	const numBlocks = 64

	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, numBlocks, blockSize)
			ctx := context.Background()

			// Each worker writes a distinctive pattern at its own
			// block offset. All workers race, then main Sync's
			// once. After Sync, every ack'd write must be visible.
			payloads := make([][]byte, workers)
			for i := range payloads {
				p := make([]byte, blockSize)
				for j := range p {
					p[j] = byte((i*31 + j) & 0xFF)
				}
				payloads[i] = p
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			errCh := make(chan error, workers)
			for i := 0; i < workers; i++ {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					off := int64(i) * int64(blockSize)
					if _, err := b.Write(ctx, off, payloads[i]); err != nil {
						errCh <- err
					}
				}()
			}
			close(start)
			wg.Wait()
			close(errCh)
			for err := range errCh {
				if err != nil {
					t.Fatalf("concurrent Write error: %v", err)
				}
			}

			// Single barrier Sync — all prior acks must be durable.
			if err := b.Sync(ctx); err != nil {
				t.Fatalf("Sync barrier: %v", err)
			}

			// Verify every worker's payload is readable byte-exact.
			for i := 0; i < workers; i++ {
				got := make([]byte, blockSize)
				off := int64(i) * int64(blockSize)
				if _, err := b.Read(ctx, off, got); err != nil {
					t.Fatalf("Read worker %d: %v", i, err)
				}
				for j := 0; j < blockSize; j++ {
					if got[j] != payloads[i][j] {
						t.Fatalf("worker %d byte %d: got 0x%02x want 0x%02x",
							i, j, got[j], payloads[i][j])
					}
				}
			}
		})
	}
}

// --- L1-2: RecoverThenServe ---
//
// V2 parity: recovery tests with pre-populated state. Adapter must
// accept a LogicalStorage that was opened against an existing file
// (not fresh-created), call Recover() implicitly via the storage
// open path, and serve I/O immediately after SetOperational(true).
//
// The test: Phase 1 creates, writes pattern, Syncs, closes. Phase 2
// re-opens (Open*, not Create*), builds fresh adapter, verifies
// Read returns Phase 1 data byte-exact.
func TestT3_Durable_RecoverThenServe(t *testing.T) {
	const blockSize = 4096
	const numBlocks = 16

	type reopenFactory struct {
		name   string
		create func(path string) (storage.LogicalStorage, error)
		reopen func(path string) (storage.LogicalStorage, error)
	}
	factories := []reopenFactory{
		{
			name:   "walstore",
			create: func(p string) (storage.LogicalStorage, error) { return storage.CreateWALStore(p, numBlocks, blockSize) },
			reopen: func(p string) (storage.LogicalStorage, error) { return storage.OpenWALStore(p) },
		},
		{
			name:   "smartwal",
			create: func(p string) (storage.LogicalStorage, error) { return smartwal.CreateStore(p, numBlocks, blockSize) },
			reopen: func(p string) (storage.LogicalStorage, error) { return smartwal.OpenStore(p) },
		},
	}

	for _, f := range factories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recover.bin")

			// --- Phase 1: create + write + sync + close ---
			s1, err := f.create(path)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			view := newStubView(healthyProj(id))
			b1 := durable.NewStorageBackend(s1, view, id)
			b1.SetOperational(true, "phase1")

			ctx := context.Background()
			pattern := make([]byte, blockSize)
			for i := range pattern {
				pattern[i] = byte((i*19 + 7) & 0xFF)
			}
			if _, err := b1.Write(ctx, 0, pattern); err != nil {
				t.Fatalf("phase1 Write: %v", err)
			}
			if err := b1.Sync(ctx); err != nil {
				t.Fatalf("phase1 Sync: %v", err)
			}
			if err := b1.Close(); err != nil {
				t.Fatalf("phase1 adapter Close: %v", err)
			}
			if err := s1.Close(); err != nil {
				t.Fatalf("phase1 storage Close: %v", err)
			}

			// --- Phase 2: reopen + recover + immediate serve ---
			s2, err := f.reopen(path)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = s2.Close() })
			if _, err := s2.Recover(); err != nil {
				t.Fatalf("Recover: %v", err)
			}

			b2 := durable.NewStorageBackend(s2, view, id)
			b2.SetOperational(true, "phase2-after-recover")

			got := make([]byte, blockSize)
			if _, err := b2.Read(ctx, 0, got); err != nil {
				t.Fatalf("phase2 Read: %v", err)
			}
			for i := 0; i < blockSize; i++ {
				if got[i] != pattern[i] {
					t.Fatalf("phase2 byte %d: got 0x%02x want 0x%02x (recover lost data)",
						i, got[i], pattern[i])
				}
			}
		})
	}
}

// --- L1-3: LargeSpanningWrite ---
//
// V2 parity: large_write_mem_test. A single adapter.Write whose
// byte range spans many LBAs (adapter must chunk into per-LBA
// storage writes + aggregate Sync). Verifies chunking correctness
// + Sync + full readback.
//
// Uses 256 KiB payload (64 blocks at 4096 B/block) — covers the
// crossover beyond single-block and multi-block shapes tested in
// ByteLBATranslation_Matrix.
func TestT3_Durable_LargeSpanningWrite(t *testing.T) {
	const blockSize = 4096
	const numBlocks = 256
	const payloadSize = 64 * blockSize // 256 KiB, spans 64 LBAs

	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, numBlocks, blockSize)
			ctx := context.Background()

			payload := make([]byte, payloadSize)
			for i := range payload {
				payload[i] = byte((i*13 + 5) & 0xFF)
			}

			if _, err := b.Write(ctx, 0, payload); err != nil {
				t.Fatalf("large Write: %v", err)
			}
			if err := b.Sync(ctx); err != nil {
				t.Fatalf("Sync: %v", err)
			}

			got := make([]byte, payloadSize)
			if _, err := b.Read(ctx, 0, got); err != nil {
				t.Fatalf("Read: %v", err)
			}
			for i := 0; i < payloadSize; i++ {
				if got[i] != payload[i] {
					t.Fatalf("byte %d: got 0x%02x want 0x%02x (large write corruption at LBA %d)",
						i, got[i], payload[i], i/blockSize)
				}
			}
		})
	}
}
