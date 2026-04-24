// Package durable implements the T3a bridge between V3
// `core/storage.LogicalStorage` and `core/frontend.Backend`.
//
// `StorageBackend` wraps any `LogicalStorage` (walstore, smartwal,
// or any future impl) and exposes the `frontend.Backend` surface
// iSCSI and NVMe consume. Responsibilities:
//
//   1. byte ↔ LBA translation (G-int.1).
//      Frontend layer speaks byte offsets + length; storage layer
//      speaks LBA + full-block data. The adapter bridges: partial-
//      block writes trigger read-modify-write; full-block writes
//      skip the read. Reads copy the slice of the backing block
//      covered by the byte range.
//
//   2. Per-I/O fence check (G-int.2).
//      Re-reads `ProjectionView` on every operation; ANY drift
//      (identity field mismatch or Healthy=false) → ErrStalePrimary.
//      Preserves INV-FRONTEND-002.{EPOCH,EV,REPLICA,HEALTHY} under
//      a durable backend.
//
//   3. Operational gate (G-int.3, Addendum A).
//      Before SetOperational(true, _) is called, every I/O returns
//      ErrNotReady with evidence. Starts non-operational — host/
//      provider flips to true after recovery succeeds. This is
//      local readiness only; NEVER advances epoch / mints authority.
//
//   4. Sync (G-int.2 sync policy locked as explicit method).
//      Dispatches to LogicalStorage.Sync and returns error
//      verbatim. iSCSI SYNCHRONIZE CACHE + NVMe Flush wiring lands
//      in T3b.
//
// StorageBackend does NOT own the LogicalStorage lifecycle — the
// Provider (T3b) opens and closes storage. Backend.Close marks the
// handle closed but does not tear down storage.
//
// Boundary guarantee: this package does not import core/authority
// or core/adapter (see core/frontend/boundary_guard_test.go).
package durable

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// WriteObserver is the narrow seam StorageBackend uses to notify the
// replication layer of each successfully-applied local write. The
// ReplicationVolume (core/replication) satisfies this interface via
// its Observe method; the indirection keeps core/frontend/durable
// from importing replication directly and lets tests inject mocks.
//
// Contract (matches V2 BlockVol.shipMu → ShipAll invariant):
//   - StorageBackend calls Observe AFTER LogicalStorage.Write
//     returns successfully, with the LBA + block data + LSN the
//     storage layer just assigned.
//   - Observe error does NOT fail the Write (best-effort
//     replication matches V2 fire-and-forget ShipAll; per-peer
//     degradation is the observer's concern, not the backend's).
//   - StorageBackend calls Observe from within writeBytes; the
//     ReplicationVolume's own mutex then serializes fan-out per
//     volume (V2 shipMu equivalent; closes
//     INV-REPL-LSN-ORDER-FANOUT-001 at the replication seam).
type WriteObserver interface {
	Observe(ctx context.Context, lba uint32, lsn uint64, data []byte) error
}

// errInvalidOffset is returned for negative byte offsets. Callers
// should never produce these; both iSCSI and NVMe clamp LBA before
// it reaches the backend. Kept as a distinct sentinel so the cause
// is visible in logs when a protocol bug does emit one.
var errInvalidOffset = errors.New("durable: invalid (negative) offset")

// errSyncBeforeOpen is returned if Sync is called before
// SetOperational(true, _). Distinct from ErrNotReady so diagnostic
// logs can tell "host sent FLUSH before we were ready" apart from
// "I/O before first-open".
var errSyncBeforeOpen = errors.New("durable: sync before operational")

// StorageBackend adapts a LogicalStorage into frontend.Backend.
type StorageBackend struct {
	storage   storage.LogicalStorage
	view      frontend.ProjectionView
	id        frontend.Identity
	blockSize int

	// operational is the T3a Addendum A readiness gate. Starts
	// false; provider flips true after Recover() succeeds (T3b).
	// opEvidence is the reason reported when operational=false
	// (e.g., "awaiting recovery", "local epoch ahead of assignment").
	operational atomic.Bool
	opEvidence  atomic.Value // string

	mu       sync.Mutex
	closed   bool
	observer WriteObserver // optional; nil = no replication fan-out
}

// NewStorageBackend constructs a backend. Starts NON-operational
// per INV-DURABLE-OPGATE-001; caller must SetOperational(true, _)
// before I/O will succeed.
//
// The storage's block size is captured once at construction
// (LogicalStorage.BlockSize() is a stable geometry field; no T3
// impl changes it mid-session).
func NewStorageBackend(s storage.LogicalStorage, view frontend.ProjectionView, id frontend.Identity) *StorageBackend {
	b := &StorageBackend{
		storage:   s,
		view:      view,
		id:        id,
		blockSize: s.BlockSize(),
	}
	b.opEvidence.Store("awaiting first SetOperational")
	// operational.Store(false) is zero-value default; be explicit.
	b.operational.Store(false)
	return b
}

// Identity returns the lineage captured at Open. Never re-reads
// from the projection — stable for the backend's lifetime.
func (b *StorageBackend) Identity() frontend.Identity { return b.id }

// Close marks the backend closed. All subsequent I/O + Sync
// return ErrBackendClosed. Does NOT close the underlying
// storage — the Provider owns LogicalStorage lifecycle.
func (b *StorageBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// SetWriteObserver installs the per-write notification seam used by
// the replication layer. Safe to call only during setup (before
// I/O begins) — the seam is intentionally not hot-swappable so a
// well-behaved caller cannot race replication wiring against live
// writes. Pass nil to disable replication fan-out on this backend.
//
// Called by: Provider (T3b) / Host (T4a-5) during volume wiring,
// once per backend lifetime.
// Owns: observer pointer under b.mu.
// Borrows: the observer — caller retains ownership; StorageBackend
// does not close or otherwise tear down the observer.
func (b *StorageBackend) SetWriteObserver(obs WriteObserver) {
	b.mu.Lock()
	b.observer = obs
	b.mu.Unlock()
}

// SetOperational flips the readiness gate. evidence is surfaced
// in error strings and diagnostic logs — pass a short reason like
// "recovered LSN=123 epoch=4" or "superblock ahead of assignment".
// Safe to call from any goroutine.
//
// Invariant: does NOT touch identity/lineage/epoch — see package
// godoc + INV-DURABLE-OPGATE-001.
func (b *StorageBackend) SetOperational(ok bool, evidence string) {
	if evidence == "" {
		if ok {
			evidence = "operational"
		} else {
			evidence = "not operational"
		}
	}
	b.opEvidence.Store(evidence)
	b.operational.Store(ok)
}

// Sync flushes everything the storage has to its durability
// boundary. Returns nil iff the stable frontier advanced without
// error. Subject to the same gate stack as I/O: closed → Err-
// BackendClosed, non-operational → ErrNotReady, lineage drift →
// ErrStalePrimary.
func (b *StorageBackend) Sync(ctx context.Context) error {
	if err := b.gate(); err != nil {
		return err
	}
	if _, err := b.storage.Sync(); err != nil {
		return fmt.Errorf("durable: storage sync: %w", err)
	}
	return nil
}

// Read copies up to len(p) bytes starting at offset. Reads past
// end-of-volume return zero bytes (no error). Every read is gated.
func (b *StorageBackend) Read(ctx context.Context, offset int64, p []byte) (int, error) {
	if err := b.gate(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	return b.readBytes(offset, p)
}

// Write persists up to len(p) bytes at offset. Partial-block
// writes perform a read-modify-write: fetch the current block,
// overlay the partial range, write the full block back.
// Every write is gated.
func (b *StorageBackend) Write(ctx context.Context, offset int64, p []byte) (int, error) {
	if err := b.gate(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	return b.writeBytes(ctx, offset, p)
}

// gate is the per-I/O precondition stack. Order matters:
//
//   1. closed: tests the lifecycle, cheapest check
//   2. operational: tests local readiness; preserves Add.A Add.1
//   3. lineage: tests that the captured Identity still matches
//      the live projection; preserves INV-FRONTEND-002
//
// Returning ANY error short-circuits — no partial I/O.
func (b *StorageBackend) gate() error {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return frontend.ErrBackendClosed
	}
	if !b.operational.Load() {
		ev, _ := b.opEvidence.Load().(string)
		return fmt.Errorf("%w: %s", frontend.ErrNotReady, ev)
	}
	return b.lineageCheck()
}

// lineageCheck re-reads the projection and compares all four
// facets plus Healthy. Any drift → ErrStalePrimary.
// INV-FRONTEND-002.{EPOCH,EV,REPLICA,HEALTHY}.
func (b *StorageBackend) lineageCheck() error {
	proj := b.view.Projection()
	if !proj.Healthy ||
		proj.VolumeID != b.id.VolumeID ||
		proj.ReplicaID != b.id.ReplicaID ||
		proj.Epoch != b.id.Epoch ||
		proj.EndpointVersion != b.id.EndpointVersion {
		return frontend.ErrStalePrimary
	}
	return nil
}

// readBytes performs the byte-range → LBA-range fanout for Read.
// Reads block-at-a-time, copying the sub-slice covered by the
// current byte range. Handles three cases:
//
//   - offset aligned, len >= block: copy full block, advance.
//   - offset misaligned: copy tail of first block only.
//   - final block, len < available: copy prefix only.
func (b *StorageBackend) readBytes(offset int64, p []byte) (int, error) {
	if offset < 0 {
		return 0, errInvalidOffset
	}
	bs := int64(b.blockSize)
	total := 0
	buf := p
	pos := offset
	for len(buf) > 0 {
		lba := uint32(pos / bs)
		block, err := b.storage.Read(lba)
		if err != nil {
			return total, fmt.Errorf("durable: read lba=%d: %w", lba, err)
		}
		inBlockOff := int(pos % bs)
		if inBlockOff >= len(block) {
			// Short block (shouldn't happen with fixed-size storage)
			// — treat as zeros, advance.
			advance := int(bs) - inBlockOff
			if advance > len(buf) {
				advance = len(buf)
			}
			for i := 0; i < advance; i++ {
				buf[i] = 0
			}
			total += advance
			buf = buf[advance:]
			pos += int64(advance)
			continue
		}
		copied := copy(buf, block[inBlockOff:])
		total += copied
		buf = buf[copied:]
		pos += int64(copied)
	}
	return total, nil
}

// writeBytes performs the byte-range → LBA-range fanout for Write.
// Full-block writes skip the read (optimization for the hot path);
// partial-block writes read-modify-write to preserve the bytes
// outside the range.
func (b *StorageBackend) writeBytes(ctx context.Context, offset int64, p []byte) (int, error) {
	if offset < 0 {
		return 0, errInvalidOffset
	}
	bs := int64(b.blockSize)
	total := 0
	buf := p
	pos := offset
	for len(buf) > 0 {
		lba := uint32(pos / bs)
		inBlockOff := int(pos % bs)
		available := int(bs) - inBlockOff
		chunkLen := len(buf)
		if chunkLen > available {
			chunkLen = available
		}

		var block []byte
		if inBlockOff == 0 && chunkLen == int(bs) {
			// Full-block write: skip the read.
			block = buf[:chunkLen]
		} else {
			// Partial-block: RMW.
			existing, err := b.storage.Read(lba)
			if err != nil {
				return total, fmt.Errorf("durable: RMW read lba=%d: %w", lba, err)
			}
			block = make([]byte, bs)
			copy(block, existing)
			copy(block[inBlockOff:], buf[:chunkLen])
		}

		lsn, err := b.storage.Write(lba, block)
		if err != nil {
			return total, fmt.Errorf("durable: write lba=%d: %w", lba, err)
		}
		// Replication fan-out (T4a-6 hook). Snapshot the observer under
		// b.mu so a concurrent SetWriteObserver doesn't see us reading
		// a stale pointer. Error is logged best-effort — peer layer
		// degrades; the Write itself has already succeeded locally.
		b.mu.Lock()
		obs := b.observer
		b.mu.Unlock()
		if obs != nil {
			if werr := obs.Observe(ctx, lba, lsn, block); werr != nil {
				log.Printf("durable: replication fan-out failed lba=%d lsn=%d: %v",
					lba, lsn, werr)
			}
		}
		total += chunkLen
		buf = buf[chunkLen:]
		pos += int64(chunkLen)
	}
	return total, nil
}

// Compile-time check that StorageBackend satisfies frontend.Backend.
// Catches interface drift at build time rather than runtime.
var _ frontend.Backend = (*StorageBackend)(nil)
