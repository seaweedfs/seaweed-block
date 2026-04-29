package storage

// LogicalStorage is the V3 persistence-seam contract. It separates the
// "what data exists, what frontier is durable" question (storage) from
// the "what does the engine decide" question (semantic core). The
// engine still owns recovery meaning; storage only persists data and
// reports its own boundaries.
//
// This is execution/persistence machinery. It MUST NOT:
//   - decide recovery class (catch_up vs rebuild)
//   - decide mode transitions (healthy / recovering / degraded)
//   - emit commands or interpret events
//   - read engine projection as control input
//
// Implementations expected behind this seam:
//   - in-memory (BlockStore) — fast, no persistence
//   - WAL+extent (WALStore) — single-file crash-safe backend
//   - smartwal.Store — experimental backend in core/storage/smartwal/
//
// What the contract guarantees (single-node):
//
//   1. Acked data (covered by a Sync that returned nil) survives
//      both clean restart AND process kill. The kill case is verified
//      by a simulated-crash test that drops the underlying file
//      handle without going through Close — no implicit fsync, no
//      housekeeping. The clean restart case is the standard
//      Sync -> Close -> Open -> Recover round trip.
//   2. Unacked data may disappear after restart but cannot corrupt
//      acked data: any write whose LSN is > the recovered frontier
//      is allowed to vanish; any LBA whose write completed before
//      a successful Sync reads back identical bytes after Recover.
//   3. Stable frontier (synced LSN) never goes backward across
//      Sync calls within one process lifetime AND across
//      Close+Recover.
//   4. Recover is idempotent: calling it twice on the same on-disk
//      state yields the same recovered LSN and the same readable
//      contents.
//   5. Boundaries() (R, S, H) reports current truth: what's durable,
//      what WAL still retains, what's the newest write. The engine
//      uses these to decide recovery class — but the storage must
//      not interpret them itself.
//
// What the contract does NOT cover:
//
//   - distributed durability across nodes
//   - power-loss durability beyond what the underlying device
//     guarantees for fsync (write-cache disable / FUA are device-
//     level concerns; we assume fsync semantics hold at the
//     OS+device boundary)
//   - replication-time deduplication or merge
//   - byte-level WAL format compatibility between implementations
type LogicalStorage interface {
	// --- Core IO ---

	// Write stores a block at the given LBA, advances the LSN, and
	// returns the assigned LSN. NOT durable until Sync returns success.
	Write(lba uint32, data []byte) (lsn uint64, err error)

	// Read returns the current bytes at the given LBA. Returns a zero
	// block if the LBA was never written.
	Read(lba uint32) ([]byte, error)

	// --- Durability ---

	// Sync marks all current writes as durable on this node. Returns the
	// stable frontier (the highest LSN that is now durable). On any
	// implementation that can fail to flush (file fsync, etc.) the
	// error path must be honored — the returned LSN is only valid when
	// err == nil.
	Sync() (stableLSN uint64, err error)

	// Recover replays any on-disk state into in-memory form and returns
	// the recovered frontier. Idempotent. After Recover, the storage is
	// ready to accept Read/Write/Sync with the recovered LSN as the
	// inherited frontier.
	//
	// Implementations with no on-disk state (in-memory BlockStore) may
	// return (0, nil) — there is nothing to recover from.
	Recover() (recoveredLSN uint64, err error)

	// --- Boundary queries used by the engine via the adapter ---

	// Boundaries returns the current R/S/H recovery boundaries:
	//   R: synced LSN (what's durable on this node)
	//   S: oldest LSN still retained for catch-up
	//   H: newest written LSN
	// The engine uses these to classify recovery; the storage must not.
	Boundaries() (R, S, H uint64)

	// NextLSN returns the LSN that will be assigned to the next Write.
	NextLSN() uint64

	// --- Geometry ---

	NumBlocks() uint32
	BlockSize() int

	// --- Frontier control used by transport / calibration scaffolding ---

	// AdvanceFrontier advances the frontier metadata without writing
	// any block data. Used after rebuild to mark the replica's frontier
	// at the primary's head once base blocks have been installed.
	AdvanceFrontier(lsn uint64)

	// AdvanceWALTail moves the retained-window tail forward, simulating
	// WAL recycling. After this, any catch-up that needs LSNs below
	// newTail must escalate to rebuild.
	AdvanceWALTail(newTail uint64)

	// ApplyEntry applies one replicated entry from a primary. Same
	// semantic effect as Write(lba, data) but takes the source LSN
	// rather than allocating one — used by the replica-side ingestion
	// path so frontier tracking stays coherent with the source.
	ApplyEntry(lba uint32, data []byte, lsn uint64) error

	// AllBlocks snapshots every written LBA's current bytes. Used by
	// the rebuild server to enumerate what to ship.
	AllBlocks() map[uint32][]byte

	// ResetForRebuild prepares the substrate to receive a full base
	// rebuild stream. After a successful return, every LBA in the
	// extent reads as zero, the WAL ring is empty, the dirty map is
	// cleared, and the durable frontier is set to 0. Subsequent
	// `ApplyEntry` calls from the rebuild stream populate a clean
	// destination — INV-G7-REBUILD-SUBSTRATE-NO-STALE-EXPOSED.
	//
	// Without this reset, a replica rejoining with a preserved
	// walstore retains stale non-zero bytes at LBAs the primary now
	// reads as zero (`AllBlocks()` filters zero blocks from the
	// rebuild stream as a sparse-stream optimization). Resetting the
	// destination makes the filter safe: skipped LBAs are guaranteed
	// zero on the receiver.
	//
	// Implementations MUST persist the reset (zeroed extent + cleared
	// metadata) before returning — partial completion is not allowed
	// because the next ApplyEntry must observe a clean substrate.
	// Idempotent: callable multiple times during a session restart.
	ResetForRebuild() error

	// ScanLBAs is the T4c-2 tier-1 recovery contract. Emits a
	// RecoveryEntry callback for each modification the substrate
	// retains within [fromLSN, head). Called by the catch-up sender
	// (`transport.BlockExecutor.doCatchUp`) to stream the missing
	// retained-WAL window to a replica.
	//
	// Substrate sub-mode (memo §5.1 / §13.0a):
	//   - walstore: `wal_replay` (V2-faithful per-LSN; 3 writes to
	//     LBA=L produce 3 entries)
	//   - smartwal: `state_convergence` (per-LBA dedup; 3 writes to
	//     LBA=L produce 1 entry, LSN is scan-time)
	//   - BlockStore (in-memory): state_convergence-equivalent
	//     synthesis (no real retention; emits current contents)
	//
	// Returns ErrWALRecycled (sentinel from this package) if fromLSN
	// is at or below the substrate's retention boundary. Callers
	// MUST treat ErrWALRecycled as a tier-class change (escalate to
	// rebuild); other errors are stream-level and may be retried.
	//
	// The callback's return value follows V2 walstore semantics: a
	// non-nil error from `fn` STOPS the scan and is returned to the
	// caller; a `nil` return continues the scan past the current
	// entry (used by the sender to skip entries past targetLSN
	// without breaking the loop) — INV-REPL-CATCHUP-CALLBACK-RETURN-
	// NIL-CONTINUES.
	ScanLBAs(fromLSN uint64, fn func(RecoveryEntry) error) error

	// RecoveryMode returns the substrate's tier-1 recovery sub-mode
	// (memo §5.1 / §13.0a). Replaces the duck-typed `CheckpointLSN`
	// probe that T4c-2 used to distinguish walstore from smartwal.
	// Per T4c §I row 6 + T4d-4 part A: substrate-reported value
	// survives the component framework's storage-wrap pattern (the
	// embedded interface forwards `RecoveryMode()` cleanly, where
	// `CheckpointLSN` was a substrate-extension method that wraps
	// did NOT forward).
	//
	// Implementations:
	//   - walstore → RecoveryModeWALReplay (V2-faithful per-LSN)
	//   - smartwal → RecoveryModeStateConvergence (per-LBA dedup)
	//   - BlockStore → RecoveryModeStateConvergence (synthesis)
	RecoveryMode() RecoveryMode

	// AppliedLSNs returns the substrate's per-LBA latest-applied-LSN
	// snapshot. T4d-2 replica recovery apply gate calls this at
	// session start to seed its in-memory `appliedLSN[LBA]` map
	// (Option C hybrid per kickoff §2.5 #1: substrate-query path
	// where available; fallback to session-only tracking otherwise).
	//
	// Substrates that do NOT maintain per-LBA applied-LSN metadata
	// MUST return `(nil, ErrAppliedLSNsNotTracked)` explicitly. The
	// gate handles the sentinel (logs INFO once, falls back). NOT a
	// panic; NOT silent degradation.
	//
	// Returned map ownership: caller MAY mutate the returned map.
	// Substrate implementations MUST snapshot internal state into a
	// fresh map (don't share internal storage).
	AppliedLSNs() (map[uint32]uint64, error)

	// --- Lifecycle ---

	// Close releases any resources (file handles, fsync queues). After
	// Close, only Recover is valid; Read/Write must error.
	Close() error
}
