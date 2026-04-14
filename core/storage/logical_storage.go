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

	// --- Lifecycle ---

	// Close releases any resources (file handles, fsync queues). After
	// Close, only Recover is valid; Read/Write must error.
	Close() error
}
