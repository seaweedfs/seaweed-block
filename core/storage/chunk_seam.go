package storage

// Chunk layer seam: an asynchronous remote copy of a walstore volume.
//
// The chunk layer sits under the flusher. The local WAL and extent stay
// authoritative: Sync still means local durability (L1). The remote copy is
// an immutable-chunk image published by a compare-and-swap of a single HEAD
// record (L2). The contract the layer must meet is C1-C11 in
// chunk_layer_contract_test.go and chunk_layer_c11_test.go.
//
// Capture is a quiesced cut, not a flusher checkpoint boundary. A checkpoint
// is not a prefix image: superseded dirty entries are skipped, so the extent
// can already hold blocks newer than the checkpoint, and old block versions
// are not kept. Capture therefore:
//  1. blocks new writes at the store and waits for in-flight ones;
//  2. Syncs; H is then the synced LSN (nextLSN-1) and no data newer than H
//     exists in the WAL or the extent. The flusher is not drained;
//  3. fixes the candidate set and installs an owned-map v3 COW window under
//     that barrier, then releases it (pause = Sync + window installation);
//  4. copies outside the barrier. Before publication, the first writer to a
//     pending candidate saves its at-H chunk; writers wait for a copying or
//     saving owner. Both paths use the validated store reader. Legacy test
//     fixtures retain the full-copy barrier;
//  5. uploads the buffers, outside every walstore lock;
//  6. CASes HEAD {gen, epoch} with RemoteLSN = H.
//
// This file declares the seam; chunk_layer.go implements it.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// chunkSink receives walstore events.
//
//   - OnExtentWrite: from the single extent write path (flusher) and from
//     WriteExtentDirect, with the block's LSN (0 for a BASE block). It runs
//     under walstore extent or publication locks and must be O(1), never
//     block, do no I/O, make no remote call, not re-enter the WALStore and
//     take no lock beyond the sink's own bit-set: it only marks chunks in
//     memory. A BASE block sets a constant-time all-dirty flag; expansion to
//     individual chunks happens later, at a persistence or capture boundary.
//   - BeforeCheckpointAdvance: from inside persistCheckpoint itself, so every
//     path that advances the checkpoint or the WAL tail (flusher, BASE Sync's
//     direct frontier, recovery, retention) reaches it. It makes the bitmap
//     state durable first (chunkBitmapStore.Save); an error refuses the
//     advance and the WAL keeps the entries. With a sink configured, a path
//     that cannot reach it refuses instead of advancing.
//   - BeginBaseInstall: before the first WriteExtentDirect of a rebuild. It
//     durably writes a header with Base = chunkBaseInstalling
//     (chunkBitmapStore.WriteHeader), which can never match a remote HEAD,
//     because BASE writes bypass the WAL and are not recoverable from it.
//     Cuts are refused from then on.
//   - EndBaseInstall(frontier): after the rebuild's direct frontier is
//     durable (the store's checkpoint >= frontier; otherwise it refuses). It
//     durably writes Base = chunkBaseInstalledAwaitingCut and re-allows cuts.
//     The first cut after it does a full per-chunk hash compare; the header
//     becomes a normal one (chunkBaseNone) only when that cut's CAS succeeds.
//
// Recovery distinguishes the two BASE states and never infers completion
// from the data: a header still in chunkBaseInstalling means the install
// was interrupted, so cuts stay refused until the owner runs a complete
// install again (Begin ... End). chunkBaseInstalledAwaitingCut means the
// install and its frontier were durable; a cut is allowed and hashes fully.
//   - OnReadMiss: from readFromExtent, after releasing the extent lock. It
//     returns nil data when the block is present locally (the store then
//     reads its extent). Otherwise it fetches the block's chunk as published
//     in HEAD, bounded by chunkLayerConfig.ReadMissTimeout, and returns the
//     block. It installs fetched bytes into the extent only for blocks that
//     have no newer local version (dirty map, WAL or extent write since the
//     fetch began); a local version always wins.
//
// The dirty bitmap stays off the write hot path. WAL entries carry their
// LBA, so the dirty set is always the persisted bitmap âˆª the chunks of WAL
// entries above the checkpoint. The persisted bitmap may over-report, never
// under-report.
type chunkSink interface {
	OnExtentWrite(lba uint64, lsn uint64)
	BeforeCheckpointAdvance(lsn uint64) error
	BeginBaseInstall() error
	EndBaseInstall(frontier uint64) error
	OnReadMiss(lba uint64) ([]byte, error)
}

// chunkBaseState records a BASE rebuild in the bitmap header.
type chunkBaseState int

const (
	chunkBaseNone                 chunkBaseState = iota // no rebuild pending
	chunkBaseInstalling                                 // Begin done, End not: an incomplete install
	chunkBaseInstalledAwaitingCut                       // End done, frontier durable; first cut not yet CAS'd
)

// chunkBitmapHeader ties a persisted bitmap to the HEAD period it covers:
// BaseLSN is the cut H of that HEAD, HeadGen and Epoch its CAS guard. A
// header whose Base is not chunkBaseNone never matches a HEAD.
type chunkBitmapHeader struct {
	BaseLSN uint64
	HeadGen uint64
	Epoch   uint64
	Base    chunkBaseState
}

// chunkDelta archives one closed HEAD period, for snapshot diff,
// replication and audit: the cuts it spans, the chunks dirtied in it, and
// the immutable keys the new HEAD published for them.
type chunkDelta struct {
	FromLSN uint64
	ToLSN   uint64
	Gen     uint64
	Dirty   []uint64
	NewKeys map[uint64]string
}

// chunkBitmapStore persists the dirty-since-HEAD chunk bitmap (one bit per
// chunk) under a chunkBitmapHeader. It lives on the local host next to the
// volume. Every method is durable on return.
//   - Load returns the header and dirty chunks; ok is false when nothing
//     usable is stored (lost or corrupt).
//   - Save stores the current period's dirty set; the header is unchanged.
//   - WriteHeader replaces the header and keeps the dirty set (used for the
//     BASE state marks).
//   - Reset runs after a successful HEAD CAS at cut H: it archives the
//     closed period as delta and starts a new bitmap with header
//     {BaseLSN: H, HeadGen, Epoch} in which only stillDirty are set.
//
// The layer serializes Save, WriteHeader and Reset. stillDirty holds every
// chunk dirtied after the cut, including chunks already saved (and their
// WAL already recycled) while the cut's upload was in flight; a Reset never
// clears post-cut dirt.
//
// Restart or failover: a header equal to HEAD {RemoteLSN, Gen, Epoch} with
// Base == chunkBaseNone means incremental, dirty = persisted bitmap âˆª
// chunks of WAL entries above the checkpoint. A lost bitmap or any header
// mismatch falls back to a per-chunk SHA-256 compare against HEAD's chunk
// refs; only differing chunks are uploaded (Base == chunkBaseInstalling
// refuses cuts instead; see BeginBaseInstall). Manifests stay logically
// full per HEAD, so restore never chains deltas.
type chunkBitmapStore interface {
	Load() (hdr chunkBitmapHeader, dirty []uint64, ok bool, err error)
	Save(dirty []uint64) error
	WriteHeader(hdr chunkBitmapHeader) error
	Reset(hdr chunkBitmapHeader, stillDirty []uint64, delta chunkDelta) error
}

// chunkRef names one published chunk: its immutable key and the SHA-256 of
// its ChunkBlocks*BlockSize bytes.
type chunkRef struct {
	Key    string
	SHA256 [32]byte
}

// chunkHeadVersion is the CAS guard of HEAD.
type chunkHeadVersion struct {
	Gen   uint64
	Epoch uint64
}

// chunkHead is the published remote state. Chunks maps a chunk index to the
// chunk holding its bytes (block i of the chunk at offset i*BlockSize). A
// chunk index missing from the map is ZERO: every block in it reads as zeros.
//
// State is chunkHeadActive or chunkHeadDetached ("" reads as active: older
// HEADs carry no state). A holder's Detach publishes its final cut with
// State = detached and DetachedBy = {its holder, its epoch, the generation
// that CAS installs}, in that one CAS: the next holder and the block master
// can tell from HEAD alone that the holder stopped after that cut (a clean
// move). Every normal cut publishes State = active and no DetachedBy; a new
// holder's takeover does the same.
type chunkHead struct {
	Gen                uint64
	Epoch              uint64
	Holder             string
	RemoteLSN          uint64
	Chunks             map[uint64]chunkRef
	Segments           map[uint64]ChunkSegment
	ChunkBytes         uint64
	ManifestChunkCount int
	State              string
	DetachedBy         *chunkDetachedBy
}

// HEAD states.
const (
	chunkHeadActive   = "active"
	chunkHeadDetached = "detached"
)

// chunkDetachedBy names the holder whose final cut a detached HEAD is.
type chunkDetachedBy struct {
	Holder string
	Epoch  uint64
	Gen    uint64
}

// chunkHeadIsDetached reports whether h is a detached HEAD.
// chunkHeadAborted marks a fork target's HEAD tombstone: a GC sweep claimed
// the HEAD of a dead fork so its late create can never land (W5). Such a
// volume never attaches.
const chunkHeadAborted = "aborted"

// chunkHeadDeleting marks a volume a delete has admitted (chunks kept, so
// the delete can still be given back); chunkHeadDeleted is the final
// tombstone (no chunks, so nothing it held stays a GC root). Neither is
// ever moved by CASHead, so no attach, claim or restore can revive them.
// chunkHeadPurging is the delete's irreversible boundary (chunks still
// kept until the final tombstone).
const (
	chunkHeadDeleting = "deleting"
	chunkHeadPurging  = "purging"
	chunkHeadDeleted  = "deleted"
)

func chunkHeadIsDetached(h chunkHead) bool { return h.State == chunkHeadDetached }

// chunkRemote is the remote store. Keys are non-empty valid UTF-8 strings
// (validChunkKey); Put and PutIntent refuse any other key before writing
// anything. Keys are immutable: Put of a key that
// exists, or once existed, fails with errChunkKeyExists and never replaces
// stored bytes. CASHead installs next only when the stored HEAD matches
// expected and returns the new generation; a mismatch fails with
// errChunkHeadPrecondition. Any other CASHead error leaves the outcome
// unknown: the caller re-reads HEAD after every such result before any
// further CAS, never retries blindly, and reports no success or frontier
// advance while it is unresolved.
//
// Intents protect chunks a holder is about to publish (W5): GC treats every
// live intent as a root. A holder durably writes an intent naming the keys
// before it uploads the first of them (or, for a snapshot or fork, before it
// writes the manifest that references them), and deletes it after the HEAD
// CAS succeeds or the attempt is cleanly abandoned.
//   - PutIntent stores in durably and returns its id, "<in.Gen>-<nonce>",
//     unique per call; the id and CreatedNs it assigns are listed back.
//   - DeleteIntent removes that one intent; a missing id is not an error.
//   - ListIntents returns every live intent of the volume, or an error; it
//     never skips an intent it cannot read.
type chunkRemote interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	GetHead(ctx context.Context) (chunkHead, error)
	CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error)
	Delete(ctx context.Context, key string) error
	PutIntent(ctx context.Context, in chunkIntent) (string, error)
	DeleteIntent(ctx context.Context, id string) error
	ListIntents(ctx context.Context) ([]chunkIntent, error)
}

// chunkIntent is one live intent: the keys a holder at HEAD generation Gen
// and epoch Epoch is about to publish.
type chunkIntent struct {
	Segments   map[uint64]ChunkSegment
	ChunkBytes uint64
	ID         string // "<Gen>-<nonce>"; assigned by PutIntent
	Gen        uint64
	Epoch      uint64
	Holder     string
	Keys       []string
	CreatedNs  int64 // assigned by PutIntent
	// DeadlineNs (unix ns) is a copy intent's publication deadline, kept as
	// metadata: a GC sweep settles a dead copy attempt by claiming its
	// publishing entry with a tombstone, never by the deadline alone (W5).
	// 0 for capture intents.
	DeadlineNs int64
}

// newChunkIntentID returns "<gen>-<16 lowercase hex digits>" from a random
// nonce.
func newChunkIntentID(gen uint64) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("storage: intent nonce: %w", err)
	}
	return strconv.FormatUint(gen, 10) + "-" + hex.EncodeToString(b[:]), nil
}

// validChunkKey is the chunk key domain: a non-empty, valid UTF-8 string.
// Every remote refuses any other key at Put and at PutIntent, before it
// persists anything, so a key protected by an intent always lists back
// byte-identical to the key stored (an encoding that replaced invalid
// bytes would otherwise protect a different key than the one stored).
func validChunkKey(key string) error {
	if key == "" || !utf8.ValidString(key) {
		return fmt.Errorf("%w: %q", errChunkKeyInvalid, key)
	}
	return nil
}

// validChunkIntentID accepts exactly the ids newChunkIntentID produces.
func validChunkIntentID(id string) bool {
	gen, nonce, ok := strings.Cut(id, "-")
	if !ok || len(nonce) != 16 || strings.ToLower(nonce) != nonce {
		return false
	}
	v, err := strconv.ParseUint(gen, 10, 64)
	if err != nil || strconv.FormatUint(v, 10) != gen {
		return false
	}
	_, err = hex.DecodeString(nonce)
	return err == nil
}

// chunkLayerConfig configures one attachment.
type chunkLayerConfig struct {
	// ChunkBlocks is the number of consecutive blocks per chunk (1024 for
	// 4 KiB blocks and 4 MiB chunks).
	ChunkBlocks uint32
	// Epoch is the epoch the block master assigned to this node; every HEAD
	// CAS carries it. It is the manifest's own epoch, kept separate from the
	// frontend Identity().Epoch, which is checked against the master
	// projection on every I/O.
	Epoch  uint64
	Holder string
	// FreshFromHead says the local volume is new and empty and starts from
	// HEAD (every block absent, read lazily). False means an existing local
	// volume. Never inferred from the local contents.
	FreshFromHead bool
	// RestoreGuard binds fresh attachment to the HEAD the provider claimed
	// and durably seeded. A changed read refuses before hooks/readiness; the
	// next recovery must repeat claim and seed. Nil is for direct layer users.
	RestoreGuard *chunkHeadVersion
	// PrepareRestore runs after ClaimChunkHead validates its observed HEAD
	// and before any ownership CAS (also for an idempotent claim). The owner
	// durably seeds the local frontier and rebases cached ship cursors here,
	// while its frontend is gated. An error leaves remote ownership unchanged.
	// No store/layer/remote lock is held across the callback.
	PrepareRestore func(head chunkHead) error
	// CaptureAllowed is supplied by the store owner. It is an admission
	// check, made when a capture starts and again right before its HEAD CAS:
	// it refuses a cut during a BASE install or rebuild, or when this node is
	// not a healthy primary. It narrows but cannot close the window between
	// that last check and the CAS. Publication safety across a demotion
	// rests on the epoch: a new primary's takeover raises the HEAD epoch, so
	// the old primary's CAS fails and it fences (C6). Nil allows capture.
	// COW workers may call this concurrently, between chunk claims.
	CaptureAllowed func() error
	// ReadMissTimeout bounds one OnReadMiss remote fetch.
	ReadMissTimeout time.Duration
	// Bitmap persists the legacy dirty-since-HEAD bitmap. Required for v2;
	// impl3 uses its owned LSN map and never calls this optional legacy object.
	Bitmap chunkBitmapStore

	// BarrierBound bounds how many chunks a cut copies under its barrier:
	// beyond it, chunks are pre-copied outside the barrier in staging
	// rounds (at most chunkStageRounds; then the barrier takes the rest).
	// Zero: unbounded, no staging.
	BarrierBound int

	// UploadConcurrency bounds capture/pre-copy Put calls (0 defaults to 8).
	UploadConcurrency int

	// COWSpillLimit caps the bytes a cut's copy window may spill to its side
	// file once the in-memory budget is used: a save beyond it aborts the cut
	// (the caller retries) instead of growing the file under the WAL/extent.
	// 0 defaults to defaultCOWSpillLimit; negative is refused.
	COWSpillLimit int64

	// testReconcileHold, set only by this package's tests, holds the
	// reconciler before each slice until a token is received or the
	// channel is closed.
	testReconcileHold chan struct{}
	// testReconcileHook, set only by this package's tests, is called by a
	// reconciliation worker with its generation before each slice
	// ("batch"), after comparing a chunk and before recording the result
	// ("compared"), and before certifying the result ("finish").
	testReconcileHook func(stage string, gen uint64)
}

// chunkCutStats describes one cut. The periodic-cut work fills it; a zero
// value means "not measured".
type chunkCutStats struct {
	COWSavedChunks int
	COWSaveNs      int64
	COWWaitNs      int64
	// BarrierPauseNs is the guest pause: from the capture barrier's Lock
	// request until it is released. LockWaitNs is its first part, the wait for
	// writers already holding the capture barrier shared.
	BarrierPauseNs  int64
	LockWaitNs      int64
	SyncNs          int64
	CopyNs          int64
	CandidateChunks int
	UploadedChunks  int
	UploadedBytes   int64
	ZeroedChunks    int
	UnchangedChunks int
	UploadNs        int64
	CASNs           int64
	CASResult       string
	TotalNs         int64
	// Pre-copy (BarrierBound > 0): StageRounds staging rounds staged
	// StagedChunks chunks outside the barrier; StagedChunksUsed of the
	// uploads in HEAD came from staging. BarrierCopyChunks were copied
	// under the barrier; BarrierOverBound: more than the bound (the
	// staging rounds did not converge).
	StageRounds       int
	StagedChunks      int
	StagedChunksUsed  int
	BarrierCopyChunks int
	BarrierOverBound  bool
}

// chunkLayerStats are a layer's cumulative cut counters and its last
// successful cut (B9 7.2-7.3). Cuts counts every Cut, Capture,
// MakeRemoteDurable and Detach attempt that got past the closed check.
//   - CutsOK counts cuts that installed a HEAD; CutsNoop those with
//     nothing new (no CAS); CutsFailed every other failure.
//   - CutsRefused counts refusals by reason: "admission" (CaptureAllowed:
//     not a healthy primary of this lineage, BASE install), "fenced",
//     "detached".
//   - CASConflicts counts CAS precondition failures; CASUnknown CAS
//     results whose outcome stayed unresolved after the HEAD re-read.
//   - PendingUnresolved: an unresolved proposal is held now.
//   - DirtyChunks is the number of chunks dirty since the last HEAD now
//     (the C11 bitmap popcount); the cut scheduler's threshold reads it.
//     Chunks still awaiting reconciliation are not counted.
//   - Reconciling: the dirty set is being reconciled against HEAD; no cut
//     publishes until it is done. ReconcileDone of ReconcileTotal chunks
//     are compared.
//   - LastCut, LastCutH and LastCutNs (wall clock, Unix ns) describe the
//     last successful cut, whatever triggered it.
type chunkLayerStats struct {
	CutsOK            uint64
	CutsNoop          uint64
	CutsFailed        uint64
	CutsRefused       map[string]uint64
	CASConflicts      uint64
	CASUnknown        uint64
	PendingUnresolved bool
	DirtyChunks       int
	// DirtyChunkBytes estimates full chunk coverage of known dirt, capped
	// at volume size, not changed payload bytes. Reconciling means incomplete.
	DirtyChunkBytes uint64
	UploadPending   int64 // current chunk batch: queued plus in-flight, not manifests
	AbsentChunks    int   // chunks still requiring lazy fill; no eager fill queue
	Reconciling     bool
	ReconcileDone   int
	ReconcileTotal  int
	LastCut         chunkCutStats
	LastCutH        uint64
	LastCutNs       int64
	// BarrierOverBound counts cuts whose barrier copied more than the
	// bound after the staging rounds.
	BarrierOverBound uint64
}

// chunkCutResult is the outcome of one successful cut.
//   - H is the cut LSN; Head.RemoteLSN = H when the cut installed a HEAD.
//     A volume's LSNs carry forward across hosts: a new host's WAL starts
//     at HEAD.RemoteLSN + 1, and a fork starts after its snapshot's
//     RemoteLSN, so RemoteLSN never decreases across generations.
//   - Head is the HEAD this cut installed (copied, owned by the caller), or
//     the current HEAD when the cut had nothing new (Noop: no CAS).
//   - FullHash says the cut compared every chunk by hash (no usable bitmap,
//     or the first cut after a BASE install).
type chunkCutResult struct {
	H        uint64
	Head     chunkHead
	Noop     bool
	FullHash bool
	Stats    chunkCutStats
}

// chunkLayer is what the store owner drives.
type chunkLayer interface {
	chunkSink
	// Cut takes a quiesced cut at H (the last written LSN), uploads the
	// chunks dirtied since the last HEAD and CASes HEAD with RemoteLSN = H.
	// It returns the HEAD it installed, so a snapshot can copy exactly the
	// image of H even when other cuts follow.
	Cut(ctx context.Context) (chunkCutResult, error)
	// Stats returns the cumulative cut counters and the last cut.
	Stats() chunkLayerStats
	// Capture is Cut, returning only H.
	Capture(ctx context.Context) (uint64, error)
	// Detach is the final cut of a clean stop: one cut whose CAS publishes
	// State = detached and DetachedBy (always a CAS, even with nothing new).
	// The caller stops new writes first. Afterwards Cut, Capture and
	// MakeRemoteDurable fail with errChunkLayerDetached, and a repeated
	// Detach returns the first result. An unresolved detach CAS is settled
	// like any other unresolved CAS, before the next capture or Detach.
	Detach(ctx context.Context) (chunkCutResult, error)
	// MakeRemoteDurable is the durability point for snapshot, detach and
	// migrate: a Capture whose H >= t. Only a nil return makes t remote
	// durable; it requires the local Sync to cover t first, and fails
	// (without waiting) when t was never written.
	MakeRemoteDurable(ctx context.Context, t uint64) error
	// EvictChunk makes a chunk absent locally. Refused while the chunk is
	// dirty since HEAD. Absence is a correctness state, not a hint: after a
	// restart of an existing local volume, an evicted chunk still reads back
	// as HEAD publishes it (refetched), never as stale or zero local bytes.
	EvictChunk(idx uint64) error
	RemoteLSN() uint64
	Fenced() bool
	// Close stops and joins every worker the layer owns and cancels its
	// remote calls. WALStore.Close calls it before closing the store.
	Close() error
}

var (
	errChunkLayerNotImplemented = errors.New("storage: chunk layer not implemented")
	errChunkKeyExists           = errors.New("storage: chunk key already exists")
	errChunkHeadPrecondition    = errors.New("storage: chunk HEAD precondition failed")
	errChunkHeadDeleted         = errors.New("storage: chunk HEAD is being deleted or deleted")
	errChunkLayerFenced         = errors.New("storage: chunk layer fenced")
	errChunkEvictRefused        = errors.New("storage: chunk eviction refused")
	errChunkCaptureRefused      = errors.New("storage: chunk capture refused")
	errChunkKeyInvalid          = errors.New("storage: chunk key is not non-empty valid UTF-8")
	errChunkLayerDetached       = errors.New("storage: chunk layer detached")
	errChunkCASOutcomeUnknown   = errors.New("storage: chunk HEAD CAS outcome unknown")
	// The causes a control plane reports as typed codes (B9 W2 Â§2.2).
	errChunkLayerClosed       = errors.New("storage: chunk layer closed")
	errChunkBaseInstalling    = errors.New("storage: chunk BASE install in progress")
	errChunkRemoteUnavailable = errors.New("storage: chunk remote unavailable")
	// errChunkLSNBehind: this host's log has not reached the HEAD's
	// remote_lsn, so a cut would move remote_lsn backwards (B9 W2 appendix;
	// LSN continuity across hosts is W4b's).
	errChunkLSNBehind = errors.New("storage: cut LSN is behind the HEAD's remote_lsn")
	// errChunkReconciling: the layer is still reconciling its dirty set
	// against HEAD (bitmap lost or mismatched, or a BASE install just
	// finished); HEAD does not move until it is done. Retryable.
	errChunkReconciling = errors.New("storage: chunk layer reconciling")
)

// attachChunkLayer attaches a chunk layer to an open store (chunk_layer.go).
func attachChunkLayer(s *WALStore, remote chunkRemote, cfg chunkLayerConfig) (chunkLayer, error) {
	return attachChunkLayerImpl(s, remote, cfg)
}
