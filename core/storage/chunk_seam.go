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
	"errors"
	"fmt"
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
// its stored object bytes, verified before any encoding is decoded.
type chunkRef struct {
	Key      string
	SHA256   [32]byte
	Encoding string
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
// (validChunkKey); Put refuses any other key before writing anything. Keys are immutable: Put of a key that
// exists, or once existed, fails with errChunkKeyExists and never replaces
// stored bytes. CASHead installs next only when the stored HEAD matches
// expected and returns the new generation; a mismatch fails with
// errChunkHeadPrecondition. Any other CASHead error leaves the outcome
// unknown: the caller re-reads HEAD after every such result before any
// further CAS, never retries blindly, and reports no success or frontier
// advance while it is unresolved.
//
// A cut runs in the holder's session (the layout kernel's Begin, Commit and
// Abort; Put of a content-named object is its Ensure):
//   - Begin opens the session before the cut's first upload and returns it;
//     a Begin while the session is open is its replay. Every object the cut
//     writes belongs to the session until CASHead commits it.
//   - Abort is the notice that a cut ended without a commit. It never
//     blocks: the remote rolls the session back at its next HEAD read.
type chunkRemote interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	GetHead(ctx context.Context) (chunkHead, error)
	CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error)
	Delete(ctx context.Context, key string) error
	Begin(ctx context.Context) (string, error)
	Abort()
}

// chunkCommitChecker is a chunkRemote that checks a commit's objects (every
// new chunk, segment and root written by this holder's session) before the
// bounded HEAD update: CheckCommit with the cut's budget, then CASHead for
// the same next skips that check.
type chunkCommitChecker interface {
	CheckCommit(ctx context.Context, expected chunkHeadVersion, next chunkHead) error
}

// validChunkKey is the chunk key domain: a non-empty, valid UTF-8 string.
// Every remote refuses any other key at Put, before it persists anything (an
// encoding that replaced invalid bytes would store another key than the
// one named).
func validChunkKey(key string) error {
	if key == "" || !utf8.ValidString(key) {
		return fmt.Errorf("%w: %q", errChunkKeyInvalid, key)
	}
	return nil
}

// chunkLayerConfig configures one attachment.
type chunkLayerConfig struct {
	// OnPublishSettled, when set, is called once per confirmed publication
	// (a cut, a detach, or an earlier unresolved CAS found applied), with the
	// HEAD before and after it (INV-PUBLISH-SETTLED-001). Calls are
	// serialized and in publication order, outside the capture lock, on
	// whichever publishing caller is draining the reports (a publisher may
	// return before its own report is delivered by the drainer). The drainer
	// is registered with Close; ctx ends with Close. Once Close has begun no
	// further report is dequeued; a report already dequeued may still reach
	// the hook after Close began (Close then waits for it).
	//
	// The hook MUST return without blocking and without remote calls; enqueue
	// work to your own worker. It runs on the cut or detach caller's
	// goroutine, before a detach returns: its duration is a guest-visible wait
	// (Part C wait sites). While a hook stalls, later reports queue without a
	// bound. A hook may publish again (its report is delivered after).
	OnPublishSettled func(ctx context.Context, prev, next ChunkHead)
	// ChunkBlocks is the number of consecutive blocks per chunk (1024 for
	// 4 KiB blocks and 4 MiB chunks).
	ChunkBlocks uint32
	// Epoch is the epoch the block master assigned to this node; every HEAD
	// CAS carries it. It is the manifest's own epoch, kept separate from the
	// frontend Identity().Epoch, which is checked against the master
	// projection on every I/O.
	Epoch  uint64
	Holder string
	// HeadCoolingWindow is zero for the supported single serialization point.
	// Nonzero is reserved for explicit multi-filer experiments, must cover
	// the HEAD operation deadline plus settling, and does not imply atomicity.
	HeadCoolingWindow time.Duration
	// FreshFromHead says the local volume is new and empty and starts from
	// HEAD (every block absent, read lazily). False means an existing local
	// volume. Never inferred from the local contents.
	FreshFromHead bool
	// CreatedThisStart says this process start created the local store (an
	// in-memory fact, never persisted). RULE-FRESH-KNOWN-ZERO samples only
	// with it and FreshFromHead: a fresh marker left by an earlier start, an
	// older binary or another opener is never trusted.
	CreatedThisStart bool
	// RestoreChunkBytes is the local, validated geometry for upgrading a flat
	// legacy image during an ownership claim; it is never inferred from data.
	RestoreChunkBytes uint64
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
	// HydrationWorkers bounds layer-owned fetches (0 defaults to 2).
	HydrationWorkers int
	// HydrationQueueDepth bounds distinct queued/in-flight chunks (0 defaults to 64).
	HydrationQueueDepth int
	// Bitmap persists the legacy dirty-since-HEAD bitmap. Required for v2;
	// impl3 uses its owned LSN map and never calls this optional legacy object.
	Bitmap chunkBitmapStore

	// BarrierBound is for legacy test fixtures only: it sets
	// BarrierOverBound when a cut copies more chunks than this under its
	// barrier. Production staging is by the copy-on-write side budget (M5
	// Stage), and production leaves this zero.
	BarrierBound int

	// UploadConcurrency bounds capture/pre-copy Put calls (0 defaults to 8).
	UploadConcurrency int

	// PayloadMemoryBudget bounds this layer's uploader, saver and cached payload
	// bytes (0 uses 64 MiB, or six chunks for larger geometry). Host-wide
	// admission is provided separately through Budget; this per-process
	// budget does not coordinate other blockvolume processes.
	PayloadMemoryBudget int64
	Budget              Budget

	// COWSpillLimit caps saved-before-write
	// side bytes. Native pre-copy is triggered when remaining candidate
	// bytes exceed half this cap; it streams directly to remote, without a
	// local payload file. Exceeding it aborts the cut (the caller
	// retries), never publishes a partial image or refuses the guest mutation.
	// 0 defaults to defaultCOWSpillLimit; negative is refused.
	COWSpillLimit int64
	// COWSpillFree, optional, returns the free bytes of the filesystem that
	// holds the side file. When set, it is read at each cut and that cut's
	// spill cap is min(COWSpillLimit, free/2 - 1 GiB), at least 0; an error
	// counts as no free space.
	COWSpillFree func() (int64, error)

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
// SyncPhaseStats are nested details of SyncNs; never add them to SyncNs.
type SyncPhaseStats struct {
	LifecycleWaitNs  int64 `json:"lifecycleWaitNs"`  // waiting for the store's lifecycle lock
	StoreWaitNs      int64 `json:"storeWaitNs"`      // waiting for the store lock
	GroupCommitNs    int64 `json:"groupCommitNs"`    // in the group commit that made the data durable
	CompletionWaitNs int64 `json:"completionWaitNs"` // waiting for the sync leader role to validate the completion
}

type chunkCutStats struct {
	SyncPhases SyncPhaseStats // the parts of SyncNs (never added to it)
	// Disjoint non-payload intervals within barrier-to-settlement fixed cost.
	CandidateNs      int64
	WindowSetupNs    int64
	PublishPrepareNs int64
	ManifestNs       int64
	CommitStateNs    int64
	NonPayloadCopyNs int64

	PayloadNs int64 // disjoint streaming/copy/upload wall intervals

	// Local validated WAL attempts and local copy duration, including savers.
	COWWALReadAttempts uint64
	COWLocalReadNs     int64
	COWSavedChunks     int
	COWSaveNs          int64
	COWWaitNs          int64
	// BarrierPauseNs is the guest pause: from the capture barrier's Lock
	// request until it is released. LockWaitNs is its first part, the wait for
	// writers already holding the capture barrier shared.
	BarrierPauseNs  int64
	LockWaitNs      int64
	DrainNs         int64
	CASWriteNs      int64
	HeadReadbackNs  int64
	SyncNs          int64
	CopyNs          int64
	CandidateChunks int
	UploadedChunks  int
	UploadedBytes   int64
	ZeroedChunks    int
	UnchangedChunks int
	UploadNs        int64
	CASNs           int64
	MapPersistNs    int64
	CASResult       string
	TotalNs         int64
	// Pre-copy (staging by the copy-on-write side budget, M5 Stage):
	// StageRounds staging rounds staged StagedChunks chunks outside the
	// barrier; StagedChunksUsed of the uploads in HEAD came from staging.
	// BarrierCopyChunks were copied under the barrier. BarrierOverBound:
	// more than the legacy fixture BarrierBound; always false in
	// production, where that bound is zero.
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
//     (the C11 bitmap popcount): the next cut's candidates, not its
//     progress (it does not fall while a cut runs or across failed
//     attempts); the cut scheduler's threshold reads it. Chunks still
//     awaiting reconciliation are not counted.
//   - StagedChunks counts the chunks staging finished (read, hashed,
//     uploaded when not zero, and remembered for reuse) in every cut
//     attempt of this layer: a failed attempt and an interrupted round
//     count what they finished. It only grows: a chunk staged again (after
//     a guest write re-dirtied it, or in a later cut) counts again; it is
//     in-process (a restart starts at 0). The progress a cut retried after
//     its deadline has made.
//   - Reconciling: the dirty set is being reconciled against HEAD; no cut
//     publishes until it is done. ReconcileDone of ReconcileTotal chunks
//     are compared.
//   - LastCut, LastCutH and LastCutNs (wall clock, Unix ns) describe the
//     last successful cut, whatever triggered it. CutSeq counts the
//     successful cuts of this layer (in-process): it identifies the latest
//     cut and orders cuts without any clock.
type chunkLayerStats struct {
	Hydration         ChunkHydrationStats // lazy fetches of absent chunks from the remote
	CutsOK            uint64              // cuts that installed a HEAD
	CutsNoop          uint64              // cuts with nothing new (no CAS)
	CutsFailed        uint64              // every other cut failure
	CutsRefused       map[string]uint64   // refusals by reason: admission, fenced, detached
	CutsAborted       map[string]uint64   // saver_capacity, cow_capacity, no_room_to_cut, free_space_unreadable, budget_revoked, upload
	CASConflicts      uint64              // HEAD CAS precondition failures
	CASUnknown        uint64              // HEAD CAS results still unresolved after the re-read
	PendingUnresolved bool                // an unresolved HEAD proposal is held now
	DirtyChunks       int                 // chunks dirty since the last HEAD (not counting those awaiting reconciliation): candidates, not progress
	StagedChunks      uint64              // chunks staged by every cut attempt, failed ones included (in-process)
	KnownZeroChunks   uint64              // chunks this layer's fresh attach certified known-zero (RULE-FRESH-KNOWN-ZERO); 0 otherwise
	Reconciling       bool                // the dirty set is being reconciled against HEAD; no cut publishes meanwhile
	ReconcileDone     int                 // chunks compared so far
	ReconcileTotal    int                 // chunks to compare
	LastCut           chunkCutStats       // the last successful cut's measurements
	LastCutH          uint64              // the LSN the last successful cut published
	LastCutNs         int64               // when it completed (wall clock, Unix ns)
	CutSeq            uint64              // successful cuts of this layer so far (in-process); the latest cut's identity
	// BarrierOverBound counts cuts whose barrier copied more than the
	// bound after the staging rounds.
	BarrierOverBound uint64
	// AbsentChunks is the number of HEAD-named chunks still absent with a
	// block not local (what HydrateAll would still fetch); 0 = fully local.
	AbsentChunks int
	// COWSpillCap is the last cut's side-file cap in bytes, read once per
	// cut; COWSpillCapReason is why it is 0, when the free space made it so:
	// no_room_to_cut (half the free space is within the 1 GiB reserve) or
	// free_space_unreadable (the free space could not be read).
	COWSpillCap       int64
	COWSpillCapReason string `json:",omitempty"`
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
//
// ChunkCandidateTail is advisory pending work sampled after a successful cut.
// Bytes and rate/estimate belong to one sampler observation; Known=false must
// never be interpreted as a zero-cost tail. Admission rechecks at its barrier.
type ChunkCandidateTail struct {
	FrozenBudget
	Bytes          uint64        `json:"bytes"`
	Estimate       time.Duration `json:"estimateNs"`
	BytesPerSecond float64       `json:"bytesPerSecond"`
	RateSamples    int           `json:"rateSamples"`
	SampleAge      time.Duration `json:"sampleAgeNs"`
	Known          bool          `json:"known"`
}

type chunkCutResult struct {
	CandidateTail *ChunkCandidateTail
	Precopy       PrecopyReport
	H             uint64
	Head          chunkHead
	Noop          bool
	FullHash      bool
	Stats         chunkCutStats
}

// chunkLayer is what the store owner drives.
// detachSettlement is how a failed Detach ended (SettleDetach).
type detachSettlement int

const (
	detachNotPublished detachSettlement = iota
	detachApplied
	detachFenced
)

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
	// SettleDetach decides how a failed Detach ended, with the caller's I/O
	// still stopped: detachNotPublished (HEAD does not and can never carry
	// this layer's detach: serving may resume; the dirty set stays dirty),
	// detachApplied (HEAD carries it: the detached result is returned) or
	// detachFenced (HEAD belongs to a newer epoch or another holder). An
	// error wrapping errChunkCASOutcomeUnknown leaves it undecided: stay
	// stopped. It never resumes on top of a possibly detached HEAD.
	SettleDetach(ctx context.Context) (detachSettlement, chunkCutResult, error)
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
	// errChunkContentMismatch: a chunk read from the remote does not hash
	// (or size) to the HEAD ref that names it. Its bytes are never served
	// or installed.
	errChunkContentMismatch = errors.New("storage: chunk content does not match HEAD")
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
