package durable

// Package durable G-int.4 — DurableProvider.
//
// DurableProvider implements frontend.Provider for production
// `cmd/blockvolume`. On Open it:
//   1. Builds the on-disk path per config for the volumeID
//   2. If the file exists, verifies on-disk magic matches the
//      configured selector (ImplKind mismatch → ErrImplKindMismatch;
//      fail-fast, never silently coerce)
//   3. Opens (or creates) the LogicalStorage
//   4. Returns a StorageBackend with operational=FALSE
//
// Operational=true is not flipped by Open. The caller (cmd/
// blockvolume) calls RecoverVolume after Open; RecoverVolume runs
// LogicalStorage.Recover and flips the backend operational bit
// based on the outcome. This keeps Recovery readiness a distinct
// step (INV-DURABLE-OPGATE-001) rather than a side-effect of Open.
//
// Per-volume handles are cached: multiple Open calls for the same
// volumeID return the same StorageBackend, sharing LogicalStorage
// state. This matches memback's per-Identity cache pattern — every
// session opened against the same volume sees the same bytes.
//
// testback.StaticProvider remains unchanged (audit §11 G-int.4).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/geometry"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/parallelwal"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

// ImplName is the user-facing selector for the LogicalStorage
// implementation. Config sets one of these; Provider picks the
// matching open/create functions.
type ImplName string

const (
	// ImplAuto (the zero value) opens an existing volume with the
	// implementation its on-disk superblock names, and creates a new
	// volume as ImplWALStore.
	ImplAuto ImplName = ""
	// ImplSmartWAL is experimental: in-place overwrites have no rollback
	// on a torn write, blocks lose their per-block CRC once their ring
	// records recycle, and it has no chunk-snapshot support. Selected only
	// explicitly, or by auto for an existing smartwal volume.
	ImplSmartWAL ImplName = "smartwal"
	ImplWALStore ImplName = "walstore"
	// ImplParallelWALStore is the explicit Phase 167 candidate. It remains
	// opt-in until the full recovery, replication, and mounted close gates pass.
	ImplParallelWALStore ImplName = "parallel-walstore"
	// ImplWALStoreChunk is a walstore volume with an attached chunk layer:
	// an asynchronous remote copy of immutable chunks published by a HEAD
	// compare-and-swap. On disk it is a walstore file. Requires
	// ProviderConfig.ChunkLayoutHeadFor and ChunkLayoutRemoteFor.
	ImplWALStoreChunk ImplName = "walstore-chunk"
)

// DefaultChunkBlocks is 4 MiB chunks of 4 KiB blocks.
const DefaultChunkBlocks = 1024

// ErrImplKindMismatch is returned by Open when the configured
// selector does not match the on-disk superblock magic. Named so
// tests can errors.Is on it.
var ErrImplKindMismatch = errors.New("durable: impl kind mismatch between selector and on-disk superblock")

// ErrVolumeNotOpen is returned by RecoverVolume / Close when the
// volumeID has not been Open'd.
var ErrVolumeNotOpen = errors.New("durable: volume not opened")

// ErrDetachAborted: the detach did not happen (its final cut provably did not
// publish), so the volume resumed serving on this host (MIGRATION E4).
var ErrDetachAborted = errors.New("durable: detach aborted; the volume keeps serving on this host")

// ErrDetachOutcomeUnknown: whether the detach landed cannot be decided yet;
// I/O stays stopped (never resumed on a possibly detached HEAD).
var ErrDetachOutcomeUnknown = errors.New("durable: detach outcome unknown; I/O stays stopped")

// ProviderConfig configures a DurableProvider.
type ProviderConfig struct {
	// Impl selects which LogicalStorage implementation to open or
	// create. Zero value (ImplAuto): an existing volume opens with its
	// on-disk implementation, and a new volume is created as walstore.
	Impl ImplName

	// StorageRoot is the directory under which per-volume files
	// live. Required.
	StorageRoot string

	// BlockSize / NumBlocks are used ONLY on create (file doesn't
	// exist yet). Zero values: BlockSize=4096; NumBlocks must be
	// non-zero on first Open, zero means "open existing only".
	BlockSize int
	NumBlocks uint32

	// OpenTimeout caps how long Open waits for the projection to
	// report healthy for the requested volumeID. Zero → default 5s.
	OpenTimeout time.Duration

	// ChunkAssignmentTimeout bounds deferred startup waiting for a healthy,
	// nonzero master assignment and attach. Local recovery returns first so
	// the host can apply its assignment. Zero defaults to 30s.
	ChunkAssignmentTimeout time.Duration

	// ChunkMasterWait, when set, replaces ChunkAssignmentTimeout's fixed
	// deadline (a workspace primary): given the wait's start, the host
	// returns the wait's one bound (zero: none, the master gives no reply or
	// refuses the node) and whether a permanent refusal (moved or deleted)
	// ends it now. It is read on the wait's poll and must not block.
	ChunkMasterWait func(start time.Time) (deadline time.Time, final bool)

	// WALRetentionLSNs is the operator-tunable retention window past
	// the substrate's checkpoint LSN (G6 §1.A α). Zero = pre-G6
	// strict checkpoint-driven recycle. Non-zero relaxes the recycle
	// gate so a slow replica still has a recovery scan path while
	// it lags within the configured envelope.
	//
	// Substrate semantics:
	//   - ImplWALStore (append-only): WAL retention is purely the
	//     scan-time gate; non-zero value relaxes the gate to
	//     fromLSN > checkpointLSN - WALRetentionLSNs.
	//   - ImplSmartWAL (fixed-size ring): retention is implicit from
	//     ring capacity (--durable-blocks). This flag is informational
	//     for ImplSmartWAL; operators size retention via NumBlocks.
	//
	// Pinned by: INV-G6-RETENTION-POLICY-OPERATOR-VISIBLE.
	WALRetentionLSNs uint64

	// WALMultiBlockRecords enables the Phase 150 multi-block WAL record
	// prototype for ImplWALStore only. Default false preserves the current
	// single-block WAL record format. Do not enable for release claims until a
	// mounted NVMe/TCP profile gate passes.
	WALMultiBlockRecords bool

	// WALRecoveryTestDisableFlusher stops walstore's background checkpoint
	// flusher before test writes are issued. It exists only for recovery gates
	// that need synced-but-uncheckpointed WAL records to survive a process
	// restart and prove real replay. Default false.
	WALRecoveryTestDisableFlusher bool

	// ChunkLayoutHeadFor and ChunkLayoutRemoteFor select the content-named layout.
	// The remote is constructed once, with the identity confirmed by Start.
	ChunkLayoutHeadFor   func(string) (layout.StartStore, error)
	ChunkLayoutRemoteFor func(string, layout.HolderID) (storage.ChunkRemote, error)
	// ChunkBlocks is the number of blocks per chunk. Zero: 1024.
	ChunkBlocks uint32
	// ChunkAttachCheck, if set, runs before every chunk layer attach, ahead
	// of any remote read or CAS, with the volume's local geometry. A
	// non-nil error refuses the attach. blockvolume wires it to the VOLUME
	// entry's attach rules (B9 W2 §4.2): create VOLUME when missing, refuse
	// a geometry mismatch and a fork whose HEAD is not written yet.
	ChunkAttachCheck func(ctx context.Context, volumeID string, g ChunkGeometry) error
	// ChunkHolder names this node in HEAD; blockvolume sets
	// <server-id>/<replica-id>. Zero: the replica ID (tests).
	ChunkHolder string
	// HostVolumeLock makes the provider hold an exclusive, nonblocking lock
	// on HostVolumeLockDir/<volume id>.lock from before the volume's store
	// opens until Close: one host runs at most one live process per volume
	// under the host-lock rule. NewDurableProvider creates the directory (0700) when
	// missing and refuses when it cannot create or use it; the tmpfiles.d
	// entry in deploy/systemd creates it for the unit's user. blockvolume
	// sets this.
	HostVolumeLock bool
	// VolumeLockDirTestOnly replaces HostVolumeLockDir for tests and test
	// harnesses (blockvolume --volume-lock-dir-test-only); the one-process-
	// per-host guarantee then holds only among processes sharing it.
	VolumeLockDirTestOnly string
	// Zero uses immediate HEAD confirmation for the supported single-filer
	// topology. Nonzero cooling is reserved for explicit topology experiments.
	ChunkHeadCoolingWindow time.Duration
	// ChunkReadMissTimeout bounds one remote fetch of an absent block.
	// Zero: the storage default.
	ChunkReadMissTimeout time.Duration
	// Per-volume hydration bounds; zero selects 2 workers and 64 distinct chunks.
	ChunkHydrationWorkers    int
	ChunkHydrationQueueDepth int
	// ChunkCaptureInterval: an automatic cut is due once this long has
	// passed since the volume's last successful cut (of any kind). Zero
	// disables the timer.
	ChunkCaptureInterval time.Duration
	// ChunkCutTimeout bounds a cut as a whole; zero uses five minutes.
	ChunkCutTimeout time.Duration
	// Maintenance runs serially for an attached healthy volume. Zero interval disables
	// it. Callback ownership ends only after cancellation and worker settlement.
	ChunkMaintenanceInterval time.Duration
	ChunkMaintenanceTimeout  time.Duration
	ChunkMaintenance         func(context.Context, string) error
	// ChunkSnapshotPeriod schedules a named snapshot of each attached
	// healthy volume once per period, at a per-volume offset (zero
	// disables it). ChunkScheduledSnapshot takes one: it cuts through cut
	// (one urgent cut) and records the snapshot of the HEAD cut returns. A
	// *SnapshotSkip skips the attempt, counted by reason; a
	// *SnapshotFailure carries the failure's code.
	ChunkSnapshotPeriod    time.Duration
	ChunkScheduledSnapshot func(ctx context.Context, volumeID string, at time.Time, cut func(context.Context) (storage.ChunkHead, error)) error
	// DetachFreezeTimeout bounds how long a failing DetachVolume keeps guest
	// I/O stopped before it resumes or settles: the drain and final cut get
	// three quarters of it, the settlement of a failed cut the rest (the
	// caller's context can only shorten the cut part). Zero: 20 s, below the
	// host's 30 s I/O timeout; a freeze past the 5 s target is logged as an
	// alarm. Cooperative: a syscall that never returns is not bounded.
	DetachFreezeTimeout time.Duration
	// ChunkDetachPrecopy attempts bounded rolling cuts before stopping I/O.
	// Nonconvergence leaves service live; no fallback to an unbounded tail.
	ChunkDetachPrecopy bool
	// OnChunkDetach, optional, is called with the volume id when DetachVolume
	// starts a detach (a graceful stop), before the drain and final cut, so
	// the host can watch for the coming lease revocation; it returns a token
	// for this attempt. OnChunkDetachResumed, optional, is called with that
	// token only when the detach provably did not happen and serving resumed;
	// a detach that applied, fenced or whose outcome is unknown never calls
	// it. Both run under the volume's attach lock: they must not block or
	// call back into the provider.
	OnChunkDetach        func(volumeID string) uint64
	OnChunkDetachResumed func(volumeID string, attempt uint64)
	// ChunkOnPublishSettled, optional, is called once per confirmed
	// publication of a chunk volume's HEAD, with the HEAD before and after it
	// (storage.ChunkLayerConfig.OnPublishSettled): serialized, in publication
	// order, outside the capture lock, by the volume's one registered drainer
	// (whichever publishing caller is draining; a publisher may return before
	// its own report is delivered). ctx ends when the volume's layer closes;
	// after that no report is dequeued, though one already dequeued may still
	// be delivered. It must return without blocking and without remote calls:
	// enqueue work to your own worker (it is a guest-visible wait on detach).
	ChunkOnPublishSettled func(ctx context.Context, volumeID string, prev, next storage.ChunkHead)
	// ChunkUploadConcurrency bounds remote uploads; zero defaults to 8.
	ChunkUploadConcurrency int
	// ChunkPayloadMemoryBudget bounds one volume's cut payloads; zero uses the storage default.
	ChunkPayloadMemoryBudget int64
	// ChunkBudget optionally admits cuts and accounts COW bytes through the host limiter.
	ChunkBudget        storage.Budget
	ChunkCOWSpillLimit int64
	// ChunkStagger spreads automatic cuts by the stable volume-ID phase.
	ChunkStagger bool
}

// DurableProvider is the production frontend.Provider implementation.
type DurableProvider struct {
	cfg  ProviderConfig
	view frontend.ProjectionView

	mu             sync.Mutex
	volumes        map[string]*volHandle  // keyed by volumeID
	opening        map[string]*sync.Mutex // keyed by volumeID; serializes open/create
	closed         bool
	startupUpdates chan struct{} // wake only; the generation-bound result stays on the handle
	fenceUpdates   chan struct{} // wake only; runtimeFenced stays on the handle

	// testAfterChunkResolve, test-only, runs in CutVolume right after the
	// volume's handle and layer are resolved.
	testAfterChunkResolve func()
	// testDetachStopIO, test-only, replaces DetachVolume's StopIO call (so a
	// test can force the failed-drain branch); testDetachTrace, test-only,
	// observes the detach's stages and their deadlines ("cut", "settle").
	testDetachStopIO func(ctx context.Context) error
	testDetachTrace  func(stage string, deadline time.Time)
	// testChunkLoopTrace, test-only, observes the cut loop's periodic cuts:
	// "due" when it decides one from lastCutNs, then "cut" with whether it
	// was skipped.
	testChunkLoopTrace func(event string, lastCutNs int64, skipped bool)

	// testBeforeChunkFreshRetire holds the real post-presence/pre-marker boundary.
	testBeforeChunkFreshRetire func() error
	// testAfterChunkFreshRetire runs after the attach commit retired the
	// fresh marker, before the known-zero marking is made durable.
	testAfterChunkFreshRetire func() error

	// testWALStoreFactory selects a dark store only in provider caller controls.
	// No production option, flag or environment variable installs it.
	testWALStoreFactory func(path string, create bool) (*storage.WALStore, error)

	// walstore-chunk: closeCtx ends with Close and cancels in-flight
	// remote attachment; inflight counts RecoverVolume calls admitted
	// before Close, and each attached layer's fence watcher, which Close
	// waits for before closing any store.
	closeCtx    context.Context
	closeCancel context.CancelFunc
	inflight    sync.WaitGroup
}

type volHandle struct {
	storage storage.LogicalStorage
	backend *StorageBackend
	path    string
	impl    ImplName // the resolved implementation, never ImplAuto

	// hostLock is the volume's host lock (HostVolumeLock), held while open.
	hostLock *os.File
	// startupCure is the operator's cure of a stale-root startup end.
	startupCure string

	// startupIdentityRefused is the definite full-id refusal for startupEnded.
	// endChunkStartup is its decision writer, guarded by p.mu.
	startupIdentityRefused bool

	// ImplWALStoreChunk. fresh: the local volume is new and starts from
	// the remote HEAD. It mirrors the durable fresh marker beside the
	// volume file, written before the file is created and removed only
	// after the first attach succeeded, so an interrupted initialization
	// is still fresh after a restart.
	fresh bool
	// createdThisStart: this process start created the store (in memory,
	// never persisted); RULE-FRESH-KNOWN-ZERO needs it with fresh.
	createdThisStart bool
	attachMu         sync.Mutex // serializes recovery and attach of this volume
	// recoverGen numbers this volume's RecoverVolume calls (written under
	// attachMu and p.mu). waitGen is the generation of the live authority
	// wait, 0 if none: a recovered volume whose layer waits for master
	// authority. A wait only acts while its generation is still the
	// latest recovery's.
	recoverGen        uint64
	waitGen           uint64
	startupSuperseded bool               // exact foreign-holder claim proof for startupEnded; guarded by p.mu
	startupEnded      uint64             // recovery generation with a terminal startup outcome; guarded by p.mu
	runtimeFenced     bool               // the attached layer fenced after startup (HEAD has another owner); guarded by p.mu
	chunk             storage.ChunkLayer // after RecoverVolume
	layoutHolder      layout.HolderID    // successful Start identity; guarded by p.mu
	layoutHead        layout.HeadStore
	layoutRemote      storage.ChunkRemote // one runtime per attachment, including attach retries
	chunkLineage      chunkLineage        // immutable authority captured by this attachment
	// Private until the owner commit succeeds; retained for attach retry.
	chunkPending storage.ChunkLayer
	// chunkDetach is set (under attachMu) at the final stop. It is cleared
	// only after settlement proves the stop was not published; an applied,
	// fenced or unknown outcome keeps frontend I/O stopped.
	chunkDetach bool
	// settling is the single automatic retry of an unknown stop outcome.
	// It is guarded by attachMu; no second stop may race its HEAD decision.
	settling  bool
	chunkStop context.CancelFunc
	chunkDone chan struct{}
	// snapshots is the scheduled-snapshot state of the running loop, nil
	// when the schedule is off.
	snapshots *snapshotSlot
	// cutWait is the automatic-cut timer's host slot waits, nil when the
	// timer is off.
	cutWait *cutWaitSlot
	// maintKick asks the running maintenance loop for one pass now (one
	// buffered token: kicks coalesce); nil when the loop is off.
	maintKick chan struct{}
}

// VolumeStatus is a read-only diagnostic snapshot of one durable volume
// handle. It intentionally exposes lineage and readiness facts without giving
// callers a control surface.
type VolumeStatus struct {
	VolumeID              string                               // the volume
	Path                  string                               // its store path under --durable-root
	Impl                  string                               // the durable implementation (walstore, smartwal, walstore-chunk, ...)
	ReplicaID             string                               // the replica this store holds
	StoreID               string                               `json:",omitempty"` // the store's superblock UUID (hex); empty when it carries none
	Epoch                 uint64                               // the epoch the store is latched to; 0 before an assignment
	EndpointVersion       uint64                               // the endpoint version it is latched to
	Latched               bool                                 // an assignment latched the store (epoch and endpoint version set)
	Operational           bool                                 // the backend admits I/O now
	FrontierKnown         bool                                 // DurableLSN, RetainedLSN and HeadLSN are the store's current boundaries
	DurableLSN            uint64                               // R: the newest LSN made durable
	RetainedLSN           uint64                               // S: the oldest LSN the WAL still retains
	HeadLSN               uint64                               // H: the newest LSN written
	Evidence              string                               // why the backend is or is not operational
	Closed                bool                                 // the backend was closed
	WriteProfile          WriteProfileStatus                   // write-path counters since start
	DataSync              *storage.DataSyncStatus              `json:",omitempty"` // data-file sync state, where the store has one
	CheckpointPersistence *storage.CheckpointPersistenceStatus `json:",omitempty"` // checkpoint persistence state, where the store has one
	// Chunk is the chunk layer's state and cut counters; walstore-chunk
	// volumes only.
	Chunk *ChunkStatus
	// Warnings are the store's typed recovery warnings (e.g.
	// wal_hole_suspected, issue #51); they do not change readiness.
	// HolesSuspected counts the recoveries that found a WAL hole.
	Warnings       []string `json:",omitempty"`
	HolesSuspected uint64   `json:",omitempty"`
}

// ChunkStatus is one walstore-chunk volume's chunk layer (B9 7.2-7.3).
// AwaitingAuthority: recovered, the layer waits for master authority to
// attach. Stats is zero until the layer is attached.
type ChunkStatus struct {
	Attached          bool                     // the chunk layer is attached to the remote
	AwaitingAuthority bool                     // recovered; the layer waits for master authority to attach
	Fenced            bool                     // a newer holder fenced this layer: it publishes nothing
	RemoteLSN         uint64                   // the LSN of the remote HEAD (what the remote holds durably)
	Stats             storage.ChunkLayerStats  // the layer's cut counters; zero until attached
	HostBudget        any                      `json:",omitempty"` // the host background-cut budget state, with --host-limit-dir
	ScheduledSnapshot *ScheduledSnapshotStatus `json:",omitempty"` // the scheduled-snapshot state; absent when the schedule is off or its loop has not started
	CutSchedule       *CutScheduleStatus       `json:",omitempty"` // the automatic-cut timer's state; absent when the timer is off or its loop has not started
}

// CutScheduleStatus is the automatic-cut timer's view of host slot waits.
type CutScheduleStatus struct {
	HostSlotWaits       uint64 // automatic cuts refused because every host slot was held
	HostSlotWaitSinceNs int64  // wall time of the first such refusal since the last successful cut; 0 when not waiting
}

// NewDurableProvider constructs a provider. view is the ONLY
// source of authority lineage the provider is allowed to consult
// (sketch §9 boundary).
func NewDurableProvider(cfg ProviderConfig, view frontend.ProjectionView) (*DurableProvider, error) {
	if cfg.StorageRoot == "" {
		return nil, fmt.Errorf("durable: ProviderConfig.StorageRoot required")
	}
	if cfg.Impl != ImplAuto && cfg.Impl != ImplSmartWAL && cfg.Impl != ImplWALStore && cfg.Impl != ImplParallelWALStore && cfg.Impl != ImplWALStoreChunk {
		return nil, fmt.Errorf("durable: unknown impl %q (want auto (empty), %q, %q, %q, or %q)",
			cfg.Impl, ImplSmartWAL, ImplWALStore, ImplParallelWALStore, ImplWALStoreChunk)
	}
	if (cfg.ChunkLayoutHeadFor == nil) != (cfg.ChunkLayoutRemoteFor == nil) {
		return nil, errors.New("durable: both layout factories are required")
	}
	if cfg.Impl == ImplWALStoreChunk && cfg.ChunkLayoutHeadFor == nil {
		return nil, fmt.Errorf("durable: impl %q requires the layout head and remote factories", ImplWALStoreChunk)
	}
	if cfg.ChunkCaptureInterval < 0 || cfg.ChunkCutTimeout < 0 || cfg.ChunkMaintenanceInterval < 0 || cfg.ChunkMaintenanceTimeout < 0 {
		return nil, fmt.Errorf("durable: chunk schedule durations must not be negative")
	}
	if cfg.ChunkMaintenanceInterval > 0 && cfg.ChunkMaintenance == nil {
		return nil, fmt.Errorf("durable: chunk maintenance interval requires its callback")
	}
	if cfg.ChunkSnapshotPeriod < 0 || (cfg.ChunkSnapshotPeriod > 0 && cfg.ChunkScheduledSnapshot == nil) {
		return nil, fmt.Errorf("durable: a chunk snapshot period must not be negative and requires its callback")
	}
	if cfg.HostVolumeLock {
		if cfg.VolumeLockDirTestOnly == "" {
			cfg.VolumeLockDirTestOnly = HostVolumeLockDir
		}
		if err := makeVolumeLockDir(cfg.VolumeLockDirTestOnly); err != nil {
			return nil, fmt.Errorf("%w %s: %w; %s", ErrVolumeLockDirUnavailable, cfg.VolumeLockDirTestOnly, err, volumeLockCure)
		}
	}
	if cfg.ChunkBlocks == 0 {
		cfg.ChunkBlocks = DefaultChunkBlocks
	}
	if cfg.BlockSize == 0 {
		cfg.BlockSize = 4096
	}
	if cfg.OpenTimeout == 0 {
		cfg.OpenTimeout = 5 * time.Second
	}
	if err := os.MkdirAll(cfg.StorageRoot, 0o755); err != nil {
		return nil, fmt.Errorf("durable: mkdir %s: %w", cfg.StorageRoot, err)
	}
	closeCtx, closeCancel := context.WithCancel(context.Background())
	return &DurableProvider{
		cfg:            cfg,
		view:           view,
		volumes:        map[string]*volHandle{},
		opening:        map[string]*sync.Mutex{},
		closeCtx:       closeCtx,
		closeCancel:    closeCancel,
		startupUpdates: make(chan struct{}, 1),
		fenceUpdates:   make(chan struct{}, 1),
	}, nil
}

// volumePath returns the on-disk path for a volumeID. Volume IDs
// are used as filenames; callers are responsible for passing
// filesystem-safe strings (the master/adapter layer already does).
func (p *DurableProvider) volumePath(volumeID string) string {
	return filepath.Join(p.cfg.StorageRoot, volumeID+".bin")
}

// VolumeExists reports whether volumeID's storage file already exists, i.e.
// whether opening it would open an existing volume rather than create one.
// Callers that must do something before a volume's creation (the replica
// floor record, fencing gap G1) ask here first.
func (p *DurableProvider) VolumeExists(volumeID string) (bool, error) {
	return fileExists(p.volumePath(volumeID))
}

// Open implements frontend.Provider. Returns the cached backend
// for volumeID if already opened; otherwise opens (or creates)
// storage and caches the handle.
//
// Invariants:
//   - Returned Backend starts operational=FALSE; caller must
//     follow up with RecoverVolume to flip it (T3b mini plan §2.3).
//   - ImplKind mismatch against on-disk superblock → ErrImplKindMismatch
//     (never silently opens a different-impl file; mini plan §2.4).
func (p *DurableProvider) Open(ctx context.Context, volumeID string) (frontend.Backend, error) {
	// Wait for projection healthy, same shape as memback.
	if err := p.waitHealthy(ctx, volumeID); err != nil {
		return nil, err
	}

	h, err := p.ensureHandle(volumeID)
	if err != nil {
		return nil, err
	}
	// Latch the backend's Identity to the now-Healthy projection. If
	// the backend was constructed via EnsureStorage pre-assignment, its
	// captured Identity has Epoch=0 even though VolumeID/ReplicaID came
	// from CLI config at construction. SetIdentity transitions Epoch=0
	// to the live projection's Epoch/EV; subsequent drift still fails
	// closed.
	proj := p.view.Projection()
	log.Printf("durable: dp.Open latching identity from projection volume=%s replica=%s epoch=%d ev=%d (post-waitHealthy)",
		proj.VolumeID, proj.ReplicaID, proj.Epoch, proj.EndpointVersion)
	target := frontend.Identity{
		VolumeID:        proj.VolumeID,
		ReplicaID:       proj.ReplicaID,
		Epoch:           proj.Epoch,
		EndpointVersion: proj.EndpointVersion,
	}
	if !h.backend.SetIdentity(target) {
		if got := h.backend.Identity(); got != target {
			return nil, durableIdentityConflict(got, target)
		}
	}
	return h.backend, nil
}

func (p *DurableProvider) ensureHandle(volumeID string) (*volHandle, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("durable: provider closed")
	}
	if h, ok := p.volumes[volumeID]; ok {
		p.mu.Unlock()
		return h, nil
	}
	openMu := p.opening[volumeID]
	if openMu == nil {
		openMu = &sync.Mutex{}
		p.opening[volumeID] = openMu
	}
	p.mu.Unlock()

	openMu.Lock()
	defer openMu.Unlock()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("durable: provider closed")
	}
	if h, ok := p.volumes[volumeID]; ok {
		p.mu.Unlock()
		return h, nil
	}
	p.mu.Unlock()

	// Host-lock rule: the host lock is taken before the store opens.
	hostLock, err := p.lockVolumeOnHost(volumeID)
	if err != nil {
		return nil, err
	}
	h, err := p.openOrCreate(volumeID)
	if err != nil {
		closeHostLock(hostLock)
		return nil, err
	}
	h.hostLock = hostLock

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = h.storage.Close()
		closeHostLock(h.hostLock)
		return nil, fmt.Errorf("durable: provider closed")
	}
	if existing, ok := p.volumes[volumeID]; ok {
		_ = h.storage.Close()
		closeHostLock(h.hostLock)
		return existing, nil
	}
	p.volumes[volumeID] = h
	return h, nil
}

// HostVolumeLockDir is the one fixed host-local directory of the volume
// host locks (HostVolumeLock).
const HostVolumeLockDir = "/run/sw-block/locks"

// ErrVolumeLockDirUnavailable refuses a provider with HostVolumeLock whose
// lock directory or lock file it cannot create or open.
var ErrVolumeLockDirUnavailable = errors.New("durable: host volume lock directory unavailable")

// volumeLockCure names the operator's cure. A directory or lock file a root
// process created is root-owned: an unprivileged unit then cannot use it.
const volumeLockCure = "install the tmpfiles.d entry (deploy/systemd/sw-block.tmpfiles.conf), or make its parent directory traversable (0755) and chown the directory and its lock files to this process's user"

// makeVolumeLockDir lays out a missing lock directory as the tmpfiles.d
// entry does: its parent 0755 (traversable by the unit's user even when root
// created it) and the directory itself 0700. Only a directory this call
// creates is chmod'ed (the umask cannot narrow it); one that exists, or that
// another process creates meanwhile, keeps its mode. An existing lock path
// that is not a directory refuses.
func makeVolumeLockDir(dir string) error {
	parent := filepath.Dir(dir)
	if err := os.MkdirAll(filepath.Dir(parent), 0o755); err != nil {
		return err
	}
	if err := os.Mkdir(parent, 0o755); err == nil {
		if err := os.Chmod(parent, 0o755); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := os.Mkdir(dir, 0o700); err == nil {
		return os.Chmod(dir, 0o700)
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s exists and is not a directory", dir)
	}
	return nil
}

// lockVolumeOnHost takes the volume's host lock (nil without
// HostVolumeLock). A second live process for the volume on this host gets
// storage.ErrWALStoreLocked.
func (p *DurableProvider) lockVolumeOnHost(volumeID string) (*os.File, error) {
	if !p.cfg.HostVolumeLock {
		return nil, nil
	}
	dir := p.cfg.VolumeLockDirTestOnly
	if volumeID == "" || volumeID == "." || volumeID == ".." || strings.ContainsAny(volumeID, "/\\") {
		return nil, fmt.Errorf("durable: volume id %q is not a plain path element", volumeID)
	}
	path := filepath.Join(dir, volumeID+".lock")
	// Read-only, 0644: a lock file another user created (the root ublk unit)
	// stays lockable by the plain unit; the 0700 directory keeps others out.
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0o644)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: volume lock %s: %w; %s", ErrVolumeLockDirUnavailable, path, err, volumeLockCure)
	}
	if err := storage.LockWALStoreFile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("durable: volume %s already has a live process on this host (lock %s held): %w", volumeID, path, err)
	}
	return f, nil
}

func closeHostLock(f *os.File) {
	if f != nil {
		_ = f.Close()
	}
}

// EnsureStorage opens (or creates) the on-disk LogicalStorage for
// volumeID WITHOUT waiting for the projection to flip Healthy. This
// is the binary-path entry point used by G5-4 to construct per-volume
// ReplicationVolume + ReplicaListener: replicas (assigned to a
// SUPPORTING role by master) never reach Healthy via the engine
// projection, but their storage still needs to be open so ReplicaListener
// can write incoming WAL entries into it.
//
// Open() (the frontend.Provider entry) keeps its Healthy-gated
// semantic — frontend consumers that need a write-ready backend
// continue to block until the projection authorizes. EnsureStorage
// is the role-agnostic alternative for binary wiring that needs raw
// storage access.
//
// The returned handle is borrowed — caller MUST NOT call Close on
// it (BUG-005 discipline: Provider owns storage lifecycle).
func (p *DurableProvider) EnsureStorage(volumeID string) (storage.LogicalStorage, error) {
	h, err := p.ensureHandle(volumeID)
	if err != nil {
		return nil, err
	}
	return h.storage, nil
}

// LogicalStorage returns the opened LogicalStorage for volumeID,
// or nil if neither Open nor EnsureStorage has been called for it.
// Read-only accessor; use EnsureStorage for the open-on-demand path.
func (p *DurableProvider) LogicalStorage(volumeID string) storage.LogicalStorage {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.volumes[volumeID]
	if !ok {
		return nil
	}
	return h.storage
}

// Backend returns the StorageBackend wrapper for volumeID, or nil
// if no Open / EnsureStorage call has populated it yet. Used by
// G5-4 binary wiring to install SetWriteObserver eagerly (before
// any frontend bind) — the same backend instance is returned by
// the future dp.Open() call when a frontend asks.
// AdmitsIO is StorageBackend.AdmitsIO for an open volume; ErrVolumeNotOpen
// for any other (fail closed).
func (p *DurableProvider) AdmitsIO(volumeID string) error {
	b := p.Backend(volumeID)
	if b == nil {
		return fmt.Errorf("%w: %s", ErrVolumeNotOpen, volumeID)
	}
	return b.AdmitsIO()
}

func (p *DurableProvider) Backend(volumeID string) *StorageBackend {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.volumes[volumeID]
	if !ok {
		return nil
	}
	return h.backend
}

// LatchVolumeIdentity latches an already-open durable backend to the current
// projection lineage without requiring frontend Healthy. Supporting replicas
// intentionally remain frontend-unhealthy while the primary line names another
// replica, but they still need durable lineage evidence for promotion-readiness
// and support-bundle diagnostics.
func (p *DurableProvider) LatchVolumeIdentity(volumeID string) (bool, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	p.mu.Unlock()
	if !ok {
		return false, ErrVolumeNotOpen
	}
	proj := p.view.Projection()
	if proj.VolumeID != volumeID {
		return false, fmt.Errorf("durable: projection volume %q does not match %q", proj.VolumeID, volumeID)
	}
	if proj.Epoch == 0 || proj.EndpointVersion == 0 {
		return false, fmt.Errorf("durable: projection lineage missing for volume=%s replica=%s epoch=%d ev=%d",
			proj.VolumeID, proj.ReplicaID, proj.Epoch, proj.EndpointVersion)
	}
	target := frontend.Identity{
		VolumeID:        proj.VolumeID,
		ReplicaID:       proj.ReplicaID,
		Epoch:           proj.Epoch,
		EndpointVersion: proj.EndpointVersion,
	}
	if h.backend.SetIdentity(target) {
		return true, nil
	}
	if got := h.backend.Identity(); got != target {
		return false, durableIdentityConflict(got, target)
	}
	return false, nil
}

func durableIdentityConflict(got, want frontend.Identity) error {
	return fmt.Errorf("durable: backend already latched to volume=%s replica=%s epoch=%d ev=%d; refusing projection volume=%s replica=%s epoch=%d ev=%d",
		got.VolumeID, got.ReplicaID, got.Epoch, got.EndpointVersion,
		want.VolumeID, want.ReplicaID, want.Epoch, want.EndpointVersion)
}

// DurableStatuses returns stable, sorted diagnostic snapshots for all opened
// volume handles. This is used by status endpoints and TestOps gates to prove
// durable lineage latching without scraping logs.
func (p *DurableProvider) DurableStatuses() []VolumeStatus {
	type chunkView struct {
		layer     storage.ChunkLayer
		awaiting  bool
		snapshots *snapshotSlot
		cutWait   *cutWaitSlot
	}
	p.mu.Lock()
	handles := make(map[string]*volHandle, len(p.volumes))
	chunks := map[string]chunkView{}
	for volumeID, h := range p.volumes {
		handles[volumeID] = h
		if p.cfg.Impl == ImplWALStoreChunk {
			chunks[volumeID] = chunkView{layer: h.chunk, awaiting: h.waitGen != 0, snapshots: h.snapshots, cutWait: h.cutWait}
		}
	}
	p.mu.Unlock()

	out := make([]VolumeStatus, 0, len(handles))
	for volumeID, h := range handles {
		st := h.backend.durableStatus(volumeID, h.path, string(h.impl))
		if cv, ok := chunks[volumeID]; ok {
			cs := &ChunkStatus{AwaitingAuthority: cv.awaiting}
			if cv.layer != nil {
				cs.Attached, cs.Fenced, cs.RemoteLSN, cs.Stats = true, cv.layer.Fenced(), cv.layer.RemoteLSN(), cv.layer.Stats()
			}
			if budget, ok := p.cfg.ChunkBudget.(interface{ Snapshot() any }); ok {
				cs.HostBudget = budget.Snapshot()
			}
			if cv.snapshots != nil {
				ss := cv.snapshots.status(time.Now())
				cs.ScheduledSnapshot = &ss
			}
			if cv.cutWait != nil {
				cw := cv.cutWait.status(cs.Stats.CutSeq)
				cs.CutSchedule = &cw
			}
			st.Chunk = cs
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VolumeID < out[j].VolumeID
	})
	return out
}

// waitHealthy polls the projection until it reports Healthy for
// volumeID, or ctx / cfg.OpenTimeout expires.
func (p *DurableProvider) waitHealthy(ctx context.Context, volumeID string) error {
	const poll = 10 * time.Millisecond
	ownCtx, cancel := context.WithTimeout(ctx, p.cfg.OpenTimeout)
	defer cancel()
	for {
		proj := p.view.Projection()
		if proj.VolumeID == volumeID && proj.Healthy {
			return nil
		}
		select {
		case <-ownCtx.Done():
			return frontend.ErrNotReady
		case <-time.After(poll):
		}
	}
}

// openOrCreate opens the on-disk file (verifying ImplKind) or
// creates it if missing (using the configured selector).
func (p *DurableProvider) openOrCreate(volumeID string) (*volHandle, error) {
	path := p.volumePath(volumeID)
	exists, err := fileExists(path)
	if err != nil {
		return nil, fmt.Errorf("durable: stat %s: %w", path, err)
	}
	if p.cfg.Impl != ImplWALStoreChunk {
		// A walstore-chunk volume shares walstore's on-disk format, and its
		// authoritative bytes may live only in the remote HEAD. Opened as
		// plain walstore it would serve local (possibly absent) extents, and
		// a write through it would reach a store whose fresh marker still
		// says no write ever did (RULE-FRESH-KNOWN-ZERO). So every other
		// implementation, auto or explicit, refuses a root carrying chunk
		// metadata; it never deletes or converts it.
		if side, err := chunkSidecars(path); err != nil {
			return nil, err
		} else if len(side) > 0 {
			return nil, fmt.Errorf("%w: %s is a chunk-managed volume (%s present); %s cannot open it: set --durable-impl=walstore-chunk with its remote configuration",
				ErrImplKindMismatch, path, strings.Join(side, ", "), p.cfg.Impl)
		}
	}

	if exists {
		impl, err := p.verifyImplOnDisk(path)
		if err != nil {
			return nil, err
		}
		h, err := p.openExisting(volumeID, path, impl)
		if err != nil || p.cfg.Impl != ImplWALStoreChunk {
			return h, err
		}
		ws := h.storage.(*storage.WALStore)
		_, fresh, ok := ws.LayoutIdentity()
		if !ok {
			_ = ws.Close()
			return nil, errors.New("durable: legacy local root has no layout identity; recreate it")
		}
		h.fresh = fresh
		return h, nil
	}
	// Existing storage ignores create geometry; admission precedes all fresh sidecars.
	if p.cfg.NumBlocks == 0 {
		return nil, fmt.Errorf("durable: NumBlocks=0; cannot create storage for %s (use open-existing mode)", volumeID)
	}
	if err := geometry.ValidateBlockGeometry(uint64(p.cfg.NumBlocks), uint64(p.cfg.BlockSize)); err != nil {
		return nil, err
	}
	// Headroom before any sidecar: a refused create leaves nothing behind.
	if err := p.refuseCreateWithoutHeadroom(volumeID); err != nil {
		return nil, err
	}
	impl := p.cfg.Impl
	if impl == ImplAuto {
		impl = ImplWALStore
	}
	h, err := p.createFresh(volumeID, path, impl)
	if err == nil && p.cfg.Impl == ImplWALStoreChunk {
		h.fresh, h.createdThisStart = true, true
	}
	return h, err
}

// verifyImplOnDisk peeks the 4-byte magic at offset 0 and returns the
// implementation to open the volume with: the on-disk one under
// ImplAuto, else the configured selector, with ErrImplKindMismatch if
// the two differ. Runs BEFORE the storage-level Open so the error
// message is selector-aware instead of cryptic magic-mismatch.
func (p *DurableProvider) verifyImplOnDisk(path string) (ImplName, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("durable: open for magic peek: %w", err)
	}
	defer f.Close()
	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		return "", fmt.Errorf("durable: read magic: %w", err)
	}
	diskImpl, ok := magicToImpl(string(magic))
	if !ok {
		// parallelwal has two independently checksummed headers. Slot 0
		// may be torn while slot 1 remains authoritative, so fall back to
		// the format's real header selector instead of rejecting on byte 0.
		if err := parallelwal.ProbeStore(path); err == nil {
			diskImpl, ok = ImplParallelWALStore, true
		} else if err := storage.ProbeWALStoreV3(path); err == nil {
			diskImpl, ok = ImplWALStore, true
		}
	}
	if !ok {
		return "", fmt.Errorf("%w: unrecognized magic %q at %s",
			ErrImplKindMismatch, string(magic), path)
	}
	if p.cfg.Impl == ImplAuto {
		return diskImpl, nil
	}
	if diskImpl == ImplWALStore && p.cfg.Impl == ImplWALStoreChunk {
		return ImplWALStoreChunk, nil // a chunk volume is a walstore file
	}
	if diskImpl != p.cfg.Impl {
		return "", fmt.Errorf("%w: selector=%s on-disk=%s at %s",
			ErrImplKindMismatch, p.cfg.Impl, diskImpl, path)
	}
	return diskImpl, nil
}

// magicToImpl maps a 4-byte magic to ImplName. Hardcoded per
// on-disk magic constants in core/storage and core/storage/smartwal.
// Keep in sync when new impls land (caught at T3c matrix test time).
func magicToImpl(magic string) (ImplName, bool) {
	switch magic {
	case "SWBK":
		return ImplWALStore, true
	case "SWAW":
		return ImplSmartWAL, true
	case "SWPW":
		return ImplParallelWALStore, true
	default:
		return "", false
	}
}

// openExisting dispatches to the Open function of impl, the volume's
// resolved implementation.
func (p *DurableProvider) openExisting(volumeID, path string, impl ImplName) (*volHandle, error) {
	var s storage.LogicalStorage
	switch impl {
	case ImplWALStore, ImplWALStoreChunk:
		var ws *storage.WALStore
		var err error
		if p.testWALStoreFactory != nil {
			ws, err = p.testWALStoreFactory(path, false)
		} else {
			ws, err = storage.OpenWALStore(path)
		}
		if err != nil {
			return nil, fmt.Errorf("durable: OpenWALStore %s: %w", path, err)
		}
		p.warnLowHeadroom(volumeID, path, int64(ws.NumBlocks())*int64(ws.BlockSize()))
		// G6 §1.A α: plumb operator-tunable retention into walstore.
		// Zero (pre-G6 default) preserves strict checkpoint-driven
		// recycle.
		// Defer before Recover can publish a completed BASE away.
		if impl == ImplWALStoreChunk {
			ws.DeferBaseRetirementV3()
			// Before Recover: a replayed write reaches the extent only
			// after the chunk layer attaches, so it is marked local.
			ws.HoldExtentDrainUntilChunkLayer()
		}
		if p.cfg.WALRetentionLSNs > 0 {
			ws.SetRecoveryRetentionLSNs(p.cfg.WALRetentionLSNs)
		}
		if p.cfg.WALMultiBlockRecords {
			ws.SetMultiBlockRecords(true)
		}
		if p.cfg.WALRecoveryTestDisableFlusher {
			ws.DisableAutoFlushForRecoveryTest()
		}
		s = ws
	case ImplSmartWAL:
		sw, err := smartwal.OpenStore(path)
		if err != nil {
			return nil, fmt.Errorf("durable: smartwal.OpenStore %s: %w", path, err)
		}
		s = sw
	case ImplParallelWALStore:
		pw, err := parallelwal.OpenStore(path)
		if err != nil {
			return nil, fmt.Errorf("durable: parallelwal.OpenStore %s: %w", path, err)
		}
		s = pw
	default:
		return nil, fmt.Errorf("durable: unknown impl %q", impl)
	}
	return p.wrap(volumeID, path, impl, s)
}

// createFresh dispatches to the Create function of impl, the volume's
// resolved implementation. Requires cfg.NumBlocks to be non-zero
// (can't create without knowing geometry).
func (p *DurableProvider) createFresh(volumeID, path string, impl ImplName) (*volHandle, error) {
	if p.cfg.NumBlocks == 0 {
		return nil, fmt.Errorf("durable: NumBlocks=0; cannot create storage for %s (use open-existing mode)",
			volumeID)
	}
	var s storage.LogicalStorage
	switch impl {
	case ImplWALStore, ImplWALStoreChunk:
		var ws *storage.WALStore
		var err error
		if p.testWALStoreFactory != nil {
			ws, err = p.testWALStoreFactory(path, true)
		} else {
			ws, err = storage.CreateWALStoreWithOptions(path, p.cfg.NumBlocks, p.cfg.BlockSize, storage.WALStoreOptions{ChunkBlocks: p.cfg.ChunkBlocks})
		}
		if err != nil {
			return nil, fmt.Errorf("durable: CreateWALStore %s: %w", path, err)
		}
		if impl == ImplWALStoreChunk {
			if err := initializeLayoutRoot(ws); err != nil {
				_ = ws.Close()
				return nil, err
			}
		}
		// G6 §1.A α (see openExisting comment for semantics).
		// Defer before Recover can publish a completed BASE away.
		if impl == ImplWALStoreChunk {
			ws.DeferBaseRetirementV3()
			// Before Recover: a replayed write reaches the extent only
			// after the chunk layer attaches, so it is marked local.
			ws.HoldExtentDrainUntilChunkLayer()
		}
		if p.cfg.WALRetentionLSNs > 0 {
			ws.SetRecoveryRetentionLSNs(p.cfg.WALRetentionLSNs)
		}
		if p.cfg.WALMultiBlockRecords {
			ws.SetMultiBlockRecords(true)
		}
		if p.cfg.WALRecoveryTestDisableFlusher {
			ws.DisableAutoFlushForRecoveryTest()
		}
		s = ws
	case ImplSmartWAL:
		sw, err := smartwal.CreateStore(path, p.cfg.NumBlocks, p.cfg.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("durable: smartwal.CreateStore %s: %w", path, err)
		}
		s = sw
	case ImplParallelWALStore:
		pw, err := parallelwal.CreateStore(path, p.cfg.NumBlocks, p.cfg.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("durable: parallelwal.CreateStore %s: %w", path, err)
		}
		s = pw
	default:
		return nil, fmt.Errorf("durable: unknown impl %q", impl)
	}
	return p.wrap(volumeID, path, impl, s)
}

// wrap builds a StorageBackend around an opened LogicalStorage
// using the current projection's Identity. Backend starts
// operational=false; the caller flips via RecoverVolume.
func (p *DurableProvider) wrap(volumeID, path string, impl ImplName, s storage.LogicalStorage) (*volHandle, error) {
	proj := p.view.Projection()
	id := frontend.Identity{
		VolumeID:        proj.VolumeID,
		ReplicaID:       proj.ReplicaID,
		Epoch:           proj.Epoch,
		EndpointVersion: proj.EndpointVersion,
	}
	b := NewStorageBackend(s, p.view, id)
	return &volHandle{storage: s, backend: b, path: path, impl: impl}, nil
}

// RecoverVolume runs LogicalStorage.Recover for an open volume
// and flips the backend operational bit based on the outcome.
//
// Must be called AFTER Open. Returns ErrVolumeNotOpen if volumeID
// was never Open'd.
//
// On recovery success: backend.SetOperational(true, "recovered LSN=N").
// On recovery error: backend.SetOperational(false, "recover failed: ...")
// — the error is also returned. Caller decides whether to surface
// as NotReady via /status (read-side), fail startup, or retry.
func (p *DurableProvider) RecoverVolume(ctx context.Context, volumeID string) (RecoveryReport, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	// Close empties p.volumes under the same lock that sets closed, so a
	// handle found here was admitted before Close; Close waits for it.
	chunked := ok && p.cfg.Impl == ImplWALStoreChunk
	if chunked {
		p.inflight.Add(1)
	}
	p.mu.Unlock()
	if !ok {
		return RecoveryReport{VolumeID: volumeID}, ErrVolumeNotOpen
	}
	var gen uint64
	if chunked {
		defer p.inflight.Done()
		h.attachMu.Lock()
		defer h.attachMu.Unlock()
		if h.chunkDetach {
			err := fmt.Errorf("durable: volume %s was detached; I/O stays stopped until a new attach: %w", volumeID, storage.ErrChunkLayerDetached)
			h.backend.SetOperational(false, "chunk detach: I/O stopped until a new attach")
			return RecoveryReport{VolumeID: volumeID, Err: err}, err
		}
		p.mu.Lock()
		h.recoverGen++ // any earlier recovery's authority wait is now stale
		gen = h.recoverGen
		h.startupEnded = 0
		h.startupSuperseded = false
		h.startupIdentityRefused = false
		p.mu.Unlock()
	}
	report := Recover(h.storage, volumeID)
	if report.Err != nil {
		h.backend.setRecoveryFailure(report.Evidence, report.Err)
		return report, report.Err
	}
	if chunked {
		attached, err := p.recoverChunk(ctx, volumeID, h, gen)
		if err != nil {
			h.backend.SetOperational(false, "chunk layer attach failed: "+err.Error())
			report.Err = err
			return report, err
		}
		if !attached {
			// Recovered; I/O stays refused until the layer attaches under
			// the master's assignment (the authority wait flips it).
			report.Evidence += "; chunk layer awaiting master authority"
			h.backend.setChunkAttachPending(report.Evidence)
			return report, nil
		}
		if err := chunkFenced(volumeID, h); err != nil {
			h.backend.SetOperational(false, "chunk layer fenced: "+err.Error())
			report.Err = err
			return report, err
		}
	}
	h.backend.SetOperational(true, report.Evidence)
	return report, nil
}

// Close closes all opened volumes in correct order: backend
// first (marks closed so any racing I/O gets ErrBackendClosed),
// then storage (flushes + releases file handles). Idempotent.
func (p *DurableProvider) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	handles := p.volumes
	p.volumes = map[string]*volHandle{}
	p.mu.Unlock()
	// walstore-chunk: cancel remote attachment and let every admitted
	// recovery settle (it sees closed and publishes nothing) before any
	// store is closed.
	p.closeCancel()
	p.inflight.Wait()

	var firstErr error
	for _, h := range handles {
		p.stopChunkLoop(h)
		_ = h.backend.Close()
		if err := h.storage.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("durable: close storage: %w", err)
		}
		closeHostLock(h.hostLock) // after the store closes
	}
	return firstErr
}

// fileExists returns (true, nil) if path exists as a regular file,
// (false, nil) if missing, (_, err) on other stat errors.
func fileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		return !info.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// chunkSidecars lists the walstore-chunk metadata files beside a volume
// file: everything named <volume file>.chunk* (the fresh marker, the dirty
// bitmap and its deltas, the presence map). Directories are ignored.
func chunkSidecars(path string) ([]string, error) {
	dir, base := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("durable: list %s for chunk metadata: %w", dir, err)
	}
	var out []string
	for _, e := range entries {
		// The owned v3 LSN map belongs to plain walstore as well. Remote
		// chunk identity still has its own presence/fresh/HEAD sidecars.
		if e.Name() == base+".chunkmap" {
			continue
		}
		if !e.IsDir() && strings.HasPrefix(e.Name(), base+".chunk") {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// Compile-time check.
var _ frontend.Provider = (*DurableProvider)(nil)
