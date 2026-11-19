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
	// ProviderConfig.ChunkRemoteFor. Opt-in proof of concept.
	ImplWALStoreChunk ImplName = "walstore-chunk"
)

// defaultChunkBlocks is 4 MiB chunks of 4 KiB blocks.
const defaultChunkBlocks = 1024

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

	// ChunkRemoteFor returns the remote that holds volumeID's chunks and
	// HEAD. Required for ImplWALStoreChunk, ignored otherwise.
	ChunkRemoteFor func(volumeID string) (storage.ChunkRemote, error)
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
	// ChunkReadMissTimeout bounds one remote fetch of an absent block.
	// Zero: the storage default.
	ChunkReadMissTimeout time.Duration
	// ChunkCaptureInterval: an automatic cut is due once this long has
	// passed since the volume's last successful cut (of any kind). Zero
	// disables the timer.
	ChunkCaptureInterval time.Duration
	// DetachFreezeTimeout bounds how long a failing DetachVolume keeps guest
	// I/O stopped before it resumes or settles: the drain and final cut get
	// three quarters of it, the settlement of a failed cut the rest (the
	// caller's context can only shorten the cut part). Zero: 20 s, below the
	// host's 30 s I/O timeout; a freeze past the 5 s target is logged as an
	// alarm. Cooperative: a syscall that never returns is not bounded.
	DetachFreezeTimeout time.Duration
	// ChunkDirtyThreshold is the bound on one cut's barrier copy, in
	// chunks. It never triggers a cut: the remote holds snapshots, and
	// cuts are published only by CaptureVolume, MakeVolumeRemoteDurable
	// (snapshot, detach) and the timer. Zero: unbounded.
	ChunkDirtyThreshold int
	// ChunkUploadConcurrency bounds remote uploads; zero defaults to 8.
	ChunkUploadConcurrency int
}

// DurableProvider is the production frontend.Provider implementation.
type DurableProvider struct {
	cfg  ProviderConfig
	view frontend.ProjectionView

	mu      sync.Mutex
	volumes map[string]*volHandle  // keyed by volumeID
	opening map[string]*sync.Mutex // keyed by volumeID; serializes open/create
	closed  bool

	// testAfterChunkResolve, test-only, runs in CutVolume right after the
	// volume's handle and layer are resolved.
	testAfterChunkResolve func()

	// testBeforeChunkFreshRetire holds the real post-presence/pre-marker boundary.
	testBeforeChunkFreshRetire func() error

	// testWALStoreFactory selects a dark store only in provider caller controls.
	// No production option, flag or environment variable installs it.
	testWALStoreFactory func(path string, create bool) (*storage.WALStore, error)

	// walstore-chunk: closeCtx ends with Close and cancels in-flight
	// remote attachment; inflight counts RecoverVolume calls admitted
	// before Close, which Close waits for before closing any store.
	closeCtx    context.Context
	closeCancel context.CancelFunc
	inflight    sync.WaitGroup
}

type volHandle struct {
	storage storage.LogicalStorage
	backend *StorageBackend
	path    string
	impl    ImplName // the resolved implementation, never ImplAuto

	// ImplWALStoreChunk. fresh: the local volume is new and starts from
	// the remote HEAD. It mirrors the durable fresh marker beside the
	// volume file, written before the file is created and removed only
	// after the first attach succeeded, so an interrupted initialization
	// is still fresh after a restart.
	fresh    bool
	attachMu sync.Mutex // serializes recovery and attach of this volume
	// recoverGen numbers this volume's RecoverVolume calls (written under
	// attachMu and p.mu). waitGen is the generation of the live authority
	// wait, 0 if none: a recovered volume whose layer waits for master
	// authority. A wait only acts while its generation is still the
	// latest recovery's.
	recoverGen uint64
	waitGen    uint64
	chunk      storage.ChunkLayer // after RecoverVolume
	// Private until the owner commit succeeds; retained for attach retry.
	chunkPending storage.ChunkLayer
	// chunkDetach is set (under attachMu) when DetachVolume starts and
	// never cleared in this process: frontend I/O stays stopped, through
	// any later RecoverVolume, until a new lifetime attaches the volume.
	chunkDetach bool
	chunkStop   context.CancelFunc
	chunkDone   chan struct{}
}

// VolumeStatus is a read-only diagnostic snapshot of one durable volume
// handle. It intentionally exposes lineage and readiness facts without giving
// callers a control surface.
type VolumeStatus struct {
	VolumeID        string
	Path            string
	Impl            string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	Latched         bool
	Operational     bool
	FrontierKnown   bool
	DurableLSN      uint64
	RetainedLSN     uint64
	HeadLSN         uint64
	Evidence        string
	Closed          bool
	WriteProfile    WriteProfileStatus
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
	Attached          bool
	AwaitingAuthority bool
	Fenced            bool
	RemoteLSN         uint64
	Stats             storage.ChunkLayerStats
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
	if cfg.Impl == ImplWALStoreChunk && cfg.ChunkRemoteFor == nil {
		return nil, fmt.Errorf("durable: impl %q requires ProviderConfig.ChunkRemoteFor", ImplWALStoreChunk)
	}
	if cfg.ChunkBlocks == 0 {
		cfg.ChunkBlocks = defaultChunkBlocks
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
		cfg:         cfg,
		view:        view,
		volumes:     map[string]*volHandle{},
		opening:     map[string]*sync.Mutex{},
		closeCtx:    closeCtx,
		closeCancel: closeCancel,
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

	h, err := p.openOrCreate(volumeID)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = h.storage.Close()
		return nil, fmt.Errorf("durable: provider closed")
	}
	if existing, ok := p.volumes[volumeID]; ok {
		_ = h.storage.Close()
		return existing, nil
	}
	p.volumes[volumeID] = h
	return h, nil
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
		layer    storage.ChunkLayer
		awaiting bool
	}
	p.mu.Lock()
	handles := make(map[string]*volHandle, len(p.volumes))
	chunks := map[string]chunkView{}
	for volumeID, h := range p.volumes {
		handles[volumeID] = h
		if p.cfg.Impl == ImplWALStoreChunk {
			chunks[volumeID] = chunkView{layer: h.chunk, awaiting: h.waitGen != 0}
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
	if p.cfg.Impl == ImplAuto {
		// A walstore-chunk volume shares walstore's on-disk format, and its
		// authoritative bytes may live only in the remote HEAD. Opened as
		// plain walstore it would serve local (possibly absent) extents, so
		// auto refuses any root carrying chunk metadata; it never deletes
		// or converts it.
		if side, err := chunkSidecars(path); err != nil {
			return nil, err
		} else if len(side) > 0 {
			return nil, fmt.Errorf("%w: %s is a chunk-managed volume (%s present); auto cannot open it: set --durable-impl=walstore-chunk with its remote configuration",
				ErrImplKindMismatch, path, strings.Join(side, ", "))
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
		if h.fresh, err = fileExists(chunkFreshMarker(path)); err != nil {
			_ = h.storage.Close()
			return nil, fmt.Errorf("durable: stat fresh marker: %w", err)
		}
		return h, nil
	}
	if p.cfg.Impl == ImplWALStoreChunk {
		// Durable before the volume file exists: a crash anywhere between
		// here and the first successful attach leaves a fresh volume.
		if err := writeChunkFreshMarker(path); err != nil {
			return nil, err
		}
	}
	impl := p.cfg.Impl
	if impl == ImplAuto {
		impl = ImplWALStore
	}
	h, err := p.createFresh(volumeID, path, impl)
	if err == nil && p.cfg.Impl == ImplWALStoreChunk {
		h.fresh = true
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
		// G6 §1.A α: plumb operator-tunable retention into walstore.
		// Zero (pre-G6 default) preserves strict checkpoint-driven
		// recycle.
		// Defer before Recover can publish a completed BASE away.
		if impl == ImplWALStoreChunk && p.cfg.ChunkRemoteFor != nil {
			ws.DeferBaseRetirementV3()
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
		// G6 §1.A α (see openExisting comment for semantics).
		// Defer before Recover can publish a completed BASE away.
		if impl == ImplWALStoreChunk && p.cfg.ChunkRemoteFor != nil {
			ws.DeferBaseRetirementV3()
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
		p.mu.Unlock()
	}
	report := Recover(h.storage, volumeID)
	if report.Err != nil {
		h.backend.SetOperational(false, report.Evidence)
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
			h.backend.SetOperational(false, report.Evidence)
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
