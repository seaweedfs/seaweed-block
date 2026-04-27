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
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

// ImplName is the user-facing selector for the LogicalStorage
// implementation. Config sets one of these; Provider picks the
// matching open/create functions.
type ImplName string

const (
	ImplSmartWAL ImplName = "smartwal" // production default per PM direction
	ImplWALStore ImplName = "walstore"
)

// ErrImplKindMismatch is returned by Open when the configured
// selector does not match the on-disk superblock magic. Named so
// tests can errors.Is on it.
var ErrImplKindMismatch = errors.New("durable: impl kind mismatch between selector and on-disk superblock")

// ErrVolumeNotOpen is returned by RecoverVolume / Close when the
// volumeID has not been Open'd.
var ErrVolumeNotOpen = errors.New("durable: volume not opened")

// ProviderConfig configures a DurableProvider.
type ProviderConfig struct {
	// Impl selects which LogicalStorage implementation to open or
	// create. Zero value (""): ImplSmartWAL (production default).
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
}

// DurableProvider is the production frontend.Provider implementation.
type DurableProvider struct {
	cfg  ProviderConfig
	view frontend.ProjectionView

	mu      sync.Mutex
	volumes map[string]*volHandle // keyed by volumeID
	closed  bool
}

type volHandle struct {
	storage storage.LogicalStorage
	backend *StorageBackend
	path    string
}

// NewDurableProvider constructs a provider. view is the ONLY
// source of authority lineage the provider is allowed to consult
// (sketch §9 boundary).
func NewDurableProvider(cfg ProviderConfig, view frontend.ProjectionView) (*DurableProvider, error) {
	if cfg.StorageRoot == "" {
		return nil, fmt.Errorf("durable: ProviderConfig.StorageRoot required")
	}
	if cfg.Impl == "" {
		cfg.Impl = ImplSmartWAL
	}
	if cfg.Impl != ImplSmartWAL && cfg.Impl != ImplWALStore {
		return nil, fmt.Errorf("durable: unknown impl %q (want %q or %q)",
			cfg.Impl, ImplSmartWAL, ImplWALStore)
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
	return &DurableProvider{
		cfg:     cfg,
		view:    view,
		volumes: map[string]*volHandle{},
	}, nil
}

// volumePath returns the on-disk path for a volumeID. Volume IDs
// are used as filenames; callers are responsible for passing
// filesystem-safe strings (the master/adapter layer already does).
func (p *DurableProvider) volumePath(volumeID string) string {
	return filepath.Join(p.cfg.StorageRoot, volumeID+".bin")
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

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("durable: provider closed")
	}
	if h, ok := p.volumes[volumeID]; ok {
		p.mu.Unlock()
		// G5-5: latch the backend's Identity to the now-Healthy
		// projection. If the backend was constructed via EnsureStorage
		// pre-assignment, its captured Identity has Epoch=0 even though
		// VolumeID/ReplicaID came from CLI config at construction.
		// SetIdentity transitions Epoch=0 → live projection's Epoch/EV;
		// subsequent drift still fails closed.
		proj := p.view.Projection()
		log.Printf("durable: dp.Open latching identity from projection volume=%s replica=%s epoch=%d ev=%d (post-waitHealthy)",
			proj.VolumeID, proj.ReplicaID, proj.Epoch, proj.EndpointVersion)
		h.backend.SetIdentity(frontend.Identity{
			VolumeID:        proj.VolumeID,
			ReplicaID:       proj.ReplicaID,
			Epoch:           proj.Epoch,
			EndpointVersion: proj.EndpointVersion,
		})
		return h.backend, nil
	}
	p.mu.Unlock()

	h, err := p.openOrCreate(volumeID)
	if err != nil {
		return nil, err
	}

	// Check-then-insert: another goroutine may have raced us.
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.volumes[volumeID]; ok {
		// Race: close the handle we just opened; return the winner.
		_ = h.storage.Close()
		return existing.backend, nil
	}
	p.volumes[volumeID] = h
	return h.backend, nil
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
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("durable: provider closed")
	}
	if h, ok := p.volumes[volumeID]; ok {
		p.mu.Unlock()
		return h.storage, nil
	}
	p.mu.Unlock()

	h, err := p.openOrCreate(volumeID)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.volumes[volumeID]; ok {
		_ = h.storage.Close()
		return existing.storage, nil
	}
	p.volumes[volumeID] = h
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

	if exists {
		if err := p.verifyImplOnDisk(path); err != nil {
			return nil, err
		}
		return p.openExisting(volumeID, path)
	}
	return p.createFresh(volumeID, path)
}

// verifyImplOnDisk peeks the 4-byte magic at offset 0 and returns
// ErrImplKindMismatch if it doesn't match the configured selector.
// Runs BEFORE the storage-level Open so the error message is
// selector-aware instead of cryptic magic-mismatch.
func (p *DurableProvider) verifyImplOnDisk(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("durable: open for magic peek: %w", err)
	}
	defer f.Close()
	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		return fmt.Errorf("durable: read magic: %w", err)
	}
	diskImpl, ok := magicToImpl(string(magic))
	if !ok {
		return fmt.Errorf("%w: unrecognized magic %q at %s",
			ErrImplKindMismatch, string(magic), path)
	}
	if diskImpl != p.cfg.Impl {
		return fmt.Errorf("%w: selector=%s on-disk=%s at %s",
			ErrImplKindMismatch, p.cfg.Impl, diskImpl, path)
	}
	return nil
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
	default:
		return "", false
	}
}

// openExisting dispatches to the selector-matching Open function.
func (p *DurableProvider) openExisting(volumeID, path string) (*volHandle, error) {
	var s storage.LogicalStorage
	switch p.cfg.Impl {
	case ImplWALStore:
		ws, err := storage.OpenWALStore(path)
		if err != nil {
			return nil, fmt.Errorf("durable: OpenWALStore %s: %w", path, err)
		}
		s = ws
	case ImplSmartWAL:
		sw, err := smartwal.OpenStore(path)
		if err != nil {
			return nil, fmt.Errorf("durable: smartwal.OpenStore %s: %w", path, err)
		}
		s = sw
	default:
		return nil, fmt.Errorf("durable: unknown impl %q", p.cfg.Impl)
	}
	return p.wrap(volumeID, path, s)
}

// createFresh dispatches to the selector-matching Create function.
// Requires cfg.NumBlocks to be non-zero (can't create without
// knowing geometry).
func (p *DurableProvider) createFresh(volumeID, path string) (*volHandle, error) {
	if p.cfg.NumBlocks == 0 {
		return nil, fmt.Errorf("durable: NumBlocks=0; cannot create storage for %s (use open-existing mode)",
			volumeID)
	}
	var s storage.LogicalStorage
	switch p.cfg.Impl {
	case ImplWALStore:
		ws, err := storage.CreateWALStore(path, p.cfg.NumBlocks, p.cfg.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("durable: CreateWALStore %s: %w", path, err)
		}
		s = ws
	case ImplSmartWAL:
		sw, err := smartwal.CreateStore(path, p.cfg.NumBlocks, p.cfg.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("durable: smartwal.CreateStore %s: %w", path, err)
		}
		s = sw
	default:
		return nil, fmt.Errorf("durable: unknown impl %q", p.cfg.Impl)
	}
	return p.wrap(volumeID, path, s)
}

// wrap builds a StorageBackend around an opened LogicalStorage
// using the current projection's Identity. Backend starts
// operational=false; the caller flips via RecoverVolume.
func (p *DurableProvider) wrap(volumeID, path string, s storage.LogicalStorage) (*volHandle, error) {
	proj := p.view.Projection()
	id := frontend.Identity{
		VolumeID:        proj.VolumeID,
		ReplicaID:       proj.ReplicaID,
		Epoch:           proj.Epoch,
		EndpointVersion: proj.EndpointVersion,
	}
	b := NewStorageBackend(s, p.view, id)
	return &volHandle{storage: s, backend: b, path: path}, nil
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
	p.mu.Unlock()
	if !ok {
		return RecoveryReport{VolumeID: volumeID}, ErrVolumeNotOpen
	}
	report := Recover(h.storage, volumeID)
	if report.Err != nil {
		h.backend.SetOperational(false, report.Evidence)
		return report, report.Err
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

	var firstErr error
	for _, h := range handles {
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

// Compile-time check.
var _ frontend.Provider = (*DurableProvider)(nil)
