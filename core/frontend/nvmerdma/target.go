package nvmerdma

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

type TargetConfig struct {
	Listen     string
	SubsysNQN  string
	VolumeID   string
	NSID       uint32
	BlockSize  uint32
	VolumeSize uint64
	Provider   frontend.Provider
}

type Target struct {
	cfg TargetConfig

	mu       sync.Mutex
	platform platformTarget
}

type platformTarget interface {
	Close() error
}

func NewTarget(cfg TargetConfig) *Target {
	if cfg.Provider == nil {
		panic("nvmerdma: NewTarget: Provider required")
	}
	return &Target{cfg: cfg}
}

func validateSubsystemNQN(nqn string) error {
	if nqn == "" || nqn == "." || nqn == ".." || strings.ContainsAny(nqn, `/\\`) {
		return fmt.Errorf("nvmerdma: invalid subsystem NQN %q", nqn)
	}
	return nil
}

func (t *Target) Start() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.platform != nil {
		return "", errors.New("nvmerdma: target already started")
	}
	backend := &providerBackend{provider: t.cfg.Provider, volumeID: t.cfg.VolumeID}
	platform, addr, err := startPlatformTarget(t.cfg, backend)
	if err != nil {
		return "", err
	}
	t.platform = platform
	return addr, nil
}

func (t *Target) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	platform := t.platform
	t.platform = nil
	t.mu.Unlock()
	if platform == nil {
		return nil
	}
	return platform.Close()
}

type providerBackend struct {
	provider frontend.Provider
	volumeID string

	mu         sync.Mutex
	backend    frontend.Backend
	generation uint64
}

func (b *providerBackend) get(ctx context.Context) (frontend.Backend, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.backend != nil {
		return b.backend, b.generation, nil
	}
	// NBD supplies a target-lifetime context, not a per-request deadline.
	// Bound readiness waits even while this target is demoted.
	openCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	backend, err := b.provider.Open(openCtx, b.volumeID)
	if err != nil {
		return nil, 0, fmt.Errorf("nvmerdma: open backend: %w", err)
	}
	b.backend = backend
	b.generation++
	return backend, b.generation, nil
}

func (b *providerBackend) Read(ctx context.Context, offset int64, p []byte) (int, error) {
	backend, generation, err := b.get(ctx)
	if err != nil {
		return 0, err
	}
	n, err := backend.Read(ctx, offset, p)
	if n != 0 || !errors.Is(err, frontend.ErrStalePrimary) {
		return n, err
	}
	backend, err = b.reopen(ctx, generation)
	if err != nil {
		return 0, err
	}
	return backend.Read(ctx, offset, p)
}

func (b *providerBackend) Write(ctx context.Context, offset int64, p []byte) (int, error) {
	backend, generation, err := b.get(ctx)
	if err != nil {
		return 0, err
	}
	n, err := backend.Write(ctx, offset, p)
	if n != 0 || !errors.Is(err, frontend.ErrStalePrimary) {
		return n, err
	}
	backend, err = b.reopen(ctx, generation)
	if err != nil {
		return 0, err
	}
	return backend.Write(ctx, offset, p)
}

func (b *providerBackend) Sync(ctx context.Context) error {
	backend, generation, err := b.get(ctx)
	if err != nil {
		return err
	}
	err = backend.Sync(ctx)
	if !errors.Is(err, frontend.ErrStalePrimary) {
		return err
	}
	backend, err = b.reopen(ctx, generation)
	if err != nil {
		return err
	}
	return backend.Sync(ctx)
}

// Stale-primary is a pre-effect lineage refusal. Retry once through Provider's
// current readiness/identity gate; never close the borrowed backend. Generation
// matching prevents a delayed old refusal from evicting a newer open handle.
func (b *providerBackend) reopen(ctx context.Context, generation uint64) (frontend.Backend, error) {
	b.mu.Lock()
	if b.generation == generation {
		b.backend = nil
	}
	b.mu.Unlock()
	backend, _, err := b.get(ctx)
	return backend, err
}
