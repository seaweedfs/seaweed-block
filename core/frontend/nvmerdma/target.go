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
	opening    *backendOpen
}

// backendOpen publishes its result by closing done. Only the opener writes it.
type backendOpen struct {
	done       chan struct{}
	backend    frontend.Backend
	generation uint64
	err        error
}

func (b *providerBackend) get(ctx context.Context) (frontend.Backend, uint64, error) {
	// Start the readiness budget before coordination, never after waiting for a lock.
	openCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := openCtx.Err(); err != nil {
		return nil, 0, err
	}
	b.mu.Lock()
	if b.backend != nil {
		backend, generation := b.backend, b.generation
		b.mu.Unlock()
		return backend, generation, nil
	}
	flight := b.opening
	if flight == nil {
		flight = &backendOpen{done: make(chan struct{})}
		b.opening = flight
		// Provider.Open may wait for readiness. Never hold the cache lock across it.
		// One flight owns that wait; other callers retain their own cancellation.
		go func() {
			backend, err := b.provider.Open(openCtx, b.volumeID)
			b.mu.Lock()
			if err == nil {
				b.backend = backend
				b.generation++
				flight.backend = backend
				flight.generation = b.generation
			} else {
				flight.err = fmt.Errorf("nvmerdma: open backend: %w", err)
			}
			b.opening = nil
			close(flight.done)
			b.mu.Unlock()
		}()
	}
	b.mu.Unlock()
	select {
	case <-openCtx.Done():
		return nil, 0, openCtx.Err()
	case <-flight.done:
		if err := openCtx.Err(); err != nil {
			return nil, 0, err
		}
		return flight.backend, flight.generation, flight.err
	}
}

func (b *providerBackend) Read(ctx context.Context, offset int64, p []byte) (int, error) {
	readinessCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	backend, generation, err := b.get(readinessCtx)
	if err != nil {
		return 0, err
	}
	n, err := backend.Read(ctx, offset, p)
	if n != 0 || !errors.Is(err, frontend.ErrStalePrimary) {
		return n, err
	}
	backend, err = b.reopen(readinessCtx, generation)
	if err != nil {
		return 0, err
	}
	return backend.Read(ctx, offset, p)
}

func (b *providerBackend) Write(ctx context.Context, offset int64, p []byte) (int, error) {
	readinessCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	backend, generation, err := b.get(readinessCtx)
	if err != nil {
		return 0, err
	}
	n, err := backend.Write(ctx, offset, p)
	if n != 0 || !errors.Is(err, frontend.ErrStalePrimary) {
		return n, err
	}
	backend, err = b.reopen(readinessCtx, generation)
	if err != nil {
		return 0, err
	}
	return backend.Write(ctx, offset, p)
}

func (b *providerBackend) Sync(ctx context.Context) error {
	readinessCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	backend, generation, err := b.get(readinessCtx)
	if err != nil {
		return err
	}
	err = backend.Sync(ctx)
	if !errors.Is(err, frontend.ErrStalePrimary) {
		return err
	}
	backend, err = b.reopen(readinessCtx, generation)
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
