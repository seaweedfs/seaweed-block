package nvmerdma

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

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

	mu      sync.Mutex
	backend frontend.Backend
}

func (b *providerBackend) get(ctx context.Context) (frontend.Backend, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.backend != nil {
		return b.backend, nil
	}
	backend, err := b.provider.Open(ctx, b.volumeID)
	if err != nil {
		return nil, fmt.Errorf("nvmerdma: open backend: %w", err)
	}
	b.backend = backend
	return backend, nil
}

func (b *providerBackend) Read(ctx context.Context, offset int64, p []byte) (int, error) {
	backend, err := b.get(ctx)
	if err != nil {
		return 0, err
	}
	return backend.Read(ctx, offset, p)
}

func (b *providerBackend) Write(ctx context.Context, offset int64, p []byte) (int, error) {
	backend, err := b.get(ctx)
	if err != nil {
		return 0, err
	}
	return backend.Write(ctx, offset, p)
}

func (b *providerBackend) Sync(ctx context.Context) error {
	backend, err := b.get(ctx)
	if err != nil {
		return err
	}
	return backend.Sync(ctx)
}
