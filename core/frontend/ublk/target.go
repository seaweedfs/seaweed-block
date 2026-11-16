package ublk

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// TargetConfig wires a ublk device to a volume through a Provider.
type TargetConfig struct {
	VolumeID   string
	Provider   frontend.Provider
	DevID      int
	BlockSize  uint32
	VolumeSize uint64
	Queues     int
	QueueDepth int
}

// Target serves one volume as /dev/ublkbN. The backend is opened from
// the Provider on first I/O, as the NVMe-RDMA target does.
type Target struct {
	cfg TargetConfig

	mu     sync.Mutex
	device targetDevice

	closeOnce sync.Once
	closeErr  error
	closed    bool // under mu: Close was called
}

// targetDevice is what Target needs from a running device.
type targetDevice interface {
	Path() string
	Stats() Stats
	Close() error
}

// NewTarget returns an unstarted target.
func NewTarget(cfg TargetConfig) *Target {
	if cfg.Provider == nil {
		panic("ublk: NewTarget: Provider required")
	}
	return &Target{cfg: cfg}
}

// Start creates the device and returns its path.
func (t *Target) Start() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return "", errors.New("ublk: target closed; a closed target never starts a device")
	}
	if t.device != nil {
		return "", errors.New("ublk: target already started")
	}
	d, err := Start(Config{
		Backend:    &providerBackend{provider: t.cfg.Provider, volumeID: t.cfg.VolumeID},
		Size:       t.cfg.VolumeSize,
		BlockSize:  t.cfg.BlockSize,
		DevID:      t.cfg.DevID,
		Queues:     t.cfg.Queues,
		QueueDepth: t.cfg.QueueDepth,
	})
	if err != nil {
		return "", err
	}
	t.device = d
	return d.Path(), nil
}

// Stats returns the device's request counters (zero before Start).
func (t *Target) Stats() Stats {
	t.mu.Lock()
	d := t.device
	t.mu.Unlock()
	if d == nil {
		return Stats{}
	}
	return d.Stats()
}

// Close stops and deletes the device. It is idempotent: the device is
// closed once, and every caller, concurrent or later, gets that result.
func (t *Target) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	t.closed = true // a later Start refuses; a running Start finishes first (it holds mu)
	t.mu.Unlock()
	t.closeOnce.Do(func() {
		t.mu.Lock()
		d := t.device
		t.mu.Unlock()
		if d != nil {
			t.closeErr = d.Close()
		}
	})
	return t.closeErr
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
		return nil, fmt.Errorf("ublk: open backend: %w", err)
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
