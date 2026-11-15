package ublk

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type failingProvider struct{ err error }

func (p failingProvider) Open(context.Context, string) (frontend.Backend, error) { return nil, p.err }

// Through the provider glue the contract still holds: requests reach the
// opened backend, and a provider that cannot open the volume fails every
// request (never acknowledged).
func TestTarget_ProviderBackendCarriesTheContract(t *testing.T) {
	rb := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := handler{backend: &providerBackend{provider: testback.NewStaticProvider(rb), volumeID: "v1"}, size: testSize}
	data := bytes.Repeat([]byte{7}, 4096)
	if got := h.handle(context.Background(), request{op: ioOpWrite, flags: ioFlagFUA, nrSectors: 8}, data); got != 4096 {
		t.Fatalf("write through the provider: %d", got)
	}
	if rb.WriteCount() != 1 || rb.SyncCount() != 1 {
		t.Fatalf("provider backend: %d writes %d syncs", rb.WriteCount(), rb.SyncCount())
	}

	down := handler{backend: &providerBackend{provider: failingProvider{err: errors.New("not healthy")}, volumeID: "v1"}, size: testSize}
	for _, r := range []request{{op: ioOpRead, nrSectors: 8}, {op: ioOpWrite, nrSectors: 8}, {op: ioOpFlush}} {
		if got := down.handle(context.Background(), r, make([]byte, 4096)); got != -int32(syscall.EIO) {
			t.Fatalf("op %d with no backend: %d, want -EIO", r.op, got)
		}
	}
}

func TestConfig_Validate(t *testing.T) {
	ok := Config{Backend: &failingBackend{}, Size: 1 << 30, BlockSize: 4096}
	if err := ok.validate(); err != nil || ok.Queues != DefaultQueues || ok.QueueDepth != DefaultQueueDepth || ok.MaxIOBytes != DefaultMaxIOBytes {
		t.Fatalf("defaults: %+v %v", ok, err)
	}
	for name, c := range map[string]Config{
		"no backend":   {Size: 4096, BlockSize: 4096},
		"block size":   {Backend: &failingBackend{}, Size: 4096, BlockSize: 1000},
		"size":         {Backend: &failingBackend{}, Size: 4097, BlockSize: 4096},
		"queues":       {Backend: &failingBackend{}, Size: 4096, BlockSize: 4096, Queues: 65},
		"queue depth":  {Backend: &failingBackend{}, Size: 4096, BlockSize: 4096, QueueDepth: 4097},
		"max io bytes": {Backend: &failingBackend{}, Size: 4096, BlockSize: 4096, MaxIOBytes: 1000},
	} {
		if err := c.validate(); err == nil {
			t.Errorf("%s: accepted %+v", name, c)
		}
	}
}

type fakeDevice struct {
	closes  atomic.Int32
	err     error
	release chan struct{}
}

func (d *fakeDevice) Path() string { return "/dev/ublkb9" }
func (d *fakeDevice) Stats() Stats { return Stats{} }
func (d *fakeDevice) Close() error {
	d.closes.Add(1)
	<-d.release
	return d.err
}

// Close is idempotent: concurrent and repeated callers all get the first
// close's result (never a nil while it is pending or after it failed),
// and the device is closed once.
func TestTarget_CloseGivesEveryCallerTheSameResult(t *testing.T) {
	dev := &fakeDevice{err: errors.New("queue 0 did not stop"), release: make(chan struct{})}
	tg := &Target{cfg: TargetConfig{Provider: testback.NewStaticProvider(nil)}, device: dev}
	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() { results <- tg.Close() }()
	}
	time.Sleep(50 * time.Millisecond)
	close(dev.release)
	for i := 0; i < 3; i++ {
		select {
		case err := <-results:
			if err == nil || err.Error() != dev.err.Error() {
				t.Fatalf("concurrent Close %d returned %v, want %v", i, err, dev.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a concurrent Close did not return")
		}
	}
	if err := tg.Close(); err == nil || err.Error() != dev.err.Error() {
		t.Fatalf("repeated Close returned %v, want %v", err, dev.err)
	}
	if n := dev.closes.Load(); n != 1 {
		t.Fatalf("device closed %d times, want once", n)
	}
}
