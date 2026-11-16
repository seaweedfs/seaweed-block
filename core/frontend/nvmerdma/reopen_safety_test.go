package nvmerdma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

type reopenScriptBackend struct {
	read     func([]byte) (int, error)
	writeN   int
	writeErr error
	writes   atomic.Int32
	closes   atomic.Int32
}

func (b *reopenScriptBackend) Read(_ context.Context, _ int64, p []byte) (int, error) {
	if b.read != nil {
		return b.read(p)
	}
	return len(p), nil
}
func (b *reopenScriptBackend) Write(context.Context, int64, []byte) (int, error) {
	b.writes.Add(1)
	return b.writeN, b.writeErr
}
func (b *reopenScriptBackend) Sync(context.Context) error  { return nil }
func (b *reopenScriptBackend) Identity() frontend.Identity { return frontend.Identity{} }
func (b *reopenScriptBackend) SetOperational(bool, string) {}
func (b *reopenScriptBackend) Close() error                { b.closes.Add(1); return nil }

type reopenScriptProvider struct {
	mu       sync.Mutex
	backends []frontend.Backend
	opens    int
}

func (p *reopenScriptProvider) Open(context.Context, string) (frontend.Backend, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.opens++
	if p.opens > len(p.backends) {
		return nil, fmt.Errorf("unexpected extra open")
	}
	return p.backends[p.opens-1], nil
}
func (p *reopenScriptProvider) count() int { p.mu.Lock(); defer p.mu.Unlock(); return p.opens }

func TestProviderBackendDoesNotReplayAmbiguousWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
		err  error
	}{{"partial-stale", 2, frontend.ErrStalePrimary}, {"transport", 0, io.ErrUnexpectedEOF}, {"partial-deadline", 2, context.DeadlineExceeded}, {"not-ready", 0, frontend.ErrNotReady}} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &reopenScriptBackend{writeN: tc.n, writeErr: tc.err}
			provider := &reopenScriptProvider{backends: []frontend.Backend{backend}}
			b := &providerBackend{provider: provider, volumeID: "v1"}
			n, err := b.Write(context.Background(), 0, []byte("data"))
			if n != tc.n || !errors.Is(err, tc.err) {
				t.Fatalf("outcome changed n%d err%v", n, err)
			}
			if provider.count() != 1 || backend.writes.Load() != 1 || backend.closes.Load() != 0 {
				t.Fatal("ambiguous write replayed or borrowed backend closed")
			}
		})
	}
}
func TestProviderBackendStaleRetryIsBounded(t *testing.T) {
	one := &reopenScriptBackend{writeErr: fmt.Errorf("wrapped: %w", frontend.ErrStalePrimary)}
	two := &reopenScriptBackend{writeErr: frontend.ErrStalePrimary}
	provider := &reopenScriptProvider{backends: []frontend.Backend{one, two}}
	b := &providerBackend{provider: provider, volumeID: "v1"}
	if n, err := b.Write(context.Background(), 0, []byte("data")); n != 0 || !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatal(n, err)
	}
	if provider.count() != 2 || one.writes.Load() != 1 || two.writes.Load() != 1 || one.closes.Load() != 0 || two.closes.Load() != 0 {
		t.Fatal("retry not bounded or borrowed backend closed")
	}
}
func TestProviderBackendLateStaleReadKeepsNewHandle(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	old := &reopenScriptBackend{read: func([]byte) (int, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return 0, frontend.ErrStalePrimary
	}}
	fresh := &reopenScriptBackend{read: func(p []byte) (int, error) { return copy(p, []byte("fresh")), nil }}
	provider := &reopenScriptProvider{backends: []frontend.Backend{old, fresh}}
	b := &providerBackend{provider: provider, volumeID: "v1"}
	var firstErr error
	first := make([]byte, 5)
	go func() { defer close(done); _, firstErr = b.Read(context.Background(), 0, first) }()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("read worker unsettled")
		}
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("old request never entered")
	}
	second := make([]byte, 5)
	if n, err := b.Read(context.Background(), 0, second); err != nil || n != 5 || !bytes.Equal(second, []byte("fresh")) {
		t.Fatal(n, err)
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("old request did not settle")
	}
	if firstErr != nil || !bytes.Equal(first, []byte("fresh")) || provider.count() != 2 || old.closes.Load() != 0 || fresh.closes.Load() != 0 {
		t.Fatal("late refusal evicted the new handle or closed borrowed state", firstErr, provider.count())
	}
}

type reopenProviderFunc func(context.Context, string) (frontend.Backend, error)

func (f reopenProviderFunc) Open(ctx context.Context, v string) (frontend.Backend, error) {
	return f(ctx, v)
}
func TestProviderBackendOpenHasBoundAndHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, done := make(chan bool, 1), make(chan error, 1)
	provider := reopenProviderFunc(func(c context.Context, _ string) (frontend.Backend, error) {
		deadline, ok := c.Deadline()
		entered <- ok && time.Until(deadline) <= 5*time.Second
		<-c.Done()
		return nil, c.Err()
	})
	b := &providerBackend{provider: provider, volumeID: "v1"}
	go func() { _, err := b.Read(ctx, 0, make([]byte, 1)); done <- err }()
	select {
	case bounded := <-entered:
		if !bounded {
			cancel()
			<-done
			t.Fatal("readiness Open has no finite bound")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Open not entered")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("caller cancellation ignored")
	}
}
