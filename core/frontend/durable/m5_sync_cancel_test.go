package durable_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type cancelSyncProxy struct {
	entered chan struct{}
	exited  atomic.Int32
	syncs   atomic.Int32
}

func (p *cancelSyncProxy) Write(ctx context.Context, _ int64, _ []byte) (int, error) {
	p.entered <- struct{}{}
	<-ctx.Done()
	p.exited.Add(1)
	return 0, ctx.Err()
}
func (p *cancelSyncProxy) Sync(context.Context) error { p.syncs.Add(1); return nil }

func TestConcurrentSyncProxyCancellationJoinsEveryWorker(t *testing.T) {
	const workers = 8
	backend := &cancelSyncProxy{entered: make(chan struct{}, workers)}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := runConcurrentSyncProxy(ctx, backend, workers, 100000); result <- err }()
	started := time.NewTimer(time.Second)
	defer started.Stop()
	for i := 0; i < workers; i++ {
		select {
		case <-backend.entered:
		case <-started.C:
			t.Fatal("workers not started")
		}
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join workers")
	}
	if backend.exited.Load() != workers || backend.syncs.Load() != 0 {
		t.Fatalf("abandoned workers or sync after cancellation: exited=%d syncs=%d", backend.exited.Load(), backend.syncs.Load())
	}
}
