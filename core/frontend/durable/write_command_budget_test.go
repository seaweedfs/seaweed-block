package durable_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type deadlineStorage struct {
	storage.LogicalStorage
	deadlines   []time.Time
	legacyCalls int
}

func (s *deadlineStorage) Write(lba uint32, p []byte) (uint64, error) {
	s.legacyCalls++
	return s.LogicalStorage.Write(lba, p)
}
func (s *deadlineStorage) WriteBatch(lba uint32, blocks [][]byte) ([]uint64, error) {
	s.legacyCalls++
	return nil, errors.New("contextless batch called")
}
func (s *deadlineStorage) WriteContext(ctx context.Context, lba uint32, p []byte) (uint64, error) {
	deadline, _ := ctx.Deadline()
	s.deadlines = append(s.deadlines, deadline)
	return s.LogicalStorage.Write(lba, p)
}
func (s *deadlineStorage) WriteBatchContext(ctx context.Context, lba uint32, blocks [][]byte) ([]uint64, error) {
	deadline, _ := ctx.Deadline()
	s.deadlines = append(s.deadlines, deadline)
	var lsns []uint64
	for i, p := range blocks {
		lsn, err := s.LogicalStorage.Write(lba+uint32(i), p)
		if err != nil {
			return lsns, err
		}
		lsns = append(lsns, lsn)
	}
	return lsns, nil
}
func TestStorageBackendWriteKeepsOneDeadlineAcrossBatches(t *testing.T) {
	_, view, base := newTestBackend(t, logicalStorageFactories()[0], 160, 4096)
	wrapped := &deadlineStorage{LogicalStorage: base}
	b := durable.NewStorageBackend(wrapped, view, frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1})
	b.SetOperational(true, "test")
	want := time.Now().Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), want)
	defer cancel()
	if n, err := b.Write(ctx, 0, make([]byte, 130*4096)); err != nil || n != 130*4096 {
		t.Fatalf("write %d %v", n, err)
	}
	if _, err := b.Write(ctx, 130*4096, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if wrapped.legacyCalls != 0 || len(wrapped.deadlines) != 4 {
		t.Fatalf("dispatch legacy=%d deadlines=%v", wrapped.legacyCalls, wrapped.deadlines)
	}
	for _, got := range wrapped.deadlines {
		if !got.Equal(want) {
			t.Fatalf("deadline reset %v want%v", got, want)
		}
	}
}
func TestStorageBackendExpiredWriteDoesNotAppend(t *testing.T) {
	b, _, s := newTestBackend(t, logicalStorageFactories()[0], 16, 4096)
	before := s.NextLSN()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	for _, size := range []int{4096, 8192} {
		n, err := b.Write(ctx, 0, make([]byte, size))
		if n != 0 || !errors.Is(err, frontend.ErrBackendBusy) || s.NextLSN() != before {
			t.Fatalf("expired write n=%d err=%v next=%d", n, err, s.NextLSN())
		}
	}
}

type deadlineAckStore struct{ storage.LogicalStorage }

func (s *deadlineAckStore) WriteContext(ctx context.Context, lba uint32, p []byte) (uint64, error) {
	<-ctx.Done()
	return s.LogicalStorage.Write(lba, p)
}
func (s *deadlineAckStore) WriteBatch(lba uint32, blocks [][]byte) ([]uint64, error) {
	var lsns []uint64
	for i, p := range blocks {
		lsn, err := s.LogicalStorage.Write(lba+uint32(i), p)
		if err != nil {
			return lsns, err
		}
		lsns = append(lsns, lsn)
	}
	return lsns, nil
}
func (s *deadlineAckStore) WriteBatchContext(ctx context.Context, lba uint32, blocks [][]byte) ([]uint64, error) {
	<-ctx.Done()
	return s.WriteBatch(lba, blocks)
}
func TestStorageBackendRequiredObserverDeadlineStaysRetryable(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, physical := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%t/physical=%t", batch, physical), func(t *testing.T) {
				id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
				s := &deadlineAckStore{storage.NewBlockStore(8, 4096)}
				b := durable.NewStorageBackend(s, newStubView(healthyProj(id)), id)
				b.SetOperational(true, "test")
				injected := errors.New("physical observer failure")
				outcome := func(ctx context.Context) error {
					if physical {
						return errors.Join(injected, ctx.Err())
					}
					return fmt.Errorf("ack wait: %w", ctx.Err())
				}
				b.SetWriteObserver(batchWriteObserverFunc{writeObserverFunc: writeObserverFunc{observe: func(ctx context.Context, _ uint32, _ uint64, _ []byte) error { return outcome(ctx) }}, observeBatch: func(ctx context.Context, _ uint32, _ []uint64, _ [][]byte) error { return outcome(ctx) }})
				b.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				defer cancel()
				size := 4096
				if batch {
					size *= 2
				}
				n, err := b.Write(ctx, 0, make([]byte, size))
				if n != size || !errors.Is(err, durable.ErrReplicationAckUnavailable) {
					t.Fatalf("committed error n=%d err=%v", n, err)
				}
				if physical {
					if !errors.Is(err, injected) || errors.Is(err, frontend.ErrBackendBusy) {
						t.Fatalf("physical fault masked: %v", err)
					}
				} else {
					if !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("observer deadline lost retryability: %v", err)
					}
				}
			})
		}
	}
}
