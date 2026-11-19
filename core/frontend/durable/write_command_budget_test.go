package durable_test

import (
	"context"
	"errors"
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
