package durable_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// partialFaultStore commits the first block of a batch and returns that prefix
// together with a physical error (the shape parallelwal and smartwal return),
// after the command's deadline has passed.
type partialFaultStore struct {
	storage.LogicalStorage
	fault error
}

func (s *partialFaultStore) WriteBatch(lba uint32, blocks [][]byte) ([]uint64, error) {
	lsn, err := s.LogicalStorage.Write(lba, blocks[0])
	if err != nil {
		return nil, err
	}
	return []uint64{lsn}, s.fault
}
func (s *partialFaultStore) WriteBatchContext(ctx context.Context, lba uint32, blocks [][]byte) ([]uint64, error) {
	<-ctx.Done()
	return s.WriteBatch(lba, blocks)
}

// #252 P1-3: a committed prefix plus a physical storage error, then a required
// batch observer that reports a pure expired deadline: the physical fault must
// stay in the error (terminal), never be reclassified as retryable busy.
func TestStorageBackendPartialBatchPhysicalFaultSurvivesObserverDeadline(t *testing.T) {
	for _, batchObserver := range []bool{true, false} {
		name := map[bool]string{true: "batch-observer", false: "per-block-observer"}[batchObserver]
		t.Run(name, func(t *testing.T) {
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			physical := errors.New("injected physical WAL append failure")
			s := &partialFaultStore{LogicalStorage: storage.NewBlockStore(8, 4096), fault: physical}
			b := durable.NewStorageBackend(s, newStubView(healthyProj(id)), id)
			b.SetOperational(true, "test")
			deadlineOnly := func(ctx context.Context) error { return ctx.Err() }
			obs := writeObserverFunc{observe: func(ctx context.Context, _ uint32, _ uint64, _ []byte) error { return deadlineOnly(ctx) }}
			if batchObserver {
				b.SetWriteObserver(batchWriteObserverFunc{writeObserverFunc: obs, observeBatch: func(ctx context.Context, _ uint32, _ []uint64, _ [][]byte) error { return deadlineOnly(ctx) }})
			} else {
				b.SetWriteObserver(obs)
				b.SetWriteAckPolicy(durable.WriteAckBestEffort) // the per-block path is taken without a batch observer only off the required policy
			}
			if batchObserver {
				b.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			data := append(bytes.Repeat([]byte{0xA1}, 4096), bytes.Repeat([]byte{0xB2}, 4096)...)
			n, err := b.Write(ctx, 0, data)
			if n != 4096 {
				t.Fatalf("committed bytes %d, want exactly the 4096-byte prefix (err %v)", n, err)
			}
			if !errors.Is(err, physical) {
				t.Fatalf("physical storage fault lost: %v", err)
			}
			if errors.Is(err, frontend.ErrBackendBusy) {
				t.Fatalf("physical storage fault reclassified as retryable busy: %v", err)
			}
			if batchObserver && !errors.Is(err, durable.ErrReplicationAckUnavailable) {
				t.Fatalf("required observer failure lost its ACK identity: %v", err)
			}
		})
	}
}
