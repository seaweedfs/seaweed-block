package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

func TestReadReadinessWaitDoesNotHoldAdmissionAndCloseWakesIt(t *testing.T) {
	for _, mode := range []string{"close", "drain-then-close", "fence", "write-close"} {
		name := mode
		t.Run(name, func(t *testing.T) {
			// No storage is needed: this read must never pass the operational gate.
			b := &StorageBackend{waitAttachReady: true}
			b.opEvidence.Store("attachment pending")
			registered := make(chan struct{}, 1)
			resume := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			b.testBeforeReadReadyWait = func() {
				select {
				case registered <- struct{}{}:
				default:
				}
				<-resume
			}
			done := make(chan error, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				release()
				_ = b.Close()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("pending read did not settle after release and Close")
				}
			})
			go func() {
				defer close(joined)
				var err error
				if mode == "write-close" {
					_, err = b.Write(context.Background(), 0, make([]byte, 4096))
				} else {
					_, err = b.Read(context.Background(), 0, make([]byte, 4096))
				}
				done <- err
			}()
			select {
			case <-registered:
			case <-time.After(time.Second):
				t.Fatal("read did not register a readiness notification")
			}
			if mode == "drain-then-close" {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				if err := b.StopIO(ctx, "attachment still pending"); err != nil {
					t.Fatalf("waiting read held I/O admission: %v", err)
				}
			}
			want := frontend.ErrBackendClosed
			if mode == "fence" {
				b.mu.Lock()
				b.chunkFenced = func() bool { return true }
				b.mu.Unlock()
				b.SetOperational(false, "fenced")
				want = frontend.ErrStalePrimary
			} else if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			release()
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("Close wake error=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not wake registered read")
			}
		})
	}
}

type readinessProjection frontend.Projection

func (p readinessProjection) Projection() frontend.Projection { return frontend.Projection(p) }

func TestAttachReadinessTimeoutPreservesStaleLineage(t *testing.T) {
	id := frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1}
	b := &StorageBackend{id: id, view: readinessProjection{VolumeID: "v", ReplicaID: "r", Epoch: 2, EndpointVersion: 1, Healthy: true}}
	b.setChunkAttachPending("pending")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.testBeforeReadReadyWait = func() { b.SetOperational(true, "attached under another epoch"); cancel() }
	_, err := b.Read(ctx, 0, make([]byte, 4096))
	if !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("timeout lost stale lineage: %v", err)
	}
}

func TestAttachReadinessPublicationRetriesAdmission(t *testing.T) {
	id := frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1}
	b := &StorageBackend{id: id, view: readinessProjection{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1, Healthy: true}}
	b.setChunkAttachPending("pending")
	b.testAfterAttachRefusal = func() { b.SetOperational(true, "attached") }
	if _, err := b.Read(context.Background(), 0, nil); err != nil {
		t.Fatalf("readiness publication returned the old refusal: %v", err)
	}
}
