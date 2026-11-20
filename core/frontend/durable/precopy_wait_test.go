package durable_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPrecopyFreezeQueuesIOUntilResume(t *testing.T) {
	for _, op := range []string{"read", "write", "sync"} {
		t.Run(op, func(t *testing.T) {
			p := precopyProvider(t, storage.NewMemChunkRemote(), &parkView{stubView: v1View()})
			openRecovered(t, p)
			b, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatal(err)
			}
			gate := b.(interface {
				TryStopIO(string) bool
				SetOperational(bool, string)
			})
			if !gate.TryStopIO("pre-copy final cut") {
				t.Fatal("idle frontend did not freeze")
			}
			done, started := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			var result error
			t.Cleanup(func() {
				cancel()
				gate.SetOperational(true, "test cleanup")
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("I/O did not join")
				}
			})
			go func() {
				defer close(done)
				close(started)
				switch op {
				case "read":
					var n int
					n, result = b.Read(ctx, 4096, make([]byte, 4096))
					if result == nil && n != 4096 {
						result = fmt.Errorf("read %d bytes", n)
					}
				case "write":
					_, result = b.Write(ctx, 8192, block(0x8a))
				case "sync":
					result = b.Sync(ctx)
				}
			}()
			<-started
			select {
			case <-done:
				t.Fatalf("guest I/O escaped tentative freeze: %v", result)
			case <-time.After(80 * time.Millisecond):
			}
			gate.SetOperational(true, "final cut safely aborted")
			select {
			case <-done:
				if result != nil {
					t.Fatal(result)
				}
			case <-ctx.Done():
				t.Fatal("resumed I/O did not finish")
			}
			if op == "write" {
				buf := make([]byte, 4096)
				if n, err := b.Read(context.Background(), 8192, buf); err != nil || n != 4096 || buf[0] != 0x8a {
					t.Fatalf("resumed write not present: n=%d err=%v", n, err)
				}
			}
		})
	}
}

func TestPrecopyFreezeCallerDeadlineIsRetryable(t *testing.T) {
	p := precopyProvider(t, storage.NewMemChunkRemote(), &parkView{stubView: v1View()})
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	gate := b.(interface {
		TryStopIO(string) bool
		SetOperational(bool, string)
	})
	if !gate.TryStopIO("pre-copy final cut") {
		t.Fatal("idle frontend did not freeze")
	}
	t.Cleanup(func() { gate.SetOperational(true, "test cleanup") })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = b.Write(ctx, 4096, block(7))
	if !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost retryable admission status: %v", err)
	}
}
