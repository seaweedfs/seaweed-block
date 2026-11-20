package nvme_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

func TestPrecopyTargetCloseDropsHeldCommandWithoutCompletion(t *testing.T) {
	_, backend, client, target := newSyncPairTargetFixture(t, filepath.Join(t.TempDir(), "precopy.bin"))
	closed := make(chan error, 1)
	closing := false
	t.Cleanup(func() {
		_ = backend.Close()
		if closing {
			select {
			case <-closed:
			case <-time.After(3 * time.Second):
				t.Error("target close did not join")
			}
		}
	})
	if !backend.TryStopIO("final detached cut") {
		t.Fatal("freeze failed")
	}
	cmd := nvme.CapsuleCommand{OpCode: 0, CID: 77, NSID: 1}
	if err := client.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for target.Stats().FlushCommands == 0 {
		if time.Now().After(deadline) {
			t.Fatal("flush did not reach target")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := backend.StopIO(ctx, "workspace lease fenced"); err != nil {
		t.Fatal(err)
	}
	_ = client.io.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	_, err := client.ioR.Dequeue()
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("host drain emitted a completion on the held path: %v", err)
	}
	_ = client.io.SetReadDeadline(time.Time{})
	closing = true
	go func() { closed <- target.Close() }()
	select {
	case err := <-closed:
		closing = false
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("target teardown waited for the frozen command budget")
	}
}
