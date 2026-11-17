//go:build swblock_format_v3_test

package recovery

import (
	"bytes"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// RED (plan row 9b): the dual-lane Receiver cannot rebuild a dark v3 store
// on b4daf46d. It writes BASE blocks with the store-level
// WriteExtentDirect, which v3 refuses (no install handle exists yet), so
// the session fails at the first BASE block. Row 9b turns this green: the
// receiver begins the install at SessionStart and writes through the handle.
func TestReceiverV3RedRebuildsADarkV3Store(t *testing.T) {
	s, err := storage.CreateMappedWALStoreV3Test(filepath.Join(t.TempDir(), "red.v3"), 64, 4096, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	primary, replica := net.Pipe()
	t.Cleanup(func() { primary.Close(); replica.Close() })
	done := make(chan error, 1)
	go func() { _, err := NewReceiverWithCadence(s, replica, 1, time.Hour).Run(); done <- err; replica.Close() }()
	frames := make(chan frameType, 16)
	go func() {
		for {
			ft, _, err := readFrame(primary)
			if err != nil {
				close(frames)
				return
			}
			frames <- ft
		}
	}()
	if err := writeFrame(primary, frameSessionStart, encodeSessionStart(sessionStartPayload{SessionID: 1, FromLSN: 5, NumBlocks: 64})); err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(primary, frameBaseBlock, encodeBaseBlock(3, bytes.Repeat([]byte{0x33}, 4096))); err != nil {
		t.Fatal(err)
	}
	select {
	case ft, ok := <-frames:
		if !ok || ft != frameBaseBatchAck {
			t.Fatalf("after a BASE block: frame %v (open %v), want a BaseBatchAck; receiver: %v", ft, ok, <-done)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no frame after a BASE block")
	}
	got, err := s.Read(3)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0x33}, 4096)) {
		t.Fatalf("LBA 3 after the BASE block: err %v", err)
	}
}
