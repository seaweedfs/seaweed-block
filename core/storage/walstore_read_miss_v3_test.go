package storage

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// This sink controls only the read-miss seam; both reads and writes use the
// actual WALStore. No remote fill or manifest behavior is simulated here.
type heldWALReadMiss struct{ entered, release chan struct{} }

func (h *heldWALReadMiss) OnReadMiss(uint64) ([]byte, error) {
	close(h.entered)
	<-h.release
	return nil, nil
}
func (*heldWALReadMiss) OnExtentWrite(uint64, uint64)         {}
func (*heldWALReadMiss) BeforeCheckpointAdvance(uint64) error { return nil }
func (*heldWALReadMiss) BeginBaseInstall() error              { return nil }
func (*heldWALReadMiss) EndBaseInstall(uint64) error          { return nil }

func TestWALStore_PostReadMissRechecksWALFormat(t *testing.T) {
	for _, version := range []string{"v2", "v3"} {
		t.Run(version, func(t *testing.T) {
			var s *WALStore
			if version == "v3" {
				s, _ = newStoreV3(t, WModeLagged)
			} else {
				var err error
				s, err = CreateWALStore(filepath.Join(t.TempDir(), "store"), 16, v3TestBlock)
				if err != nil {
					t.Fatal(err)
				}
				s.DisableAutoFlushForRecoveryTest()
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			h := &heldWALReadMiss{entered: make(chan struct{}), release: make(chan struct{})}
			s.chunk.Store(&chunkHooks{sink: h, closer: func() error { return nil }})
			var once sync.Once
			release := func() { once.Do(func() { close(h.release) }) }
			type result struct {
				data []byte
				err  error
			}
			done := make(chan result, 1)
			joined := make(chan struct{})
			// Release and join before the store closes, including on an assertion failure.
			t.Cleanup(func() {
				release()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("read-miss worker failed to join")
				}
			})
			go func() { defer close(joined); data, err := s.Read(3); done <- result{data, err} }()
			select {
			case <-h.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("read never reached OnReadMiss")
			}
			want := bytes.Repeat([]byte{0xB7}, v3TestBlock)
			// The read has already missed the dirty map. Write while OnReadMiss is held.
			if _, err := s.Write(3, want); err != nil {
				t.Fatal(err)
			}
			release()
			select {
			case got := <-done:
				if got.err != nil || !bytes.Equal(got.data, want) {
					t.Fatalf("post-OnReadMiss WAL recheck returned stale or misdecoded bytes: len=%d err=%v", len(got.data), got.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("read did not finish after OnReadMiss release")
			}
		})
	}
}
