//go:build swblock_format_v3_test

package transport

import (
	"bytes"
	"errors"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Plan row 10b controls (DESIGN r17.4 section 8.7, caller 2; owner
// claude03): the legacy single-lane rebuild on a dark v3 replica. The
// first admitted rebuild message begins one install at its frontier hint;
// blocks go through the handle; done completes, then Syncs, and a failure
// closes the connection with no MsgBarrierResp.

const v3RepBlock = 4096

func v3Rep(t *testing.T) *storage.WALStore {
	t.Helper()
	s, err := storage.CreateWALStoreV3Test(filepath.Join(t.TempDir(), "replica.v3"), 64, v3RepBlock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func v3Listen(t *testing.T, store storage.LogicalStorage) *ReplicaListener {
	t.Helper()
	l, err := NewReplicaListener("127.0.0.1:0", store)
	if err != nil {
		t.Fatal(err)
	}
	l.Serve()
	t.Cleanup(func() { l.Stop() })
	return l
}

func v3Dial(t *testing.T, l *ReplicaListener) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", l.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func v3RepBlk(b byte) []byte { return bytes.Repeat([]byte{b}, v3RepBlock) }

// done sends MsgRebuildDone and returns the BarrierResp, or ok=false when
// the replica closed the connection without one.
func v3Done(t *testing.T, c net.Conn, l RecoveryLineage) (BarrierResponse, bool) {
	t.Helper()
	if err := WriteMsg(c, MsgRebuildDone, EncodeLineage(l)); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	mt, payload, err := ReadMsg(c)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("no reply and no close after MsgRebuildDone")
		}
		return BarrierResponse{}, false
	}
	if mt != MsgBarrierResp {
		t.Fatalf("reply %d, want MsgBarrierResp", mt)
	}
	r, err := DecodeBarrierResp(payload)
	if err != nil {
		t.Fatal(err)
	}
	return r, true
}

func v3Read(t *testing.T, s *storage.WALStore, lba uint32) []byte {
	t.Helper()
	b, err := s.Read(lba)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The positive and the empty rebuild through the replica handler: installed,
// AchievedLSN = the hint, the blocks readable. Mutants: no begin (the
// unanchored jump: the store-level direct write is refused on v3) and no
// MarkBaseComplete (the achieved-0 shape: the gated Sync fails the done
// path and nothing is replied) both fail here.
func TestReplicaV3LegacyRebuildInstallsThroughTheHandle(t *testing.T) {
	for _, empty := range []bool{false, true} {
		name := "blocks"
		if empty {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			s := v3Rep(t)
			l := v3Listen(t, s)
			c := v3Dial(t, l)
			lin := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 5}
			if !empty {
				for lba := uint32(0); lba < 3; lba++ {
					if err := WriteMsg(c, MsgRebuildBlock, EncodeRebuildBlock(lin, lba, v3RepBlk(0x10+byte(lba)))); err != nil {
						t.Fatal(err)
					}
				}
			}
			resp, ok := v3Done(t, c, lin)
			if !ok || resp.AchievedLSN != 5 {
				t.Fatalf("done: replied %v, AchievedLSN %d, want 5", ok, resp.AchievedLSN)
			}
			if st, _ := storage.BaseStateV3Test(s); st != storage.BaseInstalled {
				t.Fatalf("state %d, want installed", st)
			}
			if !empty {
				for lba := uint32(0); lba < 3; lba++ {
					if got := v3Read(t, s, lba); !bytes.Equal(got, v3RepBlk(0x10+byte(lba))) {
						t.Fatalf("LBA %d = %x...", lba, got[0])
					}
				}
			}
		})
	}
}

// The actual caller: BlockExecutor.StartRebuild -> doRebuild -> the
// replica handler, onto a dark v3 replica. The session closes successfully
// and the replica holds the primary's blocks, installed.
func TestReplicaV3RebuildThroughTheBlockExecutor(t *testing.T) {
	primary := storage.NewBlockStore(64, v3RepBlock)
	for lba := uint32(0); lba < 4; lba++ {
		if _, err := primary.Write(lba, v3RepBlk(0x20+byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	_, _, pH := primary.Boundaries()
	s := v3Rep(t)
	l := v3Listen(t, s)
	exec := NewBlockExecutor(primary, l.Addr())
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
	if err := exec.StartRebuild("r1", 7, 1, 1, pH); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-closeCh:
		if !r.Success {
			t.Fatalf("rebuild closed unsuccessfully: %+v", r)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("rebuild did not close")
	}
	if st, _ := storage.BaseStateV3Test(s); st != storage.BaseInstalled {
		t.Fatalf("state %d, want installed", st)
	}
	for lba := uint32(0); lba < 4; lba++ {
		if got := v3Read(t, s, lba); !bytes.Equal(got, v3RepBlk(0x20+byte(lba))) {
			t.Fatalf("LBA %d = %x...", lba, got[0])
		}
	}
}

// failingDoneStore injects a done-path failure: the completion or the
// logical Sync after it.
type failingDoneStore struct {
	*storage.WALStore
	failComplete, failSync atomic.Bool
}

type failingHandle struct {
	storage.InstallHandle
	s *failingDoneStore
}

func (h failingHandle) MarkBaseComplete() error {
	if h.s.failComplete.Load() {
		return errors.New("injected MarkBaseComplete failure")
	}
	return h.InstallHandle.MarkBaseComplete()
}

func (s *failingDoneStore) BeginBaseInstall(baseLSN uint64, id [16]byte) (storage.InstallHandle, error) {
	h, err := s.WALStore.BeginBaseInstall(baseLSN, id)
	if err != nil {
		return nil, err
	}
	return failingHandle{InstallHandle: h, s: s}, nil
}

func (s *failingDoneStore) Sync() (uint64, error) {
	if s.failSync.Load() {
		return 0, errors.New("injected Sync failure")
	}
	return s.WALStore.Sync()
}

// Injected done-path failures (DESIGN 8.7): a failed MarkBaseComplete or a
// failed Sync closes the connection with no MsgBarrierResp. Mutants: a
// handler that discards either error replies and fails here.
func TestReplicaV3RebuildDoneFailureRepliesNothing(t *testing.T) {
	for _, which := range []string{"complete", "sync"} {
		t.Run(which, func(t *testing.T) {
			s := &failingDoneStore{WALStore: v3Rep(t)}
			if which == "complete" {
				s.failComplete.Store(true)
			} else {
				s.failSync.Store(true)
			}
			l := v3Listen(t, s)
			c := v3Dial(t, l)
			lin := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 5}
			if err := WriteMsg(c, MsgRebuildBlock, EncodeRebuildBlock(lin, 0, v3RepBlk(0x11))); err != nil {
				t.Fatal(err)
			}
			if resp, ok := v3Done(t, c, lin); ok {
				t.Fatalf("a failed %s replied MsgBarrierResp AchievedLSN %d", which, resp.AchievedLSN)
			}
		})
	}
}

// A reconnect is a new connection and so a superseding install: refused
// unless its hint covers W (the replica acknowledged LSN 4 before the
// rebuild), allowed when it does.
func TestReplicaV3RebuildReconnectSupersedesOnlyUnderCoverage(t *testing.T) {
	s := v3Rep(t)
	for lba := uint32(10); lba < 14; lba++ {
		if _, err := s.Write(lba, v3RepBlk(byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	l := v3Listen(t, s)
	a := v3Dial(t, l)
	linA := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 5}
	if err := WriteMsg(a, MsgRebuildBlock, EncodeRebuildBlock(linA, 0, v3RepBlk(0xAA))); err != nil {
		t.Fatal(err)
	}
	waitInstalling(t, s)
	_, idA := storage.BaseStateV3Test(s)
	b := v3Dial(t, l)
	linB := RecoveryLineage{SessionID: 2, Epoch: 2, EndpointVersion: 2, TargetLSN: 3} // below W 4
	if err := WriteMsg(b, MsgRebuildBlock, EncodeRebuildBlock(linB, 0, v3RepBlk(0xBB))); err != nil {
		t.Fatal(err)
	}
	if _, ok := v3Done(t, b, linB); ok {
		t.Fatal("an uncovered reconnect completed")
	}
	if st, id := storage.BaseStateV3Test(s); st != storage.BaseInstalling || id != idA {
		t.Fatalf("after the uncovered reconnect: state %d, id changed %v", st, id != idA)
	}
	c := v3Dial(t, l)
	linC := RecoveryLineage{SessionID: 3, Epoch: 3, EndpointVersion: 3, TargetLSN: 6}
	if err := WriteMsg(c, MsgRebuildBlock, EncodeRebuildBlock(linC, 0, v3RepBlk(0xCC))); err != nil {
		t.Fatal(err)
	}
	if resp, ok := v3Done(t, c, linC); !ok || resp.AchievedLSN != 6 {
		t.Fatalf("covered reconnect: replied %v AchievedLSN %d, want 6", ok, resp.AchievedLSN)
	}
	if got := v3Read(t, s, 0); !bytes.Equal(got, v3RepBlk(0xCC)) {
		t.Fatalf("LBA 0 = %x..., want the covering install's bytes", got[0])
	}
}

func waitInstalling(t *testing.T, s *storage.WALStore) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if st, _ := storage.BaseStateV3Test(s); st == storage.BaseInstalling {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the rebuild never began its install")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
