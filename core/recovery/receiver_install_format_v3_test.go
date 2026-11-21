//go:build swblock_format_v3_test

package recovery

import (
	"bytes"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Controls for the dual-lane Receiver on a dark v3 store. Every BASE block
// and WAL entry goes through the session's install handle; both admission
// schedules run through the real receiver (WriteBase through frameBaseBlock,
// ApplyInstallEntry through RebuildSession.ApplyWALEntry from frameWALEntry).
// Mutants are the store mutants (shared-begin, no-id-check, premature-check)
// plus the caller mutants named per control.

const v3RecvBlock = 4096

func v3RecvStore(t *testing.T) *storage.WALStore {
	t.Helper()
	s, _ := v3RecvStoreAt(t)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func v3RecvStoreAt(t *testing.T) (*storage.WALStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "recv.v3")
	s, err := storage.CreateWALStoreV3Test(path, 64, v3RecvBlock)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func v3Blk(b byte) []byte { return bytes.Repeat([]byte{b}, v3RecvBlock) }

// v3Peer is the primary side of one receiver connection. Every frame the
// receiver writes is read into frames, so the receiver never blocks on the
// synchronous pipe.
type v3Peer struct {
	conn   net.Conn
	frames chan frameType
	acks   chan uint64 // AcknowledgedLSN of every BaseBatchAck, in order
	done   chan error
}

func startV3Receiver(t *testing.T, store storage.LogicalStorage) *v3Peer {
	t.Helper()
	primary, replica := net.Pipe()
	t.Cleanup(func() { primary.Close(); replica.Close() })
	p := &v3Peer{conn: primary, frames: make(chan frameType, 64), acks: make(chan uint64, 64), done: make(chan error, 1)}
	r := NewReceiverWithCadence(store, replica, 1, time.Hour) // an ack after every BASE block
	go func() { _, err := r.Run(); p.done <- err; replica.Close() }()
	go func() {
		for {
			ft, payload, err := readFrame(primary)
			if err != nil {
				close(p.frames)
				return
			}
			if ft == frameBaseBatchAck {
				if a, err := decodeBaseBatchAck(payload); err == nil {
					p.acks <- a.AcknowledgedLSN
				}
			}
			p.frames <- ft
		}
	}()
	return p
}

func (p *v3Peer) send(t *testing.T, ft frameType, payload []byte) {
	t.Helper()
	if err := writeFrame(p.conn, ft, payload); err != nil {
		t.Fatalf("write frame %v: %v", ft, err)
	}
}

func (p *v3Peer) start(t *testing.T, fromLSN uint64) {
	t.Helper()
	p.send(t, frameSessionStart, encodeSessionStart(sessionStartPayload{SessionID: 1, FromLSN: fromLSN, NumBlocks: 64}))
}

func (p *v3Peer) want(t *testing.T, ft frameType) {
	t.Helper()
	select {
	case got, ok := <-p.frames:
		if !ok || got != ft {
			t.Fatalf("frame %v (open %v), want %v", got, ok, ft)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no %v frame", ft)
	}
}

func (p *v3Peer) runErr(t *testing.T) error {
	t.Helper()
	select {
	case err := <-p.done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("receiver did not end")
		return nil
	}
}

// holdFirst returns a hook that parks the first call until release runs.
// The rendezvous is bounded (a hook that never fires fails the test instead
// of hanging it) and cleanup always releases.
func holdFirst(t *testing.T) (hook func(), waitParked func(), release func()) {
	parked, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var armed atomic.Bool
	armed.Store(true)
	waitParked = func() {
		t.Helper()
		select {
		case <-parked:
		case <-time.After(10 * time.Second):
			t.Fatal("the held frame never reached the install hook")
		}
	}
	return func() {
		if armed.CompareAndSwap(true, false) {
			close(parked)
			<-gate
		}
	}, waitParked, release
}

func readV3Recv(t *testing.T, s *storage.WALStore, lba uint32) []byte {
	t.Helper()
	b, err := s.Read(lba)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The positive receiver rebuild: SessionStart begins the install, a BASE
// block and a WAL entry go through the handle, BaseDone completes it
// (installed) and the barrier completes the session. BaseLSN 5 is above
// the checkpoint, so installed is not retired yet (8.4).
// Mutant: a receiver that skips the begin (the legacy handle on v3) is
// refused by the v3 store (errV3NotYet) and fails here.
func TestReceiverV3InstallThroughTheHandle(t *testing.T) {
	s := v3RecvStore(t)
	p := startV3Receiver(t, s)
	p.start(t, 5)
	p.send(t, frameBaseBlock, encodeBaseBlock(3, v3Blk(0x33)))
	p.want(t, frameBaseBatchAck)
	if st, _ := storage.BaseStateV3Test(s); st != storage.BaseInstalling {
		t.Fatalf("state %d during the session, want installing", st)
	}
	p.send(t, frameWALEntry, encodeWALEntry(WALKindBacklog, 4, 6, v3Blk(0x44)))
	p.send(t, frameBaseDone, nil)
	p.want(t, frameBaseBatchAck)
	p.send(t, frameBarrierReq, nil)
	p.want(t, frameBarrierResp)
	if err := p.runErr(t); err != nil {
		t.Fatalf("receiver: %v", err)
	}
	if st, _ := storage.BaseStateV3Test(s); st != storage.BaseInstalled {
		t.Fatalf("state %d after BaseDone, want installed", st)
	}
	for lba, want := range map[uint32]byte{3: 0x33, 4: 0x44} {
		if got := readV3Recv(t, s, lba); !bytes.Equal(got, v3Blk(want)) {
			t.Fatalf("LBA %d = %x..., want %x", lba, got[0], want)
		}
	}
}

// Drain setup through the receiver: A's admitted BASE
// write is held; B's SessionStart begin waits for it; B writes the target
// block and stays installing; a further A frame fails A's session with
// ErrInstallSuperseded and B's block is byte-identical.
func TestReceiverV3HeldOldBaseWriteDrainsBeforeANewSession(t *testing.T) {
	s := v3RecvStore(t)
	a := startV3Receiver(t, s)
	a.start(t, 0)
	hook, waitParked, release := holdFirst(t)
	storage.SetInstallHooksV3Test(s, nil, hook)
	a.send(t, frameBaseBlock, encodeBaseBlock(7, v3Blk(0xAA)))
	waitParked()
	_, idA := storage.BaseStateV3Test(s)
	b := startV3Receiver(t, s)
	bSent := make(chan struct{})
	go func() {
		b.start(t, 0)
		b.send(t, frameBaseBlock, encodeBaseBlock(7, v3Blk(0xBB)))
		close(bSent)
	}()
	// B must not get past its begin while A's write is held; give a wrong
	// (shared) begin time to write first. The oracle is the bytes below.
	select {
	case ft := <-b.frames:
		if ft != frameBaseBatchAck {
			t.Fatalf("B frame %v", ft)
		}
		b.frames <- ft
	case <-time.After(300 * time.Millisecond):
	}
	release()
	// A's write finishes; its progress ack (SyncInstallLog, row 10) either
	// runs before B's begin, or queues behind B's exclusive begin and fails
	// with ErrInstallIDMismatch. Either way A cannot write again.
	aAcked := false
	select {
	case ft, ok := <-a.frames:
		if !ok || ft != frameBaseBatchAck {
			if err := a.runErr(t); !errors.Is(err, storage.ErrInstallIDMismatch) {
				t.Fatalf("A's ack after the drain: %v, want ErrInstallIDMismatch", err)
			}
		} else {
			aAcked = true
		}
	case <-time.After(10 * time.Second):
		t.Fatal("A neither acked nor ended")
	}
	<-bSent
	b.want(t, frameBaseBatchAck)
	if got := readV3Recv(t, s, 7); !bytes.Equal(got, v3Blk(0xBB)) {
		t.Fatalf("LBA 7 = %x..., want B's bytes", got[0])
	}
	if aAcked {
		a.send(t, frameBaseBlock, encodeBaseBlock(7, v3Blk(0xAA)))
		if err := a.runErr(t); !errors.Is(err, storage.ErrInstallSuperseded) {
			t.Fatalf("A after the supersession: %v, want ErrInstallSuperseded", err)
		}
	}
	if got := readV3Recv(t, s, 7); !bytes.Equal(got, v3Blk(0xBB)) {
		t.Fatalf("LBA 7 = %x... after A's stale frame, want B's bytes", got[0])
	}
	if st, id := storage.BaseStateV3Test(s); st != storage.BaseInstalling || id == idA {
		t.Fatalf("state %d id unchanged %v, want installing under B", st, id == idA)
	}
}

// Premature-check setup through the receiver: A's frame parks
// before acquiring installMu; B supersedes, writes the target block and
// stays installing; A resumes and its session fails with
// ErrInstallSuperseded, B's block byte-identical. WriteBase runs through
// frameBaseBlock, ApplyInstallEntry through frameWALEntry.
func TestReceiverV3ParkedOldFrameIsRefusedAfterANewSession(t *testing.T) {
	for _, lane := range []string{"base", "wal"} {
		t.Run(lane, func(t *testing.T) {
			s := v3RecvStore(t)
			a := startV3Receiver(t, s)
			a.start(t, 0)
			hook, waitParked, release := holdFirst(t)
			storage.SetInstallHooksV3Test(s, hook, nil)
			if lane == "base" {
				a.send(t, frameBaseBlock, encodeBaseBlock(7, v3Blk(0xAA)))
			} else {
				a.send(t, frameWALEntry, encodeWALEntry(WALKindBacklog, 7, 1, v3Blk(0xAA)))
			}
			waitParked()
			b := startV3Receiver(t, s)
			b.start(t, 0)
			b.send(t, frameBaseBlock, encodeBaseBlock(7, v3Blk(0xBB)))
			b.want(t, frameBaseBatchAck)
			release()
			if err := a.runErr(t); !errors.Is(err, storage.ErrInstallSuperseded) {
				t.Fatalf("parked A (%s): %v, want ErrInstallSuperseded", lane, err)
			}
			if got := readV3Recv(t, s, 7); !bytes.Equal(got, v3Blk(0xBB)) {
				t.Fatalf("LBA 7 = %x..., want B's bytes", got[0])
			}
			if st, _ := storage.BaseStateV3Test(s); st != storage.BaseInstalling {
				t.Fatalf("state %d, want installing under B", st)
			}
		})
	}
}

// A superseded session's BaseDone fails (DESIGN 8.2 step 4): the
// completion error propagates, no BaseBatchAck is sent and the volume stays
// installing under the newer ID. Mutant: a receiver that discards the
// MarkBaseComplete error acks and fails here.
func TestReceiverV3SupersededBaseDoneFailsWithoutAnAck(t *testing.T) {
	s := v3RecvStore(t)
	a := startV3Receiver(t, s)
	a.start(t, 0)
	a.send(t, frameBaseBlock, encodeBaseBlock(2, v3Blk(0x22)))
	a.want(t, frameBaseBatchAck)
	b := startV3Receiver(t, s)
	b.start(t, 0)
	b.send(t, frameBaseBlock, encodeBaseBlock(3, v3Blk(0x33)))
	b.want(t, frameBaseBatchAck)
	a.send(t, frameBaseDone, nil)
	if err := a.runErr(t); !errors.Is(err, storage.ErrInstallSuperseded) {
		t.Fatalf("A BaseDone: %v, want ErrInstallSuperseded", err)
	}
	if ft, ok := <-a.frames; ok {
		t.Fatalf("A sent %v after a failed completion", ft)
	}
	if st, _ := storage.BaseStateV3Test(s); st != storage.BaseInstalling {
		t.Fatalf("state %d, want installing", st)
	}
}

// The real Sender sends BaseDone from its base lane while
// its WAL lane is still sending (sender.go base goroutine). A WAL entry of
// the same session after BaseDone must still apply, and the barrier then
// completes with both lanes' bytes. Mutant: admitting install-lane WAL
// only while installing fails here (the entry after BaseDone is refused).
func TestReceiverV3WALAfterBaseDoneStillApplies(t *testing.T) {
	s := v3RecvStore(t)
	p := startV3Receiver(t, s)
	p.start(t, 5)
	p.send(t, frameBaseBlock, encodeBaseBlock(3, v3Blk(0x33)))
	p.want(t, frameBaseBatchAck)
	p.send(t, frameWALEntry, encodeWALEntry(WALKindBacklog, 4, 6, v3Blk(0x44)))
	p.send(t, frameBaseDone, nil)
	p.want(t, frameBaseBatchAck)
	p.send(t, frameWALEntry, encodeWALEntry(WALKindSessionLive, 5, 7, v3Blk(0x55)))
	p.send(t, frameBarrierReq, nil)
	p.want(t, frameBarrierResp)
	if err := p.runErr(t); err != nil {
		t.Fatalf("receiver: %v", err)
	}
	for lba, want := range map[uint32]byte{3: 0x33, 4: 0x44, 5: 0x55} {
		if got := readV3Recv(t, s, lba); !bytes.Equal(got, v3Blk(want)) {
			t.Fatalf("LBA %d = %x..., want %x", lba, got[0], want)
		}
	}
}

// The same Sender order with a crash after the barrier: every byte of both
// lanes, including the WAL entry sent after BaseDone, survives a crash and
// a reopen (DESIGN r17.4, section 11).
func TestReceiverV3WALAfterBaseDoneSurvivesACrash(t *testing.T) {
	s, path := v3RecvStoreAt(t)
	p := startV3Receiver(t, s)
	p.start(t, 5)
	p.send(t, frameBaseBlock, encodeBaseBlock(3, v3Blk(0x33)))
	p.want(t, frameBaseBatchAck)
	p.send(t, frameWALEntry, encodeWALEntry(WALKindBacklog, 4, 6, v3Blk(0x44)))
	p.send(t, frameBaseDone, nil)
	p.want(t, frameBaseBatchAck)
	p.send(t, frameWALEntry, encodeWALEntry(WALKindSessionLive, 5, 7, v3Blk(0x55)))
	p.send(t, frameBarrierReq, nil)
	p.want(t, frameBarrierResp)
	if err := p.runErr(t); err != nil {
		t.Fatalf("receiver: %v", err)
	}
	r, err := storage.CrashReopenV3Test(s, path)
	if err != nil {
		t.Fatalf("crash and reopen: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	for lba, want := range map[uint32]byte{3: 0x33, 4: 0x44, 5: 0x55} {
		if got := readV3Recv(t, r, lba); !bytes.Equal(got, v3Blk(want)) {
			t.Fatalf("after reopen LBA %d = %x..., want %x", lba, got[0], want)
		}
	}
}

// Row 10a: a progress ack during the install is the install log's durable
// claim (SyncInstallLog, DESIGN 7): it reports min(WALApplied, H) and the
// slot carries W = H with the volume still installing; the logical Sync
// stays gated. Mutant: sendAck through the gated logical Sync fails the
// session at its first ack.
func TestReceiverV3ProgressAckIsTheInstallLog(t *testing.T) {
	s := v3RecvStore(t)
	p := startV3Receiver(t, s)
	p.start(t, 5)
	p.send(t, frameBaseBlock, encodeBaseBlock(3, v3Blk(0x33)))
	p.want(t, frameBaseBatchAck)
	p.send(t, frameWALEntry, encodeWALEntry(WALKindBacklog, 4, 6, v3Blk(0x44)))
	p.send(t, frameBaseBlock, encodeBaseBlock(2, v3Blk(0x22)))
	p.want(t, frameBaseBatchAck)
	<-p.acks // the first ack, before the WAL entry
	if got := <-p.acks; got != 6 {
		t.Fatalf("progress ack AcknowledgedLSN %d, want 6", got)
	}
	if st, _ := storage.BaseStateV3Test(s); st != storage.BaseInstalling {
		t.Fatalf("state %d after a progress ack, want installing", st)
	}
	if f, err := s.Sync(); f != 0 || !errors.Is(err, storage.ErrBaseInstalling) {
		t.Fatalf("logical Sync during the install = (%d, %v), want (0, ErrBaseInstalling)", f, err)
	}
}
