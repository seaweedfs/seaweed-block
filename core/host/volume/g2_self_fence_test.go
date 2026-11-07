package volume

// G2 (internal/docs/protocol/fencing-gaps-todo.md): a host that becomes
// primary must fence its own replica listener. Before the fix, Fence sent
// barriers to peers only, so a deposed primary that could still reach the new
// primary's listener kept getting its writes applied there (and could count
// that ACK toward its quorum).

import (
	"bytes"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

const g2BlockSize = 4096

// g2Ship opens a connection to addr and sends one MsgShipEntry, as the
// (deposed) primary P1 would.
func g2Ship(t *testing.T, addr string, lineage transport.RecoveryLineage, lsn uint64, fill byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial replica listener: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	entry := transport.ShipEntry{Lineage: lineage, LBA: 0, LSN: lsn, Data: bytes.Repeat([]byte{fill}, g2BlockSize)}
	if err := transport.WriteMsg(conn, transport.MsgShipEntry, transport.EncodeShipEntry(entry)); err != nil {
		t.Fatalf("ship: %v", err)
	}
	return conn
}

// g2Closed reports whether the listener closed conn within d (a refused
// mutation closes the connection; an applied ship sends nothing back).
func g2Closed(conn net.Conn, d time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(d))
	_, err := conn.Read(make([]byte, 1))
	return err != nil && !errors.Is(err, os.ErrDeadlineExceeded)
}

func g2Block(t *testing.T, store storage.LogicalStorage) []byte {
	t.Helper()
	b, err := store.Read(0)
	if err != nil {
		t.Fatalf("read LBA 0: %v", err)
	}
	return b
}

// TestG2_PromotedHostRefusesShipFromDeposedPrimary — the G2 red test, as the
// doc describes it: ship at epoch 1 to listener B; master promotes B at epoch
// 2 through the host role path (Host.applyFact with a self fact, not a
// test-only setter); the old primary ships at epoch 1 again. The second ship
// must be refused: the connection closed and LBA 0 unchanged.
func TestG2_PromotedHostRefusesShipFromDeposedPrimary(t *testing.T) {
	store := storage.NewBlockStore(16, g2BlockSize)
	ln, err := transport.NewReplicaListener("127.0.0.1:0", store)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	h, a, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.SetSelfFence(ln) // as cmd/blockvolume wires it, before Serve
	ln.Serve()
	t.Cleanup(ln.Stop)

	// P1 is primary at epoch 1 and ships to B.
	p1 := transport.RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, FrontierHint: 1}
	g2Ship(t, ln.Addr(), p1, 1, 'X')
	deadline := time.Now().Add(2 * time.Second)
	for !bytes.Equal(g2Block(t, store), bytes.Repeat([]byte{'X'}, g2BlockSize)) {
		if time.Now().After(deadline) {
			t.Fatal("fixture: the epoch-1 ship was never applied before promotion")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Master promotes B to primary at epoch 2: the production role path.
	h.applyFact(&control.AssignmentFact{
		VolumeId:          "vol1",
		ReplicaId:         "rB", // self
		Epoch:             2,
		EndpointVersion:   1,
		DataAddr:          ln.Addr(),
		CtrlAddr:          "127.0.0.1:1",
		PeerSetGeneration: 2,
	})
	if got := a.callCount.Load(); got != 1 || a.lastInfo.Epoch != 2 {
		t.Fatalf("fixture: host did not adopt primary at epoch 2 (OnAssignment calls=%d epoch=%d)", got, a.lastInfo.Epoch)
	}

	// P1, cut off from master but still reaching B, ships at epoch 1 again.
	conn := g2Ship(t, ln.Addr(), p1, 2, 'Y')
	closed := g2Closed(conn, 2*time.Second)
	if got := g2Block(t, store); bytes.Equal(got, bytes.Repeat([]byte{'Y'}, g2BlockSize)) {
		t.Fatal("G2: the new primary (epoch 2) applied the deposed primary's epoch-1 ship")
	}
	if !closed {
		t.Fatal("G2: the new primary's listener kept the deposed primary's connection open (mutation not refused)")
	}
}

// stubFencer fails FenceAsPrimary, as a listener whose in-flight mutations did
// not drain within the bound would.
type stubFencer struct{ err error }

func (s stubFencer) FenceAsPrimary(uint64) error { return s.err }
func (s stubFencer) AdmitAsReplica(uint64)       {}

// If the host cannot fence its own listener, it must not adopt the primary
// role: OnAssignment is skipped (frontend stays gated), as for an
// UpdateReplicaSet failure, and master's stream replay redelivers the fact.
func TestG2_HostDoesNotAdoptPrimaryWhenItsListenerCannotBeFenced(t *testing.T) {
	h, a, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.SetSelfFence(stubFencer{err: errors.New("2 replication mutation(s) still in flight")})
	h.applyFact(&control.AssignmentFact{
		VolumeId:          "vol1",
		ReplicaId:         "rB",
		Epoch:             2,
		EndpointVersion:   1,
		DataAddr:          "127.0.0.1:1",
		CtrlAddr:          "127.0.0.1:2",
		PeerSetGeneration: 2,
	})
	if got := a.callCount.Load(); got != 0 {
		t.Fatalf("fail-closed violated: OnAssignment calls=%d want 0", got)
	}
}
