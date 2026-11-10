package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// G6: the dual-lane rebuild port (--recovery-mode=dual-lane) obeys the same
// epoch and role rules as the replication port. Frames are written by hand
// because the encoders are private to the recovery package:
// [1 byte type][4 byte big-endian length][payload].

const (
	g6Blocks    = 8
	g6BlockSize = 4096
)

func g6Frame(kind byte, payload []byte) []byte {
	out := make([]byte, 5, 5+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:], uint32(len(payload)))
	return append(out, payload...)
}

func g6Start(epoch uint64) []byte {
	p := make([]byte, 36) // session, from LSN, target LSN, block count, epoch
	binary.BigEndian.PutUint64(p[0:], 1)
	binary.BigEndian.PutUint32(p[24:], g6Blocks)
	binary.BigEndian.PutUint64(p[28:], epoch)
	return g6Frame(1, p)
}

func g6Base(fill byte) []byte {
	p := make([]byte, 4+g6BlockSize) // LBA 0, then the block bytes
	copy(p[4:], bytes.Repeat([]byte{fill}, g6BlockSize))
	return g6Frame(2, p)
}

// g6Port serves the gated dual-lane port and dials it.
func g6Port(t *testing.T, store storage.LogicalStorage, ln *ReplicaListener) net.Conn {
	t.Helper()
	dualLane, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = dualLane.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go recovery.NewReplicaBridgeWithGate(store, ln).AcceptDualLaneLoop(ctx, dualLane)
	conn, err := net.DialTimeout("tcp", dualLane.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial dual-lane port: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func g6Send(t *testing.T, conn net.Conn, frames ...[]byte) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	for _, f := range frames {
		// A refusal may close the connection first; the assertions are on the store.
		if _, err := conn.Write(f); err != nil {
			t.Logf("write ended early: %v", err)
			return
		}
	}
}

func g6Applied(t *testing.T, store storage.LogicalStorage, fill byte, d time.Duration) bool {
	t.Helper()
	want := bytes.Repeat([]byte{fill}, g6BlockSize)
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if bytes.Equal(g1Block(t, store), want) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func g6Assert(t *testing.T, store storage.LogicalStorage, fill byte) {
	t.Helper()
	if g6Applied(t, store, fill, 500*time.Millisecond) {
		t.Fatalf("a refused sender wrote LBA 0 through the dual-lane port")
	}
}

func TestDualLaneRebuildPort_RefusesStaleSenderAfterFence(t *testing.T) {
	store := storage.NewBlockStore(g6Blocks, g6BlockSize)
	ln := g1Listener(t, store, g1Fresh(t))
	if err := ln.FenceAsPrimary(5); err != nil {
		t.Fatalf("FenceAsPrimary(5): %v", err)
	}
	g6Send(t, g6Port(t, store, ln), g6Start(1), g6Base(0xAB))
	g6Assert(t, store, 0xAB)
}

func TestDualLaneRebuildPort_PrimaryRefusesEvenCurrentEpoch(t *testing.T) {
	store := storage.NewBlockStore(g6Blocks, g6BlockSize)
	ln := g1Listener(t, store, g1Fresh(t))
	if err := ln.FenceAsPrimary(5); err != nil {
		t.Fatalf("FenceAsPrimary(5): %v", err)
	}
	g6Send(t, g6Port(t, store, ln), g6Start(5), g6Base(0xAC))
	g6Assert(t, store, 0xAC)
}

func TestDualLaneRebuildPort_RefusesBeforeRoleKnown(t *testing.T) {
	store := storage.NewBlockStore(g6Blocks, g6BlockSize)
	ln := g1Listener(t, store, g1Fresh(t))
	g6Send(t, g6Port(t, store, ln), g6Start(1), g6Base(0xAD))
	g6Assert(t, store, 0xAD)
}

func TestDualLaneRebuildPort_RefusesBelowFloorAndZeroEpoch(t *testing.T) {
	for _, epoch := range []uint64{0, 4} {
		store := storage.NewBlockStore(g6Blocks, g6BlockSize)
		ln := g1Listener(t, store, g1Fresh(t))
		if err := ln.AdmitAsReplica(5); err != nil {
			t.Fatalf("AdmitAsReplica(5): %v", err)
		}
		g6Send(t, g6Port(t, store, ln), g6Start(epoch), g6Base(0xAE))
		g6Assert(t, store, 0xAE)
	}
}

func TestDualLaneRebuildPort_AcceptsCurrentReplicaEpoch(t *testing.T) {
	store := storage.NewBlockStore(g6Blocks, g6BlockSize)
	ln := g1Listener(t, store, g1Fresh(t))
	if err := ln.AdmitAsReplica(5); err != nil {
		t.Fatalf("AdmitAsReplica(5): %v", err)
	}
	g6Send(t, g6Port(t, store, ln), g6Start(5), g6Base(0xAF))
	if !g6Applied(t, store, 0xAF, 2*time.Second) {
		t.Fatalf("a current-epoch sender was refused on a replica")
	}
}

// A fence must also stop a session admitted before it.
func TestDualLaneRebuildPort_FenceStopsAdmittedSession(t *testing.T) {
	store := storage.NewBlockStore(g6Blocks, g6BlockSize)
	ln := g1Listener(t, store, g1Fresh(t))
	if err := ln.AdmitAsReplica(5); err != nil {
		t.Fatalf("AdmitAsReplica(5): %v", err)
	}
	conn := g6Port(t, store, ln)
	g6Send(t, conn, g6Start(5), g6Base(0xB0))
	if !g6Applied(t, store, 0xB0, 2*time.Second) {
		t.Fatalf("first base block not applied")
	}
	if err := ln.FenceAsPrimary(6); err != nil {
		t.Fatalf("FenceAsPrimary(6): %v", err)
	}
	g6Send(t, conn, g6Base(0xB1))
	g6Assert(t, store, 0xB1)
}

func TestDualLaneRebuildPort_RefusesZeroEpochAtFloorZero(t *testing.T) {
	store := storage.NewBlockStore(g6Blocks, g6BlockSize)
	ln, err := NewReplicaListenerWithApplyHook("127.0.0.1:0", store, nil)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	ln.Serve()
	t.Cleanup(ln.Stop)
	g6Send(t, g6Port(t, store, ln), g6Start(0), g6Base(0xB2))
	g6Assert(t, store, 0xB2)
}
