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

// A host fenced as primary must refuse rebuild traffic from an older epoch.
// The G1 and G2 checks live in ReplicaListener; the dual-lane rebuild port
// (--recovery-mode=dual-lane) is served by recovery.ReplicaBridge, which sees
// no floor, role or epoch. The dual-lane SessionStart frame has no epoch field
// at all, so a sender from epoch 1 looks the same as any other sender.
//
// This test is expected to FAIL until the port is fenced.
func TestDualLaneRebuildPort_RefusesStaleSenderAfterFence(t *testing.T) {
	const blocks, blockSize = 8, 4096
	store := storage.NewBlockStore(blocks, blockSize)

	ln := g1Listener(t, store, g1Fresh(t))
	if err := ln.FenceAsPrimary(5); err != nil {
		t.Fatalf("FenceAsPrimary(5): %v", err)
	}
	before := append([]byte(nil), g1Block(t, store)...)

	dualLane, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = dualLane.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go recovery.NewReplicaBridge(store).AcceptDualLaneLoop(ctx, dualLane)

	conn, err := net.DialTimeout("tcp", dualLane.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial dual-lane port: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// The frames of the dual-lane protocol, written by hand because the
	// encoders are private to the recovery package:
	// [1 byte type][4 byte big-endian length][payload].
	frame := func(kind byte, payload []byte) []byte {
		out := make([]byte, 5, 5+len(payload))
		out[0] = kind
		binary.BigEndian.PutUint32(out[1:], uint32(len(payload)))
		return append(out, payload...)
	}
	start := make([]byte, 28) // session 1, from LSN 0, target LSN 0, block count
	binary.BigEndian.PutUint64(start[0:], 1)
	binary.BigEndian.PutUint32(start[24:], blocks)
	block := make([]byte, 4+blockSize) // LBA 0, then the block bytes
	copy(block[4:], bytes.Repeat([]byte{0xAB}, blockSize))

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	// A refusal may close the connection before these writes finish, so a write
	// error is acceptable here; the assertion below is on the store.
	if _, err := conn.Write(append(frame(1, start), frame(2, block)...)); err != nil {
		t.Logf("write ended early, as a refusal would: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := g1Block(t, store); !bytes.Equal(got, before) {
			t.Fatalf("a sender with no epoch wrote LBA 0 through the dual-lane port after FenceAsPrimary(5)")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
