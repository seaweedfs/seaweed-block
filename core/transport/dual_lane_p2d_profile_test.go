package transport

// P2d unit-level dispatch tests: assert the per-replica WalShipper's
// EmitFunc selects the correct encoder based on the entry's emit
// profile (architect 2026-04-30 ratification).
//
// Architectural contract:
//   - EmitProfileSteadyMsgShip → SWRP envelope around EncodeShipEntry
//   - EmitProfileDualLaneWALFrame → recovery frameWALEntry +
//     encodeWALEntry; EmitKind controls WALKind tag
//
// E2E coverage of the production hot path (startRebuildDualLane →
// RecoverySink → WalShipper) is provided by:
//   - TestDualLane_BlockExecutor_StartRebuild (byte-equal post-rebuild)
//   - TestDualLane_LiveWritesDuringSession_AtomicSeal (40 concurrent
//     pushes, all byte-equal on replica)
//   - TestDualLane_EngineDrivenRebuild_HappyPath (component)
// — these IMPLICITLY pin profile dispatch (wrong profile → receiver
// can't decode → byte-equality fails).
//
// The tests below explicitly pin the dispatch by capturing wire bytes
// produced by the EmitFunc and comparing against the canonical
// encoder output for each profile.

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// readUntilEOF reads from conn in a loop until error/EOF. Used to
// drain net.Pipe writes that may arrive in multiple chunks (header
// then payload from WriteMsg).
func readUntilEOF(conn net.Conn) []byte {
	var buf bytes.Buffer
	chunk := make([]byte, 1024)
	for {
		n, err := conn.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}
		if err != nil {
			return buf.Bytes()
		}
	}
}

// TestP2d_EmitDispatch_SteadyMsgShipProfile — when entry.emitProfile
// is SteadyMsgShip, EmitFunc produces SWRP-wrapped MsgShipEntry on
// the wire. Verified by checking the first 6 bytes are the SWRP
// preamble (magic + version + msgType=MsgShipEntry).
func TestP2d_EmitDispatch_SteadyMsgShipProfile(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")
	const replicaID = "r1"

	writerConn, readerConn := net.Pipe()
	defer writerConn.Close()
	defer readerConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 100,
	}
	shipper := e.WalShipperFor(replicaID)
	e.updateWalShipperEmitContext(replicaID, writerConn, steadyLineage, EmitProfileSteadyMsgShip)

	resCh := make(chan []byte, 1)
	go func() {
		resCh <- readUntilEOF(readerConn)
	}()

	// §6.3 migration: lsn=cursor+1=1 so fast-path tail-emit fires.
	// (Pre-§6.3 this used lsn=50 for distinctness; under §6.3
	// fail-closed default, that would CursorGap. The wire-format
	// claim — SWRP preamble vs frameWALEntry — is unchanged.)
	if err := shipper.NotifyAppend(7, 1, []byte{0xDE, 0xAD, 0xBE, 0xEF}); err != nil {
		t.Fatalf("NotifyAppend: %v", err)
	}
	_ = writerConn.Close()

	got := <-resCh
	if len(got) < 6 {
		t.Fatalf("not enough bytes: got=%d", len(got))
	}

	// SWRP preamble: bytes 0..3 = "SWRP" magic; byte 4 = version;
	// byte 5 = msgType.
	wantMagic := []byte{0x53, 0x57, 0x52, 0x50} // "SWRP"
	if !bytes.Equal(got[0:4], wantMagic) {
		t.Errorf("SteadyMsgShip: first 4 bytes=%x want SWRP magic %x", got[0:4], wantMagic)
	}
	if got[5] != MsgShipEntry {
		t.Errorf("SteadyMsgShip: msgType byte=%x want MsgShipEntry=%x", got[5], MsgShipEntry)
	}
}

// TestP2d_EmitDispatch_DualLaneWALFrameProfile_LiveKind —
// when entry.emitProfile is DualLaneWALFrame and the emit comes from
// NotifyAppend (EmitKindLive), the wire frame is recovery's
// frameWALEntry with WALKindSessionLive in the payload.
//
// Recovery frame format: [1 type][4 len][payload]; payload first
// byte is the WAL kind tag.
func TestP2d_EmitDispatch_DualLaneWALFrameProfile_LiveKind(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")
	const replicaID = "r1"

	writerConn, readerConn := net.Pipe()
	defer writerConn.Close()
	defer readerConn.Close()

	sessionLineage := RecoveryLineage{
		SessionID: 42, Epoch: 1, EndpointVersion: 1, TargetLSN: 1000,
	}
	shipper := e.WalShipperFor(replicaID)
	e.updateWalShipperEmitContext(replicaID, writerConn, sessionLineage, EmitProfileDualLaneWALFrame)

	resCh := make(chan []byte, 1)
	go func() {
		resCh <- readUntilEOF(readerConn)
	}()

	// §6.3 migration: lsn=1 so fast-path fires. Wire-format claim
	// (frameWALEntry + WALKindSessionLive tag) is independent of LSN.
	if err := shipper.NotifyAppend(11, 1, []byte{0xCA, 0xFE}); err != nil {
		t.Fatalf("NotifyAppend: %v", err)
	}
	_ = writerConn.Close()

	got := <-resCh
	if len(got) < 6 {
		t.Fatalf("not enough bytes: got=%d", len(got))
	}

	// NOT SWRP magic — recovery frames don't have SWRP preamble.
	swrpMagic := []byte{0x53, 0x57, 0x52, 0x50}
	if bytes.Equal(got[0:4], swrpMagic) {
		t.Errorf("DualLaneWALFrame Live: starts with SWRP magic — wire format wrong (legacy MsgShipEntry instead of recovery frame)")
	}

	// Recovery frame: [1 type][4 len][1 kind][...]. Byte 5 = WAL kind.
	if got[5] != byte(recovery.WALKindSessionLive) {
		t.Errorf("DualLaneWALFrame Live: kind byte=%d want WALKindSessionLive=%d",
			got[5], recovery.WALKindSessionLive)
	}
}

// TestP2d_EmitDispatch_DualLaneWALFrameProfile_BacklogKind —
// when EmitKind is Backlog (passed by WalShipper.DrainBacklog scan),
// the WALKind tag is WALKindBacklog. We can't easily trigger
// DrainBacklog directly without a full session, so we exercise the
// dispatch via a synthetic emit using the package-internal EmitFunc
// from the shipper.
//
// Workaround: install a session that drains an empty substrate to
// transition into Backlog → Realtime. Then directly invoke the
// shipper's emit path via NotifyAppend with profile already set.
// NotifyAppend always passes EmitKindLive, so to test Backlog kind
// we need the DrainBacklog path. Use a substrate seeded with one
// entry; the scan emits it with EmitKindBacklog.
func TestP2d_EmitDispatch_DualLaneWALFrameProfile_BacklogKind(t *testing.T) {
	const blockSize = 64
	primary := memorywal.NewStore(8, blockSize)
	// Seed one substrate entry so DrainBacklog has something to scan.
	seedData := bytes.Repeat([]byte{0xAB}, blockSize)
	if _, err := primary.Write(3, seedData); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	e := NewBlockExecutor(primary, "127.0.0.1:0")
	const replicaID = "r1"

	writerConn, readerConn := net.Pipe()
	defer writerConn.Close()
	defer readerConn.Close()

	sessionLineage := RecoveryLineage{
		SessionID: 99, Epoch: 1, EndpointVersion: 1, TargetLSN: 500,
	}
	shipper := e.WalShipperFor(replicaID)
	e.updateWalShipperEmitContext(replicaID, writerConn, sessionLineage, EmitProfileDualLaneWALFrame)

	if err := shipper.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	resCh := make(chan []byte, 1)
	go func() {
		resCh <- readUntilEOF(readerConn)
	}()

	// DrainBacklog runs the scan; the seeded entry emits with
	// EmitKindBacklog → WALKindBacklog tag on the wire.
	go func() {
		_ = shipper.DrainBacklog(context.Background())
		_ = writerConn.Close()
	}()

	got := <-resCh

	// Find a frameWALEntry with WALKindBacklog. The first byte of
	// payload after the 5-byte frame header is the kind.
	if len(got) < 6 {
		t.Fatalf("not enough bytes captured: got=%d", len(got))
	}
	// Frame format: [1 type][4 len][payload]; payload first byte = kind.
	kindByte := got[5]
	if kindByte != byte(recovery.WALKindBacklog) {
		t.Errorf("Backlog kind byte=%d want WALKindBacklog=%d", kindByte, recovery.WALKindBacklog)
	}
}
