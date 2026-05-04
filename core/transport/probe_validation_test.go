package transport

import (
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// T4c-1 validation tests — replica-side and primary-side lineage
// validation around the new symmetric ProbeReq + ProbeResponse pair.
//
// Replica direction (3 tests):
//   - happy: accepted lineage echoes correctly + R/S/H returned
//   - stale: rejected; conn closed without echo
//   - zeroed: decode fails; handler closes conn without echo
//
// Primary direction (3 tests):
//   - happy: full round-trip, lineage echo validated, R/S/H surfaced
//   - lineage mismatch: ErrProbeLineageMismatch surfaced via FailReason
//   - empty primary (H=0): TargetLSN floored to 1 so wire stays non-zero

// --- Replica direction ---

func TestReplica_ProbeReq_AcceptedLineage_EchoesAndReturnsRSH(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)
	// Seed replica with one block so R is non-zero.
	data := make([]byte, 4096)
	data[0] = 0xAA
	replica.ApplyEntry(0, data, 5)
	replica.Sync()

	conn, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	probeLineage := RecoveryLineage{SessionID: 11, Epoch: 1, EndpointVersion: 1, TargetLSN: 5}
	if err := WriteMsg(conn, MsgProbeReq, EncodeProbeReq(ProbeRequest{Lineage: probeLineage})); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := ReadMsg(conn)
	if err != nil {
		t.Fatalf("read resp: %v", err)
	}
	if msgType != MsgProbeResp {
		t.Fatalf("unexpected msg type 0x%02x", msgType)
	}
	resp, err := DecodeProbeResp(payload)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.Lineage != probeLineage {
		t.Errorf("echoed lineage mismatch: want=%+v got=%+v", probeLineage, resp.Lineage)
	}
	if resp.SyncedLSN == 0 {
		t.Error("expected non-zero SyncedLSN (replica seeded with one block)")
	}
}

func TestReplica_ProbeReq_StaleLineage_Rejected(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)

	// Establish a higher-epoch active mutating lineage by sending a
	// rebuild block first.
	highLineage := RecoveryLineage{SessionID: 7, Epoch: 5, EndpointVersion: 5, TargetLSN: 100}
	connA, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Close()
	data := make([]byte, 4096)
	data[0] = 0xBE
	if err := WriteMsg(connA, MsgRebuildBlock, EncodeRebuildBlock(highLineage, 0, data)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	_ = replica // confirm replica still serves

	// Stale probe at strictly older (epoch, endpointVersion).
	stale := RecoveryLineage{SessionID: 99, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	connP, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connP.Close()
	if err := WriteMsg(connP, MsgProbeReq, EncodeProbeReq(ProbeRequest{Lineage: stale})); err != nil {
		t.Fatal(err)
	}
	connP.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, _, err := ReadMsg(connP); err == nil {
		t.Fatal("stale probe must NOT receive an echo")
	}
}

func TestReplica_ProbeReq_ZeroedLineage_Rejected(t *testing.T) {
	_, _, listener := setupPrimaryReplica(t)

	conn, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Hand-craft a 32B payload that decodes to a zero-lineage —
	// the decoder will reject it BEFORE the handler runs, so the
	// handler simply closes the conn.
	zero := make([]byte, probeReqSize)
	if err := WriteMsg(conn, MsgProbeReq, zero); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, _, err := ReadMsg(conn); err == nil {
		t.Fatal("zeroed-lineage probe must NOT receive an echo")
	}
}

// --- Primary direction ---

func TestExecutor_Probe_Happy(t *testing.T) {
	primary, replica, listener := setupPrimaryReplica(t)
	// Seed both stores so probe returns nonzero R/H.
	for i := uint32(0); i < 3; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		lsn, _ := primary.Write(i, data)
		_ = replica.ApplyEntry(i, data, lsn)
	}
	primary.Sync()
	replica.Sync()

	exec := NewBlockExecutor(primary, listener.Addr())
	result := exec.Probe("r1", listener.Addr(), listener.Addr(), 7, 1, 1)
	if !result.Success {
		t.Fatalf("probe failed: %s", result.FailReason)
	}
	_, _, pH := primary.Boundaries()
	if result.PrimaryHeadLSN != pH {
		t.Errorf("PrimaryHeadLSN=%d, want %d", result.PrimaryHeadLSN, pH)
	}
}

// TestExecutor_Probe_LineageMismatch_Rejected runs a custom replica
// listener that intentionally echoes a different lineage than the
// request. Primary must surface ErrProbeLineageMismatch in
// ProbeResult.FailReason and return Success=false.
func TestExecutor_Probe_LineageMismatch_Rejected(t *testing.T) {
	primary := storage.NewBlockStore(64, 4096)
	// Seed primary so probe lineage TargetLSN is non-zero (H>0).
	data := make([]byte, 4096)
	data[0] = 1
	primary.Write(0, data)
	primary.Sync()

	// Fake replica that echoes a wrong lineage.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read whatever the primary sent (we don't care about content).
		if _, _, err := ReadMsg(conn); err != nil {
			return
		}
		// Echo a deliberately different lineage.
		bad := EncodeProbeResp(ProbeResponse{
			Lineage:   RecoveryLineage{SessionID: 999, Epoch: 999, EndpointVersion: 999, TargetLSN: 999},
			SyncedLSN: 1, WalTail: 1, WalHead: 1,
		})
		_ = WriteMsg(conn, MsgProbeResp, bad)
	}()

	exec := NewBlockExecutor(primary, ln.Addr().String())
	result := exec.Probe("r1", ln.Addr().String(), ln.Addr().String(), 7, 1, 1)
	wg.Wait()

	if result.Success {
		t.Fatal("probe must fail when replica echoes wrong lineage")
	}
	if !strings.Contains(result.FailReason, ErrProbeLineageMismatch.Error()) {
		t.Errorf("FailReason should contain ErrProbeLineageMismatch, got: %s", result.FailReason)
	}
}

// TestExecutor_Probe_EmptyPrimary_TargetLSNFloor pins the architect's
// no-zero-TargetLSN rule: when the primary has no writes (H=0), the
// transient probe lineage's TargetLSN MUST be floored to 1 — the
// wire fails closed on any zero lineage field, so a literal H=0 would
// produce an undeliverable probe.
func TestExecutor_Probe_EmptyPrimary_TargetLSNFloor(t *testing.T) {
	primary := storage.NewBlockStore(64, 4096) // empty: H=0
	_, replica, listener := setupPrimaryReplica(t)
	_ = replica

	exec := NewBlockExecutor(primary, listener.Addr())
	result := exec.Probe("r1", listener.Addr(), listener.Addr(), 7, 1, 1)
	if !result.Success {
		t.Fatalf("probe on empty primary must succeed (TargetLSN floor=1): %s", result.FailReason)
	}
}

// Sanity: ErrProbeLineageMismatch is a stable sentinel.
func TestErrProbeLineageMismatch_IsSentinel(t *testing.T) {
	if !errors.Is(ErrProbeLineageMismatch, ErrProbeLineageMismatch) {
		t.Fatal("sentinel must satisfy errors.Is reflexively")
	}
	if ErrProbeLineageMismatch.Error() == "" {
		t.Fatal("sentinel must have a message")
	}
}
