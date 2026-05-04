package transport

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// --- Test 1: Barrier happy round-trip ---

// TestExecutor_Barrier_Happy — end-to-end round-trip against a real
// ReplicaListener. Primary registers a session, calls Barrier; the
// listener echoes the full lineage per T4b-1; Barrier returns a
// BarrierAck with Lineage + AchievedLSN validated.
func TestExecutor_Barrier_Happy(t *testing.T) {
	_, _, listener := setupPrimaryReplica(t)
	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, listener.Addr())

	lineage := shipTestLineage()
	if err := exec.registerShipSessionForTest(lineage); err != nil {
		t.Fatalf("register session: %v", err)
	}
	// Close the session's lazy-dialed conn at end so the listener's
	// handleConn can exit cleanly (same pattern as Ship tests).
	t.Cleanup(func() {
		exec.mu.Lock()
		if s := exec.sessions[lineage.SessionID]; s != nil && s.conn != nil {
			_ = s.conn.Close()
		}
		exec.mu.Unlock()
	})

	ack, err := exec.Barrier("r1", lineage, 500)
	if err != nil {
		t.Fatalf("Barrier: %v", err)
	}
	if !ack.Success {
		t.Fatalf("ack.Success=false")
	}
	if ack.Lineage != lineage {
		t.Fatalf("lineage echo mismatch: got %+v want %+v", ack.Lineage, lineage)
	}
	// Replica's initial frontier is 0 (fresh store); AchievedLSN is
	// whatever store.Sync() returns. We don't pin an exact value
	// here because the harness store has no writes — we just require
	// the Success path was taken and the lineage round-tripped.
}

// --- Test 2: Barrier timeout ---

// TestExecutor_Barrier_TimeoutFires — replica accepts the conn but
// never reads / responds; Barrier's recv deadline must fire within
// ~recoveryConnTimeout (5s) so a silent replica cannot wedge the
// caller on the TCP retransmission timeout (V2 pattern; mirrors
// T4a-2 TestExecutor_Ship_WriteDeadline_Fires).
func TestExecutor_Barrier_TimeoutFires(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// Accept one conn, drain the request, never respond.
	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		accepted <- c
		// Drain the barrier request so sendBarrierReq doesn't block;
		// then sit idle — the recv side of Barrier must time out.
		buf := make([]byte, 128)
		_, _ = c.Read(buf)
	}()
	t.Cleanup(func() {
		select {
		case c := <-accepted:
			_ = c.Close()
		default:
		}
	})

	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, ln.Addr().String())
	lineage := shipTestLineage()
	if err := exec.registerShipSessionForTest(lineage); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		exec.mu.Lock()
		if s := exec.sessions[lineage.SessionID]; s != nil && s.conn != nil {
			_ = s.conn.Close()
		}
		exec.mu.Unlock()
	})

	start := time.Now()
	_, err = exec.Barrier("r1", lineage, 99)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("Barrier returned nil after %v; expected timeout", elapsed)
	}
	// Budget: recoveryConnTimeout is 5s; allow up to 10s for OS
	// scheduling slop. If we hit 15s the deadline didn't fire.
	if elapsed > 15*time.Second {
		t.Fatalf("Barrier took %v; expected ~5s (recoveryConnTimeout). Deadline likely not firing — invariant regression.",
			elapsed)
	}
	// And: must have wedged for most of the deadline. A fast-return
	// error isn't a deadline-driven timeout.
	if elapsed < 4*time.Second {
		t.Fatalf("Barrier returned in %v — too fast to be deadline-driven", elapsed)
	}
	if !isTimeoutErr(err) && !strings.Contains(err.Error(), "barrier") {
		t.Fatalf("error does not look like a barrier timeout: %v", err)
	}
}

// --- Test 3: Barrier lineage mismatch rejection ---

// TestExecutor_Barrier_LineageMismatch_Rejected — replica sends a
// valid-decode BarrierResponse whose lineage differs from the
// primary's request. Barrier MUST return ErrBarrierLineageMismatch
// (architect round-21 uniform rule; full lineage is the authority
// identity of the ack). Log format must include peer ID + expected
// + actual tuple.
func TestExecutor_Barrier_LineageMismatch_Rejected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// Custom replica: decode the barrier request, then respond with
	// a DIFFERENT (but valid-decode-able) lineage.
	wrongLineage := RecoveryLineage{
		SessionID:       999,
		Epoch:           999,
		EndpointVersion: 999,
		TargetLSN:       999,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		// Read request (barrier req with self lineage).
		if _, _, err := ReadMsg(c); err != nil {
			return
		}
		// Echo WRONG lineage.
		payload := EncodeBarrierResp(BarrierResponse{
			Lineage:     wrongLineage,
			AchievedLSN: 42,
		})
		_ = WriteMsg(c, MsgBarrierResp, payload)
	}()

	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, ln.Addr().String())
	lineage := shipTestLineage()
	if err := exec.registerShipSessionForTest(lineage); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		exec.mu.Lock()
		if s := exec.sessions[lineage.SessionID]; s != nil && s.conn != nil {
			_ = s.conn.Close()
		}
		exec.mu.Unlock()
	})

	_, err = exec.Barrier("r1", lineage, 100)
	if !errors.Is(err, ErrBarrierLineageMismatch) {
		t.Fatalf("expected ErrBarrierLineageMismatch, got: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("custom replica goroutine didn't finish")
	}
}

// --- Test 4: Fence lineage mismatch marks failure ---

// TestExecutor_Fence_LineageMismatch_MarksFailure — the architect
// round-22 correction pin. executor.doFence previously discarded
// the BarrierResponse ("_ = resp // AchievedLSN is not used by
// fence"). After T4b-2, it MUST validate the echoed lineage and
// fire OnFenceComplete with Success=false on mismatch.
//
// This is the "first existing consumer of the extended wire respects
// the uniform rule" fence (round-22).
func TestExecutor_Fence_LineageMismatch_MarksFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// Custom replica that echoes wrong lineage for any barrier request.
	wrongLineage := RecoveryLineage{
		SessionID:       100,
		Epoch:           100,
		EndpointVersion: 100,
		TargetLSN:       100,
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				if _, _, err := ReadMsg(conn); err != nil {
					return
				}
				payload := EncodeBarrierResp(BarrierResponse{
					Lineage:     wrongLineage,
					AchievedLSN: 42,
				})
				_ = WriteMsg(conn, MsgBarrierResp, payload)
			}(c)
		}
	}()

	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, ln.Addr().String())

	resultCh := make(chan adapter.FenceResult, 1)
	exec.SetOnFenceComplete(func(r adapter.FenceResult) { resultCh <- r })

	if err := exec.Fence("r1", 7, 5, 3); err != nil {
		t.Fatalf("Fence: %v", err)
	}

	select {
	case r := <-resultCh:
		if r.Success {
			t.Fatal("Fence should have failed on lineage mismatch")
		}
		if !strings.Contains(r.FailReason, "lineage mismatch") {
			t.Fatalf("FailReason missing 'lineage mismatch': %q", r.FailReason)
		}
		// Diagnostic must include expected + actual tuples for
		// operators to diagnose asymmetric authority bugs.
		if !strings.Contains(r.FailReason, "expected=") || !strings.Contains(r.FailReason, "actual=") {
			t.Fatalf("FailReason missing expected=/actual= tuple: %q", r.FailReason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnFenceComplete never fired")
	}
}

// --- Test 5: Fence short/zeroed payload marks failure ---

// TestExecutor_Fence_ShortOrZeroedLineage_MarksFailure — doFence
// propagates the T4b-1 decode-layer failure (short / zero-valued /
// malformed payload) via the OnFenceComplete callback with
// Success=false. No silent-accept, per round-22.
//
// Tests two shapes:
//   (a) short payload (< 40B)
//   (b) zeroed lineage in a full-size payload
// Both must reach the callback as Success=false.
func TestExecutor_Fence_ShortOrZeroedLineage_MarksFailure(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "short_payload",
			payload: make([]byte, 8), // old 8-byte format — < 40B
		},
		{
			name:    "zeroed_lineage", // full 40B but lineage all zero
			payload: make([]byte, 40),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ln.Close() })

			payloadCopy := tc.payload
			go func() {
				for {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					go func(conn net.Conn) {
						defer conn.Close()
						if _, _, err := ReadMsg(conn); err != nil {
							return
						}
						_ = WriteMsg(conn, MsgBarrierResp, payloadCopy)
					}(c)
				}
			}()

			primary := storage.NewBlockStore(64, 4096)
			exec := NewBlockExecutor(primary, ln.Addr().String())

			resultCh := make(chan adapter.FenceResult, 1)
			exec.SetOnFenceComplete(func(r adapter.FenceResult) { resultCh <- r })

			if err := exec.Fence("r1", 7, 5, 3); err != nil {
				t.Fatalf("Fence: %v", err)
			}

			select {
			case r := <-resultCh:
				if r.Success {
					t.Fatalf("Fence should have failed on %s", tc.name)
				}
				if !strings.Contains(r.FailReason, "fence barrier resp") {
					t.Fatalf("FailReason should describe resp-side failure, got: %q", r.FailReason)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s: OnFenceComplete never fired", tc.name)
			}
		})
	}
}
