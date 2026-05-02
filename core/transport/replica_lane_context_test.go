package transport

import (
	"net"
	"testing"
	"time"
)

type recordingApplyHook struct {
	calls chan string
}

func newRecordingApplyHook() *recordingApplyHook {
	return &recordingApplyHook{calls: make(chan string, 8)}
}

func (h *recordingApplyHook) ApplyRecovery(lineage RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	h.calls <- "recovery"
	return nil
}

func (h *recordingApplyHook) ApplyLive(lineage RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	h.calls <- "live"
	return nil
}

func (h *recordingApplyHook) expect(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-h.calls:
		if got != want {
			t.Fatalf("apply lane = %s, want %s", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s apply", want)
	}
}

func runReplicaHandlerPipe(t *testing.T, hook ApplyHook) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	r := &ReplicaListener{applyHook: hook}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.handleConn(server)
	}()
	t.Cleanup(func() {
		_ = client.Close()
		<-done
	})
	return client
}

func TestReplicaListener_RecoveryLaneStart_TargetLSN1_RoutesRecovery(t *testing.T) {
	hook := newRecordingApplyHook()
	conn := runReplicaHandlerPipe(t, hook)
	lineage := RecoveryLineage{SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}

	if err := WriteMsg(conn, MsgRecoveryLaneStart, EncodeLineage(lineage)); err != nil {
		t.Fatalf("write recovery lane start: %v", err)
	}
	if err := WriteMsg(conn, MsgShipEntry, EncodeShipEntry(ShipEntry{
		Lineage: lineage,
		LBA:     5,
		LSN:     30,
		Data:    []byte{0xAA},
	})); err != nil {
		t.Fatalf("write recovery ship entry: %v", err)
	}

	hook.expect(t, "recovery")
}

func TestReplicaListener_DefaultMsgShipEntry_TargetLSNHigh_RoutesLive(t *testing.T) {
	hook := newRecordingApplyHook()
	conn := runReplicaHandlerPipe(t, hook)
	lineage := RecoveryLineage{SessionID: 9, Epoch: 1, EndpointVersion: 1, TargetLSN: 100}

	if err := WriteMsg(conn, MsgShipEntry, EncodeShipEntry(ShipEntry{
		Lineage: lineage,
		LBA:     5,
		LSN:     50,
		Data:    []byte{0xBB},
	})); err != nil {
		t.Fatalf("write live ship entry: %v", err)
	}

	hook.expect(t, "live")
}
