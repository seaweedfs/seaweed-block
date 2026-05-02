package transport

import (
	"bytes"
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"
)

// pipeConn returns a connected pair of in-memory TCP conns for preamble tests.
func pipeConn(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- nil
			return
		}
		done <- c
	}()

	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case server = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("accept timed out")
	}
	if server == nil {
		t.Fatal("listener accept failed")
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

// TestProtocol_EnvelopeRoundTrip writes one frame of every defined message
// type and verifies the reader recovers the same type + payload bytes.
func TestProtocol_EnvelopeRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		msgType byte
		payload []byte
	}{
		{"MsgShipEntry", MsgShipEntry, EncodeShipEntry(ShipEntry{
			Lineage: RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 10},
			LBA:     7,
			LSN:     10,
			Data:    bytes.Repeat([]byte{0xAB}, 4096),
		})},
		{"MsgProbeReq", MsgProbeReq, EncodeProbeReq(ProbeRequest{
			Lineage: RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 10},
		})},
		{"MsgProbeResp", MsgProbeResp, EncodeProbeResp(ProbeResponse{
			Lineage:   RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 10},
			SyncedLSN: 100, WalTail: 1, WalHead: 100,
		})},
		{"MsgRebuildBlock", MsgRebuildBlock, EncodeRebuildBlock(
			RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 5},
			3, bytes.Repeat([]byte{0xCD}, 4096))},
		{"MsgRebuildDone", MsgRebuildDone, nil},
		{"MsgBarrierReq", MsgBarrierReq, nil},
		{"MsgBarrierResp", MsgBarrierResp, make([]byte, 8)},
		{"MsgRecoveryLaneStart", MsgRecoveryLaneStart, EncodeLineage(
			RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1},
		)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, s := pipeConn(t)

			errCh := make(chan error, 1)
			go func() { errCh <- WriteMsg(c, tc.msgType, tc.payload) }()

			gotType, gotPayload, err := ReadMsg(s)
			if err != nil {
				t.Fatalf("ReadMsg: %v", err)
			}
			if err := <-errCh; err != nil {
				t.Fatalf("WriteMsg: %v", err)
			}
			if gotType != tc.msgType {
				t.Fatalf("type: got 0x%02x want 0x%02x", gotType, tc.msgType)
			}
			if !bytes.Equal(gotPayload, tc.payload) {
				t.Fatalf("payload mismatch")
			}
		})
	}
}

// TestProtocol_MagicRejected_WrongMagic fabricates a frame with a bogus magic
// and expects ReadMsg to fail-closed.
func TestProtocol_MagicRejected_WrongMagic(t *testing.T) {
	c, s := pipeConn(t)

	// Construct a valid-looking preamble except for the magic.
	hdr := make([]byte, preambleLen)
	binary.BigEndian.PutUint32(hdr[0:4], 0xDEADBEEF)
	hdr[4] = ProtocolVersion
	hdr[5] = MsgShipEntry
	// length=0
	go func() {
		_, _ = c.Write(hdr)
	}()

	_, _, err := ReadMsg(s)
	if err == nil {
		t.Fatal("expected error on bad magic, got nil")
	}
	if !strings.Contains(err.Error(), "bad magic") {
		t.Fatalf("expected 'bad magic' error, got: %v", err)
	}
}

// TestProtocol_VersionRejected_UnknownVersion fabricates a frame with a
// known magic but unsupported version byte; reader must reject.
func TestProtocol_VersionRejected_UnknownVersion(t *testing.T) {
	c, s := pipeConn(t)

	hdr := make([]byte, preambleLen)
	binary.BigEndian.PutUint32(hdr[0:4], MagicV3Repl)
	hdr[4] = 99 // future / unsupported
	hdr[5] = MsgShipEntry
	go func() {
		_, _ = c.Write(hdr)
	}()

	_, _, err := ReadMsg(s)
	if err == nil {
		t.Fatal("expected error on unknown version, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("expected 'unsupported version' error, got: %v", err)
	}
}
