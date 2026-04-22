// Ownership: sw unit + component tests for SendTargets discovery.
// T2 ckpt 9 mechanism port from V2 discovery.go.
package iscsi_test

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func TestEncodeDiscoveryTargets_OneTarget(t *testing.T) {
	got := iscsi.EncodeDiscoveryTargets([]iscsi.DiscoveryTarget{
		{Name: "iqn.example:t1", Address: "127.0.0.1:3260,1"},
	})
	want := "TargetName=iqn.example:t1\x00TargetAddress=127.0.0.1:3260,1\x00"
	if string(got) != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}

func TestEncodeDiscoveryTargets_Empty(t *testing.T) {
	if got := iscsi.EncodeDiscoveryTargets(nil); got != nil {
		t.Fatalf("nil input → %q, want nil", got)
	}
}

func TestEncodeDiscoveryTargets_OmitsEmptyAddress(t *testing.T) {
	got := iscsi.EncodeDiscoveryTargets([]iscsi.DiscoveryTarget{
		{Name: "iqn.example:t1"},
	})
	if !bytes.Contains(got, []byte("TargetName=iqn.example:t1\x00")) {
		t.Fatalf("missing TargetName: %q", got)
	}
	if bytes.Contains(got, []byte("TargetAddress=")) {
		t.Fatalf("empty Address should not emit TargetAddress: %q", got)
	}
}

func TestHandleTextRequest_SendTargetsAll(t *testing.T) {
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpTextReq)
	p := iscsi.NewParams()
	p.Set("SendTargets", "All")
	req.DataSegment = p.Encode()

	targets := []iscsi.DiscoveryTarget{
		{Name: "iqn.a", Address: "1.1.1.1:3260,1"},
		{Name: "iqn.b", Address: "1.1.1.2:3260,1"},
	}
	resp := iscsi.HandleTextRequest(req, targets)
	if resp.Opcode() != iscsi.OpTextResp {
		t.Fatalf("opcode=0x%02x want TextResp", resp.Opcode())
	}
	body := string(resp.DataSegment)
	if !strings.Contains(body, "TargetName=iqn.a") || !strings.Contains(body, "TargetName=iqn.b") {
		t.Fatalf("All did not emit both targets: %q", body)
	}
}

func TestHandleTextRequest_SendTargetsByName(t *testing.T) {
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpTextReq)
	p := iscsi.NewParams()
	p.Set("SendTargets", "iqn.b")
	req.DataSegment = p.Encode()

	targets := []iscsi.DiscoveryTarget{
		{Name: "iqn.a", Address: "1.1.1.1:3260,1"},
		{Name: "iqn.b", Address: "1.1.1.2:3260,1"},
	}
	resp := iscsi.HandleTextRequest(req, targets)
	body := string(resp.DataSegment)
	if !strings.Contains(body, "TargetName=iqn.b") {
		t.Fatalf("by-name did not emit iqn.b: %q", body)
	}
	if strings.Contains(body, "TargetName=iqn.a") {
		t.Fatalf("by-name leaked iqn.a: %q", body)
	}
}

func TestHandleTextRequest_NoSendTargets_EmptyResponse(t *testing.T) {
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpTextReq)
	resp := iscsi.HandleTextRequest(req, []iscsi.DiscoveryTarget{
		{Name: "iqn.a", Address: "1.1.1.1:3260,1"},
	})
	if len(resp.DataSegment) != 0 {
		t.Fatalf("no SendTargets key → expected empty data, got %q", resp.DataSegment)
	}
}

// Component-level: open a Discovery session against a real
// in-process Target, issue Text/SendTargets=All, verify the
// target's IQN comes back. This is the iscsiadm discovery
// pre-condition that ckpt 9 enables.
func TestT2Route_ISCSI_SendTargetsDiscovery(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	prov := testback.NewStaticProvider(rec)

	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:   "127.0.0.1:0",
		IQN:      "iqn.2026-04.example.v3:v1",
		VolumeID: "v1",
		Provider: prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndLoginOpts(t, addr, loginOptions{
		SessionType: iscsi.SessionTypeDiscovery,
		// No TargetName — Discovery sessions don't bind a target.
	})
	defer func() {
		_ = cli.conn.Close()
	}()

	// Send Text/SendTargets=All over the established session.
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpTextReq)
	req.SetOpSpecific1(iscsi.FlagF)
	req.SetInitiatorTaskTag(cli.itt)
	cli.itt++
	req.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	tparams := iscsi.NewParams()
	tparams.Set("SendTargets", "All")
	req.DataSegment = tparams.Encode()
	if err := iscsi.WritePDU(cli.conn, req); err != nil {
		t.Fatalf("write Text req: %v", err)
	}
	_ = cli.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("read Text resp: %v", err)
	}
	if resp.Opcode() != iscsi.OpTextResp {
		t.Fatalf("resp opcode=0x%02x want TextResp", resp.Opcode())
	}
	if !bytes.Contains(resp.DataSegment, []byte("TargetName=iqn.2026-04.example.v3:v1")) {
		t.Fatalf("SendTargets did not include the target's IQN: %q", resp.DataSegment)
	}
	if !bytes.Contains(resp.DataSegment, []byte("TargetAddress=")) {
		t.Fatalf("SendTargets missing TargetAddress: %q", resp.DataSegment)
	}
	// Verify the advertised address is loopback (matches our bind).
	if !bytes.Contains(resp.DataSegment, []byte("127.0.0.1")) {
		t.Fatalf("expected loopback in TargetAddress: %q", resp.DataSegment)
	}
	// Verify it includes the portal-group tag.
	if !bytes.Contains(resp.DataSegment, []byte(",1")) {
		t.Fatalf("expected portal-group tag '1': %q", resp.DataSegment)
	}
	_ = net.IPv4zero // silence unused-import linter if net needed elsewhere
}
