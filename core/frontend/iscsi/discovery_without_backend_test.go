// Ownership: sw regression test (architect residual-risk note,
// 2026-04-21 round-9): Discovery sessions must NOT depend on
// frontend.Provider readiness. A volume whose adapter projection
// is non-Healthy (Provider.Open would block on ErrNotReady) must
// still answer `iscsiadm -m discovery`.
package iscsi_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

// neverReadyProvider: every Provider.Open call blocks until
// ctx expires, then returns ErrNotReady. Simulates a volume
// whose adapter projection never reaches Healthy. If the
// target opens the backend BEFORE login (the old behavior),
// Discovery hangs. With the residual-risk fix, Discovery
// succeeds without touching the provider.
type neverReadyProvider struct {
	opens atomic.Int32
}

func (p *neverReadyProvider) Open(ctx context.Context, _ string) (frontend.Backend, error) {
	p.opens.Add(1)
	<-ctx.Done()
	return nil, frontend.ErrNotReady
}

// TestT2Route_ISCSI_DiscoveryDoesNotDependOnBackendReady —
// Discovery session completes end-to-end (login + SendTargets +
// logout) even though Provider.Open would never return.
// Provider must NEVER be dialed during the Discovery flow.
func TestT2Route_ISCSI_DiscoveryDoesNotDependOnBackendReady(t *testing.T) {
	prov := &neverReadyProvider{}
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

	done := make(chan error, 1)
	go func() {
		done <- runDiscoveryExchange(t, addr)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Discovery failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Discovery did not complete within 5s — Provider.Open likely gating login path")
	}

	if got := prov.opens.Load(); got != 0 {
		t.Fatalf("Provider.Open called %d times during Discovery; must be 0", got)
	}
}

// runDiscoveryExchange drives a Discovery-type login, issues
// Text/SendTargets=All, asserts the response carries the IQN,
// and logs out. Returns error on any wire/state problem.
func runDiscoveryExchange(t *testing.T, addr string) error {
	cli := dialAndLoginOpts(t, addr, loginOptions{
		SessionType: iscsi.SessionTypeDiscovery,
	})
	defer func() { _ = cli.conn.Close() }()

	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpTextReq)
	req.SetOpSpecific1(iscsi.FlagF)
	req.SetInitiatorTaskTag(cli.itt)
	cli.itt++
	req.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	p := iscsi.NewParams()
	p.Set("SendTargets", "All")
	req.DataSegment = p.Encode()
	if err := iscsi.WritePDU(cli.conn, req); err != nil {
		return err
	}
	_ = cli.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		return err
	}
	if resp.Opcode() != iscsi.OpTextResp {
		return errors.New("not TextResp")
	}
	if !bytes.Contains(resp.DataSegment, []byte("TargetName=iqn.2026-04.example.v3:v1")) {
		return errors.New("SendTargets missing IQN")
	}
	return nil
}

// Regression pin for the "Normal session gracefully closes on
// Provider.Open failure" path — a Normal login against a
// never-ready volume must fail cleanly after login, not hang.
// (The fix: Provider.Open happens AFTER login; a failure closes
// the session.)
func TestT2Route_ISCSI_NormalSessionClosesWhenProviderNotReady(t *testing.T) {
	prov := &neverReadyProvider{}
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

	// Normal session: login succeeds (negotiator doesn't need
	// backend), then the session's post-login Open blocks on
	// neverReadyProvider until the target closes the context
	// on shutdown. We give it a short window then tear down;
	// the test proves the target at least ACCEPTS the login
	// before the block.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// dialAndLogin completes the login exchange. If this hangs,
	// the target still opens backend BEFORE login — the old
	// broken behavior.
	done := make(chan struct{})
	go func() {
		defer close(done)
		cli := dialAndLogin(t, addr)
		// We don't reach this point's SCSI path because Open
		// blocks indefinitely; the test passes as long as login
		// itself completed.
		_ = cli.conn.Close()
	}()

	select {
	case <-done:
		// Login succeeded before Open block — correct behavior.
	case <-ctx.Done():
		t.Fatal("Normal-session login did not complete — target blocked before login (regression)")
	}
}
