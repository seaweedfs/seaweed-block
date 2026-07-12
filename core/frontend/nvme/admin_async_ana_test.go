package nvme_test

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

type mutableANAEventProvider struct {
	state       atomic.Uint32
	changeCount atomic.Uint64
	calls       atomic.Uint32
}

func newMutableANAEventProvider(state nvme.ANAState, changeCount uint64) *mutableANAEventProvider {
	p := &mutableANAEventProvider{}
	p.state.Store(uint32(state))
	p.changeCount.Store(changeCount)
	return p
}

func (p *mutableANAEventProvider) ANAState() nvme.ANAState {
	return nvme.ANAState(p.state.Load())
}

func (p *mutableANAEventProvider) ANAGroupID() uint32 { return 1 }

func (p *mutableANAEventProvider) ANAChangeCount() uint64 {
	p.calls.Add(1)
	return p.changeCount.Load()
}

func TestNVMeIdentifyController_OAESANAChangeAdvertisedWithProvider(t *testing.T) {
	_, cli := newANAHarness(t, newMutableANAEventProvider(nvme.ANAOptimized, 1))

	status, data := cli.adminIdentify(t, 0x01, 0)
	expectStatusSuccess(t, status, "Identify Controller")

	oaes := idCtrlU32LE(t, data, idCtrlOffsetOAES)
	if oaes != 1<<11 {
		t.Fatalf("OAES=0x%08x want only ANA Change Notice bit 11", oaes)
	}
	if !oaesBit(t, data, 11) {
		t.Fatal("OAES ANA Change Notice bit not set with ANA provider")
	}
}

func TestNVMeAER_CompletesOnANAChangeNotice(t *testing.T) {
	ana := newMutableANAEventProvider(nvme.ANAOptimized, 7)
	_, cli := newANAHarness(t, ana)

	aerCID := cli.adminAER(t)
	_ = cli.admin.(*net.TCPConn).SetReadDeadline(time.Now().Add(2 * time.Second))
	waitForANAChangeCountRead(t, ana)
	ana.changeCount.Store(8)

	resp := recvCapsuleResp(t, cli.adminR)
	if resp.CID != aerCID {
		t.Fatalf("AER completion CID=%d want %d", resp.CID, aerCID)
	}
	expectStatusSuccess(t, resp.Status, "AER ANA Change Notice")

	const wantDW0 = uint32(0x02) | uint32(0x03)<<8 | uint32(0x0c)<<16
	if resp.DW0 != wantDW0 {
		t.Fatalf("AER DW0=0x%08x want Notice/ANAChange/ANALogPage 0x%08x", resp.DW0, wantDW0)
	}
	_ = cli.admin.(*net.TCPConn).SetReadDeadline(time.Time{})
}

func waitForANAChangeCountRead(t *testing.T, ana *mutableANAEventProvider) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ana.calls.Load() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not read ANA change count baseline")
}
