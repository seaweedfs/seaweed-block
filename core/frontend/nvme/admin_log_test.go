package nvme_test

import (
	"encoding/binary"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type testANAProvider struct {
	state       nvme.ANAState
	groupID     uint32
	changeCount uint64
}

func (p testANAProvider) ANAState() nvme.ANAState { return p.state }
func (p testANAProvider) ANAGroupID() uint32      { return p.groupID }
func (p testANAProvider) ANAChangeCount() uint64  { return p.changeCount }

func newANAHarness(t *testing.T, p nvme.ANAProvider) (*nvme.Target, *nvmeClient) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	prov := testback.NewStaticProvider(rec)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
		Handler: nvme.HandlerConfig{
			ANA: p,
		},
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}
	cli := dialAndConnectOpts(t, addr, connectOptions{SkipIOQueue: true})
	t.Cleanup(func() {
		cli.close()
		_ = tg.Close()
	})
	return tg, cli
}

func TestNVMeANALogPage_NoProviderRejected(t *testing.T) {
	_, cli := newANAHarness(t, nil)
	status, data := cli.adminGetLogPage(t, 0x0C, 9)
	if status == 0 {
		t.Fatal("ANA log page succeeded without ANA provider")
	}
	if len(data) != 0 {
		t.Fatalf("ANA no-provider returned %d bytes, want none", len(data))
	}
}

func TestNVMeANALogPage_ReportsProviderState(t *testing.T) {
	_, cli := newANAHarness(t, testANAProvider{
		state:       nvme.ANANonOptimized,
		groupID:     1,
		changeCount: 42,
	})
	status, data := cli.adminGetLogPage(t, 0x0C, 9) // 10 dwords = 40 bytes.
	expectStatusSuccess(t, status, "GetLogPage(ANA)")
	if len(data) != 40 {
		t.Fatalf("ANA log len=%d want 40", len(data))
	}
	if got := binary.LittleEndian.Uint64(data[0:8]); got != 42 {
		t.Fatalf("ANA CHGCNT=%d want 42", got)
	}
	if got := binary.LittleEndian.Uint16(data[8:10]); got != 1 {
		t.Fatalf("ANA NGRPS=%d want 1", got)
	}
	if got := binary.LittleEndian.Uint32(data[16:20]); got != 1 {
		t.Fatalf("ANA group id=%d want 1", got)
	}
	if got := binary.LittleEndian.Uint32(data[20:24]); got != 1 {
		t.Fatalf("ANA NNSID=%d want 1", got)
	}
	if got := binary.LittleEndian.Uint64(data[24:32]); got != 42 {
		t.Fatalf("ANA group CHGCNT=%d want 42", got)
	}
	if got := data[32]; got != byte(nvme.ANANonOptimized) {
		t.Fatalf("ANA state=0x%02x want 0x%02x", got, byte(nvme.ANANonOptimized))
	}
	if got := binary.LittleEndian.Uint32(data[36:40]); got != 1 {
		t.Fatalf("ANA NSID=%d want 1", got)
	}
}

func TestNVMeANAIdentifyAndLogGroupIDsAreConsistent(t *testing.T) {
	_, cli := newANAHarness(t, testANAProvider{
		state:       nvme.ANAOptimized,
		groupID:     1,
		changeCount: 42,
	})

	status, ctrl := cli.adminIdentify(t, 0x01, 0)
	expectStatusSuccess(t, status, "Identify Controller")
	status, ns := cli.adminIdentify(t, 0x00, 1)
	expectStatusSuccess(t, status, "Identify Namespace")
	status, logData := cli.adminGetLogPage(t, 0x0C, 9)
	expectStatusSuccess(t, status, "GetLogPage(ANA)")

	anaGrpMax := binary.LittleEndian.Uint32(ctrl[344:348])
	anaGrpCount := binary.LittleEndian.Uint32(ctrl[348:352])
	nsGroupID := binary.LittleEndian.Uint32(ns[92:96])
	logGroupID := binary.LittleEndian.Uint32(logData[16:20])

	if anaGrpMax != 1 || anaGrpCount != 1 {
		t.Fatalf("Identify ANA limits mismatch: ANAGRPMAX=%d NANAGRPID=%d want 1/1", anaGrpMax, anaGrpCount)
	}
	if logGroupID == 0 || logGroupID > anaGrpMax {
		t.Fatalf("ANA log group id=%d outside advertised range 1..%d", logGroupID, anaGrpMax)
	}
	if nsGroupID != logGroupID {
		t.Fatalf("Identify NS ANAGRPID=%d does not match ANA log group id=%d", nsGroupID, logGroupID)
	}
}

func TestNVMeANALogPage_TruncatesToRequestedLength(t *testing.T) {
	_, cli := newANAHarness(t, testANAProvider{
		state:       nvme.ANAOptimized,
		groupID:     1,
		changeCount: 9,
	})
	status, data := cli.adminGetLogPage(t, 0x0C, 0) // 1 dword = 4 bytes.
	expectStatusSuccess(t, status, "GetLogPage(ANA truncated)")
	if len(data) != 4 {
		t.Fatalf("ANA truncated len=%d want 4", len(data))
	}
	if got := binary.LittleEndian.Uint32(data); got != 9 {
		t.Fatalf("truncated CHGCNT low32=%d want 9", got)
	}
}

func TestNVMeGetLogPage_UnknownLIDRejected(t *testing.T) {
	_, cli := newANAHarness(t, testANAProvider{
		state:       nvme.ANAOptimized,
		groupID:     1,
		changeCount: 1,
	})
	status, data := cli.adminGetLogPage(t, 0x7F, 0)
	if status == 0 {
		t.Fatal("unknown log page succeeded")
	}
	if len(data) != 0 {
		t.Fatalf("unknown log page returned %d bytes, want none", len(data))
	}
}
