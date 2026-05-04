// Ownership: QA (Batch 11 A-tier Phase 3 per
// sw-block/design/v3-phase-15-t2-batch-11-test-skeleton.md §A11.3).
// sw may NOT modify this file without architect approval via §8B.4
// Discovery Bridge.
//
// Maps to ledger row:
//   PCDD-NVME-FABRIC-CNTLID-ECHO-001
//
// sw's identify_test.go covers:
//   - Happy-path CNTLID echo (CNTLIDEchoesConnect)
//   - IOConnect_UnknownCNTLID_Rejected (SCT=1 SC=0x82)
//   - AdminConnect_PresetCNTLID_Rejected (SCT=1 SC=0x80)
//   - AdminQueue_RejectsIOOpcode / IOQueue_RejectsAdminOpcode
//
// This file adds the QA-owned allocator invariants sw did not pin:
//   - Multiple hosts on the same Target receive DISTINCT CNTLIDs
//     (allocator cross-host isolation — hosts must not collide)
//   - CNTLID is monotonically increasing within a Target lifetime
//     (mirror of V2 TestQA_CNTLID_MonotonicallyIncreasing; the
//     allocator semantic exists in target.go:allocAdminController
//     but had no test pin)

package nvme_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// targetBare builds a Target with no auto-connected client — the
// tests below open multiple admin sessions from different hosts
// against a single Target, which is the shape targetForNVMe's
// auto-Connect helper cannot express.
func targetBare(t *testing.T, subsysNQN, volumeID string) (*nvme.Target, string) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: volumeID, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	prov := testback.NewStaticProvider(rec)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: subsysNQN,
		VolumeID:  volumeID,
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}
	t.Cleanup(func() { _ = tg.Close() })
	return tg, addr
}

// connectAdmin opens one admin queue session (QID=0) from the given
// host against addr. Returns the assigned CNTLID.
func connectAdmin(t *testing.T, addr, subsysNQN, hostNQN string) (uint16, *nvmeClient) {
	t.Helper()
	cli := dialAndConnectOpts(t, addr, connectOptions{
		SubNQN:      subsysNQN,
		HostNQN:     hostNQN,
		SkipIOQueue: true, // allocator tests only need admin
	})
	t.Cleanup(func() { cli.close() })
	return cli.cntlID, cli
}

// --- QA A11.3 — CNTLID allocator invariants (port plan R2) ---

func TestT2V2Port_NVMe_Fabric_MultipleHostsGetDistinctCNTLIDs(t *testing.T) {
	_, addr := targetBare(t, "nqn.2026-04.m01:sub1", "v1")

	hosts := []string{
		"nqn.2026-04.host-alpha",
		"nqn.2026-04.host-beta",
		"nqn.2026-04.host-gamma",
		"nqn.2026-04.host-delta",
	}
	seen := make(map[uint16]string, len(hosts))
	for _, h := range hosts {
		cid, _ := connectAdmin(t, addr, "nqn.2026-04.m01:sub1", h)
		if cid == 0 || cid == 0xFFFF {
			t.Fatalf("host %q: allocated CNTLID=0x%04x is a reserved sentinel", h, cid)
		}
		if prior, dup := seen[cid]; dup {
			t.Fatalf("CNTLID collision: host %q and %q both allocated CNTLID=%d — "+
				"allocator reuse within a Target lifetime violates R2+monotonic invariant",
				prior, h, cid)
		}
		seen[cid] = h
	}
}

func TestT2V2Port_NVMe_Fabric_CNTLIDMonotonicallyIncreasing(t *testing.T) {
	// Mirror of V2 TestQA_CNTLID_MonotonicallyIncreasing — pins
	// target.go:allocAdminController's "monotonic; never reuses"
	// semantic as a lasting invariant. Without this test, a future
	// refactor could introduce a circular allocator (e.g., "reuse
	// after release") that silently breaks host-side CNTLID
	// assumptions.
	_, addr := targetBare(t, "nqn.2026-04.m01:sub1", "v1")

	const n = 6
	cids := make([]uint16, 0, n)
	for i := 0; i < n; i++ {
		cid, _ := connectAdmin(t, addr, "nqn.2026-04.m01:sub1",
			// Distinct HostNQN per iteration — keeps each Connect
			// eligible for a fresh allocation path (the allocator
			// does not dedupe by HostNQN per port plan §N2).
			nqn("host", i))
		cids = append(cids, cid)
	}

	for i := 1; i < len(cids); i++ {
		if cids[i] <= cids[i-1] {
			t.Fatalf("CNTLID not monotonically increasing: %v — allocator reuse/reset detected",
				cids)
		}
	}
}

func TestT2V2Port_NVMe_Fabric_CNTLIDSeparateTargetsStartFresh(t *testing.T) {
	// Allocator is Target-scoped, not process-scoped: two
	// independent Targets must each start their CNTLID sequence
	// from the same initial value (typically 1). This pins the
	// boundary so a future shared-state bug (e.g., package-level
	// global counter) would fire.
	_, addr1 := targetBare(t, "nqn.2026-04.m01:tgt1", "v1")
	_, addr2 := targetBare(t, "nqn.2026-04.m01:tgt2", "v1")

	cid1, _ := connectAdmin(t, addr1, "nqn.2026-04.m01:tgt1", "nqn.host1")
	cid2, _ := connectAdmin(t, addr2, "nqn.2026-04.m01:tgt2", "nqn.host1")

	if cid1 != cid2 {
		t.Fatalf("first-Connect CNTLID differs across independent Targets: addr1=%d addr2=%d — "+
			"allocator may be sharing state across Target instances", cid1, cid2)
	}
}

// nqn builds a test HostNQN string with a numeric suffix.
func nqn(prefix string, i int) string {
	const digits = "0123456789"
	return "nqn.2026-04.test:" + prefix + string(digits[i%10])
}
