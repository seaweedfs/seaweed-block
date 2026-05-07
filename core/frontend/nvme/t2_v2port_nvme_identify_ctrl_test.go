// Ownership: QA (Batch 11 A-tier Phase 3 per
// sw-block/design/v3-phase-15-t2-batch-11-test-skeleton.md §A11.2).
// sw may NOT modify this file without architect approval via §8B.4
// Discovery Bridge.
//
// Maps to ledger row:
//   PCDD-NVME-IDENTIFY-CTRL-ADVERTISED-LIST-001
//
// Mirror of iSCSI VPD 0x00 "advertised ≡ implemented" pin (N3 /
// port plan R4). Identify Controller advertises several optional
// capability bitmaps (OACS, OAES, ONCS, ANA*). Batch 11 implements
// NONE of them, so every bit MUST be 0. If a future sw PR adds a
// capability bit without wiring its dispatch path, this test fires.
//
// sw's identify_test.go covers:
//   - ONCS == 0 (TestT2Batch11a_IdentifyController_ONCSAllZero)
//   - ANA fields == 0 (TestT2Batch11a_IdentifyController_ANAFieldsAllZero)
//   - AERL / OFCS / SubNQN shape (D6/D7/D8)
//
// This file adds the bitmaps sw did not enumerate: OACS + OAES,
// plus the OAES/ANA cross-consistency pin.

package nvme_test

import (
	"testing"
)

// --- QA A11.2 — Advertised ≡ Implemented on all capability bitmaps ---

func TestT2V2Port_NVMe_IdentifyCtrl_OACSAllBitsZero(t *testing.T) {
	_, cli := targetForNVMe(t, "nqn.2026-04.m01:sub1", "v1")
	status, data := cli.adminIdentify(t, 0x01 /* Controller */, 0)
	if status != 0 {
		t.Fatalf("Identify Controller non-success: 0x%04x", status)
	}

	oacs := idCtrlU16LE(t, data, idCtrlOffsetOACS)
	if oacs != 0 {
		// Enumerate which bits fired for actionable diagnostics.
		t.Fatalf("OACS=0x%04x; want 0 in Batch 11 (no optional admin commands implemented — "+
			"Security=bit0, Format=bit1, Firmware=bit2, NSMgmt=bit3, SelfTest=bit4, "+
			"Directives=bit5, NVMeMI=bit6, Virtualization=bit7, DoorbellBufCfg=bit8, LBAStatus=bit9)",
			oacs)
	}
}

func TestT2V2Port_NVMe_IdentifyCtrl_OAESAllBitsZero(t *testing.T) {
	_, cli := targetForNVMe(t, "nqn.2026-04.m01:sub1", "v1")
	status, data := cli.adminIdentify(t, 0x01, 0)
	if status != 0 {
		t.Fatalf("Identify Controller non-success: 0x%04x", status)
	}

	oaes := idCtrlU32LE(t, data, idCtrlOffsetOAES)
	if oaes != 0 {
		// Critical bit enumeration — sw AER handler returns stub
		// success per feedback #2 (non-blocking park) but MUST
		// NOT advertise any event source. If this fires, some OAES
		// bit was set without wiring an event producer.
		t.Fatalf("OAES=0x%08x; want 0 in Batch 11 (AER parks but advertises no events — "+
			"NamespaceAttrNotice=bit8, FirmwareActivation=bit9, ANAChange=bit11, "+
			"PredictableLatency=bit12, LBAStatusInfo=bit13, EnduranceGroup=bit14, "+
			"NormalNVMSubsysShutdown=bit15, DiscoveryLogChange=bit31)",
			oaes)
	}
}

func TestT2V2Port_NVMe_IdentifyCtrl_OAESANACrossConsistency(t *testing.T) {
	// OAES bit 11 is ANA Change Notice, an async event source. It is
	// stronger than ANA log support and must not be advertised unless an event
	// producer exists. ANA Identify fields may become non-zero before OAES bit
	// 11; the reverse is invalid.
	_, cli := targetForNVMe(t, "nqn.2026-04.m01:sub1", "v1")
	_, data := cli.adminIdentify(t, 0x01, 0)

	anaChangeNoticeBit := oaesBit(t, data, 11)
	cmicANA := cmicANASupported(t, data)
	anacapByte := anacap(t, data)
	anaGrpMax, numGrp := anaGroupFields(t, data)

	anaFieldsAllZero := !cmicANA && anacapByte == 0 && anaGrpMax == 0 && numGrp == 0

	if anaChangeNoticeBit && anaFieldsAllZero {
		t.Fatal("OAES bit 11 (ANA Change Notice) set but ANA fields all zero — " +
			"advertising an event source that cannot exist (inconsistent)")
	}
	// Both zero (no ANA) and ANA-without-OAES are valid. OAES-without-ANA is
	// the invalid combination guarded above.
}
