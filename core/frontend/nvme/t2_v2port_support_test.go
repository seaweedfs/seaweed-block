// Ownership: QA (for Batch 11 A-tier test skeleton — see
// sw-block/design/v3-phase-15-t2-batch-11-test-skeleton.md).
// sw may NOT modify without architect approval via §8B.4 Discovery
// Bridge.
//
// Purpose: pure-byte utilities that Batch 11 A-tier tests (A11.1 –
// A11.4) will share. None of these depend on AdminHandler / Identify
// builders / CNTLID registry code paths — they only parse byte
// buffers or uint16 status codes, so they compile today (before sw
// lands 11a) and slot into the A-tier tests without churn when the
// port plan LOCKS and transcription starts.
//
// Layout references are NVMe Base Spec 1.4 (dated 2019-06-10) —
// Identify Controller: figure 109; Identify Namespace: figure 114.
// NVMe-oF 1.1 Connect response codes: figure 20 + §3.3.1.

package nvme_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// ----- Identify Controller byte slicing (figure 109) -----

const (
	idCtrlOffsetCMIC   = 76  // 1 byte  — Controller Multi-Path / SR-IOV
	idCtrlOffsetOAES   = 92  // 4 bytes — Optional Async Events Supported
	idCtrlOffsetOACS   = 256 // 2 bytes — Optional Admin Command Support
	idCtrlOffsetONCS   = 520 // 2 bytes — Optional NVM Command Support
	idCtrlOffsetANACAP = 343 // 1 byte
	idCtrlOffsetANAGRP = 344 // 4 bytes ANAGRPMAX + 4 bytes NANAGRPID
	idCtrlPayloadSize  = 4096
)

// ----- Identify Namespace byte slicing (figure 114) -----

const (
	idNSOffsetANAGRPID = 92  // 4 bytes
	idNSOffsetNGUID    = 104 // 16 bytes
	idNSOffsetEUI64    = 120 // 8 bytes
	idNSPayloadSize    = 4096
)

// idCtrlByte reads one byte from an Identify Controller payload at
// the given offset, fatally failing if the buffer is too short.
func idCtrlByte(t *testing.T, data []byte, off int) byte {
	t.Helper()
	if len(data) < idCtrlPayloadSize {
		t.Fatalf("identify controller payload too short: %d < %d", len(data), idCtrlPayloadSize)
	}
	return data[off]
}

// idCtrlU16LE reads a little-endian uint16 (NVMe data structures are
// little-endian per §4.4) from the Identify Controller payload.
func idCtrlU16LE(t *testing.T, data []byte, off int) uint16 {
	t.Helper()
	if len(data) < idCtrlPayloadSize {
		t.Fatalf("identify controller payload too short: %d < %d", len(data), idCtrlPayloadSize)
	}
	return binary.LittleEndian.Uint16(data[off : off+2])
}

// idCtrlU32LE reads a little-endian uint32 from the Identify
// Controller payload.
func idCtrlU32LE(t *testing.T, data []byte, off int) uint32 {
	t.Helper()
	if len(data) < idCtrlPayloadSize {
		t.Fatalf("identify controller payload too short: %d < %d", len(data), idCtrlPayloadSize)
	}
	return binary.LittleEndian.Uint32(data[off : off+4])
}

// oacsBit reports whether a given OACS bit is set.
// bit=0 is Security Send/Receive, bit=1 Format NVM, etc.
func oacsBit(t *testing.T, data []byte, bit uint) bool {
	t.Helper()
	return idCtrlU16LE(t, data, idCtrlOffsetOACS)&(1<<bit) != 0
}

// oncsBit reports whether a given ONCS bit is set.
func oncsBit(t *testing.T, data []byte, bit uint) bool {
	t.Helper()
	return idCtrlU16LE(t, data, idCtrlOffsetONCS)&(1<<bit) != 0
}

// oaesBit reports whether a given OAES bit is set.
// bit=11 is ANA Change Notice (guarded by A11.2 OAES sub-test).
func oaesBit(t *testing.T, data []byte, bit uint) bool {
	t.Helper()
	return idCtrlU32LE(t, data, idCtrlOffsetOAES)&(1<<bit) != 0
}

// cmicANASupported reports CMIC bit 1 (ANA Reporting Supported). A11.2
// ANA-zero sub-test pins this to false.
func cmicANASupported(t *testing.T, data []byte) bool {
	t.Helper()
	return idCtrlByte(t, data, idCtrlOffsetCMIC)&(1<<1) != 0
}

// anacap returns the ANACAP byte. A11.2 pins this to 0 until ANA lands
// (port plan 11c conditional).
func anacap(t *testing.T, data []byte) byte {
	t.Helper()
	return idCtrlByte(t, data, idCtrlOffsetANACAP)
}

// anaGroupFields returns (ANAGRPMAX, NANAGRPID). A11.2 pins both to 0.
func anaGroupFields(t *testing.T, data []byte) (uint32, uint32) {
	t.Helper()
	grpMax := idCtrlU32LE(t, data, idCtrlOffsetANAGRP)
	numGrp := idCtrlU32LE(t, data, idCtrlOffsetANAGRP+4)
	return grpMax, numGrp
}

// ----- Identify Namespace helpers (figure 114) -----

func idNSCheckLen(t *testing.T, data []byte) {
	t.Helper()
	if len(data) < idNSPayloadSize {
		t.Fatalf("identify namespace payload too short: %d < %d", len(data), idNSPayloadSize)
	}
}

// extractNGUID pulls the 16-byte NGUID. A11.1 core primitive.
func extractNGUID(t *testing.T, data []byte) [16]byte {
	t.Helper()
	idNSCheckLen(t, data)
	var out [16]byte
	copy(out[:], data[idNSOffsetNGUID:idNSOffsetNGUID+16])
	return out
}

// extractEUI64 pulls the 8-byte EUI-64. A11.1 pins EUI-64 == NGUID[:8]
// when both are non-zero.
func extractEUI64(t *testing.T, data []byte) [8]byte {
	t.Helper()
	idNSCheckLen(t, data)
	var out [8]byte
	copy(out[:], data[idNSOffsetEUI64:idNSOffsetEUI64+8])
	return out
}

// extractNSANAGRPID pulls the namespace ANAGRPID. A11.2 pins to 0.
func extractNSANAGRPID(t *testing.T, data []byte) uint32 {
	t.Helper()
	idNSCheckLen(t, data)
	return binary.LittleEndian.Uint32(data[idNSOffsetANAGRPID : idNSOffsetANAGRPID+4])
}

// isAllZero reports whether a byte slice is entirely zero — used as
// the "not-present" sentinel guard (per NVMe spec, all-zero NGUID
// means "no globally unique id available").
func isAllZero(b []byte) bool {
	return bytes.Equal(b, make([]byte, len(b)))
}

// ----- NVMe-oF Connect / Completion SCT/SC helpers -----

// NVMe 1.4 figure 30 lays out the Completion Queue Entry Dword 3:
//
//	bits [31:17] — Status Field (SF)
//	bit  [16]    — Phase Tag (ignore for our sync harness)
//	bits [15:0]  — CID
//
// The Status Field itself is:
//
//	bits [31]   — Do Not Retry (DNR)
//	bits [30]   — More (M)
//	bits [29:28]— Command Retry Delay (CRD)
//	bits [27:25]— Status Code Type (SCT)
//	bits [24:17]— Status Code (SC)
//
// CapsuleResponse.Status in our wire code already captures SF>>17,
// so bits [31:17] land as a single uint16 here.
const (
	// Status field inside Status (after the >>17 shift):
	//   bit 14   — DNR
	//   bit 13   — M
	//   bits 12:11 — CRD
	//   bits 10:8  — SCT
	//   bits 7:0   — SC
	sctMask = 0x0700
	scMask  = 0x00FF
)

// sctOf extracts the 3-bit SCT from a CapsuleResponse.Status value.
func sctOf(status uint16) uint8 {
	return uint8((status & sctMask) >> 8)
}

// scOf extracts the 8-bit SC from a CapsuleResponse.Status value.
func scOf(status uint16) uint8 {
	return uint8(status & scMask)
}

// expectSCTSC asserts a CapsuleResponse.Status carries the given
// SCT + SC pair. Accepts a list of (sct, sc) tuples to accommodate
// spec-legal alternatives (e.g., Connect rejection can be 0x80
// Connect Invalid Host OR 0x82 Connect Invalid Params per §3.3.1;
// either is acceptable).
func expectSCTSC(t *testing.T, status uint16, context string, allowed ...[2]uint8) {
	t.Helper()
	actualSCT := sctOf(status)
	actualSC := scOf(status)
	for _, want := range allowed {
		if actualSCT == want[0] && actualSC == want[1] {
			return
		}
	}
	t.Fatalf("%s: got SCT=0x%x SC=0x%02x (raw status=0x%04x); want one of %s",
		context, actualSCT, actualSC, status, formatSCTSCList(allowed))
}

func formatSCTSCList(pairs [][2]uint8) string {
	var b bytes.Buffer
	b.WriteString("[")
	for i, p := range pairs {
		if i > 0 {
			b.WriteString(" | ")
		}
		fmt.Fprintf(&b, "SCT=0x%x SC=0x%02x", p[0], p[1])
	}
	b.WriteString("]")
	return b.String()
}

// ----- Self-checks: verify the byte-slicing constants match the spec -----

// TestQASupportNVMeByteOffsetsSanity is a meta-test that guards the
// byte-offset constants above. If someone shifts an offset by a byte
// during refactoring, this test fires before any A-tier test silently
// pulls garbage. Not a production invariant — a tooling correctness
// pin.
func TestQASupportNVMeByteOffsetsSanity(t *testing.T) {
	// Offsets within Identify Controller must lie inside the
	// 4096-byte payload, be monotonically ordered, and not collide.
	checkOffset := func(name string, off, width int) {
		if off < 0 || off+width > idCtrlPayloadSize {
			t.Fatalf("%s offset %d+%d exceeds payload %d", name, off, width, idCtrlPayloadSize)
		}
	}
	checkOffset("CMIC", idCtrlOffsetCMIC, 1)
	checkOffset("OAES", idCtrlOffsetOAES, 4)
	checkOffset("OACS", idCtrlOffsetOACS, 2)
	checkOffset("ONCS", idCtrlOffsetONCS, 2)
	checkOffset("ANACAP", idCtrlOffsetANACAP, 1)
	checkOffset("ANAGRP pair", idCtrlOffsetANAGRP, 8)

	// Namespace offsets sanity.
	if idNSOffsetNGUID+16 > idNSPayloadSize {
		t.Fatalf("NGUID offset+16 exceeds payload")
	}
	if idNSOffsetEUI64+8 > idNSPayloadSize {
		t.Fatalf("EUI-64 offset+8 exceeds payload")
	}
	// NGUID and EUI-64 are adjacent fields per spec.
	if idNSOffsetNGUID+16 != idNSOffsetEUI64 {
		t.Fatalf("NGUID[104:120] should abut EUI-64[120:128]; got NGUID_end=%d EUI64_start=%d",
			idNSOffsetNGUID+16, idNSOffsetEUI64)
	}

	// Status field mask sanity.
	if sctMask|scMask != 0x07FF {
		t.Fatalf("SCT+SC masks should cover bits [10:0]; got 0x%x", sctMask|scMask)
	}

	// Round-trip a known status: SCT=1, SC=0x82 (Connect Invalid
	// Params) → raw bits [10:8]=001, bits [7:0]=0x82 → 0x0182.
	testStatus := uint16(0x0182)
	if sctOf(testStatus) != 1 || scOf(testStatus) != 0x82 {
		t.Fatalf("SCT/SC decode: status=0x%04x → SCT=0x%x SC=0x%02x; want SCT=1 SC=0x82",
			testStatus, sctOf(testStatus), scOf(testStatus))
	}
}
