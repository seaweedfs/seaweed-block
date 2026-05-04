// Ownership: QA (from v3-phase-15-t2-batch-10-5-port-plan.md §4 A-tier Phase 3).
// sw may NOT modify this file without architect approval via §8B.4
// Discovery Bridge. See: sw-block/design/v3-phase-15-t2-batch-10-5-port-plan.md §3.3.
//
// V2 intent source: port-plan §3.3 invariants N3 (advertised list ==
// implemented set) and N1 (NAA derivation determinism + collision).
// V2 scsi.go itself did not have these invariant tests because V2
// used ALUA or a hardcoded stub; V3 introduces the sha256 derivation
// so the invariant is V3-native.
//
// Maps to ledger rows:
//   - PCDD-ISCSI-VPD00-ADVERTISED-LIST-001 (advertised pages ≡ implemented pages)
//   - PCDD-ISCSI-VPD83-NAA-DETERMINISM-001 (same VolumeID → same NAA; different → different)
//   - PCDD-ISCSI-VPD80-SERIAL-DETERMINISM-001 (same VolumeID → same Serial; different → different; N1 symmetry with VPD 0x83)
//
// Test layer: Unit (library-level; no target or session required).

package iscsi_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// handlerForVolume builds a SCSIHandler backed by a RecordingBackend
// with the given VolumeID. Used by QA-owned determinism + collision
// tests to drive the NAA derivation with controlled inputs.
func handlerForVolume(t *testing.T, volumeID string) *iscsi.SCSIHandler {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: volumeID, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	return iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
}

// vpdQuery issues INQUIRY VPD pageCode against h with alloc length 255.
func vpdQuery(t *testing.T, h *iscsi.SCSIHandler, pageCode byte) []byte {
	t.Helper()
	// CDB shape mirrors sw's inquiryVPDCDB in scsi_batch10_5_test.go:
	// opcode 0x12, EVPD=1 in byte 1, page code in byte 2, alloc length in bytes 3-4.
	var cdb [16]byte
	cdb[0] = 0x12
	cdb[1] = 0x01
	cdb[2] = pageCode
	cdb[3] = 0x00
	cdb[4] = 0xFF
	r := h.HandleCommand(context.Background(), cdb, nil)
	if r.AsError() != nil {
		t.Fatalf("VPD 0x%02x: %v", pageCode, r.AsError())
	}
	return r.Data
}

// --- QA A-tier Phase 3 — invariant assertions the sw-side port plan §3.3 flagged ---

// QA.A3.1 — §3.3 N3
//
// Asserts: VPD 0x00's advertised supported-pages list is EXACTLY
// {0x00, 0x80, 0x83}. Stricter than sw's TestT2Batch10_5_InquiryVPD00
// because we pin the set equality as a long-term invariant that
// prevents future regressions where someone adds 0xB0/0xB2 to the
// advertised list without implementing them (which would cause
// kernel log errors every probe cycle — the N3 failure mode).
//
// Also asserts: every advertised page is actually served. This is
// the symmetric check; if an advertised page returns IllegalRequest,
// the advertised list is lying to the kernel.
func TestT2V2Port_SCSI_InquiryVPD00_AdvertisesOnlyImplemented(t *testing.T) {
	h := handlerForVolume(t, "v1-phase3-inv")

	// 1. Read VPD 0x00.
	data := vpdQuery(t, h, 0x00)
	if len(data) < 4 {
		t.Fatalf("VPD 0x00 too short: %d", len(data))
	}
	if data[0] != 0x00 || data[1] != 0x00 {
		t.Fatalf("VPD 0x00 header: device=0x%02x page=0x%02x", data[0], data[1])
	}
	pageListLen := int(binary.BigEndian.Uint16(data[2:4]))
	if pageListLen != len(data)-4 {
		t.Fatalf("VPD 0x00 declared length=%d != actual %d", pageListLen, len(data)-4)
	}
	advertised := data[4 : 4+pageListLen]

	// 2. Pin strict equality: advertised == {0x00, 0x80, 0x83}.
	//    Intentionally not ⊇ — if implementation adds new pages,
	//    this test must be updated in the same commit and must add
	//    the new page to the symmetric check below.
	want := []byte{0x00, 0x80, 0x83}
	if !bytes.Equal(advertised, want) {
		t.Fatalf("advertised pages = %x; want exactly %x (N3: advertised list MUST match implemented set)", advertised, want)
	}

	// 3. Symmetric check: every advertised page actually serves.
	for _, page := range advertised {
		r := h.HandleCommand(context.Background(), func() [16]byte {
			var cdb [16]byte
			cdb[0] = 0x12
			cdb[1] = 0x01
			cdb[2] = page
			cdb[4] = 0xFF
			return cdb
		}(), nil)
		if r.AsError() != nil {
			t.Fatalf("page 0x%02x advertised by VPD 0x00 but HandleCommand returns %v (N3 violation: advertised but not served)", page, r.AsError())
		}
		if len(r.Data) < 2 || r.Data[1] != page {
			t.Fatalf("page 0x%02x: response header page byte = 0x%02x (served payload mismatch)", page, func() byte {
				if len(r.Data) >= 2 {
					return r.Data[1]
				}
				return 0xFF
			}())
		}
	}

	// 4. Defense-in-depth: pages NOT advertised must NOT serve.
	//    If batch-10.5 §3.2 skip list ever regresses, this fires.
	forbidden := []byte{0xB0, 0xB2}
	for _, page := range forbidden {
		var cdb [16]byte
		cdb[0] = 0x12
		cdb[1] = 0x01
		cdb[2] = page
		cdb[4] = 0xFF
		r := h.HandleCommand(context.Background(), cdb, nil)
		if r.AsError() == nil {
			t.Fatalf("page 0x%02x MUST NOT serve (skip-list §3.2) but HandleCommand succeeded with %d bytes", page, len(r.Data))
		}
	}
}

// QA.A3.2 — §3.3 N1
//
// Asserts: the NAA-6 identifier in VPD 0x83 is deterministic across
// calls for the same VolumeID, AND different across different
// VolumeIDs. Protects the multipath / udev unique-ID invariant that
// port plan N1 flagged as a bug risk.
//
// V2 had ALUA providing DeviceNAA or a hardcoded stub. V3 derives
// from sha256(VolumeID) so every V3 volume must have a stable,
// distinct NAA. Breaking either leg causes kernel-level conflation
// of different volumes. This test pins both legs as lasting
// invariants.
func TestT2V2Port_SCSI_InquiryVPD83_NAADerivationDeterministic(t *testing.T) {
	// naaFromVPD extracts the 8-byte NAA-6 payload from a VPD 0x83
	// response. Shape: Header(4) + Designator header(4) + NAA(8).
	naaFromVPD := func(data []byte) [8]byte {
		t.Helper()
		if len(data) < 16 {
			t.Fatalf("VPD 0x83 response too short: %d (want ≥16)", len(data))
		}
		if data[0] != 0x00 || data[1] != 0x83 {
			t.Fatalf("VPD 0x83 header: device=0x%02x page=0x%02x", data[0], data[1])
		}
		// Designator header at offset 4: [0] code set, [1] type, [2] reserved, [3] length.
		if data[5]&0x0f != 0x03 { // type 3 = NAA
			t.Fatalf("designator type = 0x%02x; want 0x03 (NAA)", data[5]&0x0f)
		}
		if data[7] != 0x08 {
			t.Fatalf("designator length = %d; want 8", data[7])
		}
		// NAA-6 must have high nibble of byte 0 == 0x6.
		if data[8]>>4 != 0x06 {
			t.Fatalf("NAA high nibble = 0x%x; want 0x6 (NAA-6)", data[8]>>4)
		}
		var out [8]byte
		copy(out[:], data[8:16])
		return out
	}

	// Sub-test: determinism — same VolumeID produces the same NAA
	// across repeated queries AND across fresh handlers.
	t.Run("SameVolumeIDProducesSameNAA", func(t *testing.T) {
		h1 := handlerForVolume(t, "volume-alpha")
		naa1a := naaFromVPD(vpdQuery(t, h1, 0x83))
		naa1b := naaFromVPD(vpdQuery(t, h1, 0x83))
		if naa1a != naa1b {
			t.Fatalf("NAA not stable across calls on same handler: %x vs %x", naa1a, naa1b)
		}

		h2 := handlerForVolume(t, "volume-alpha") // fresh handler, same VolumeID
		naa2 := naaFromVPD(vpdQuery(t, h2, 0x83))
		if naa1a != naa2 {
			t.Fatalf("NAA not stable across handlers for same VolumeID: %x vs %x (N1 violation: determinism broken)", naa1a, naa2)
		}
	})

	// Sub-test: collision — different VolumeID produces different NAA.
	// Also checks a handful of representative names to make the
	// intent clear to future readers.
	t.Run("DifferentVolumeIDsProduceDifferentNAA", func(t *testing.T) {
		cases := []string{
			"volume-alpha",
			"volume-beta",
			"v1",
			"v1-phase3-inv",
			"iqn.2026-04.example.v3:v1",
			"iqn.2026-04.example.v3:v2",
			// Adversarial: very similar VolumeIDs.
			"volume-aaaa",
			"volume-aaab",
		}
		seen := make(map[[8]byte]string, len(cases))
		for _, vid := range cases {
			h := handlerForVolume(t, vid)
			naa := naaFromVPD(vpdQuery(t, h, 0x83))
			if prior, dup := seen[naa]; dup {
				t.Fatalf("NAA collision: VolumeID %q and %q both produce NAA=%x (N1 violation: collision guard broken)", prior, vid, naa)
			}
			seen[naa] = vid
		}
	})

	// Sub-test: empty VolumeID still produces a valid NAA-6
	// (high nibble 0x6) — we don't panic or drop the designator.
	t.Run("EmptyVolumeIDProducesValidNAA", func(t *testing.T) {
		h := handlerForVolume(t, "")
		naa := naaFromVPD(vpdQuery(t, h, 0x83))
		if naa[0]>>4 != 0x06 {
			t.Fatalf("empty VolumeID: NAA high nibble = 0x%x; want 0x6", naa[0]>>4)
		}
	})
}

// QA.A3.3 — port-plan §3.3 N1 symmetry (VPD 0x80)
//
// Asserts that the Unit Serial Number in VPD 0x80, when not overridden
// by operator config, is derived per-volume from sha256(VolumeID) and
// is therefore deterministic + collision-free across VolumeIDs. This
// is the SYMMETRIC counterpart to the VPD 0x83 NAA invariant: Linux
// udev fingerprints devices using BOTH pages, so if 0x80 returns the
// same hardcoded constant for every volume (the V2 bug that N1 called
// out) while 0x83 is per-volume unique, the pair is inconsistent and
// latently breaks downstream storage stacks that weight 0x80.
//
// History: batch 10.5 initially shipped with Serial defaulting to the
// hardcoded literal "SWF00001" — same pattern N1 rejected for NAA.
// QA Owner flagged the asymmetry; sw fixed in follow-up commit
// d649b43 by deriving the default Serial as hex(sha256(VolumeID)[:8]).
// This test pins that fix as a lasting invariant so nobody reverts to
// a shared constant without also updating this assertion.
func TestT2V2Port_SCSI_InquiryVPD80_SerialDerivationDeterministic(t *testing.T) {
	// serialFromVPD extracts the raw Serial bytes from a VPD 0x80
	// response. Shape: header[4] + serial payload; length at [2:4].
	serialFromVPD := func(data []byte) []byte {
		t.Helper()
		if len(data) < 4 {
			t.Fatalf("VPD 0x80 too short: %d", len(data))
		}
		if data[0] != 0x00 || data[1] != 0x80 {
			t.Fatalf("VPD 0x80 header: device=0x%02x page=0x%02x", data[0], data[1])
		}
		ln := int(binary.BigEndian.Uint16(data[2:4]))
		if 4+ln > len(data) {
			t.Fatalf("VPD 0x80 declared length=%d exceeds data=%d", ln, len(data)-4)
		}
		return data[4 : 4+ln]
	}

	// Sub-test: determinism — same VolumeID produces the same Serial
	// across repeated queries AND across fresh handlers.
	t.Run("SameVolumeIDProducesSameSerial", func(t *testing.T) {
		h1 := handlerForVolume(t, "volume-alpha")
		s1a := serialFromVPD(vpdQuery(t, h1, 0x80))
		s1b := serialFromVPD(vpdQuery(t, h1, 0x80))
		if !bytes.Equal(s1a, s1b) {
			t.Fatalf("Serial not stable across calls: %q vs %q", s1a, s1b)
		}

		h2 := handlerForVolume(t, "volume-alpha") // fresh handler, same VolumeID
		s2 := serialFromVPD(vpdQuery(t, h2, 0x80))
		if !bytes.Equal(s1a, s2) {
			t.Fatalf("Serial not stable across handlers for same VolumeID: %q vs %q (N1 symmetry broken)", s1a, s2)
		}
	})

	// Sub-test: collision — different VolumeIDs produce different
	// Serials. Uses the same case set as the NAA collision test so
	// the symmetry is explicit: every VolumeID that produces a unique
	// NAA must also produce a unique Serial.
	t.Run("DifferentVolumeIDsProduceDifferentSerial", func(t *testing.T) {
		cases := []string{
			"volume-alpha",
			"volume-beta",
			"v1",
			"v1-phase3-inv",
			"iqn.2026-04.example.v3:v1",
			"iqn.2026-04.example.v3:v2",
			// Adversarial: very similar VolumeIDs.
			"volume-aaaa",
			"volume-aaab",
		}
		seen := make(map[string]string, len(cases))
		for _, vid := range cases {
			h := handlerForVolume(t, vid)
			s := string(serialFromVPD(vpdQuery(t, h, 0x80)))
			if prior, dup := seen[s]; dup {
				t.Fatalf("Serial collision: VolumeID %q and %q both produce Serial=%q (N1 symmetry: collision guard broken)", prior, vid, s)
			}
			seen[s] = vid
		}
	})

	// Sub-test: shape guard — default Serial must be 16-char
	// lowercase hex (sha256[:8] encoded). If someone reverts to the
	// 8-char "SWF00001" literal or any other fixed constant, this
	// fires — symmetrically with the VPD 0x83 high-nibble guard.
	t.Run("DefaultSerialIsSha256HexShape", func(t *testing.T) {
		h := handlerForVolume(t, "volume-alpha")
		s := serialFromVPD(vpdQuery(t, h, 0x80))
		if len(s) != 16 {
			t.Fatalf("default Serial length = %d; want 16 (hex of sha256[:8])", len(s))
		}
		for i, b := range s {
			isHexDigit := (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f')
			if !isHexDigit {
				t.Fatalf("Serial[%d] = 0x%02x (%q); not lowercase hex — shape regression (possible revert to shared constant)", i, b, s)
			}
		}
		if bytes.HasPrefix(s, []byte("SWF")) {
			t.Fatalf("Serial %q begins with SWF — regression to pre-d649b43 hardcoded constant", s)
		}
	})

	// Sub-test: operator override — non-empty cfg.SerialNo bypasses
	// derivation, so operators who explicitly set it own uniqueness.
	// Pins the config precedence rule from addendum A.
	t.Run("OperatorOverrideBypassesDerivation", func(t *testing.T) {
		override := "ASSET-TAG-042"
		rec := testback.NewRecordingBackend(frontend.Identity{
			VolumeID: "volume-alpha", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		})
		h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec, SerialNo: override})
		s := serialFromVPD(vpdQuery(t, h, 0x80))
		// Override may be right-padded to a fixed width; the prefix
		// must equal the configured value verbatim.
		if !bytes.HasPrefix(s, []byte(override)) {
			t.Fatalf("override Serial prefix = %q; want prefix %q (operator override path broken)", s, override)
		}
	})
}
