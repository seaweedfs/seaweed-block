// Ownership: QA (from v3-phase-15-t2-v2-test-audit.md A-tier phase 2 rewrite).
// sw may NOT modify this file without architect approval via §8B.4
// Discovery Bridge. See: sw-block/design/v3-phase-15-t2-v2-test-audit.md §5.
//
// V2 intent source:
//   - weed/storage/blockvol/iscsi/qa_test.go adversarial sub-tests:
//     * "dataseg_underflow"           → TestT2V2Port_PDU_DatasegDeclaredZero_ActualBytesIgnored
//     * "huge_ahs_len"                → TestT2V2Port_PDU_AHSLengthOverflow_Rejected
//     * "dataseg_len_vs_actual"       → TestT2V2Port_PDU_DatasegLengthBeyondMax_Rejected
//     * "very_long_value"             → TestT2V2Port_Params_VeryLongValue_RoundTrip
//     * "100_keys"                    → TestT2V2Port_Params_ManyKeys_RoundTrip
//     * "no_null_terminator"          → TestT2V2Port_Params_MissingTrailingNul_ParsesGracefully
//
// V3 rewrite rationale: These are pure library-level adversarial tests
// (PDU framing + params encode/parse). Unlike B-tier phase violations,
// these need no target or session state — they exercise the encoding
// primitives V3 ported from V2 at ckpt 3 (pdu.go) and ckpt 9 (params.go).
//
// Maps to ledger rows:
//   - PCDD-ISCSI-PDU-MALFORMED-001 (PDU robustness)
//   - PCDD-ISCSI-PARAMS-ROBUST-001 (Params robustness)
//
// Test layer: Unit (library-level adversarial)

package iscsi_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

// A1. V2 qa_test.go:"dataseg_underflow"
//
// V2 intent: a PDU that declares DataSegmentLength=0 but has trailing
// bytes in the stream should parse cleanly (only 48 BHS bytes consumed;
// trailing bytes remain for the next PDU).
//
// V3 fail-closed shape: ReadPDU reads exactly BHS(48) + AHS(len*4) +
// DataSegment(declared, padded). If declared=0, no data bytes consumed.
func TestT2V2Port_PDU_DatasegDeclaredZero_ActualBytesIgnored(t *testing.T) {
	// Build a valid BHS-only PDU with DataSegmentLength=0.
	p := &iscsi.PDU{}
	p.SetOpcode(iscsi.OpNOPOut)
	p.SetOpSpecific1(iscsi.FlagF)
	p.SetInitiatorTaskTag(0x1234)
	// DataSegmentLength stays at 0.

	var buf bytes.Buffer
	if err := iscsi.WritePDU(&buf, p); err != nil {
		t.Fatalf("WritePDU: %v", err)
	}
	// Append trailing bytes that belong to a hypothetical next PDU.
	trailing := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	buf.Write(trailing)

	got, err := iscsi.ReadPDU(&buf)
	if err != nil {
		t.Fatalf("ReadPDU: %v", err)
	}
	if got.Opcode() != iscsi.OpNOPOut {
		t.Fatalf("opcode=%s want NOPOut", iscsi.OpcodeName(got.Opcode()))
	}
	if got.DataSegmentLength() != 0 {
		t.Fatalf("DataSegmentLength=%d want 0", got.DataSegmentLength())
	}
	// Trailing bytes still in buffer, un-consumed.
	if buf.Len() != len(trailing) {
		t.Fatalf("buf.Len=%d want %d (trailing bytes should be untouched)",
			buf.Len(), len(trailing))
	}
}

// A2. V2 qa_test.go:"huge_ahs_len"
//
// V2 intent: an abnormally large TotalAHSLength claim (stream doesn't
// have that many bytes) must fail cleanly with ErrPDUTruncated, not
// panic or hang.
func TestT2V2Port_PDU_AHSLengthOverflow_Rejected(t *testing.T) {
	// Construct a BHS that claims TotalAHSLength = 255 (units of 4
	// bytes; = 1020 bytes of AHS promised) but provide only 100 bytes
	// of AHS. io.ReadFull sees a partial read and must convert EOF to
	// ErrUnexpectedEOF, which ReadPDU maps to ErrPDUTruncated.
	var bhs [48]byte
	bhs[0] = iscsi.OpNOPOut
	bhs[4] = 255 // TotalAHSLength (units of 4 bytes)
	partialAHS := make([]byte, 100)

	stream := append([]byte{}, bhs[:]...)
	stream = append(stream, partialAHS...)
	r := bytes.NewReader(stream) // 148 bytes; ReadPDU needs 48+1020

	_, err := iscsi.ReadPDU(r)
	if err == nil {
		t.Fatal("ReadPDU: expected error (truncated AHS), got nil")
	}
	if !errors.Is(err, iscsi.ErrPDUTruncated) {
		t.Fatalf("ReadPDU: expected ErrPDUTruncated, got %v", err)
	}
}

// A3. V2 qa_test.go:"dataseg_len_vs_actual"
//
// V2 intent: a DataSegmentLength that exceeds MaxDataSegmentLength
// must be rejected with ErrPDUTooLarge — prevents unbounded allocation
// from a malicious initiator.
func TestT2V2Port_PDU_DatasegLengthBeyondMax_Rejected(t *testing.T) {
	var bhs [48]byte
	bhs[0] = iscsi.OpNOPOut
	// DataSegmentLength lives in BHS[5..7] (24-bit big-endian).
	// Encode a length larger than MaxDataSegmentLength.
	declared := uint32(iscsi.MaxDataSegmentLength) + 1024
	bhs[5] = byte(declared >> 16)
	bhs[6] = byte(declared >> 8)
	bhs[7] = byte(declared)

	r := bytes.NewReader(bhs[:])
	_, err := iscsi.ReadPDU(r)
	if err == nil {
		t.Fatal("ReadPDU: expected error (data-segment too large), got nil")
	}
	if !errors.Is(err, iscsi.ErrPDUTooLarge) {
		t.Fatalf("ReadPDU: expected ErrPDUTooLarge, got %v", err)
	}
}

// A4. V2 qa_test.go:"very_long_value"
//
// V2 intent: a single iSCSI key=value pair with an abnormally long
// value must round-trip through encode/parse without truncation or
// corruption, as long as the whole text fits in one data segment.
func TestT2V2Port_Params_VeryLongValue_RoundTrip(t *testing.T) {
	p := iscsi.NewParams()
	longValue := strings.Repeat("X", 64*1024) // 64 KB value
	p.Set("InitiatorName", "iqn.example.host:test")
	p.Set("CustomLongKey", longValue)

	encoded := p.Encode()
	parsed, err := iscsi.ParseParams(encoded)
	if err != nil {
		t.Fatalf("ParseParams: %v", err)
	}

	got, ok := parsed.Get("CustomLongKey")
	if !ok {
		t.Fatal("Get(CustomLongKey): missing after roundtrip")
	}
	if len(got) != len(longValue) {
		t.Fatalf("value length=%d want %d", len(got), len(longValue))
	}
	if got != longValue {
		t.Fatal("value corrupted during roundtrip")
	}

	// Sanity on the other key — no cross-contamination.
	other, _ := parsed.Get("InitiatorName")
	if other != "iqn.example.host:test" {
		t.Fatalf("other key corrupted: %q", other)
	}
}

// A5. V2 qa_test.go:"100_keys"
//
// V2 intent: parsing many keys in a single buffer must preserve each
// key=value pair, insertion order, and not OOM or hang.
func TestT2V2Port_Params_ManyKeys_RoundTrip(t *testing.T) {
	p := iscsi.NewParams()
	const N = 100
	for i := range N {
		p.Set(keyN(i), valueN(i))
	}
	if p.Len() != N {
		t.Fatalf("Len=%d want %d", p.Len(), N)
	}

	encoded := p.Encode()
	parsed, err := iscsi.ParseParams(encoded)
	if err != nil {
		t.Fatalf("ParseParams: %v", err)
	}
	if parsed.Len() != N {
		t.Fatalf("parsed.Len=%d want %d", parsed.Len(), N)
	}

	// Spot-check every 10th key.
	for i := 0; i < N; i += 10 {
		v, ok := parsed.Get(keyN(i))
		if !ok {
			t.Fatalf("missing key %s", keyN(i))
		}
		if v != valueN(i) {
			t.Fatalf("key %s: value=%q want %q", keyN(i), v, valueN(i))
		}
	}
}

// A6. V2 qa_test.go:"no_null_terminator"
//
// V2 intent: a key=value buffer without a trailing NUL byte must still
// parse the final pair cleanly (not silently drop it, and not panic on
// an unterminated buffer).
func TestT2V2Port_Params_MissingTrailingNul_ParsesGracefully(t *testing.T) {
	// Manually construct buffer without trailing NUL.
	raw := []byte("InitiatorName=iqn.example:test\x00TargetName=iqn.example:t1")
	// ^^^ first pair NUL-terminated; last pair NOT NUL-terminated.

	parsed, err := iscsi.ParseParams(raw)
	if err != nil {
		t.Fatalf("ParseParams: %v", err)
	}
	if parsed.Len() != 2 {
		t.Fatalf("parsed.Len=%d want 2 (both pairs must be captured)", parsed.Len())
	}

	first, _ := parsed.Get("InitiatorName")
	if first != "iqn.example:test" {
		t.Fatalf("InitiatorName=%q want iqn.example:test", first)
	}
	second, _ := parsed.Get("TargetName")
	if second != "iqn.example:t1" {
		t.Fatalf("TargetName=%q want iqn.example:t1", second)
	}
}

// --- helpers ---

func keyN(i int) string {
	// Generate distinct keys that aren't iSCSI-reserved.
	return "X-QA-K" + itoa(i)
}

func valueN(i int) string {
	return "v" + itoa(i) + "-payload"
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	n := 0
	for x := i; x > 0; x /= 10 {
		buf[n] = byte('0' + x%10)
		n++
	}
	// Reverse.
	out := make([]byte, n)
	for j := range n {
		out[j] = buf[n-1-j]
	}
	return string(out)
}
