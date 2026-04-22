// Ownership: sw unit tests for parameter parsing/encoding +
// negotiation helpers. T2 ckpt 9 mechanism port from V2.
package iscsi_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

func TestParams_RoundtripEncodeParse(t *testing.T) {
	p := iscsi.NewParams()
	p.Set("InitiatorName", "iqn.example.host:1")
	p.Set("SessionType", "Normal")
	p.Set("HeaderDigest", "None")
	encoded := p.Encode()

	out, err := iscsi.ParseParams(encoded)
	if err != nil {
		t.Fatalf("ParseParams: %v", err)
	}
	if out.Len() != 3 {
		t.Fatalf("got %d params, want 3", out.Len())
	}
	for k, want := range map[string]string{
		"InitiatorName": "iqn.example.host:1",
		"SessionType":   "Normal",
		"HeaderDigest":  "None",
	} {
		if v, ok := out.Get(k); !ok || v != want {
			t.Errorf("Get(%q)=%q,%v want %q", k, v, ok, want)
		}
	}
}

func TestParams_ParseRejectsMissingEquals(t *testing.T) {
	_, err := iscsi.ParseParams([]byte("KeyOnly\x00"))
	if !errors.Is(err, iscsi.ErrMalformedParam) {
		t.Fatalf("got %v, want ErrMalformedParam", err)
	}
}

func TestParams_ParseRejectsEmptyKey(t *testing.T) {
	_, err := iscsi.ParseParams([]byte("=value\x00"))
	if !errors.Is(err, iscsi.ErrEmptyKey) {
		t.Fatalf("got %v, want ErrEmptyKey", err)
	}
}

func TestParams_ParseRejectsDuplicateKeys(t *testing.T) {
	_, err := iscsi.ParseParams([]byte("K=a\x00K=b\x00"))
	if !errors.Is(err, iscsi.ErrDuplicateKey) {
		t.Fatalf("got %v, want ErrDuplicateKey", err)
	}
}

func TestParams_ParseSkipsTrailingNul(t *testing.T) {
	p, err := iscsi.ParseParams([]byte("K=v\x00\x00"))
	if err != nil {
		t.Fatalf("ParseParams: %v", err)
	}
	if p.Len() != 1 {
		t.Fatalf("got %d, want 1", p.Len())
	}
}

func TestParams_EncodeEmpty_ReturnsNil(t *testing.T) {
	if got := iscsi.NewParams().Encode(); got != nil {
		t.Fatalf("empty Encode = %v, want nil", got)
	}
}

func TestParams_SetReplacesExisting(t *testing.T) {
	p := iscsi.NewParams()
	p.Set("K", "v1")
	p.Set("K", "v2")
	if v, _ := p.Get("K"); v != "v2" {
		t.Fatalf("K=%q want v2", v)
	}
	if p.Len() != 1 {
		t.Fatalf("Len=%d want 1 (no duplicate)", p.Len())
	}
}

// V2 quirk preservation: Encode emits in insertion order so
// negotiation output matches the order keys were processed.
func TestParams_EncodePreservesInsertionOrder(t *testing.T) {
	p := iscsi.NewParams()
	p.Set("First", "1")
	p.Set("Second", "2")
	p.Set("Third", "3")
	encoded := p.Encode()
	idx1 := bytes.Index(encoded, []byte("First="))
	idx2 := bytes.Index(encoded, []byte("Second="))
	idx3 := bytes.Index(encoded, []byte("Third="))
	if !(idx1 < idx2 && idx2 < idx3) {
		t.Fatalf("order lost: idx=(%d,%d,%d)", idx1, idx2, idx3)
	}
}

// Numeric negotiation rule: result is min(offered, ours), with
// offered clamped to [min, max].
func TestNegotiateNumber_Behaviors(t *testing.T) {
	cases := []struct {
		name              string
		offered           string
		ours, min, max    int
		want              int
		wantErr           bool
	}{
		{"smaller_offer_wins", "1024", 8192, 512, 16777215, 1024, false},
		{"smaller_ours_wins", "65536", 8192, 512, 16777215, 8192, false},
		{"clamp_below_min", "100", 8192, 512, 16777215, 512, false},
		{"clamp_above_max", "99999999", 8192, 512, 1024, 1024, false},
		{"reject_non_numeric", "abc", 8192, 512, 16777215, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := iscsi.NegotiateNumber(tc.offered, tc.ours, tc.min, tc.max)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

// Boolean negotiation: AND semantics. (InitialR2T's OR semantics
// is enforced in negotiateOperational, not in NegotiateBool.)
func TestNegotiateBool_AndSemantics(t *testing.T) {
	cases := []struct {
		offered string
		ours    bool
		want    bool
		wantErr bool
	}{
		{"Yes", true, true, false},
		{"Yes", false, false, false},
		{"No", true, false, false},
		{"No", false, false, false},
		{"yes", false, false, true}, // case-sensitive per RFC
		{"", false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.offered+"|"+iscsi.BoolStr(tc.ours), func(t *testing.T) {
			got, err := iscsi.NegotiateBool(tc.offered, tc.ours)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestBoolStr_YesNo(t *testing.T) {
	if iscsi.BoolStr(true) != "Yes" || iscsi.BoolStr(false) != "No" {
		t.Fatal("BoolStr mismatch")
	}
}
