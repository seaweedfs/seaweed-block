package nvme

import (
	"encoding/binary"
	"testing"
)

func TestPhase141MaxH2CDataLengthDefaultAndCandidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  uint32
		want uint32
	}{
		{name: "default", cfg: 0, want: DefaultMaxH2CDataLength},
		{name: "candidate64k", cfg: 64 * 1024, want: 64 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Session{
				target:  &Target{cfg: TargetConfig{MaxH2CDataLength: tc.cfg, VolumeID: "v1"}},
				handler: &IOHandler{},
			}
			if got := s.maxH2CDataLength(); got != tc.want {
				t.Fatalf("maxH2CDataLength=%d want %d", got, tc.want)
			}
			ctrl := s.buildIdentifyController()
			if got := binary.LittleEndian.Uint32(ctrl[1792:1796]); got != ioccszForMaxH2CDataLength(tc.want) {
				t.Fatalf("IOCCSZ=%d want %d", got, ioccszForMaxH2CDataLength(tc.want))
			}
			if got := ctrl[77]; got != mdtsForMaxH2CDataLength(tc.want) {
				t.Fatalf("MDTS=%d want %d", got, mdtsForMaxH2CDataLength(tc.want))
			}
		})
	}
}

func TestPhase141ValidateMaxH2CDataLength(t *testing.T) {
	for _, value := range []uint32{0, DefaultMaxH2CDataLength, 64 * 1024, 1024 * 1024} {
		if err := ValidateMaxH2CDataLength(value); err != nil {
			t.Fatalf("ValidateMaxH2CDataLength(%d): %v", value, err)
		}
	}
	for _, value := range []uint32{16 * 1024, 48 * 1024, MaxH2CDataLengthLimit + 4096} {
		if err := ValidateMaxH2CDataLength(value); err == nil {
			t.Fatalf("ValidateMaxH2CDataLength(%d) succeeded; want error", value)
		}
	}
}
