package nvme

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

// Exercise the real TCP framing and receive path, without a socket or backend.
func TestRecvH2CDataRangeBounds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		offset  uint32
		payload []byte
		reject  bool
	}{
		{"uint32-wrap", math.MaxUint32, []byte{1, 2}, true},
		{"exact-bound", 0, []byte{1, 2, 3, 4}, false},
		{"bound-plus-one", 1, []byte{1, 2, 3, 4}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stream bytes.Buffer
			w := NewWriter(&stream)
			h := H2CDataHeader{DATAO: tc.offset, DATAL: uint32(len(tc.payload))}
			if err := w.SendWithData(pduH2CData, 0, &h, h2cDataHdrSize, tc.payload); err != nil {
				t.Fatal(err)
			}
			s := &Session{wire: newTCPPDUTransport(&stream)}
			// A panic is a test failure, never an acceptable malformed-input outcome.
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("invalid range panicked instead of refusing: %v", p)
				}
			}()
			data, err := s.recvH2CData(4)
			if tc.reject {
				if err == nil || !strings.Contains(err.Error(), "data exceeds expected size") || data != nil {
					t.Fatalf("range not refused: data=%v err=%v", data, err)
				}
			} else if err != nil || !bytes.Equal(data, tc.payload) {
				t.Fatalf("exact-bound payload changed: %v %v", data, err)
			}
		})
	}
}
