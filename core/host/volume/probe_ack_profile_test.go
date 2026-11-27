package volume

import "testing"

// Every probe answer names the volume's own ack profile (Config.AckProfile),
// which the master compares with its own before any automatic failover.
func TestProbeAnswerNamesTheVolumesAckProfile(t *testing.T) {
	h, _, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.cfg.AckProfile = "sync_quorum"
	if got := h.probeAnswer(1).GetAckProfile(); got != "sync_quorum" {
		t.Fatalf("probe answer ack profile %q; want sync_quorum", got)
	}
}
