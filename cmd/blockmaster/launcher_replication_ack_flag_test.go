package main

import (
	"strings"
	"testing"
)

// The promotion gate reads every frontier rule from
// --launcher-replication-ack, so blockmaster refuses to start on a profile it
// does not know (as blockvolume refuses its own --replication-ack).
func TestBlockmasterRefusesAnUnknownLauncherReplicationAck(t *testing.T) {
	for _, bad := range []string{"bogus", "sync_quorum", ""} {
		_, err := parseFlags([]string{"--authority-store", t.TempDir(), "--launcher-replication-ack=" + bad})
		if err == nil || !strings.Contains(err.Error(), "--launcher-replication-ack") {
			t.Fatalf("--launcher-replication-ack=%q: err=%v; want a refusal naming the flag", bad, err)
		}
	}
	for _, good := range []string{"best-effort", "sync-quorum", "sync-all"} {
		if _, err := parseFlags([]string{"--authority-store", t.TempDir(), "--launcher-replication-ack=" + good}); err != nil {
			t.Fatalf("--launcher-replication-ack=%q refused: %v", good, err)
		}
	}
}
