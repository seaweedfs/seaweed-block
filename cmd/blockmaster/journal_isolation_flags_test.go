package main

import (
	"strings"
	"testing"
)

// --strict-journal-isolation and --status-listen are off by default; a
// non-loopback --status-listen is refused at startup with a clear error.
func TestParseFlags_JournalIsolationAndStatusListen(t *testing.T) {
	f, err := parseFlags([]string{"--authority-store", t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if f.strictJournalIsolation || f.statusListen != "" {
		t.Fatalf("defaults: strict=%v status-listen=%q, want off", f.strictJournalIsolation, f.statusListen)
	}
	f, err = parseFlags([]string{"--authority-store", t.TempDir(), "--strict-journal-isolation", "--status-listen", "127.0.0.1:9335"})
	if err != nil || !f.strictJournalIsolation || f.statusListen != "127.0.0.1:9335" {
		t.Fatalf("set: %+v %v", f, err)
	}
	for _, bad := range []string{"0.0.0.0:9335", ":9335", "192.168.1.10:9335"} {
		_, err := parseFlags([]string{"--authority-store", t.TempDir(), "--status-listen", bad})
		if err == nil || !strings.Contains(err.Error(), "must be a loopback address") {
			t.Fatalf("--status-listen %s: %v, want a loopback refusal", bad, err)
		}
	}
}
