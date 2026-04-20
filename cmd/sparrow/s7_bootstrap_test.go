package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// ============================================================
// P14 S7 — Component-layer (in-process) tests
//
// These close proof-matrix rows 8, 9, 10 from
// sw-block/design/v3-phase-14-s7-sketch.md §13.2. They run
// Bootstrap() twice against the same t.TempDir() inside ONE
// test process, asserting directly on Bootstrap fields and
// Publisher state — the assertions that are awkward to read
// through stdout. The real-subprocess gate (row 10b) lives in
// s7_restart_smoke_test.go per §8.1 shape 2.
//
// Strict Windows residual scope (sketch §8.5): a cleanup-phase
// teardown error may be residual, but a store write / reload
// error is a hard test failure. These tests use only the
// high-level Bootstrap surface; any error returned by
// Bootstrap() fails the test.
// ============================================================

// Row 8: sparrow Bootstrap with durable store, reload, publisher
// route construction, clean exit — component layer.
func TestS7Process_BootstrapReloadRouteSmoke(t *testing.T) {
	dir := t.TempDir()

	// Fresh dir: ReloadedRecords==0, no skips.
	boot1, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("first Bootstrap: %v", err)
	}
	if boot1.ReloadedRecords != 0 {
		t.Fatalf("fresh dir reload count: got %d want 0", boot1.ReloadedRecords)
	}
	if len(boot1.ReloadSkips) != 0 {
		t.Fatalf("fresh dir skips: got %v want none", boot1.ReloadSkips)
	}
	if err := boot1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Seed one record via the real publisher-run path.
	seedDurableRecord(t, dir, authority.AssignmentAsk{
		VolumeID: "s7-vol", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: authority.IntentBind,
	})

	// Second Bootstrap reloads and exits cleanly.
	boot2, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("second Bootstrap: %v", err)
	}
	defer func() {
		if err := boot2.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	}()
	if boot2.ReloadedRecords != 1 {
		t.Fatalf("reload count: got %d want 1", boot2.ReloadedRecords)
	}
}

// Row 9: second in-process Bootstrap on same dir sees prior
// authority and does NOT mint backward. Verifies restart
// truth is preserved across in-process teardown + rebuild.
func TestS7Process_RestartDoesNotRegressAuthorityLine(t *testing.T) {
	dir := t.TempDir()

	// Seed one Bind through the real publisher-run path so the
	// durable store holds r1@1. A single record is sufficient to
	// prove "no backward mint on restart" — any reload-driven
	// re-mint (even a fresh Epoch=1 that rewrites the same line)
	// would be a violation of the §3 load-bearing constraint.
	seedDurableRecord(t, dir, authority.AssignmentAsk{
		VolumeID: "s7-vol", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: authority.IntentBind,
	})

	// Bootstrap round 1 — should reload r1@1.
	boot1, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("Bootstrap round 1: %v", err)
	}
	line1, ok := boot1.Publisher.VolumeAuthorityLine("s7-vol")
	if !ok {
		t.Fatal("round 1: VolumeAuthorityLine missing after reload")
	}
	if line1.ReplicaID != "r1" || line1.Epoch != 1 {
		t.Fatalf("round 1 line: got %+v want r1@1", line1)
	}
	if err := boot1.Close(); err != nil {
		t.Fatalf("Close round 1: %v", err)
	}

	// Bootstrap round 2 — MUST see the same line. No reload-
	// driven mint may advance OR rewrite the Epoch.
	boot2, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("Bootstrap round 2: %v", err)
	}
	defer boot2.Close()
	line2, ok := boot2.Publisher.VolumeAuthorityLine("s7-vol")
	if !ok {
		t.Fatal("round 2: VolumeAuthorityLine missing after reload")
	}
	if line2.ReplicaID != line1.ReplicaID ||
		line2.Epoch != line1.Epoch ||
		line2.EndpointVersion != line1.EndpointVersion {
		t.Fatalf("restart changed authority line (§3 violation): round1=%+v round2=%+v", line1, line2)
	}
}

// Row 10: reload skips and unsupported evidence surface in
// structured form. The test drops a corrupt store file and
// checks that Bootstrap.ReloadSkips carries the per-file
// error; controller-side UnsupportedEvidence is exercised in
// the L1 route tests, not here (this is a bootstrap-surface
// assertion).
func TestS7Process_RestartReportsEvidence(t *testing.T) {
	dir := t.TempDir()

	// Seed one good record.
	seedDurableRecord(t, dir, authority.AssignmentAsk{
		VolumeID: "s7-good", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: authority.IntentBind,
	})

	// Write one corrupt record next to it.
	corrupt := "{not valid json}"
	badPath := filepath.Join(dir, "s7-bad.v3auth.json")
	if err := os.WriteFile(badPath, []byte(corrupt), 0o644); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}

	boot, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("Bootstrap must survive corrupt record: %v", err)
	}
	defer boot.Close()

	if boot.ReloadedRecords != 1 {
		t.Fatalf("reloaded count: got %d want 1 (the good record)", boot.ReloadedRecords)
	}
	if len(boot.ReloadSkips) == 0 {
		t.Fatal("ReloadSkips must report the corrupt record as a structured error")
	}
}

// TestParseFlags_S7RestartSmoke covers the flag-combination
// rules for --s7-restart-smoke.
func TestParseFlags_S7RestartSmoke(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{
			name:    "s7-smoke-without-store rejected",
			args:    []string{"--s7-restart-smoke"},
			wantErr: true,
		},
		{
			name:    "s7-smoke-with-store accepted",
			args:    []string{"--authority-store", "/tmp/sparrow", "--s7-restart-smoke"},
			wantErr: false,
		},
		{
			name:    "s5 and s7 mutually exclusive",
			args:    []string{"--authority-store", "/tmp/sparrow", "--s5-bootstrap", "--s7-restart-smoke"},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseFlags(c.args)
			if c.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
