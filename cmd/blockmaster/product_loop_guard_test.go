package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// clusterSpecWithRF writes a cluster spec of three nodes and one volume v1
// with the given replication factor (placements on the first rf nodes).
func clusterSpecWithRF(t *testing.T, rf int) string { return clusterSpecText(t, rf, true) }

// clusterSpecText is clusterSpecWithRF with or without size_bytes (without:
// a placement-only entry, which has a placement intent and no volume spec).
func clusterSpecText(t *testing.T, rf int, sized bool) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("nodes:\n")
	for i := 1; i <= 3; i++ {
		b.WriteString("  - server_id: s" + string(rune('0'+i)) + "\n    data_addr: 127.0.0.1:1" + string(rune('0'+i)) + "601\n    ctrl_addr: 127.0.0.1:1" + string(rune('0'+i)) + "602\n")
	}
	b.WriteString("volumes:\n  - id: v1\n")
	if sized {
		b.WriteString("    size_bytes: 268435456\n")
	}
	b.WriteString("    replication_factor: " + string(rune('0'+rf)) + "\n    placements:\n")
	for i := 1; i <= rf; i++ {
		b.WriteString("      - server_id: s" + string(rune('0'+i)) + "\n        replica_id: r" + string(rune('0'+i)) + "\n")
	}
	p := filepath.Join(t.TempDir(), "cluster-spec.yaml")
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// A replicated cluster-spec volume with the product loop off is refused at
// start, naming the flag, before anything is opened (run returns 2).
func TestReplicatedClusterSpecWithTheProductLoopOffIsRefused(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", filepath.Join(t.TempDir(), "authority"),
		"--lifecycle-store", filepath.Join(t.TempDir(), "lifecycle"),
		"--cluster-spec", clusterSpecWithRF(t, 3),
		"--expected-slots-per-volume", "3",
		"--listen", "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stderr
	os.Stderr = w
	done := make(chan int, 1)
	go func() { done <- run(f) }() // without the guard run starts and serves
	var code int
	select {
	case code = <-done:
	case <-time.After(10 * time.Second):
		os.Stderr = saved
		t.Fatal("blockmaster started with a replicated cluster spec and the product loop off")
	}
	os.Stderr = saved
	w.Close()
	out, _ := io.ReadAll(r)
	if code != 2 || !strings.Contains(string(out), "--lifecycle-product-loop-interval") || !strings.Contains(string(out), "v1") {
		t.Fatalf("run=%d, stderr %q; want 2 and the flag named", code, out)
	}
}

// The same spec with the loop on, a single-replica spec with the loop off,
// and no cluster spec (a static topology) all pass the guard.
func TestTheProductLoopGuardPassesWhatItShould(t *testing.T) {
	for name, c := range map[string]struct {
		rf       int
		interval time.Duration
	}{
		"rf3 with the loop on":  {3, 100 * time.Millisecond},
		"rf1 with the loop off": {1, 0},
	} {
		spec, err := loadClusterSpec(clusterSpecWithRF(t, c.rf))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := requireProductLoop(c.interval, c.rf, spec); err != nil {
			t.Fatalf("%s: refused: %v", name, err)
		}
	}
	if err := requireProductLoop(0, 3, clusterSpecImport{}); err != nil {
		t.Fatalf("no cluster spec (static topology): refused: %v", err)
	}
	spec, err := loadClusterSpec(clusterSpecWithRF(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := requireProductLoop(0, 2, spec); err == nil {
		t.Fatal("rf2 with the loop off passed")
	}
}

// A replicated cluster-spec volume whose replication factor differs from
// --expected-slots-per-volume is refused at start, naming the volume and
// both counts, also with the loop on; a matching one passes.
func TestAClusterSpecVolumeOfAnotherReplicationFactorIsRefused(t *testing.T) {
	spec, err := loadClusterSpec(clusterSpecWithRF(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	err = requireProductLoop(100*time.Millisecond, 3, spec)
	if err == nil || !strings.Contains(err.Error(), "v1") || !strings.Contains(err.Error(), "--expected-slots-per-volume is 3") {
		t.Fatalf("an RF2 volume on an expected-3 master: %v; want a refusal naming v1 and both counts", err)
	}
	if err := requireProductLoop(100*time.Millisecond, 2, spec); err != nil {
		t.Fatalf("an RF2 volume on an expected-2 master: refused: %v", err)
	}
}

// A placement-only replicated entry (no size_bytes: no volume spec, only a
// placement intent) is checked the same way: RF2 on an expected-3 master is
// refused, and RF3 with the loop off is refused.
func TestAPlacementOnlyClusterSpecVolumeIsChecked(t *testing.T) {
	for _, c := range []struct {
		name     string
		rf       int
		interval time.Duration
		want     string
	}{
		{"rf2 on expected 3", 2, 100 * time.Millisecond, "--expected-slots-per-volume is 3"},
		{"rf3 with the loop off", 3, 0, "--lifecycle-product-loop-interval is 0"},
	} {
		spec, err := loadClusterSpec(clusterSpecText(t, c.rf, false))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(spec.Volumes) != 0 || len(spec.Placements) != 1 {
			t.Fatalf("%s: fixture: %d volume specs, %d placements; want a placement-only entry", c.name, len(spec.Volumes), len(spec.Placements))
		}
		if err := requireProductLoop(c.interval, 3, spec); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v; want a refusal with %q", c.name, err, c.want)
		}
	}
}
