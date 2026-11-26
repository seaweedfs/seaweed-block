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
func clusterSpecWithRF(t *testing.T, rf int) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("nodes:\n")
	for i := 1; i <= 3; i++ {
		b.WriteString("  - server_id: s" + string(rune('0'+i)) + "\n    data_addr: 127.0.0.1:1" + string(rune('0'+i)) + "601\n    ctrl_addr: 127.0.0.1:1" + string(rune('0'+i)) + "602\n")
	}
	b.WriteString("volumes:\n  - id: v1\n    size_bytes: 268435456\n    replication_factor: " + string(rune('0'+rf)) + "\n    placements:\n")
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
		if err := requireProductLoop(c.interval, spec); err != nil {
			t.Fatalf("%s: refused: %v", name, err)
		}
	}
	if err := requireProductLoop(0, clusterSpecImport{}); err != nil {
		t.Fatalf("no cluster spec (static topology): refused: %v", err)
	}
	spec, err := loadClusterSpec(clusterSpecWithRF(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := requireProductLoop(0, spec); err == nil {
		t.Fatal("rf2 with the loop off passed")
	}
}
