package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRetiredMigrationCommandRefusesWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy")
	want := []byte("unmodified legacy volume")
	if err := os.WriteFile(path, want, 0600); err != nil {
		t.Fatal(err)
	}
	var out, errout bytes.Buffer
	if code := run([]string{"migrate-v3", "--path", path}, &out, &errout); code == 0 {
		t.Fatal("retired converter accepted input")
	}
	if !bytes.Contains(errout.Bytes(), []byte("unknown command")) {
		t.Fatalf("converter still dispatched: %s", errout.Bytes())
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("retired converter changed input: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("retired converter created sidecars: %v %v", entries, err)
	}
}
