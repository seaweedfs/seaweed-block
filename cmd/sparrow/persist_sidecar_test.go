package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunPersistDemoRetiresOnlyItsOwnedMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sparrow-persist.dat")
	if err := os.WriteFile(path, []byte("prior demo"), 0600); err != nil {
		t.Fatal(err)
	}
	stale := path + ".chunkmap"
	if err := os.WriteFile(stale, []byte("stale map"), 0600); err != nil {
		t.Fatal(err)
	}
	other := path + ".unrelated"
	if err := os.WriteFile(other, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if code := runPersistDemo(options{json: true, persistDemo: true, persistDir: dir}); code != 0 {
		t.Fatalf("demo exit=%d", code)
	}
	if got, err := os.ReadFile(stale); err == nil && string(got) == "stale map" {
		t.Fatal("prior map file survives")
	} else if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(other); err != nil || string(got) != "keep" {
		t.Fatalf("unrelated sibling changed: %q %v", got, err)
	}
}

func TestRunPersistDemoRefusesUnexpectedMapDirectory(t *testing.T) {
	dir := t.TempDir()
	mapPath := filepath.Join(dir, "sparrow-persist.dat.chunkmap")
	if err := os.Mkdir(mapPath, 0700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(mapPath, "keep")
	if err := os.WriteFile(keep, []byte("unrecognized directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if code := runPersistDemo(options{json: true, persistDemo: true, persistDir: dir}); code != 3 {
		t.Fatalf("unexpected map directory exit=%d want3", code)
	}
	if got, err := os.ReadFile(keep); err != nil || string(got) != "unrecognized directory" {
		t.Fatalf("unexpected directory changed: %q %v", got, err)
	}
}
