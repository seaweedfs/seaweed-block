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
	if err := os.Mkdir(path+".chunkmap", 0700); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(path+".chunkmap", "prior-generation")
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
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("prior demo map survives: %v", err)
	}
	if got, err := os.ReadFile(other); err != nil || string(got) != "keep" {
		t.Fatalf("unrelated sibling changed: %q %v", got, err)
	}
}
