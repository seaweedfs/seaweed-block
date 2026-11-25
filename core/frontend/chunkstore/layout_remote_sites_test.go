package chunkstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// layoutRemoteSites are the production calls of NewLayoutRemote: one, the
// blockvolume's ChunkLayoutRemoteFor, which the durable provider calls once
// per attachment and caches (its LayoutRemote accessor returns that one).
// A second LayoutRemote of a volume in a process would roll back the
// first's live session at its first HEAD read, so a new site fails here.
var layoutRemoteSites = map[string]int{"cmd/blockvolume/main.go": 1}

func TestLayoutRemote_OneConstructorSite(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	found := map[string]int{}
	for _, top := range []string{"core", "cmd"} {
		err := filepath.Walk(filepath.Join(root, top), func(p string, fi os.FileInfo, err error) error {
			if err != nil || fi.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			if n := strings.Count(string(b), "NewLayoutRemote("); n > 0 && !strings.HasSuffix(filepath.ToSlash(p), "chunkstore/layout_remote.go") {
				rel, _ := filepath.Rel(root, p)
				found[filepath.ToSlash(rel)] = n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(found) != len(layoutRemoteSites) {
		t.Fatalf("NewLayoutRemote production sites %v; want %v (one runtime per attachment)", found, layoutRemoteSites)
	}
	for f, n := range layoutRemoteSites {
		if found[f] != n {
			t.Fatalf("NewLayoutRemote production sites %v; want %v (one runtime per attachment)", found, layoutRemoteSites)
		}
	}
}
