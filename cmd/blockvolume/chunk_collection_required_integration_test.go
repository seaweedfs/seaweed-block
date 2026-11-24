//go:build subprocess && chunkstore_integration

package main_test

import (
	"os"
	"strings"
	"testing"
	"time"
)

// First public release, G3 red: chunk needles must live in a collection used
// only by sw-block, the only fence between SeaweedFS's own fsck and vacuum
// and the chunk data. A chunk volume whose effective collection is empty
// (no --chunk-collection, and the filer gives none) must refuse to start:
// exit 78 (no restart under the shipped unit), a line naming
// --chunk-collection as the cure, and nothing created under --durable-root.
// On main it starts and serves with the chunks in the filer's default
// collection.
func TestChunkVolumeWithoutACollectionIsRefused(t *testing.T) {
	env := startHandoffEnv(t, 1)
	b := env.hosts["s1"]
	var args []string
	all := env.volArgsFor(b)
	for i := 0; i < len(all); i++ {
		if all[i] == "--chunk-collection" {
			i++
			continue
		}
		args = append(args, all[i])
	}
	d := env.h.start(t, "blockvolume-no-collection", env.h.volume, args...)
	err := d.wait(60 * time.Second)
	served := volumeFrontendReady(b.status, "v1")
	cure := d.line("--chunk-collection")
	entries, _ := os.ReadDir(b.root)
	t.Logf("no chunk collection: exit %v; served %v; cure line %q; files under --durable-root %d", err, served, cure, len(entries))
	if exitCode(err) != 78 || served || !strings.Contains(cure, "collection") || len(entries) != 0 {
		t.Fatalf("G3: a chunk volume without a dedicated collection must refuse to start (exit 78, a --chunk-collection cure line, nothing created); exit %v, served %v, %d files", err, served, len(entries))
	}

	// The operator guide's cure, as written: set the collection and start
	// the same volume again. It creates its root, logs the collection and serves.
	d = env.h.start(t, "blockvolume-with-collection", env.h.volume, all...)
	poll(t, 60*time.Second, "the volume never served after --chunk-collection was set", func() bool { return volumeFrontendReady(b.status, "v1") })
	if logged := d.line("chunk collection"); !strings.Contains(logged, `"swblock"`) {
		t.Fatalf("G3: the collection must be logged at start; got %q", logged)
	}
}
