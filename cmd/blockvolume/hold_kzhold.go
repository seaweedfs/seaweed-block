//go:build kzhold

package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// applyBuildTagHold: in a kzhold build (test-only, never released) with
// SWBLOCK_KZHOLD_DIR set, the provider holds before the fresh marker is
// retired: it writes <dir>/reached and waits for <dir>/release.
func applyBuildTagHold(dp *durable.DurableProvider) {
	dir := os.Getenv("SWBLOCK_KZHOLD_DIR")
	if dir == "" {
		return
	}
	log.Printf("blockvolume: TEST-ONLY build (kzhold): holding before the fresh-marker retire; reached=%s release=%s",
		filepath.Join(dir, "reached"), filepath.Join(dir, "release"))
	dp.HoldBeforeChunkFreshRetireForTest(filepath.Join(dir, "reached"), filepath.Join(dir, "release"))
}
