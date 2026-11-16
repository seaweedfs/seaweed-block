package main

import (
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"testing"
)

// Centralize the temporary authority runtime configuration separately from
// chunk-upload callers. Authority is issued by the real master runtime;
// callers never seed durable authority records directly.
func gcTestMasterConfig(t *testing.T) master.Config {
	t.Helper()
	return master.Config{AuthorityStoreDir: t.TempDir(), Listen: "127.0.0.1:0"}
}
