package main

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storeident"
)

// The host config carries the durable root's file-system identity (what the
// heartbeat reports); a memback run (no durable root) reports unknown.
func TestHostConfigStorageIdentityFromDurableRoot(t *testing.T) {
	root := t.TempDir()
	if got, want := hostConfig(flags{durableRoot: root}, nil).StorageIdentity, storeident.Of(root); got != want {
		t.Fatalf("durable root identity = %+v, want %+v", got, want)
	}
	if got := hostConfig(flags{}, nil).StorageIdentity; got != (storeident.Identity{}) {
		t.Fatalf("memback identity = %+v, want unknown", got)
	}
}
