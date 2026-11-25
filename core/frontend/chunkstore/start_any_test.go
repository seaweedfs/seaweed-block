package chunkstore

import "github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"

// anyStart is a local root that can start from any HEAD: these tests do
// not model the local store.
func anyStart(layout.Head) error { return nil }
