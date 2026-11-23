package durable

import (
	"fmt"
	"log"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// rootFree probes the free bytes of the durable root's file system (nil where
// it cannot tell); tests replace it.
var rootFree = spillFree

// Operator guide, Size a host: a full local disk fails guest writes, so a
// volume is created only with room for its full size and its WAL, and an
// existing volume opens with a loud warning when its remaining sparse growth
// and the WAL no longer fit. Neither adds state.
func (p *DurableProvider) refuseCreateWithoutHeadroom(volumeID string) error {
	free := rootFree(p.cfg.StorageRoot)
	if free == nil {
		return nil
	}
	have, err := free()
	size := int64(p.cfg.NumBlocks) * int64(p.cfg.BlockSize)
	if need := size + storage.DefaultWALBytes; err == nil && have < need {
		return fmt.Errorf("durable: %s has %d bytes free; volume %s needs %d (size %d + WAL %d): free space on it or use a larger file system",
			p.cfg.StorageRoot, have, volumeID, need, size, storage.DefaultWALBytes)
	}
	return nil
}

func (p *DurableProvider) warnLowHeadroom(volumeID, path string, size int64) {
	free := rootFree(p.cfg.StorageRoot)
	if free == nil {
		return
	}
	have, err := free()
	// The store's allocation includes its WAL; only the rest is data.
	growth := max(size-max(allocatedBytes(path)-storage.DefaultWALBytes, 0), 0)
	if err == nil && have < growth+storage.DefaultWALBytes {
		log.Printf("durable: WARNING volume %s: %s has %d bytes free, less than the volume's remaining growth %d + WAL %d; when the disk fills, guest writes fail: free space on it",
			volumeID, p.cfg.StorageRoot, have, growth, storage.DefaultWALBytes)
	}
}
