//go:build swblock_format_v3_test

package storage

// CreateMappedWALStoreV3Test is available only to tagged cross-package caller
// controls. Production Create/Open remain unchanged until the activation gate.
func CreateMappedWALStoreV3Test(path string, blocks, blockSize, chunkBlocks uint32) (*WALStore, error) {
	return createMappedWALStoreV3ForTest(path, blocks, blockSize, 1<<20, WModeLagged, uint64(chunkBlocks))
}

func OpenMappedWALStoreV3Test(path string) (*WALStore, error) {
	return openWALStoreV3ForTest(path)
}

func FlushMappedWALStoreV3Test(s *WALStore) error {
	return s.flusher.flushOnce()
}
