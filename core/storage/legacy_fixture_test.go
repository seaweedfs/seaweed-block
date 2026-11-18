package storage

// TODO(format-v3 row 14a): relocate or retire only the inventory-listed v2
// engine controls after the named v3 replacements and migration receipts pass.
// These explicit fixture constructors are never production selectors.
func createLegacyWALStoreForTest(path string, blocks uint32, blockSize int) (*WALStore, error) {
	return createLegacyWALStoreWithOptions(path, blocks, blockSize, WALStoreOptions{})
}
func openLegacyWALStoreForTest(path string) (*WALStore, error) {
	return openLegacyWALStoreWithOptions(path, WALStoreOptions{})
}
func openLegacyReadOnlyForTest(path string) (*WALStoreReader, error) {
	return openLegacyReadOnlyWithOptions(path, WALStoreOptions{})
}
