package storage

// makeBlock is a tiny test helper shared across the package's internal
// _test.go files. It used to live in contract_test.go, but that file
// moved to package `storage_test` (external) when memorywal was added
// to the contract suite — so this internal copy keeps the existing
// internal tests (flusher_test, walstore_test) compiling.
func makeBlock(blockSize int, fillByte byte) []byte {
	b := make([]byte, blockSize)
	for i := range b {
		b[i] = fillByte
	}
	return b
}
