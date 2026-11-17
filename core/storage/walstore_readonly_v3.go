package storage

// Dark selector for row12a caller controls; activation later changes only the
// public selector. The current reader still understands only legacy layout.
func openReadOnlyV3ForTest(path string, opts WALStoreOptions) (*WALStoreReader, error) {
	return OpenReadOnlyWithOptions(path, opts)
}
