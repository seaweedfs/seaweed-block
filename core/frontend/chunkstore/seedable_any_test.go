package chunkstore

// anySeedable is a local root whose store can seed any position: these
// tests do not model the store limit (layout start_test does).
func anySeedable(uint64) error { return nil }
