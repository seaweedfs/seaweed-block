package layout

// anyStart is a local root that can start from any HEAD: these tests do
// not model the local store (start_seed_test does).
func anyStart(Head) error { return nil }
