package durable

// SetRootFreeForTest replaces the durable root's free-space probe and returns
// the restore function.
func SetRootFreeForTest(free int64) func() {
	old := rootFree
	rootFree = func(string) func() (int64, error) { return func() (int64, error) { return free, nil } }
	return func() { rootFree = old }
}
