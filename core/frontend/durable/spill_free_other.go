//go:build !linux

package durable

// spillFree is nil off Linux: the COW spill cap is the configured one.
func spillFree(string) func() (int64, error) { return nil }
