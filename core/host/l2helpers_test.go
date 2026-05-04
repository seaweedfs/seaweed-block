package host_test

import "os"

// writeFileRaw is a thin shim around os.WriteFile used by L2
// artifact writers. Kept in a separate file so the subprocess
// test file's import set stays minimal.
func writeFileRaw(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}
