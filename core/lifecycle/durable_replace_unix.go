//go:build !windows

package lifecycle

import "os"

func replaceDurableFile(source, target string) error {
	return os.Rename(source, target)
}

func syncLifecycleDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
