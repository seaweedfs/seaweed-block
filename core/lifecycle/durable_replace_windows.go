//go:build windows

package lifecycle

import (
	"errors"
	"os"
)

func replaceDurableFile(source, target string) error {
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(source, target)
}

func syncLifecycleDirectory(string) error {
	return nil
}
