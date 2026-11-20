//go:build !linux

package storage

import (
	"errors"
	"os"
)

func dataSyncSupported() error {
	return errors.New("storage: fdatasync/prezero experiment requires Linux")
}
func fdatasyncFile(*os.File) error         { return dataSyncSupported() }
func verifyInitializedFile(*os.File) error { return dataSyncSupported() }

func fallocateFile(*os.File, int64) error      { return dataSyncSupported() }
func verifyAllocatedFile(*os.File, bool) error { return dataSyncSupported() }
