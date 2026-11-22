package hostlimit

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CheckLaunch refuses a host-limit configuration whose premises do not
// hold, before the process uses it. The directory must exist as a real
// directory, outside temporary directories that cleaners empty, on a local
// disk file system (ext4, xfs, btrfs, zfs, f2fs: kernel file locks and the
// host monotonic clock that orders tickets belong to one host), and a
// configuration already published there by other
// processes must equal this one. The premise that every process on the
// host uses the same directory cannot be checked from one process.
func CheckLaunch(c Config) error {
	return checkLaunch(c, []string{"/tmp", "/var/tmp", "/dev/shm", os.TempDir()}, unsupportedFS)
}

func checkLaunch(c Config, tempRoots []string, unsupported func(string) (string, bool)) error {
	if err := c.Validate(); err != nil {
		return err
	}
	dir := filepath.Clean(c.Directory)
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("hostlimit: --host-limit-dir %s: %w", dir, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("hostlimit: --host-limit-dir %s must be a directory, not a symlink or a file", dir)
	}
	for _, root := range tempRoots {
		root = filepath.Clean(root)
		if root != "" && root != "." && (dir == root || strings.HasPrefix(dir, root+string(filepath.Separator))) {
			return fmt.Errorf("hostlimit: --host-limit-dir %s is under %s, which temporary-file cleaners empty", dir, root)
		}
	}
	if fs, bad := unsupported(dir); bad {
		return fmt.Errorf("hostlimit: --host-limit-dir %s is on %s, not a local disk file system (ext4, xfs, btrfs, zfs, f2fs); slot locks and ticket order need one host's local disk", dir, fs)
	}
	existing, err := os.ReadFile(filepath.Join(dir, "configuration.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("hostlimit: --host-limit-dir %s: %w", dir, err)
	}
	expected, _ := json.Marshal(c)
	if string(existing) != string(expected) {
		return fmt.Errorf("hostlimit: --host-limit-dir %s is in use with other host-limit flags (%s); every process on the host must use identical flags (this one: %s)", dir, existing, expected)
	}
	return nil
}
