//go:build linux

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func defaultDataSyncMode(f *os.File) (string, string) {
	info, err := f.Stat()
	if err != nil {
		return "fsync", "device stat unavailable: " + err.Error()
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "fsync", "device stat unavailable"
	}
	return deviceDataSyncMode("/sys", unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)))
}

// Resolve the mounted filesystem's device, including partition parents and
// stacked devices. Only an entirely known write-through backing set opts in.
func deviceDataSyncMode(root string, major, minor uint32) (string, string) {
	path := filepath.Join(root, "dev", "block", fmt.Sprintf("%d:%d", major, minor))
	if err := requireWriteThrough(path, make(map[string]bool), 0); err != nil {
		return "fsync", fmt.Sprintf("device=%d:%d %v", major, minor, err)
	}
	return "fdatasync", fmt.Sprintf("device=%d:%d all backing devices write through", major, minor)
}

func requireWriteThrough(path string, visiting map[string]bool, depth int) error {
	if depth > 8 {
		return fmt.Errorf("backing device depth exceeded")
	}
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("device unavailable: %w", err)
	}
	if visiting[path] {
		return fmt.Errorf("backing device cycle at %s", path)
	}
	visiting[path] = true
	defer delete(visiting, path)
	if _, err := os.Stat(filepath.Join(path, "partition")); err == nil {
		return requireWriteThrough(filepath.Dir(path), visiting, depth+1)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("partition identity unavailable: %w", err)
	}
	slaves, err := os.ReadDir(filepath.Join(path, "slaves"))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("backing devices unavailable: %w", err)
	}
	if len(slaves) > 0 {
		for _, slave := range slaves {
			if err := requireWriteThrough(filepath.Join(path, "slaves", slave.Name()), visiting, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	// A virtual dm/md queue alone does not establish the physical cache policy.
	for _, kind := range []string{"dm", "md"} {
		if _, err := os.Stat(filepath.Join(path, kind)); err == nil {
			return fmt.Errorf("%s backing devices unavailable", kind)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("%s identity unavailable: %w", kind, err)
		}
	}
	cache, err := os.ReadFile(filepath.Join(path, "queue", "write_cache"))
	if err != nil {
		return fmt.Errorf("write_cache unavailable: %w", err)
	}
	if strings.TrimSpace(string(cache)) != "write through" {
		return fmt.Errorf("device=%s write_cache=%q", filepath.Base(path), strings.TrimSpace(string(cache)))
	}
	return nil
}
