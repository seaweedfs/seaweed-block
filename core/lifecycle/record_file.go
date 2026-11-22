package lifecycle

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// Hooks are installed only in isolated subprocess tests. Production uses
// the real filesystem operations; one helper owns all publication seams.
type durableRecordHooks struct {
	crash    func(string)
	fileSync func(*os.File) error
	dirSync  func(string) error
}

var durableRecordHooksForTest atomic.Pointer[durableRecordHooks]

// writeRecordDurably implements INV-LIFECYCLE-STORE-DURABLE-001 for puts and
// replacements. A post-rename error may leave the new record on disk.
func writeRecordDurably(dir, name string, raw []byte) error {
	f, err := os.CreateTemp(dir, name+".*.tmp")
	if err != nil {
		return fmt.Errorf("lifecycle: temp record %s: %w", name, err)
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		return fmt.Errorf("lifecycle: write record %s: %w", name, err)
	}
	hooks := durableRecordHooksForTest.Load()
	fileSync := f.Sync
	if hooks != nil && hooks.fileSync != nil {
		fileSync = func() error { return hooks.fileSync(f) }
	}
	if err := fileSync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("lifecycle: sync record %s: %w", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("lifecycle: close record %s: %w", name, err)
	}
	if hooks != nil && hooks.crash != nil {
		hooks.crash("before-rename")
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("lifecycle: rename record %s: %w", name, err)
	}
	if hooks != nil && hooks.crash != nil {
		hooks.crash("after-rename")
	}
	dirSync := syncBindingParent
	if hooks != nil && hooks.dirSync != nil {
		dirSync = hooks.dirSync
	}
	if err := dirSync(dir); err != nil {
		return fmt.Errorf("lifecycle: sync record directory %s: %w", name, err)
	}
	return nil
}
