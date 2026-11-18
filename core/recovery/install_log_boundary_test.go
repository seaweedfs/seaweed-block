package recovery

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// DESIGN 7 and section 11 item 6 (plan row 10a): the install-log ack is not
// callable from the frontend. LogicalStorage has no SyncInstallLog, and no
// package outside core/storage (which defines it) and core/recovery (the
// receiver) names InstallLogSyncer. Mutant: adding SyncInstallLog to
// LogicalStorage, or using the interface elsewhere, fails here.
func TestInstallLogSyncerIsReceiverOnly(t *testing.T) {
	if _, ok := reflect.TypeOf((*storage.LogicalStorage)(nil)).Elem().MethodByName("SyncInstallLog"); ok {
		t.Fatal("LogicalStorage exposes SyncInstallLog to every caller")
	}
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "vendor") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if dir := filepath.ToSlash(filepath.Dir(rel)); dir == "core/storage" || dir == "core/recovery" {
			return nil // the defining package and the receiver
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), "InstallLogSyncer") {
			t.Errorf("%s names InstallLogSyncer outside core/storage and core/recovery", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
