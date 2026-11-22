//go:build linux

package lifecycle

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// INV-LIFECYCLE-STORE-DURABLE-001: creation syncs every new directory's
// parent, including a new ancestor; a trailing separator changes no target.
func TestLifecycleStoreParentSync(t *testing.T) {
	for _, kind := range []string{"node", "volume", "placement"} {
		for _, shape := range []string{"nested", "trailing-slash"} {
			t.Run(kind+"/"+shape, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLifecycleStoreParentSyncChild$", "-test.timeout=8s")
				cmd.Env = append(os.Environ(), "LIFECYCLE_PARENT_ROOT="+t.TempDir(), "LIFECYCLE_PARENT_KIND="+kind, "LIFECYCLE_PARENT_SHAPE="+shape)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("parent sync control: %v context=%v\n%s", err, ctx.Err(), out)
				}
			})
		}
	}
}

func TestLifecycleStoreParentSyncChild(t *testing.T) {
	root := os.Getenv("LIFECYCLE_PARENT_ROOT")
	if root == "" {
		t.Skip("isolated syscall-boundary control")
	}
	dir := filepath.Join(root, "store")
	want := []string{root}
	if os.Getenv("LIFECYCLE_PARENT_SHAPE") == "nested" {
		dir = filepath.Join(root, "new-parent", "store")
		want = []string{filepath.Dir(dir), root}
	} else {
		dir += string(os.PathSeparator)
	}
	var calls []string
	realSync := syncBindingParent
	syncBindingParent = func(path string) error {
		calls = append(calls, path)
		return realSync(path)
	}
	open := func() error {
		switch os.Getenv("LIFECYCLE_PARENT_KIND") {
		case "node":
			_, err := OpenNodeInventoryStore(dir)
			return err
		case "volume":
			_, err := OpenFileStore(dir)
			return err
		case "placement":
			_, err := OpenPlacementIntentStore(dir)
			return err
		default:
			t.Fatal("unknown store kind")
			return nil
		}
	}
	if err := open(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("parent syncs %v want %v", calls, want)
	}
	calls = nil
	if err := open(); err != nil {
		t.Fatal(err)
	}
	want = []string{filepath.Dir(filepath.Clean(dir))}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("existing-store retry syncs %v want %v", calls, want)
	}
}
