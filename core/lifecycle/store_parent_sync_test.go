//go:build linux

package lifecycle

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// INV-LIFECYCLE-STORE-DURABLE-001: every open syncs the full ancestor chain,
// including a retry after failed creation; a trailing separator changes no target.
func TestLifecycleStoreParentSync(t *testing.T) {
	for _, kind := range []string{"node", "volume", "placement"} {
		for _, shape := range []string{"nested", "trailing-slash", "failed-parent-sync", "relative-path"} {
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
	shape := os.Getenv("LIFECYCLE_PARENT_SHAPE")
	dir := filepath.Join(root, "new-parent", "deeper", "store")
	if shape == "trailing-slash" {
		dir = filepath.Join(root, "store") + string(os.PathSeparator)
	}
	if shape == "relative-path" {
		if err := os.Chdir(root); err != nil {
			t.Fatal(err)
		}
		dir = filepath.Join("new-parent", "deeper", "store")
	}
	abs, err := filepath.Abs(filepath.Clean(dir))
	if err != nil {
		t.Fatal(err)
	}
	// Independently enumerate absolute path prefixes, deepest first, down to /.
	parts := strings.Split(strings.TrimPrefix(filepath.Dir(abs), "/"), "/")
	var want []string
	for n := len(parts); n > 0; n-- {
		want = append(want, "/"+strings.Join(parts[:n], "/"))
	}
	want = append(want, "/")
	var calls []string
	realSync := syncBindingParent
	failFirst := shape == "failed-parent-sync"
	fault := &os.PathError{Op: "open", Path: want[0], Err: os.ErrPermission}
	syncBindingParent = func(path string) error {
		calls = append(calls, path)
		if failFirst {
			failFirst = false
			return fault
		}
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
	if shape == "failed-parent-sync" {
		if err := open(); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("ancestor sync failure hidden: %v", err)
		}
		if !reflect.DeepEqual(calls, want[:1]) {
			t.Fatalf("failed syncs %v want %v", calls, want[:1])
		}
		calls = nil
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
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("existing-store retry syncs %v want %v", calls, want)
	}
}
