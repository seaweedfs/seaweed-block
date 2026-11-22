//go:build linux

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func crashNodeRegistration(updated bool) NodeRegistration {
	value := "old"
	if updated {
		value = "new"
	}
	return NodeRegistration{
		ServerID: "node-a", Addr: "127.0.0.1:9100",
		SeenAt: time.Unix(1, 0).UTC(),
		Labels: map[string]string{"payload": strings.Repeat(value, 4096)},
	}
}

// RegisterNode must leave a complete old or new registration across a process
// crash at either side of the publication rename (INV-LIFECYCLE-STORE-DURABLE-001).
func TestNodeInventoryCrashKeepsCompleteRegistration(t *testing.T) {
	for _, phase := range []string{"before-rename", "after-rename"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			s, err := OpenNodeInventoryStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			old, next := crashNodeRegistration(false), crashNodeRegistration(true)
			if _, err := s.RegisterNode(old); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNodeInventoryCrashChild$", "-test.timeout=8s")
			cmd.Env = append(os.Environ(), "NODE_INVENTORY_CRASH_DIR="+dir, "NODE_INVENTORY_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("crash point not reached: err=%v context=%v output=%s", err, ctx.Err(), out)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "node-a.json"))
			if err != nil {
				t.Fatalf("crash lost the committed registration: %v", err)
			}
			var got NodeRegistration
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("crash left a partial registration: %v", err)
			}
			if !reflect.DeepEqual(got, old) && !reflect.DeepEqual(got, next) {
				t.Fatal("crash left neither the complete old nor the complete new registration")
			}
			want := old
			if phase == "after-rename" {
				want = next
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("wrong generation at %s", phase)
			}
			reopened, err := OpenNodeInventoryStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := reopened.GetNodeSnapshot("node-a"); !ok || !reflect.DeepEqual(got, want) {
				t.Fatal("reopened snapshot lost the complete registration")
			}
		})
	}
}

func TestNodeInventoryCrashChild(t *testing.T) {
	dir := os.Getenv("NODE_INVENTORY_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess crash injector")
	}
	s, err := OpenNodeInventoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.persistHookForTest = func(phase string) {
		if phase == os.Getenv("NODE_INVENTORY_CRASH_PHASE") {
			os.Exit(73) // abrupt exit: no deferred cleanup or close
		}
	}
	if _, err := s.RegisterNode(crashNodeRegistration(true)); err != nil {
		t.Fatal(err)
	}
	t.Fatal("write returned without reaching the crash point")
}
