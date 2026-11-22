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

// INV-LIFECYCLE-STORE-DURABLE-001: each public write retains the complete
// old/new record across abrupt process exit at both rename boundaries.
func TestFileStoreCrashKeepsCompleteRecord(t *testing.T)      { checkStoreCrash(t, "volume") }
func TestPlacementStoreCrashKeepsCompleteRecord(t *testing.T) { checkStoreCrash(t, "placement") }

type recordCrashFixture struct {
	old, next any
	put       func(bool) error
	get       func() (any, bool)
	setSync   func(func(*os.File) error, func(string) error)
}

func openRecordCrashFixture(t *testing.T, kind, dir string, hook func(string)) recordCrashFixture {
	t.Helper()
	if kind == "node" {
		s, err := OpenNodeInventoryStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		s.persistHookForTest = hook
		return recordCrashFixture{old: crashNodeRegistration(false), next: crashNodeRegistration(true),
			put: func(updated bool) error { _, err := s.RegisterNode(crashNodeRegistration(updated)); return err },
			get: func() (any, bool) {
				r, ok := s.GetNodeSnapshot("node-a")
				memory, exists := s.GetNode("node-a")
				if exists != ok || !reflect.DeepEqual(memory, r) {
					t.Fatal("node records and snapshot diverged")
				}
				return r, ok
			},
			setSync: func(f func(*os.File) error, d func(string) error) { s.fileSyncForTest, s.dirSyncForTest = f, d },
		}
	}
	if kind == "volume" {
		s, err := OpenFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		s.persistHookForTest = hook
		old := VolumeRecord{Spec: VolumeSpec{VolumeID: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 1, Protocol: "iscsi", PVCName: strings.Repeat("payload", 1024)}}
		next := old
		next.AttachedTo = "node-b"
		return recordCrashFixture{old: old, next: next,
			put: func(updated bool) error {
				if updated {
					_, err := s.AttachVolume("vol-a", "node-b")
					return err
				}
				_, err := s.CreateVolume(old.Spec)
				return err
			}, get: func() (any, bool) {
				r, ok := s.GetVolumeSnapshot("vol-a")
				memory, exists := s.GetVolume("vol-a")
				if exists != ok || !reflect.DeepEqual(memory, r) {
					t.Fatal("volume records and snapshot diverged")
				}
				return r, ok
			},
			setSync: func(f func(*os.File) error, d func(string) error) { s.fileSyncForTest, s.dirSyncForTest = f, d },
		}
	}
	if kind != "placement" {
		t.Fatalf("unknown fixture %q", kind)
	}
	s, err := OpenPlacementIntentStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.persistHookForTest = hook
	old := PlacementIntent{VolumeID: "vol-a", DesiredRF: 1, Slots: []PlacementSlotIntent{{ServerID: "node-a", PoolID: strings.Repeat("old", 4096), Source: PlacementSourceBlankPool}}}
	next := copyPlacementIntent(old)
	next.Slots[0].PoolID = strings.Repeat("new", 4096)
	return recordCrashFixture{old: old, next: next,
		put: func(updated bool) error {
			r := old
			if updated {
				r = next
			}
			_, err := s.ApplyPlan(PlacementPlan{VolumeID: "vol-a", DesiredRF: 1, Candidates: []PlacementCandidate{{VolumeID: "vol-a", ServerID: "node-a", PoolID: r.Slots[0].PoolID, Source: PlacementSourceBlankPool}}})
			return err
		}, get: func() (any, bool) {
			r, ok := s.GetPlacementSnapshot("vol-a")
			memory, exists := s.GetPlacement("vol-a")
			if exists != ok || !reflect.DeepEqual(memory, r) {
				t.Fatal("placement records and snapshot diverged")
			}
			return r, ok
		},
		setSync: func(f func(*os.File) error, d func(string) error) { s.fileSyncForTest, s.dirSyncForTest = f, d },
	}
}

func checkStoreCrash(t *testing.T, kind string) {
	t.Helper()
	for _, phase := range []string{"before-rename", "after-rename"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			f := openRecordCrashFixture(t, kind, dir, nil)
			if err := f.put(false); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecordStoreCrashChild$", "-test.timeout=8s")
			cmd.Env = append(os.Environ(), "RECORD_STORE_CRASH_DIR="+dir, "RECORD_STORE_CRASH_KIND="+kind, "RECORD_STORE_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("crash point not reached: %v context=%v output=%s", err, ctx.Err(), out)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "vol-a.json"))
			if err != nil {
				t.Fatalf("crash lost the committed record: %v", err)
			}
			want := f.old
			if phase == "after-rename" {
				want = f.next
			}
			wantRaw, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			var gotJSON, wantJSON any
			if err := json.Unmarshal(raw, &gotJSON); err != nil {
				t.Fatalf("partial record: %v", err)
			}
			if err := json.Unmarshal(wantRaw, &wantJSON); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotJSON, wantJSON) {
				t.Fatalf("wrong or incomplete record at %s", phase)
			}
			reopened := openRecordCrashFixture(t, kind, dir, nil)
			if got, ok := reopened.get(); !ok || !reflect.DeepEqual(got, want) {
				t.Fatal("reopened snapshot differs from the complete record")
			}
		})
	}
}

func TestRecordStoreCrashChild(t *testing.T) {
	dir := os.Getenv("RECORD_STORE_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess crash injector")
	}
	f := openRecordCrashFixture(t, os.Getenv("RECORD_STORE_CRASH_KIND"), dir, func(phase string) {
		if phase == os.Getenv("RECORD_STORE_CRASH_PHASE") {
			os.Exit(73)
		}
	})
	if err := f.put(true); err != nil {
		t.Fatal(err)
	}
	t.Fatal("write returned without reaching the crash point")
}

// The injected operations delegate to real fsync, so omitting either call
// loses its event or fails to surface the injected error.
func TestLifecyclePutSyncOrderAndFailure(t *testing.T) {
	for _, kind := range []string{"node", "volume", "placement"} {
		for _, failure := range []string{"none", "file", "directory", "nil-hooks"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				dir := t.TempDir()
				f := openRecordCrashFixture(t, kind, dir, nil)
				if err := f.put(false); err != nil {
					t.Fatal(err)
				}
				fault := errors.New("injected sync failure")
				var events []string
				if failure != "nil-hooks" {
					f.setSync(func(file *os.File) error {
						events = append(events, "file")
						if filepath.Ext(file.Name()) != ".tmp" {
							t.Fatal("sync must target the candidate file")
						}
						if failure == "file" {
							return fault
						}
						return file.Sync()
					}, func(path string) error {
						events = append(events, "directory")
						if path != dir {
							t.Fatal("sync must target the record directory")
						}
						if failure == "directory" {
							return fault
						}
						return syncBindingParent(path)
					})
				}
				err := f.put(true)
				want := f.next
				if failure == "file" || failure == "directory" {
					if !errors.Is(err, fault) {
						t.Fatalf("sync failure hidden: %v", err)
					}
					want = f.old
				} else if err != nil {
					t.Fatal(err)
				}
				if got, ok := f.get(); !ok || !reflect.DeepEqual(got, want) {
					t.Fatal("memory published before successful sync")
				}
				wantEvents := []string{"file", "directory"}
				if failure == "file" {
					wantEvents = []string{"file"}
				}
				if failure == "nil-hooks" {
					wantEvents = nil
				}
				if !reflect.DeepEqual(events, wantEvents) {
					t.Fatalf("sync events %v want %v", events, wantEvents)
				}
				reopened := openRecordCrashFixture(t, kind, dir, nil)
				diskWant := f.next
				if failure == "file" {
					diskWant = f.old
				}
				if got, ok := reopened.get(); !ok || !reflect.DeepEqual(got, diskWant) {
					t.Fatal("wrong durable record after sync outcome")
				}
			})
		}
	}
}
