package authority

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

const failStopHelperEnv = "SWB_AUTHORITY_FAILSTOP_DIR"

func failStopRecord(kind string) DurableRecord {
	r := DurableRecord{VolumeID: "v1", ReplicaID: "r1", Epoch: 7, EndpointVersion: 1, DataAddr: "d", CtrlAddr: "c", WriteSeq: 1}
	if kind == "workspace" {
		r.Workspace = &WorkspaceState{Lease: &WorkspaceLease{ReplicaID: "r1", Epoch: 7}}
	}
	return r
}

// TestAuthorityStoreFailStopHelper is the child process of
// TestPutFailStopsWhenTheDirectoryFsyncFailsAfterRename: one Put with an
// injected directory fsync failure. Reaching the line after Put means Put
// returned to its caller, which the fail-stop rule forbids.
func TestAuthorityStoreFailStopHelper(t *testing.T) {
	dir := os.Getenv(failStopHelperEnv)
	if dir == "" {
		t.Skip("helper process only")
	}
	s, err := NewFileAuthorityStore(dir)
	if err != nil {
		fmt.Println("HELPER-SETUP-FAILED", err)
		os.Exit(3)
	}
	step := os.Getenv(failStopHelperEnv + "_STEP")
	SetDirFaultForTest(func(at string) error {
		if at == step {
			return errors.New("injected directory " + step + " failure")
		}
		return nil
	})
	err = s.Put(failStopRecord(os.Getenv(failStopHelperEnv + "_KIND")))
	fmt.Printf("PUT-RETURNED err=%v\n", err)
	os.Exit(0)
}

// A directory open or fsync failure after the rename leaves the record's
// durability unknown: blockmaster fail-stops (the conventional answer,
// PostgreSQL after fsyncgate). No caller is ever told "failed" for a record
// that may survive; the restart reloads whatever is on disk. Same rule for
// every record type.
func TestPutFailStopsWhenTheDirectoryFsyncFailsAfterRename(t *testing.T) {
	for _, c := range []struct{ kind, step string }{
		{"assignment", "fsync"}, {"workspace", "fsync"}, {"assignment", "open"}, {"workspace", "open"},
	} {
		kind, step := c.kind, c.step
		t.Run(kind+"/"+step, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestAuthorityStoreFailStopHelper$", "-test.count=1")
			cmd.Env = append(os.Environ(), failStopHelperEnv+"="+dir, failStopHelperEnv+"_KIND="+kind, failStopHelperEnv+"_STEP="+step)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != ExitDurabilityUnknown {
				t.Fatalf("child: err=%v, want exit %d\n%s", err, ExitDurabilityUnknown, out)
			}
			if strings.Contains(string(out), "PUT-RETURNED") {
				t.Fatalf("Put returned to its caller after a failed directory fsync:\n%s", out)
			}
			if !strings.Contains(string(out), "authority_store: FAIL-STOP: directory fsync after rename") ||
				!strings.Contains(string(out), "directory "+step+": injected directory "+step+" failure") {
				t.Fatalf("missing the typed fail-stop line:\n%s", out)
			}
			// The restart reloads whatever survived. The child stopped after the
			// rename without a power loss, so the record is on disk: the reload
			// must return exactly it, with no skipped (corrupt) record.
			if err := reloadExactly(dir, failStopRecord(kind)); err != nil {
				t.Fatalf("reload after the fail-stop: %v", err)
			}
		})
	}
}

// reloadExactly reopens the store at dir and returns nil only if it loads
// exactly want and skips nothing.
func reloadExactly(dir string, want DurableRecord) error {
	s, err := NewFileAuthorityStore(dir)
	if err != nil {
		return err
	}
	defer s.Close()
	recs, skips, err := s.LoadWithSkips()
	switch {
	case err != nil:
		return err
	case len(skips) != 0:
		return fmt.Errorf("%d skipped record(s): %v", len(skips), skips)
	case len(recs) != 1:
		return fmt.Errorf("%d records, want exactly the written one", len(recs))
	case !reflect.DeepEqual(recs[0], want):
		return fmt.Errorf("reloaded %+v, want %+v", recs[0], want)
	}
	return nil
}

// The reload check refuses each way the restart could mislead it: a corrupt
// record (skipped by Load), no record, a different record.
func TestReloadExactlyRefusesCorruptMissingOrDifferent(t *testing.T) {
	want := failStopRecord("assignment")
	put := func(t *testing.T, r DurableRecord) string {
		dir := t.TempDir()
		s, err := NewFileAuthorityStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Put(r); err != nil {
			t.Fatal(err)
		}
		s.Close()
		return dir
	}
	if err := reloadExactly(put(t, want), want); err != nil {
		t.Fatalf("control: the written record must reload: %v", err)
	}
	if reloadExactly(t.TempDir(), want) == nil {
		t.Fatal("an empty store passed")
	}
	other := want
	other.Epoch = 8
	if reloadExactly(put(t, other), want) == nil {
		t.Fatal("a different record passed")
	}
	dir := put(t, want)
	ents, err := os.ReadDir(dir)
	if err != nil || len(ents) == 0 {
		t.Fatalf("store dir: %v %d", err, len(ents))
	}
	for _, e := range ents {
		if err := os.WriteFile(filepath.Join(dir, e.Name()), []byte("corrupt"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if reloadExactly(dir, want) == nil {
		t.Fatal("a corrupt record passed")
	}
}

// A close error after a successful directory fsync changes nothing on disk:
// the rename is durable, so Put reports the record written (telling the
// caller "failed" would make two truths).
func TestPutReportsWrittenWhenOnlyTheDirectoryCloseFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows cannot fsync a directory: the close step follows a fsync that fails there")
	}
	dir := t.TempDir()
	s, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	SetDirFaultForTest(func(step string) error {
		if step == "close" {
			return errors.New("injected directory close failure")
		}
		return nil
	})
	defer SetDirFaultForTest(nil)
	if err := s.Put(failStopRecord("assignment")); err != nil {
		t.Fatalf("Put with only a failed directory close: %v; want written", err)
	}
	recs, err := s.Load()
	if err != nil || len(recs) != 1 || recs[0].Epoch != 7 {
		t.Fatalf("after Put: %+v %v; want the written record", recs, err)
	}
}
