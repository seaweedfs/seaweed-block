package authority

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
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
			// The restart reloads whatever survived: the new record or none, never an error.
			s, err := NewFileAuthorityStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			recs, err := s.Load()
			if err != nil {
				t.Fatalf("reload after the fail-stop: %v", err)
			}
			if len(recs) > 1 || (len(recs) == 1 && (recs[0].VolumeID != "v1" || recs[0].Epoch != 7)) {
				t.Fatalf("reload after the fail-stop: %+v, want the written record or none", recs)
			}
		})
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
