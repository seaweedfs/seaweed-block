package lifecycle

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

func TestLifecycleLayoutIdentity(t *testing.T) {
	// Isolate the persistence seams. Windows cannot sync a directory handle;
	// its child exercises record identity, not filesystem crash durability.
	if os.Getenv("SWB_LAYOUT_ID_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLifecycleLayoutIdentity$", "-test.v")
		cmd.Env = append(os.Environ(), "SWB_LAYOUT_ID_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("identity child: %v\n%s", err, out)
		}
		return
	}
	hooks := &durableRecordHooks{}
	if runtime.GOOS == "windows" {
		hooks.dirSync = func(string) error { return nil }
	}
	durableRecordHooksForTest.Store(hooks)
	defer durableRecordHooksForTest.Store(nil)
	dir := t.TempDir()
	s, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "v", SizeBytes: 1 << 20, ReplicationFactor: 1}
	a, err := s.CreateVolume(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := layout.ParseVolumeID(a.Spec.LayoutID); err != nil {
		t.Fatal("create did not mint a layout id", err)
	}
	b, err := s.CreateVolume(spec)
	if err != nil || a.Spec.LayoutID != b.Spec.LayoutID {
		t.Fatalf("retry changed identity: %v %+v", err, b)
	}
	metadata := spec
	metadata.PVCName = "claim"
	if rec, err := s.CreateVolume(metadata); err != nil || rec.Spec.LayoutID != a.Spec.LayoutID {
		t.Fatalf("metadata changed identity: %v", err)
	}
	if rec, err := s.AttachVolume("v", "node"); err != nil || rec.Spec.LayoutID != a.Spec.LayoutID {
		t.Fatalf("attach changed identity: %v", err)
	}
	if rec, err := s.DetachVolume("v", "node"); err != nil || rec.Spec.LayoutID != a.Spec.LayoutID {
		t.Fatalf("detach changed identity: %v", err)
	}
	concurrent := spec
	concurrent.VolumeID = "concurrent"
	type result struct {
		rec VolumeRecord
		err error
	}
	results := make(chan result, 16)
	for i := 0; i < 16; i++ {
		go func() { rec, err := s.CreateVolume(concurrent); results <- result{rec, err} }()
	}
	var minted string
	for i := 0; i < 16; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if minted == "" {
			minted = r.rec.Spec.LayoutID
		}
		if minted != r.rec.Spec.LayoutID {
			t.Fatal("concurrent creates minted multiple identities")
		}
	}
	loaded, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err = loaded.CreateVolume(spec)
	if err != nil || a.Spec.LayoutID != b.Spec.LayoutID {
		t.Fatalf("restart changed identity: %v %+v", err, b)
	}
	other, _ := layout.NewVolumeID()
	bad := spec
	bad.LayoutID = other.String()
	if _, err := loaded.CreateVolume(bad); !errors.Is(err, ErrVolumeConflict) {
		t.Fatalf("replacement id accepted: %v", err)
	}
	if err := loaded.DeleteVolume("v"); err != nil {
		t.Fatal(err)
	}
	b, err = loaded.CreateVolume(spec)
	if err != nil || a.Spec.LayoutID == b.Spec.LayoutID {
		t.Fatalf("recreate reused identity: %v %+v", err, b)
	}
	// A failed directory sync may have left the complete first record on disk.
	// Its retry must settle that same identity, without publishing on failure.
	failed := true
	hooks.dirSync = func(string) error {
		if failed {
			return errors.New("sync failed")
		}
		return nil
	}
	failedSpec := spec
	failedSpec.VolumeID = "uncertain"
	if _, err := loaded.CreateVolume(failedSpec); err == nil {
		t.Fatal("sync failure hidden")
	}
	if _, ok := loaded.GetVolume("uncertain"); ok {
		t.Fatal("failed put published memory")
	}
	raw, err := os.ReadFile(loaded.path("uncertain"))
	if err != nil {
		t.Fatal(err)
	}
	var written VolumeRecord
	if err := json.Unmarshal(raw, &written); err != nil {
		t.Fatal(err)
	}
	failed = false
	b, err = loaded.CreateVolume(failedSpec)
	if err != nil || b.Spec.LayoutID != written.Spec.LayoutID {
		t.Fatalf("unknown put retry changed identity: %v %+v", err, b)
	}
}
