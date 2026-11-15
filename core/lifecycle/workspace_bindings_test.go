package lifecycle

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceBindingAbsentRetirementKeepsWatermark(t *testing.T) {
	dir := t.TempDir()
	s, e := OpenWorkspaceBindingStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Retire("v", "old", 3); e != nil {
		t.Fatal(e)
	}
	if _, bound, e := s.Get("v"); e != nil || bound {
		t.Fatal("retirement-only history entered explicit binding mode", bound, e)
	}
	b := WorkspaceBinding{VolumeID: "v", ServerID: "old", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
	if e = s.Register(b); e != nil {
		t.Fatal(e)
	}
	s, e = OpenWorkspaceBindingStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Retire("v", "old", 3); e != nil {
		t.Fatal(e)
	}
	list, bound, e := s.Get("v")
	if e != nil || !bound || len(list) != 1 || list[0].Retired {
		t.Fatal("absent retirement lost watermark and retired new target", list, bound, e)
	}
}

func TestWorkspaceBindingRetirementReplayDoesNotRetireRepreparedTarget(t *testing.T) {
	dir := t.TempDir()
	s, e := OpenWorkspaceBindingStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	b := WorkspaceBinding{VolumeID: "v", ServerID: "node", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
	if e = s.Register(b); e != nil {
		t.Fatal(e)
	}
	if e = s.Retire("v", "node", 1); e != nil {
		t.Fatal(e)
	}
	if list, bound, e := s.Get("v"); e != nil || !bound || !list[0].Retired {
		t.Fatal("retirement lost explicit marker", list, bound, e)
	}
	b.DataAddr = "127.0.0.1:10002"
	if e = s.Register(b); e != nil {
		t.Fatal(e)
	}
	if e = s.Retire("v", "node", 1); e != nil {
		t.Fatal(e)
	}
	s, e = OpenWorkspaceBindingStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	list, bound, e := s.Get("v")
	if e != nil || !bound || len(list) != 1 || list[0].Retired || list[0].DataAddr != b.DataAddr {
		t.Fatal("old retirement killed new binding", list, bound, e)
	}
	if e = s.Retire("v", "node", 2); e != nil {
		t.Fatal(e)
	}
	list, _, e = s.Get("v")
	if e != nil || !list[0].Retired {
		t.Fatal(list, e)
	}
}

func TestWorkspaceBindingParentSyncBeforeOpen(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "bindings")
	injected := errors.New("parent sync failed")
	called := false
	s, e := openWorkspaceBindingStore(dir, func(got string) error {
		called = true
		if got != parent {
			t.Errorf("sync %q want %q", got, parent)
		}
		if _, e := os.Stat(dir); e != nil {
			t.Error("directory must exist before parent sync", e)
		}
		return injected
	})
	if !called || !errors.Is(e, injected) || s != nil {
		t.Fatalf("open acknowledged unsynced parent: called=%v store=%v err=%v", called, s, e)
	}
	if _, e = OpenWorkspaceBindingStore(dir); e != nil {
		t.Fatal("real parent sync positive", e)
	}
}
