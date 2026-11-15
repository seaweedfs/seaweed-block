package lifecycle

import "testing"

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
