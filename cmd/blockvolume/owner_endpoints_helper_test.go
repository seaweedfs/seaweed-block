//go:build subprocess && chunkstore_integration

package main_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #425: the procedure gate's owner-endpoint check rejects when EITHER recipient's evidence is missing, unreadable,
// has no forced-epoch delivery, or carries another host's endpoints; it passes only with positive evidence from both.
func TestWrongOwnerEndpointsNeedsPositiveEvidenceFromEachRecipient(t *testing.T) {
	owner := bvHost{id: "s2", data: "127.0.0.1:2001", ctrl: "127.0.0.1:2002"}
	dir := t.TempDir()
	good := `blockvolume: assignment stream delivery process="X" server="s2" volume="v1" replica="r1" epoch=2 ev=1 data="127.0.0.1:2001" ctrl="127.0.0.1:2002"`
	wrong := `blockvolume: assignment stream delivery process="X" server="s2" volume="v1" replica="r1" epoch=2 ev=2 data="127.0.0.1:1001" ctrl="127.0.0.1:1002"`
	other := `blockvolume: assignment stream delivery process="X" server="s1" volume="v1" replica="r1" epoch=1 ev=1 data="127.0.0.1:1001" ctrl="127.0.0.1:1002"`
	logOf := func(name string, lines ...string) *daemon {
		p := filepath.Join(dir, name+".log")
		if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return &daemon{name: name, log: p}
	}
	missing := &daemon{name: "gone", log: filepath.Join(dir, "does-not-exist.log")}
	for _, c := range []struct {
		name   string
		ds     []*daemon
		reject string // "" = must pass
	}{
		{"both positive", []*daemon{logOf("a1", good), logOf("b1", other, good)}, ""},
		{"second unreadable", []*daemon{logOf("a2", good), missing}, "log unreadable"},
		{"first has no epoch delivery", []*daemon{logOf("a3", other), logOf("b3", good)}, "no epoch delivery logged"},
		{"second carries another host's endpoints", []*daemon{logOf("a4", good), logOf("b4", good, wrong)}, "127.0.0.1:1001"},
		{"first empty log", []*daemon{logOf("a5"), logOf("b5", good)}, "no epoch delivery logged"},
	} {
		bad := wrongOwnerEndpoints(2, owner, c.ds...)
		switch {
		case c.reject == "" && len(bad) != 0:
			t.Errorf("%s: rejected %v, want pass", c.name, bad)
		case c.reject != "" && !strings.Contains(strings.Join(bad, "\n"), c.reject):
			t.Errorf("%s: got %v, want a rejection naming %q", c.name, bad, c.reject)
		}
	}
}
