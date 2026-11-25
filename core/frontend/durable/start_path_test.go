package durable

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// fakeRoot is a local store as the start decision reads it.
type fakeRoot struct {
	base     uint64
	complete bool
	seedErr  func(uint64) error
}

func (r fakeRoot) InstalledBaseLSN() (uint64, bool)    { return r.base, r.complete }
func (r fakeRoot) CanSeedChunkFrontier(p uint64) error { return r.seedErr(p) }

var errNoNext = errors.New("no room for a next LSN")

func seedsUnlessMax(p uint64) error {
	if p == ^uint64(0) {
		return errNoNext
	}
	return nil
}

// The start decision over local-root states x HEAD positions: the path the
// root takes after its claim, and whether that path refuses (before the
// claim Start sees the same refusal). States from the code: a fresh root
// without a base seeds; with a completed base it keeps the base; with a base
// install in progress the seed refuses (the store's precondition); a
// non-fresh root continues.
func TestCanStartFrom_LocalRootStateByHeadPosition(t *testing.T) {
	roots := map[string]struct {
		root  fakeRoot
		fresh bool
	}{
		"fresh, no base":             {fakeRoot{seedErr: seedsUnlessMax}, true},
		"fresh, completed base at 5": {fakeRoot{base: 5, complete: true, seedErr: seedsUnlessMax}, true},
		"fresh, base in progress":    {fakeRoot{seedErr: func(uint64) error { return errors.New("install in progress") }}, true},
		"not fresh":                  {fakeRoot{seedErr: func(uint64) error { return errors.New("must not be asked") }}, false},
	}
	heads := map[string]uint64{"position 0": 0, "position 3": 3, "position 9": 9, "position MaxUint64": ^uint64(0)}
	// want: the path, then "ok" or "refused"
	want := map[string]map[string]string{
		"fresh, no base":             {"position 0": "seed ok", "position 3": "seed ok", "position 9": "seed ok", "position MaxUint64": "seed refused"},
		"fresh, completed base at 5": {"position 0": "base ok", "position 3": "base ok", "position 9": "base refused", "position MaxUint64": "base refused"},
		"fresh, base in progress":    {"position 0": "seed refused", "position 3": "seed refused", "position 9": "seed refused", "position MaxUint64": "seed refused"},
		"not fresh":                  {"position 0": "continue ok", "position 3": "continue ok", "position 9": "continue ok", "position MaxUint64": "continue ok"},
	}
	name := map[startPath]string{startContinue: "continue", startSeed: "seed", startAttachBase: "base"}
	for r, c := range roots {
		for hn, pos := range heads {
			path, err := canStartFrom(c.root, c.fresh, layout.Head{Position: pos})
			got := name[path] + " ok"
			if err != nil {
				got = name[path] + " refused"
			}
			if w, ok := want[r][hn]; !ok || got != w {
				t.Errorf("%s on HEAD %s: %s; want %s", r, hn, got, w)
			}
		}
	}
}
