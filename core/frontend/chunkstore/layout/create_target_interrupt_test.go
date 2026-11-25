package layout

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// CreateTarget killed at every step, before the step applies and after it
// applied with its reply lost: the source's AddDependent, the root and
// segment copy, the target's VOLUME and its HEAD. Until the HEAD exists
// the target is not startable (no HEAD: Start refuses); a re-run converges
// to the same target as an uninterrupted create, with the dependent
// recorded once and the same root copied.
func TestCreateTarget_InterruptAtEveryStep(t *testing.T) {
	ctx := context.Background()
	v := Volume{ID: idFor("t"), Name: "t", Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: VolumeFormat}
	setup := func(t *testing.T) (*memHead, *Reader, *memNewVolume) {
		o, h, _, _ := removeFixture(t)
		r, err := NewReader(removeBase, "v1", o.fetch)
		if err != nil {
			t.Fatal(err)
		}
		h.Activity = Activity{Kind: ActIdle}
		return &memHead{h: h}, r, newMemNewVolume()
	}

	// The uninterrupted create every re-run must reach.
	src, r, st := setup(t)
	var want RootName
	ref, err := CreateTarget(ctx, src, r, "daily", st, v, func(_ context.Context, rn RootName) error { want = rn; return nil })
	if err != nil {
		t.Fatal(err)
	}
	wantDeps := src.h.Snapshots["daily"].Dependents

	for _, step := range []string{"AddDependent", "copy", "VOLUME", "HEAD"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after=%t", step, after), func(t *testing.T) {
				src, r, st := setup(t)
				var copies []RootName
				killCopy := step == "copy"
				copyRoot := func(_ context.Context, rn RootName) error {
					if killCopy {
						killCopy = false
						if after {
							copies = append(copies, rn) // copied, the reply lost
						}
						return errKilled
					}
					copies = append(copies, rn)
					return nil
				}
				switch step {
				case "AddDependent":
					src.killAt, src.after = 1, after
				case "VOLUME":
					st.killAt, st.after = 1, after
				case "HEAD":
					st.killAt, st.after = 2, after
				}
				if _, err := CreateTarget(ctx, src, r, "daily", st, v, copyRoot); !errors.Is(err, errKilled) {
					t.Fatalf("killed run: %v; want the kill", err)
				}
				if st.memHead.h != nil && !(step == "HEAD" && after) {
					t.Fatalf("killed at %s: the target has a HEAD (startable) before the create finished: %+v", step, st.memHead.h)
				}
				// restart the process
				src.dead, src.killAt, src.after = false, 0, false
				st.restart()
				got, err := CreateTarget(ctx, src, r, "daily", st, v, copyRoot)
				if err != nil {
					t.Fatalf("re-run: %v", err)
				}
				// Gen is the store's: a replayed create reads it back (1 here),
				// a first create returns the HEAD it wrote without one.
				got.Gen, ref.Gen = 0, 0
				if !reflect.DeepEqual(got, ref) {
					t.Fatalf("re-run HEAD %+v; the uninterrupted create gives %+v", got, ref)
				}
				if d := src.h.Snapshots["daily"].Dependents; !reflect.DeepEqual(d, wantDeps) {
					t.Fatalf("source dependents %+v; want %+v (recorded once)", d, wantDeps)
				}
				if st.vol == nil || st.vol.ID != v.ID {
					t.Fatalf("target VOLUME %+v", st.vol)
				}
				for _, rn := range copies {
					if rn != want {
						t.Fatalf("copied %s; the snapshot's root is %s", rn, want)
					}
				}
			})
		}
	}
}
