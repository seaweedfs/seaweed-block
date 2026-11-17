package storage

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type mapPublicationHarnessV3 struct {
	h       *descHarnessV3
	m       *chunkLSNMapV3
	domain  *mapProofEpochV3
	initial *chunkMapCandidateV3
}

func newMapPublicationHarnessV3(t *testing.T, gen uint64) *mapPublicationHarnessV3 {
	t.Helper()
	h := newDescHarnessV3(t)
	d := h.pub.current()
	d.W = 200
	d.CheckpointLSN = 100
	d.MapCoveredCheckpoint = 100
	cb := d.VolumeSize / uint64(d.BlockSize)
	m, e := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, cb)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	c, e := m.Prepare(gen, 100, []chunkLSNEntryV3{{LSN: 100}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	d = c.descriptor(d)
	h.pub.cur = d
	if _, e = h.f.WriteAt(d.encode(), 0); e != nil {
		t.Fatal(e)
	}
	domain := newMapProofEpochV3()
	if e = h.pub.bindMapV3(c, domain); e != nil {
		t.Fatal(e)
	}
	return &mapPublicationHarnessV3{h, m, domain, c}
}
func (x *mapPublicationHarnessV3) prepare(t *testing.T, g, C uint64) *preparedMapPublicationV3 {
	t.Helper()
	c, e := prepareMapPublicationV3(x.m, x.domain, g, C, []chunkLSNEntryV3{{LSN: C}}, x.initial)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func mapSlotsV3(t *testing.T, h *descHarnessV3) []byte {
	t.Helper()
	b := make([]byte, 8192)
	if _, e := h.f.ReadAt(b, 0); e != nil {
		t.Fatal(e)
	}
	return b
}
func awaitMapPublishV3(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("map publication did not settle")
		return nil
	}
}
func TestMapPublicationV3_HeldPublicationsBothOrders(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "A_then_B", true: "B_then_A"}[reverse], func(t *testing.T) {
			x := newMapPublicationHarnessV3(t, 11)
			a := x.prepare(t, 12, 120)
			b := x.prepare(t, 13, 110)
			releaseA := make(chan struct{})
			releaseB := make(chan struct{})
			aDone := make(chan error, 1)
			bDone := make(chan error, 1)
			joined := make(chan struct{}, 2)
			t.Cleanup(func() {
				select {
				case <-releaseA:
				default:
					close(releaseA)
				}
				select {
				case <-releaseB:
				default:
					close(releaseB)
				}
				timer := time.NewTimer(5 * time.Second)
				defer timer.Stop()
				for i := 0; i < 2; i++ {
					select {
					case <-joined:
					case <-timer.C:
						t.Error("publication workers did not join")
						return
					}
				}
			})
			go func() {
				defer func() { joined <- struct{}{} }()
				<-releaseA
				_, e := x.h.pub.publishMapV3(&x.h.storeMu, nil, a, func(d *descriptorV3) error { d.CheckpointLSN = 120; return nil })
				aDone <- e
			}()
			go func() {
				defer func() { joined <- struct{}{} }()
				<-releaseB
				_, e := x.h.pub.publishMapV3(&x.h.storeMu, nil, b, func(d *descriptorV3) error { d.CheckpointLSN = 110; return nil })
				bDone <- e
			}()
			if reverse {
				close(releaseB)
				if e := awaitMapPublishV3(t, bDone); e != nil {
					t.Fatal(e)
				}
				before := mapSlotsV3(t, x.h)
				close(releaseA)
				if e := awaitMapPublishV3(t, aDone); !errors.Is(e, ErrMapCoverageStale) {
					t.Fatalf("lower-generation A=%v", e)
				}
				if !bytes.Equal(before, mapSlotsV3(t, x.h)) {
					t.Fatal("refused A wrote slots")
				}
			} else {
				close(releaseA)
				if e := awaitMapPublishV3(t, aDone); e != nil {
					t.Fatal(e)
				}
				before := mapSlotsV3(t, x.h)
				close(releaseB)
				if e := awaitMapPublishV3(t, bDone); !errors.Is(e, ErrMapCoverageStale) {
					t.Fatalf("stale-coverage B=%v", e)
				}
				if !bytes.Equal(before, mapSlotsV3(t, x.h)) {
					t.Fatal("refused B wrote slots")
				}
			}
			fresh := x.prepare(t, 14, 130)
			if _, e := x.h.pub.publishMapV3(&x.h.storeMu, nil, fresh, func(d *descriptorV3) error { d.CheckpointLSN = 125; return nil }); e != nil {
				t.Fatal(e)
			}
			d := x.h.pub.current()
			if d.MapGeneration != 14 || d.MapCoveredCheckpoint != 130 || d.CheckpointLSN != 125 {
				t.Fatalf("fresh tuple=%+v", d)
			}
		})
	}
}
func TestMapPublicationV3_IsolatedMergedCoverageAndWPositive(t *testing.T) {
	x := newMapPublicationHarnessV3(t, 12)
	c := x.prepare(t, 13, 110)
	before := mapSlotsV3(t, x.h)
	d0 := x.h.pub.current()
	floor, ok := x.h.pub.reuseFloor()
	if _, e := x.h.pub.publishMapV3(&x.h.storeMu, nil, c, func(d *descriptorV3) error { d.CheckpointLSN = 120; return nil }); !errors.Is(e, ErrMapCoverageStale) {
		t.Fatalf("isolated merged coverage=%v", e)
	}
	f2, o2 := x.h.pub.reuseFloor()
	if d0 != x.h.pub.current() || floor != f2 || ok != o2 || !bytes.Equal(before, mapSlotsV3(t, x.h)) {
		t.Fatal("refused candidate changed descriptor/tail/floor/slots")
	}
	if _, e := x.h.pub.publish(&x.h.storeMu, nil, func(d *descriptorV3) error { d.W = 210; return nil }); e != nil {
		t.Fatal(e)
	}
	if x.h.pub.current().MapRootHash != d0.MapRootHash {
		t.Fatal("W-only changed root")
	}
}
func TestMapPublicationV3_ProofEpochAndMutationRefusal(t *testing.T) {
	for _, event := range []string{"recover_start", "recover_end_success", "recover_end_failure", "reset", "install_begin", "install_complete", "install_abort", "mutation", "domain_identity", "new_open"} {
		t.Run(event, func(t *testing.T) {
			x := newMapPublicationHarnessV3(t, 12)
			if event == "recover_end_success" || event == "recover_end_failure" {
				if e := x.domain.invalidateV3(); e != nil {
					t.Fatal(e)
				}
			}
			c := x.prepare(t, 13, 110)
			if event == "mutation" {
				if e := x.domain.mutatedV3(); e != nil {
					t.Fatal(e)
				}
			} else if event == "domain_identity" {
				x.domain = newMapProofEpochV3()
				if e := x.h.pub.bindMapV3(x.initial, x.domain); e != nil {
					t.Fatal(e)
				}
			} else if event == "new_open" {
				if e := x.m.Close(); e != nil {
					t.Fatal(e)
				}
				m, loaded, e := openChunkLSNMapV3(x.m.fd.Name(), x.h.pub.current(), x.m.chunkBlocks)
				if e != nil {
					t.Fatal(e)
				}
				x.m = m
				t.Cleanup(func() { m.Close() })
				x.initial = loaded
				x.domain = newMapProofEpochV3()
				if e = x.h.pub.bindMapV3(loaded, x.domain); e != nil {
					t.Fatal(e)
				}
			} else {
				if e := x.domain.invalidateV3(); e != nil {
					t.Fatal(e)
				}
			}
			before := mapSlotsV3(t, x.h)
			if _, e := x.h.pub.publishMapV3(&x.h.storeMu, nil, c, nil); !errors.Is(e, ErrMapCoverageStale) {
				t.Fatalf("%s stale proof=%v", event, e)
			}
			if !bytes.Equal(before, mapSlotsV3(t, x.h)) {
				t.Fatal("stale proof wrote slots")
			}
			// Reprepare with current epoch/open owner and a strictly higher generation.
			fresh := x.prepare(t, 14, 110)
			if _, e := x.h.pub.publishMapV3(&x.h.storeMu, nil, fresh, nil); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestMapPublicationV3_UncertainFinalResponsePinsCandidate(t *testing.T) {
	x := newMapPublicationHarnessV3(t, 12)
	c := x.prepare(t, 13, 110)
	boom := errors.New("response lost after descriptor write")
	x.h.writeHook = func(off int64, b []byte) error {
		if _, e := x.h.f.WriteAt(b, off); e != nil {
			return e
		}
		return boom
	}
	if _, e := x.h.pub.publishMapV3(&x.h.storeMu, nil, c, nil); !errors.Is(e, ErrDescriptorPublishUncertain) {
		t.Fatalf("unknown final response=%v", e)
	}
	pending, ok := x.h.pub.uncertainCandidate()
	if !ok || pending.MapRootHash != c.candidate.hash {
		t.Fatal("uncertain map dependency unpinned")
	}
	if _, _, e := readChunkLSNMapV3(x.m.fd, pending, x.m.chunkBlocks); e != nil {
		t.Fatalf("candidate pages lost: %v", e)
	}
	if _, ok := x.h.pub.reuseFloor(); ok {
		t.Fatal("unknown publication allowed retirement")
	}
}
func TestMapPublicationV3_CoverageWAndNoUnpreparedRoot(t *testing.T) {
	x := newMapPublicationHarnessV3(t, 12)
	c := x.prepare(t, 13, 201)
	if _, e := x.h.pub.publishMapV3(&x.h.storeMu, nil, c, nil); !errors.Is(e, ErrMapCoverageStale) {
		t.Fatalf("C>W=%v", e)
	}
	if _, e := x.h.pub.publish(&x.h.storeMu, nil, func(d *descriptorV3) error { *d = c.candidate.descriptor(*d); return nil }); !errors.Is(e, ErrMapCoverageStale) {
		t.Fatalf("unprepared root bypass=%v", e)
	}
}

func TestMapPublicationV3_ProofChangedDuringPersistenceRefuses(t *testing.T) {
	x := newMapPublicationHarnessV3(t, 12)
	before := mapSlotsV3(t, x.h)
	x.m.syncDirectory = func() error { return x.domain.invalidateV3() }
	if c, e := prepareMapPublicationV3(x.m, x.domain, 13, 110, []chunkLSNEntryV3{{LSN: 110}}, x.initial); !errors.Is(e, ErrMapCoverageStale) || c != nil {
		t.Fatalf("overlapping invalidation yielded candidate: %v %v", c, e)
	}
	if !bytes.Equal(before, mapSlotsV3(t, x.h)) {
		t.Fatal("preparation changed slots")
	}
}
func TestMapPublicationV3_ProofCountersNeverWrap(t *testing.T) {
	d := newMapProofEpochV3()
	d.epoch.Store(counterMaxV3)
	if e := d.invalidateV3(); !errors.Is(e, ErrCounterExhausted) {
		t.Fatal(e)
	}
	if d.snapshotV3().validV3(d) {
		t.Fatal("exhausted epoch accepted")
	}
	d = newMapProofEpochV3()
	d.mutation.Store(counterMaxV3)
	if e := d.mutatedV3(); !errors.Is(e, ErrCounterExhausted) {
		t.Fatal(e)
	}
	if d.snapshotV3().validV3(d) {
		t.Fatal("exhausted mutation version accepted")
	}
}
