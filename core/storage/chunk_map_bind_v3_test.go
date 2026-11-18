package storage

import (
	"bytes"
	"testing"
)

// Binding is the open/quiesced publication seam. The selected map alone is
// insufficient: the valid fallback slot still pins its own immutable pages.
func TestMapPublicationV3_BindValidatesBothSlotDependencies(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "valid_fallback"
		if corrupt {
			name = "corrupt_fallback"
		}
		t.Run(name, func(t *testing.T) {
			x := newMapPublicationHarnessV3(t, 11)
			next := x.prepare(t, 12, 110)
			if _, err := x.h.pub.publishMapV3(&x.h.storeMu, nil, next, func(n *descriptorV3) error { n.CheckpointLSN = 110; return nil }); err != nil {
				t.Fatal(err)
			}
			if next.candidate.refs[0].Offset == x.initial.refs[0].Offset {
				t.Fatal("control requires disjoint old/new pages")
			}
			if corrupt {
				if _, err := x.m.fd.WriteAt([]byte{0xff}, int64(x.initial.refs[0].Offset)+64); err != nil {
					t.Fatal(err)
				}
				if err := x.m.fd.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			// The selected generation is intact in both cases, so only checking it
			// would incorrectly accept the damaged fallback.
			d := x.h.pub.current()
			if _, _, err := readChunkLSNMapV3(x.m.fd, d, x.m.chunkBlocks); err != nil {
				t.Fatalf("selected generation damaged: %v", err)
			}
			before := mapSlotsV3(t, x.h)
			oldDomain := x.h.pub.mapDomain
			freshDomain := newMapProofEpochV3()
			err := x.h.pub.bindMapV3(next.candidate, freshDomain)
			if corrupt {
				if err == nil {
					t.Error("bind accepted a valid fallback descriptor with corrupt map dependencies")
				}
				if x.h.pub.mapDomain != oldDomain {
					t.Error("failed bind replaced the open proof domain")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if x.h.pub.mapDomain != freshDomain {
					t.Error("valid both-slot bind did not install its domain")
				}
			}
			if !bytes.Equal(before, mapSlotsV3(t, x.h)) {
				t.Error("bind wrote descriptor slots")
			}
		})
	}
}
