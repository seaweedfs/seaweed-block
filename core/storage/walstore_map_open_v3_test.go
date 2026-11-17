package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

// Actual store opener, not a manual bind after open. Bootstrap is explicit
// because this RED also runs before the automatic mapped creator exists.
func TestWALStoreV3_OpenRequiresBothMapGenerations(t *testing.T) {
	for _, fault := range []string{"none", "missing", "fallback_page", "zero_root"} {
		t.Run(fault, func(t *testing.T) {
			s, path := newStoreV3(t, WModeStrict)
			crashed := false
			t.Cleanup(func() {
				if !crashed {
					crashRecoveryHardeningStore(t, s)
				}
			})
			d := s.v3.pub.current()
			m, err := createChunkLSNMapV3(path+".chunkmap", d, 4)
			if err != nil {
				t.Fatal(err)
			}
			mapClosed := false
			t.Cleanup(func() {
				if !mapClosed {
					m.Close()
				}
			})
			entries := make([]chunkLSNEntryV3, 16)
			for i := range entries {
				entries[i].Unknown = true
			}
			old, err := m.Prepare(2, 0, entries, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.v3.pub.publish(&s.mu, nil, func(n *descriptorV3) error { *n = old.descriptor(*n); return nil }); err != nil {
				t.Fatal(err)
			}
			entries[0].LSN = 1 // force a distinct immutable page in the newer generation
			next, err := m.Prepare(3, 0, entries, old)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.v3.pub.publish(&s.mu, nil, func(n *descriptorV3) error { *n = next.descriptor(*n); return nil }); err != nil {
				t.Fatal(err)
			}
			if fault == "fallback_page" {
				if _, err = m.fd.WriteAt([]byte{0xff}, int64(old.refs[0].Offset)+64); err != nil {
					t.Fatal(err)
				}
				if err = m.fd.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "zero_root" {
				d = s.v3.pub.current()
				d.MapRootHash = [32]byte{}
				if _, err = s.fd.WriteAt(d.encode(), int64(s.v3.pub.curSlot)*4096); err != nil {
					t.Fatal(err)
				}
				if err = s.fd.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			mapClosed = true
			if fault == "missing" {
				if err = os.Remove(path + ".chunkmap"); err != nil {
					t.Fatal(err)
				}
			}
			crashRecoveryHardeningStore(t, s)
			crashed = true
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			mapBefore, mapBeforeErr := os.ReadFile(path + ".chunkmap")
			r, err := openWALStoreV3ForTest(path)
			if r != nil {
				r.DisableAutoFlushForRecoveryTest()
				t.Cleanup(func() { r.Close() })
			}
			if fault != "none" {
				after, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(before, after) {
					t.Error("failed map open changed the store file")
				}
				mapAfter, mapAfterErr := os.ReadFile(path + ".chunkmap")
				if !bytes.Equal(mapBefore, mapAfter) || os.IsNotExist(mapBeforeErr) != os.IsNotExist(mapAfterErr) {
					t.Error("failed map open changed sidecar state")
				}
				if fault == "missing" && !os.IsNotExist(err) {
					t.Errorf("missing map error=%v", err)
				}
				if fault != "missing" && !errors.Is(err, ErrMapInvalid) {
					t.Errorf("corrupt map error=%v", err)
				}
				if err == nil {
					t.Fatalf("store open accepted %s map dependency", fault)
				}
				if r != nil {
					t.Error("failed map open returned a store")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.v3.pub.mapDomain == nil || r.v3.pub.mapCurrent == nil {
				t.Fatal("store open did not automatically bind its durable map")
			}
			if _, err = r.Recover(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWALStoreV3_MappedCreateOpenOwnsMap(t *testing.T) {
	path := t.TempDir() + "/mapped"
	s, err := createMappedWALStoreV3ForTest(path, 65, 4096, v3TestWAL, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	closed := false
	t.Cleanup(func() {
		if !closed {
			s.Close()
		}
	})
	if s.v3.ownedMap == nil || s.v3.pub.mapDomain == nil {
		t.Fatal("created store has no owned map/proof")
	}
	es, _, err := readChunkLSNMapV3(s.v3.ownedMap.fd, s.v3.pub.current(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 17 {
		t.Fatalf("clipped geometry entries=%d want17", len(es))
	}
	for _, e := range es {
		if !e.Unknown || e.LSN != 0 {
			t.Fatal("initial map is not conservative UNKNOWN")
		}
	}
	oldDomain := s.v3.pub.mapDomain
	mapFD := s.v3.ownedMap.fd
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if _, err = mapFD.Stat(); err == nil {
		t.Fatal("store Close leaked owned sidecar")
	}
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { r.Close() })
	if r.v3.pub.mapDomain == nil || r.v3.pub.mapDomain == oldDomain {
		t.Fatal("reopen did not create a fresh proof domain")
	}
	if _, err = r.Recover(); err != nil {
		t.Fatal(err)
	}
}
