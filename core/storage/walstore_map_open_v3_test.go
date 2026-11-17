package storage

import (
	"os"
	"testing"
)

// Actual store opener, not a manual bind after open. Bootstrap is explicit
// because this RED also runs before the automatic mapped creator exists.
func TestWALStoreV3_OpenRequiresBothMapGenerations(t *testing.T) {
	for _, fault := range []string{"none", "missing", "fallback_page"} {
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
			t.Cleanup(func() { m.Close() })
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
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			// Cleanup is idempotent for the already closed map only through this flag.
			// m.Close is replaced below by its normal cleanup ownership in the fixture.
			if fault == "missing" {
				if err = os.Remove(path + ".chunkmap"); err != nil {
					t.Fatal(err)
				}
			}
			crashRecoveryHardeningStore(t, s)
			crashed = true
			r, err := openWALStoreV3ForTest(path)
			if r != nil {
				r.DisableAutoFlushForRecoveryTest()
				t.Cleanup(func() { r.Close() })
			}
			if fault != "none" {
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
