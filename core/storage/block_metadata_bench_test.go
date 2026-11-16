package storage

import (
	"path/filepath"
	"testing"
)

// Same source runs on unchanged v1 main and the nil-hook seam candidate.
// Existing extent-read allocation is measured; the hook adds none.
func BenchmarkWALStoreNilMetadata(b *testing.B) {
	for _, write := range []bool{false, true} {
		name := "Read"
		if write {
			name = "Write"
		}
		b.Run(name, func(b *testing.B) {
			s, err := CreateWALStore(filepath.Join(b.TempDir(), "store"), 16, 4096)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			s.flusher.Stop()
			data := makeBlock(4096, 0x31)
			if err = s.WriteExtentDirect(3, data); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if write {
					err = s.WriteExtentDirect(3, data)
				} else {
					_, err = s.Read(3)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
