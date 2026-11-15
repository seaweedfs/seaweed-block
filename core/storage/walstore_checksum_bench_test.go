package storage

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// Opt-in file-backed WAL/extent proxy, not a block-frontend or mounted fio
// benchmark. The same source runs unchanged on the v1 base and v2 candidate.
func TestC12FileBackedProxy(t *testing.T) {
	if os.Getenv("C12_PROXY") != "1" {
		t.Skip("opt-in file-backed proxy")
	}
	const blocks, operations = 4096, 2048
	for repeat := 0; repeat < 3; repeat++ {
		for _, write := range []bool{false, true} {
			for _, syncIO := range []bool{false, true} {
				s, err := CreateWALStore(filepath.Join(t.TempDir(), "store"), blocks, 4096)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.flusher.Stop(); err != nil {
					t.Fatal(err)
				}
				data := makeBlock(4096, 0x51)
				for lba := uint32(0); lba < blocks; lba++ {
					if err = s.WriteExtentDirect(lba, data); err != nil {
						t.Fatal(err)
					}
				}
				if err = s.fd.Sync(); err != nil {
					t.Fatal(err)
				}
				rng := rand.New(rand.NewSource(1212))
				samples := make([]int64, 0, operations)
				start := time.Now()
				for i := 0; i < operations; i++ {
					lba := uint32(rng.Intn(blocks))
					before := time.Now()
					if write {
						_, err = s.Write(lba, data)
					} else {
						_, err = s.Read(lba)
					}
					if err != nil {
						t.Fatal(err)
					}
					if syncIO {
						if _, err = s.Sync(); err != nil {
							t.Fatal(err)
						}
					}
					samples = append(samples, time.Since(before).Nanoseconds())
					// Drain at a fixed cadence on both revisions. It is included in total
					// throughput, separately from foreground operation latency percentiles.
					if write && i%64 == 63 {
						if err = s.flusher.flushOnce(); err != nil {
							t.Fatal(err)
						}
					}
				}
				elapsed := time.Since(start)
				sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
				row := map[string]any{"format": WALStoreImplVersion, "repeat": repeat, "write": write, "sync": syncIO, "operations": operations, "p50_ns": samples[len(samples)/2], "p99_ns": samples[len(samples)*99/100], "ops_per_sec": float64(operations) / elapsed.Seconds(), "elapsed_ns": elapsed.Nanoseconds(), "scope": "4K random WAL write / checkpointed extent read; sync mode calls WALStore.Sync after each operation; 64-write flusher cadence"}
				raw, _ := json.Marshal(row)
				fmt.Printf("C12_PROXY %s\n", raw)
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
