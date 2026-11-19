package master

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// QD8 on ONE store, with only the actual RF1 write observer changed per arm.
func TestChunkPressureConcurrentObserverDiagnostic(t *testing.T) {
	for _, observer := range []bool{false, true} {
		t.Run(fmt.Sprintf("observer-%v", observer), func(t *testing.T) {
			h, conn, ctx := reviewBindingHost(t)
			reviewLegacyBind(t, h, conn, ctx, "v1")
			line, _ := h.Publisher().VolumeAuthorityLine("v1")
			view := workspaceRPCView{frontend.Projection{VolumeID: "v1", ReplicaID: line.ReplicaID, Epoch: line.Epoch, EndpointVersion: 1, Healthy: true}}
			g, e := volume.NewWorkspaceLeaseGuard(view, control.NewWorkspaceServiceClient(conn), &control.WorkspaceIdentity{VolumeId: "v1", ReplicaId: line.ReplicaID, Epoch: line.Epoch}, func(context.Context) (uint64, error) { return 0, nil }, nil)
			if e != nil {
				t.Fatal(e)
			}
			if e = g.Refresh(context.Background()); e != nil {
				t.Fatal(e)
			}
			runCtx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- g.Run(runCtx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(8 * time.Second):
					t.Error("lease loop did not settle")
				}
			})
			remote := storage.NewMemChunkRemote()
			p, e := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 1 << 18, ChunkBlocks: 1024, ChunkCaptureInterval: 0, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, g)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = p.Close() })
			s, e := p.EnsureStorage("v1")
			if e != nil {
				t.Fatal(e)
			}
			if _, e = p.RecoverVolume(context.Background(), "v1"); e != nil {
				t.Fatal(e)
			}
			be, e := p.Open(context.Background(), "v1")
			if e != nil {
				t.Fatal(e)
			}
			if observer {
				rv := replication.NewReplicationVolume("v1", s)
				t.Cleanup(func() { _ = rv.Close() })
				if e = p.Backend("v1").SetWriteObserver(rv); e != nil {
					t.Fatal(e)
				}
			}
			handler := nvme.NewIOHandler(nvme.HandlerConfig{Backend: be, BlockSize: 4096, VolumeSize: 1 << 30})
			const qd = 8
			const perWorker = 128 * 1024 * 1024 / 4096 / qd
			var wg sync.WaitGroup
			var maxWrite, maxFlush atomic.Int64
			var completed atomic.Uint64
			observe := func(m *atomic.Int64, d time.Duration) {
				v := int64(d)
				for {
					old := m.Load()
					if v <= old || m.CompareAndSwap(old, v) {
						return
					}
				}
			}
			errs := make(chan error, qd)
			latest := make([]map[uint64]uint64, qd)
			start := time.Now()
			for w := 0; w < qd; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(1200 + int64(w)))
					latest[w] = map[uint64]uint64{}
					data := make([]byte, 4096)
					for i := 0; i < perWorker; i++ {
						lba := uint64(w + qd*rng.Intn((1<<18)/qd))
						value := uint64(w+1)<<32 | uint64(i+1)
						for j := 0; j < len(data); j += 8 {
							binary.LittleEndian.PutUint64(data[j:j+8], value)
						}
						begin := time.Now()
						r := handler.Handle(context.Background(), nvme.IOCommand{Opcode: 1, NSID: 1, SLBA: lba, NLB: 1, Data: data, FUA: i%8 == 0})
						observe(&maxWrite, time.Since(begin))
						if e := r.AsError(); e != nil {
							errs <- fmt.Errorf("worker%d write%d: %w", w, i, e)
							return
						}
						latest[w][lba] = value
						completed.Add(1)
						if (i+1)%16 == 0 {
							begin = time.Now()
							r = handler.Handle(context.Background(), nvme.IOCommand{Opcode: 0, NSID: 1})
							observe(&maxFlush, time.Since(begin))
							if e := r.AsError(); e != nil {
								errs <- fmt.Errorf("worker%d flush%d: %w", w, i, e)
								return
							}
						}
					}
				}(w)
			}
			joined := make(chan struct{})
			go func() { wg.Wait(); close(joined) }()
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()
		wait:
			for {
				select {
				case <-joined:
					break wait
				case <-tick.C:
					t.Logf("observer=%v completed=%d/%d elapsed=%v maxWrite=%v maxFlush=%v", observer, completed.Load(), perWorker*qd, time.Since(start), time.Duration(maxWrite.Load()), time.Duration(maxFlush.Load()))
				}
			}
			close(errs)
			for e := range errs {
				t.Error(e)
			}
			if t.Failed() {
				return
			}
			for _, m := range latest {
				for lba, want := range m {
					b := make([]byte, 4096)
					n, e := be.Read(context.Background(), int64(lba*4096), b)
					if e != nil || n != 4096 {
						t.Fatalf("read%d: %d %v", lba, n, e)
					}
					for j := 0; j < len(b); j += 8 {
						if got := binary.LittleEndian.Uint64(b[j : j+8]); got != want {
							t.Fatalf("read lba%d word%d got%d want%d", lba, j, got, want)
						}
					}
				}
			}
			st := p.DurableStatuses()[0]
			head, e := remote.GetHead(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			if head.RemoteLSN != 0 || st.Chunk.Stats.CutsOK != 0 || st.WriteProfile.WALAppendWrapCount == 0 {
				t.Fatal("unexpected cut/no wrap")
			}
			t.Logf("DONE observer=%v QD=%d bytes=%d FUA=%d Flush=%d elapsed=%v maxWrite=%v maxFlush=%v cp=%d wraps=%d remoteLSN=%d", observer, qd, perWorker*qd*4096, perWorker*qd/8, perWorker*qd/16, time.Since(start), time.Duration(maxWrite.Load()), time.Duration(maxFlush.Load()), s.(*storage.WALStore).CheckpointLSN(), st.WriteProfile.WALAppendWrapCount, head.RemoteLSN)
		})
	}
}
