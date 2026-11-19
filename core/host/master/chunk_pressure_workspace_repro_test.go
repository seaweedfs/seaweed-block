package master

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"math/rand"
	"testing"
	"time"
)

// Diagnostic: change only the real lease gate/renewal between the two arms.
// Public provider and NVMe handler, actual master RPC/authority stores, no filer,
// replication observer, kernel transport, guest filesystem, cut or four-VM load.
func TestChunkPressureSmallRandomWithWorkspaceLeaseDiagnostic(t *testing.T) {
	runChunkPressureRandomDiagnostic(t, []bool{false, true}, nil)
}
func TestChunkPressureFourLocalStreamsDiagnostic(t *testing.T) {
	for i := 0; i < 4; i++ {
		t.Run(fmt.Sprintf("stream-%d", i), func(t *testing.T) { t.Parallel(); runChunkPressureRandomDiagnostic(t, []bool{true}, nil) })
	}
}
func runChunkPressureRandomDiagnostic(t *testing.T, leaseValues []bool, remoteOverride storage.ChunkRemote) {
	h, conn, setupCtx := reviewBindingHost(t)
	reviewLegacyBind(t, h, conn, setupCtx, "v1")
	line, ok := h.Publisher().VolumeAuthorityLine("v1")
	if !ok {
		t.Fatal("missing authority")
	}
	for _, lease := range leaseValues {
		name := "without-lease"
		if lease {
			name = "with-real-master-lease"
		}
		t.Run(name, func(t *testing.T) {
			const volumeBytes = 1 << 30
			const writes = 160 * 1024 * 1024 / 4096
			ctx := context.Background()
			var view frontend.ProjectionView = workspaceRPCView{frontend.Projection{VolumeID: "v1", ReplicaID: line.ReplicaID, Epoch: line.Epoch, EndpointVersion: 1, Healthy: true}}
			if lease {
				g, err := volume.NewWorkspaceLeaseGuard(view, control.NewWorkspaceServiceClient(conn), &control.WorkspaceIdentity{VolumeId: "v1", ReplicaId: line.ReplicaID, Epoch: line.Epoch}, func(context.Context) (uint64, error) { return 0, nil }, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := g.Refresh(ctx); err != nil {
					t.Fatal(err)
				}
				runCtx, cancel := context.WithCancel(ctx)
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
				view = g
			}
			remote := remoteOverride
			if remote == nil {
				remote = storage.NewMemChunkRemote()
			}
			p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: volumeBytes / 4096, ChunkBlocks: 1024, ChunkCaptureInterval: 0, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, view)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := p.Close(); err != nil {
					t.Logf("close: %v", err)
				}
			})
			s, err := p.EnsureStorage("v1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			be, err := p.Open(ctx, "v1")
			if err != nil {
				t.Fatal(err)
			}
			handler := nvme.NewIOHandler(nvme.HandlerConfig{Backend: be, BlockSize: 4096, VolumeSize: volumeBytes})
			rng := rand.New(rand.NewSource(731))
			latest := map[uint64]uint64{}
			data := make([]byte, 4096)
			start := time.Now()
			var maxWrite, maxFlush time.Duration
			flushes, fuas := 0, 0
			for i := 0; i < writes; i++ {
				lba := uint64(rng.Intn(volumeBytes / 4096))
				value := uint64(i + 1)
				for j := 0; j < len(data); j += 8 {
					binary.LittleEndian.PutUint64(data[j:j+8], value)
				}
				fua := i%8 == 0
				if fua {
					fuas++
				}
				begin := time.Now()
				r := handler.Handle(ctx, nvme.IOCommand{Opcode: 1, NSID: 1, SLBA: lba, NLB: 1, Data: data, FUA: fua})
				d := time.Since(begin)
				if d > maxWrite {
					maxWrite = d
				}
				if e := r.AsError(); e != nil {
					t.Fatalf("write %d/%d lba%d elapsed%v: %v", i, writes, lba, time.Since(start), e)
				}
				latest[lba] = value
				if (i+1)%16 == 0 {
					begin = time.Now()
					r = handler.Handle(ctx, nvme.IOCommand{Opcode: 0, NSID: 1})
					d = time.Since(begin)
					if d > maxFlush {
						maxFlush = d
					}
					flushes++
					if e := r.AsError(); e != nil {
						t.Fatalf("flush at%d: %v", i, e)
					}
				}
				if (i+1)%8192 == 0 {
					t.Logf("progress bytes=%d elapsed=%v maxWrite=%v maxFlush=%v", (i+1)*4096, time.Since(start), maxWrite, maxFlush)
				}
			}
			for lba, want := range latest {
				got := make([]byte, 4096)
				n, e := be.Read(ctx, int64(lba*4096), got)
				if e != nil || n != 4096 {
					t.Fatalf("read %d: %d %v", lba, n, e)
				}
				for j := 0; j < len(got); j += 8 {
					if v := binary.LittleEndian.Uint64(got[j : j+8]); v != want {
						t.Fatalf("read lba%d word%d=%d want%d", lba, j, v, want)
					}
				}
			}
			st := p.DurableStatuses()[0]
			head, e := remote.GetHead(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if head.Gen != 0 || head.RemoteLSN != 0 || st.Chunk == nil || st.Chunk.Stats.CutsOK != 0 || st.WriteProfile.WALAppendWrapCount == 0 {
				t.Fatalf("unexpected no-cut state: head=%+v status=%+v", head, st)
			}
			t.Logf("DONE lease=%v bytes=%d FUA=%d Flush=%d elapsed=%v maxWrite=%v maxFlush=%v cp=%d wraps=%d remoteLSN=%d cuts=%d", lease, writes*4096, fuas, flushes, time.Since(start), maxWrite, maxFlush, s.(*storage.WALStore).CheckpointLSN(), st.WriteProfile.WALAppendWrapCount, head.RemoteLSN, st.Chunk.Stats.CutsOK)
		})
	}
}
