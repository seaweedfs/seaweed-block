//go:build chunkstore_integration

package master

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type workspaceTakeoverWitness struct {
	storage.ChunkRemote
	reads, calls       atomic.Uint64
	checked, committed atomic.Bool
}

func (r *workspaceTakeoverWitness) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	r.reads.Add(1)
	return r.ChunkRemote.GetHead(ctx)
}
func (r *workspaceTakeoverWitness) CASHead(ctx context.Context, v storage.ChunkHeadVersion, h storage.ChunkHead) (uint64, error) {
	r.calls.Add(1)
	gen, err := r.ChunkRemote.CASHead(ctx, v, h)
	if err == nil && r.checked.Load() && h.State == "active" && h.DetachedBy == nil {
		r.committed.Store(true)
	}
	return gen, err
}

func TestIntegrationWorkspaceProductionDetachAndForcedMove(t *testing.T) {
	c := startWorkspaceWeed(t)
	for _, kind := range []string{"clean", "forced"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			base := "/swblock/workspace-" + kind
			swfs := c.client(t, base+"/vol-a")
			cat, err := chunkstore.NewCatalog(swfs, base)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := NewWorkspaceHEADReader("http://" + c.filerHTTP + base)
			if err != nil {
				t.Fatal(err)
			}
			h, err := New(Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0", WorkspaceRPC: true, WorkspaceHeadReader: reader})
			if err != nil {
				t.Fatal(err)
			}
			h.Start()
			defer closeTestMaster(t, h)
			if _, err = h.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "vol-a", SizeBytes: 64 * 4096, ReplicationFactor: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node-b", DataAddr: "127.0.0.1:9201", CtrlAddr: "127.0.0.1:9101"}); err != nil {
				t.Fatal(err)
			}
			seedVerifiedExistingReplicaPlacement(t, h)
			if _, err = h.RunLifecycleProductTick(); err != nil {
				t.Fatal(err)
			}
			line := waitAuthorityLine(t, h.Publisher(), "vol-a")
			conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := control.NewWorkspaceServiceClient(conn)
			id := &control.WorkspaceIdentity{VolumeId: "vol-a", ReplicaId: line.ReplicaID, Epoch: line.Epoch}
			lease, err := client.RenewWorkspaceLease(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			remote, err := chunkstore.NewSWFSChunkRemote(swfs, base+"/vol-a")
			if err != nil {
				t.Fatal(err)
			}
			p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8,
				ChunkHolder:    "node-a/" + line.ReplicaID,
				ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
				ChunkAttachCheck: func(ctx context.Context, v string, g durable.ChunkGeometry) error {
					_, err := cat.EnsureVolume(ctx, v, chunkstore.Geometry{NumBlocks: g.NumBlocks, BlockSize: g.BlockSize, ChunkBlocks: g.ChunkBlocks})
					return err
				},
			}, workspaceRPCView{frontend.Projection{VolumeID: "vol-a", ReplicaID: line.ReplicaID, Epoch: line.Epoch, EndpointVersion: line.EndpointVersion, Healthy: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err = p.EnsureStorage("vol-a"); err != nil {
				t.Fatal(err)
			}
			if _, err = p.RecoverVolume(ctx, "vol-a"); err != nil {
				t.Fatal(err)
			}
			backend, err := p.Open(ctx, "vol-a")
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte{0xD7}, 4096)
			// A nontrivial source frontier makes a reset-to-local-LSN regression visible.
			for i := 0; i < 8; i++ {
				if n, err := backend.Write(ctx, 9*4096, want); err != nil || n != len(want) {
					t.Fatal(n, err)
				}
			}
			if err = backend.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = p.CaptureVolume(ctx, "vol-a"); err != nil {
				t.Fatal(err)
			}
			q := &control.WorkspaceMove{RequestId: kind + "-actual", VolumeId: "vol-a", ExpectedReplicaId: line.ReplicaID, ExpectedEpoch: line.Epoch, TargetReplicaId: line.ReplicaID, TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:9201", TargetCtrlAddr: "127.0.0.1:9101", Kind: kind, Force: kind == "forced"}
			if _, err = client.MoveWorkspace(ctx, q); err == nil {
				t.Fatal("move accepted before detach/ACK or lease expiry")
			}
			var detached volume.ChunkDetachResponse
			if kind == "clean" {
				if _, err = client.RevokeWorkspace(ctx, id); err != nil {
					t.Fatal(err)
				}
				// Real W2 adapter -> provider StopIO/drain -> final-cut serializer -> filer CAS.
				detached, err = volume.NewChunkControl(p, cat).Detach(ctx, "vol-a")
				if err != nil {
					t.Fatal(err)
				}
				stored, err := swfs.LookupEntry(ctx, base+"/vol-a", "HEAD")
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("producer HEAD persisted by filer: %s", stored.Content)
				if _, err = backend.Write(ctx, 0, want); err == nil {
					t.Fatal("W2 detach left frontend writable")
				}
				if _, err = p.CaptureVolume(ctx, "vol-a"); err == nil {
					t.Fatal("W2 detach left cuts running")
				}
				observed, err := reader(ctx, "vol-a")
				if err != nil || !observed.Detached || observed.Gen != detached.HeadGen {
					t.Fatalf("production HEAD -> HTTP reader: %+v %v", observed, err)
				}
				if _, err = client.MoveWorkspace(ctx, q); err == nil {
					t.Fatal("detached production HEAD accepted without ACK")
				}
				if _, err = client.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: detached.HeadGen}); err != nil {
					t.Fatal(err)
				}
			} else {
				// Actual master lease expiry, no test clock and no revocation ACK.
				timer := time.NewTimer(time.Until(time.Unix(0, lease.ExpiresAtNs)) + 20*time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				q.Force = false
				if _, err = client.MoveWorkspace(ctx, q); err == nil {
					t.Fatal("forced move accepted without explicit force")
				}
				q.Force = true
			}
			before, err := remote.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			grant, err := client.MoveWorkspace(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if !grant.Issued || grant.NewEpoch <= line.Epoch || grant.HeadGen != before.Gen || grant.RemoteLsn != before.RemoteLSN || grant.OldHost != "node-a" || grant.NewHost != "node-b" {
				t.Fatal("grant not bound to production HEAD", grant, before)
			}
			status, err := client.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-a"})
			if err != nil || len(status.Moves) != 1 || !status.Moves[0].Issued {
				t.Fatal(status, err)
			}
			if kind == "clean" && (status.RevocationAck != nil || detached.HeadGen != grant.HeadGen) {
				t.Fatal("grant did not consume matching ACK", status)
			}
			after, err := remote.GetHead(ctx)
			if err != nil || after.Gen != before.Gen || after.Epoch != before.Epoch {
				t.Fatal("grant itself must not claim remote fencing", after, err)
			}
			if _, err = client.RenewWorkspaceLease(ctx, id); err == nil {
				t.Fatal("old authority renewed")
			}
			// Resume on an actually empty provider. No manual HEAD takeover here.
			newView := workspaceRPCView{frontend.Projection{VolumeID: "vol-a", ReplicaID: grant.NewReplicaId, Epoch: grant.NewEpoch, EndpointVersion: 1, Healthy: true}}
			makeTarget := func(deny bool) (*durable.DurableProvider, *workspaceTakeoverWitness) {
				t.Helper()
				w := &workspaceTakeoverWitness{ChunkRemote: remote}
				target, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8,
					ChunkHolder:    "node-b/" + grant.NewReplicaId,
					ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return w, nil },
					ChunkAttachCheck: func(ctx context.Context, v string, g durable.ChunkGeometry) error {
						blocks := g.NumBlocks
						if deny {
							blocks++
						}
						_, err := cat.EnsureVolume(ctx, v, chunkstore.Geometry{NumBlocks: blocks, BlockSize: g.BlockSize, ChunkBlocks: g.ChunkBlocks})
						if err == nil {
							w.checked.Store(true)
						}
						return err
					}}, newView)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = target.Close() })
				if _, err := target.EnsureStorage("vol-a"); err != nil {
					t.Fatal(err)
				}
				return target, w
			}
			denied, deniedWitness := makeTarget(true)
			if _, err := denied.RecoverVolume(ctx, "vol-a"); chunkstore.CodeOf(err) != chunkstore.CodeGeometryMismatch {
				t.Fatal("geometry refusal", err)
			}
			if deniedWitness.reads.Load() != 0 || deniedWitness.calls.Load() != 0 {
				t.Fatal("VOLUME refusal came after HEAD read/CAS")
			}
			target, witness := makeTarget(false)
			if _, err := target.RecoverVolume(ctx, "vol-a"); err != nil {
				t.Fatal("h2 recovery", err)
			}
			beforeIO, err := remote.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !witness.committed.Load() || beforeIO.Epoch != grant.NewEpoch || beforeIO.Holder != grant.NewHost+"/"+grant.NewReplicaId || beforeIO.State != "active" || beforeIO.DetachedBy != nil {
				t.Errorf("h2 ready before active takeover: HEAD=%+v committed=%v", beforeIO, witness.committed.Load())
			}
			if beforeIO.RemoteLSN != before.RemoteLSN {
				t.Errorf("frontier changed before first I/O: %d -> %d", before.RemoteLSN, beforeIO.RemoteLSN)
			}
			newBackend, err := target.Open(ctx, "vol-a")
			if err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(want))
			if n, err := newBackend.Read(ctx, 9*4096, got); err != nil || n != len(got) || !bytes.Equal(got, want) {
				t.Fatal("h2 first exact I/O", n, err)
			}
			if n, err := newBackend.Write(ctx, 0, bytes.Repeat([]byte{0x88}, 4096)); err != nil || n != 4096 {
				t.Fatal(n, err)
			}
			if err := newBackend.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			cut, err := target.CaptureVolume(ctx, "vol-a")
			if err != nil {
				t.Fatal("h2 cut", err)
			}
			if cut <= before.RemoteLSN {
				t.Errorf("h2 remote_lsn regressed or reused source frontier: %d -> %d", before.RemoteLSN, cut)
			}
			t.Logf("h2 first-I/O witness: gen=%d epoch=%d holder=%s state=%s LSN=%d; next cut=%d", beforeIO.Gen, beforeIO.Epoch, beforeIO.Holder, beforeIO.State, beforeIO.RemoteLSN, cut)
			if _, err = remote.CASHead(ctx, storage.ChunkHeadVersion{Gen: before.Gen, Epoch: before.Epoch}, before); !errors.Is(err, storage.ErrChunkHeadPrecondition) {
				t.Fatal("old HEAD CAS not fenced after takeover", err)
			}
			if _, err := p.CaptureVolume(ctx, "vol-a"); err == nil {
				t.Error("old host cut after h2 takeover")
			}
			if _, err := backend.Read(ctx, 0, make([]byte, 4096)); err == nil {
				t.Error("old host served after observing h2 takeover")
			}
			now, err := reader(ctx, "vol-a")
			if err != nil || now.Detached || now.Epoch != grant.NewEpoch {
				t.Fatal("takeover failed to clear canonical proof", now, err)
			}
			// Bytes written by the real provider survive both metadata transitions.
			entry, err := remote.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ref, ok := entry.Chunks[1]
			if !ok {
				t.Fatal("published data chunk missing")
			}
			data, err := remote.Get(ctx, ref.Key)
			if err != nil || len(data) < 8192 || !bytes.Equal(data[4096:8192], want) {
				t.Fatal("data changed across move", err)
			}
			if !t.Failed() {
				t.Logf("%s production chain PASS: HEAD gen=%d LSN=%d -> grant epoch=%d -> takeover epoch=%d", kind, grant.HeadGen, grant.RemoteLsn, grant.NewEpoch, now.Epoch)
			}
		})
	}
}
