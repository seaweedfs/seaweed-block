package storage

import (
	"bytes"
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// This test-only metadata adapter parks a real extent read after ReadAt.
// It changes no bytes and does not replace the v3 reader, WAL or flusher.
type cowReadGate struct {
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *cowReadGate) OnExtentWrite(uint32, []byte) error   { return nil }
func (g *cowReadGate) BeforeCheckpointAdvance(uint64) error { return nil }
func (g *cowReadGate) VerifyExtent(lba uint32, _ []byte) error {
	g.mu.Lock()
	hold := g.armed && lba == 0
	if hold {
		g.armed = false
	}
	g.mu.Unlock()
	if hold {
		close(g.entered)
		<-g.release
	}
	return nil
}
func (g *cowReadGate) unblock() { g.once.Do(func() { close(g.release) }) }
func cowWaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(ctLocalBound):
		stacks := make([]byte, 1<<20)
		n := runtime.Stack(stacks, true)
		t.Fatalf("%s did not finish within %v\n%s", what, ctLocalBound, stacks[:n])
	}
}

// R1/R11: a cut copying chunk 0 must allow an at-H image of pending chunk 1
// to be saved and a new mutation published there. With one copy worker, the
// second chunk is still pending. The first HEAD stays at H; the next cut
// publishes the post-H mutation. Main's full-copy barrier fails progress.
func TestCutCOWV3_MutationCompletesWhileEarlierChunkCopyIsHeld(t *testing.T) {
	for _, method := range []string{"Write", "WriteBatch", "ApplyEntry", "ApplyInstallEntry", "WriteOutsideD"} {
		t.Run(method, func(t *testing.T) {
			gate := &cowReadGate{entered: make(chan struct{}), release: make(chan struct{})}
			opts := WALStoreOptions{ChunkBlocks: ctChunkBlocks, MetadataFactory: func(_ BlockMetadataFile, layout BlockMetadataLayout) (BlockMetadataBinding, error) {
				return BlockMetadataBinding{ImplVersion: layout.ImplVersion, Hook: gate}, nil
			}}
			s, err := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "volume"), ctNumBlocks, ctBlockSize, opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			s.DisableAutoFlushForRecoveryTest()
			if _, err = s.Recover(); err != nil {
				t.Fatal(err)
			}
			var completed InstallHandle
			if method == "ApplyInstallEntry" {
				completed, err = s.BeginBaseInstall(7, [16]byte{0xc1})
				if err != nil {
					t.Fatal(err)
				}
				if err = completed.MarkBaseComplete(); err != nil {
					t.Fatal(err)
				}
			}
			remote := newFakeChunkRemote()
			cfg := ctConfig(1)
			cfg.Bitmap = nil
			cfg.BarrierBound = 0
			cfg.UploadConcurrency = 1
			layer := mustAttach(t, s, remote, cfg)
			// Certify initially UNKNOWN chunks through the real capture, so the
			// measured cut has precisely chunks 0 and 1 as its candidates.
			if _, err = layer.Cut(context.Background()); err != nil {
				t.Fatal(err)
			}
			for lba, value := range map[uint32]byte{1: 0x21, 5: 0x65} {
				if _, err = s.Write(lba, makeBlock(ctBlockSize, value)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			nextLSN := s.NextLSN()
			gate.mu.Lock()
			gate.armed = true
			gate.mu.Unlock()
			type cutResult struct {
				cut chunkCutResult
				err error
			}
			cutResultCh := make(chan cutResult, 1)
			cutDone, writeDone := make(chan struct{}), make(chan struct{})
			writeErr := make(chan error, 1)
			writerStarted := false
			// Register release and actual joins before starting either worker.
			t.Cleanup(func() {
				gate.unblock()
				cowWaitSignal(t, cutDone, "cut cleanup")
				if writerStarted {
					cowWaitSignal(t, writeDone, "mutation cleanup")
				}
			})
			go func() {
				defer close(cutDone)
				result, e := layer.Cut(context.Background())
				cutResultCh <- cutResult{result, e}
			}()
			cowWaitSignal(t, gate.entered, "actual chunk 0 extent-read admission")
			writerStarted = true
			go func() {
				defer close(writeDone)
				var e error
				switch method {
				case "Write":
					_, e = s.Write(5, makeBlock(ctBlockSize, 0xa5))
				case "WriteBatch":
					_, e = s.WriteBatch(5, [][]byte{makeBlock(ctBlockSize, 0xa5), makeBlock(ctBlockSize, 0xa6), makeBlock(ctBlockSize, 0xa7), makeBlock(ctBlockSize, 0xa8)})
				case "ApplyEntry":
					e = s.ApplyEntry(5, makeBlock(ctBlockSize, 0xa5), nextLSN)
				case "ApplyInstallEntry":
					e = completed.ApplyInstallEntry(5, makeBlock(ctBlockSize, 0xa5), nextLSN)
				case "WriteOutsideD":
					_, e = s.Write(21, makeBlock(ctBlockSize, 0xa5))
				}
				writeErr <- e
			}()
			// Require positive completion while the extent-read handshake is held.
			// The deadline bounds a liveness requirement, not a sleep proving entry.
			writeResultRead := false
			select {
			case e := <-writeErr:
				writeResultRead = true
				if e != nil {
					t.Fatalf("post-H mutation: %v", e)
				}
			case <-time.After(ctLocalBound):
				t.Error("RED: mutation blocked behind another chunk's held copy; capture barrier still covers chunk I/O")
			}
			gate.unblock()
			cowWaitSignal(t, cutDone, "cut after read release")
			cowWaitSignal(t, writeDone, "mutation after read release")
			if !writeResultRead {
				if e := <-writeErr; e != nil {
					t.Fatalf("post-release mutation: %v", e)
				}
			}
			result := <-cutResultCh
			if result.err != nil {
				t.Fatal(result.err)
			}
			if result.cut.H != nextLSN-1 {
				t.Fatalf("cut H=%d want %d", result.cut.H, nextLSN-1)
			}
			image := remote.imageOf(t, result.cut.Head)
			want := make([]byte, ctNumBlocks*ctBlockSize)
			copy(want[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 0x21))
			copy(want[5*ctBlockSize:6*ctBlockSize], makeBlock(ctBlockSize, 0x65))
			if !bytes.Equal(image, want) {
				t.Fatal("first HEAD is not the literal pre-H image")
			}
			changedLBA := 5
			if method == "WriteOutsideD" {
				changedLBA = 21
			}
			if got, e := s.Read(uint32(changedLBA)); e != nil || !bytes.Equal(got, makeBlock(ctBlockSize, 0xa5)) {
				t.Fatalf("local post-H image: %v", e)
			}
			second, e := layer.Cut(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			copy(want[changedLBA*ctBlockSize:(changedLBA+1)*ctBlockSize], makeBlock(ctBlockSize, 0xa5))
			if method == "WriteBatch" {
				for i, value := range []byte{0xa6, 0xa7, 0xa8} {
					copy(want[(6+i)*ctBlockSize:(7+i)*ctBlockSize], makeBlock(ctBlockSize, value))
				}
			}
			if second.H <= result.cut.H || !bytes.Equal(remote.imageOf(t, second.Head), want) {
				t.Fatal("next cut omitted post-H mutation")
			}
			assertNoRemoteMisuse(t, remote)
			t.Logf("image controls passed: first H=%d has pre-H bytes, second H=%d has %s bytes", result.cut.H, second.H, method)
		})
	}
}
