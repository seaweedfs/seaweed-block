//go:build linux

package durable_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Payloads live on disk so the remote fixture cannot account for a volume's
// worth of anonymous RSS in the process under measurement.
type memoryDiskRemote struct {
	*storage.MemChunkRemote
	dir        string
	mu         sync.Mutex
	head       storage.ChunkHead
	hashes     map[string][32]byte
	firstPut   sync.Once
	atFirstPut func()
}

func (r *memoryDiskRemote) path(key string) string {
	h := sha256.Sum256([]byte(key))
	return filepath.Join(r.dir, hex.EncodeToString(h[:]))
}
func (r *memoryDiskRemote) Put(ctx context.Context, key string, data []byte) error {
	r.firstPut.Do(func() {
		if r.atFirstPut != nil {
			r.atFirstPut()
		}
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.hashes[key]; ok {
		return storage.ErrChunkKeyExists
	}
	if err := os.WriteFile(r.path(key), data, 0600); err != nil {
		return err
	}
	r.hashes[key] = sha256.Sum256(data)
	return nil
}
func (r *memoryDiskRemote) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(r.path(key))
}
func (r *memoryDiskRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.head, ctx.Err()
}
func (r *memoryDiskRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if expected.Gen != r.head.Gen || expected.Epoch != r.head.Epoch {
		return 0, storage.ErrChunkHeadPrecondition
	}
	for _, ref := range next.Chunks {
		if sum, ok := r.hashes[ref.Key]; !ok || sum != ref.SHA256 {
			return 0, fmt.Errorf("invalid stored ref %s", ref.Key)
		}
	}
	next.Gen = r.head.Gen + 1
	r.head = next
	return next.Gen, nil
}

func memoryAnonBytes() (uint64, error) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "RssAnon:") {
			v, err := strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			return v * 1024, err
		}
	}
	return 0, fmt.Errorf("RssAnon missing")
}

// Opt-in process-wide measurement: run alone, without -race or other tests.
// Matches S2's initial 64 MiB fill at 896 MiB, then its QD32 random writes
// and first explicit cut. Every writer owns only one 4 KiB request buffer.
func TestWALStoreChunk_InitialWriteAnonymousMemory(t *testing.T) {
	if os.Getenv("SWBLOCK_MEMORY_REPRO") == "" {
		t.Skip("isolated memory reproduction")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	r := &memoryDiskRemote{MemChunkRemote: storage.NewMemChunkRemote(), dir: t.TempDir(), hashes: make(map[string][32]byte)}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 1 << 18, ChunkBlocks: 1024, ChunkCaptureInterval: 0, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return r, nil }}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	be, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	profileDir := os.Getenv("SWBLOCK_MEMORY_PROFILES")
	if profileDir == "" {
		profileDir = t.TempDir()
	}
	if err = os.MkdirAll(profileDir, 0700); err != nil {
		t.Fatal(err)
	}
	r.atFirstPut = func() {
		// The first upload is an external seam after the cut's copy phase.
		// A GC here distinguishes live retained payloads from garbage awaiting GC.
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		anon, _ := memoryAnonBytes()
		t.Logf("first upload after GC anon=%d MiB liveHeap=%d MiB", anon>>20, m.HeapAlloc>>20)
		f, e := os.Create(filepath.Join(profileDir, "first-upload.pprof"))
		if e == nil {
			_ = pprof.WriteHeapProfile(f)
			_ = f.Close()
		}
	}
	var peak uint64
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		var recorded uint64
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				anon, e := memoryAnonBytes()
				if e != nil {
					continue
				}
				if anon > peak {
					peak = anon
				}
				if anon > recorded+64<<20 {
					recorded = anon
					f, e := os.Create(filepath.Join(profileDir, fmt.Sprintf("peak-%d.pprof", anon>>20)))
					if e == nil {
						_ = pprof.WriteHeapProfile(f)
						_ = f.Close()
					}
				}
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
		t.Logf("peak anonymous RSS=%d MiB profiles=%s", peak>>20, profileDir)
		if peak > 256<<20 {
			t.Errorf("peak anonymous RSS %d exceeds 256 MiB", peak)
		}
	}()
	write := func(off int64, data []byte) error {
		for {
			n, e := be.Write(ctx, off, data)
			if e == nil {
				if n != len(data) {
					return fmt.Errorf("short write %d", n)
				}
				return nil
			}
			if !errors.Is(e, frontend.ErrBackendBusy) {
				return e
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	buf := make([]byte, 64<<10)
	for i := range buf {
		buf[i] = byte(i*7 + 1)
	}
	for off := int64(896 << 20); off < 960<<20; off += int64(len(buf)) {
		if err = write(off, buf); err != nil {
			t.Fatal(err)
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	anon, _ := memoryAnonBytes()
	t.Logf("after initial fill anon=%d MiB heap=%d MiB", anon>>20, m.HeapAlloc>>20)
	if err = be.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			b := make([]byte, 4096)
			b[0] = byte(w + 1)
			for i := 0; i < 256; i++ {
				block := (w*8191 + i*97) % (128 << 20 / 4096)
				if e := write(int64(896<<20)+int64(block)*4096, b); e != nil {
					errs <- e
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	anon, _ = memoryAnonBytes()
	t.Logf("after QD32 anon=%d MiB", anon>>20)
	if os.Getenv("SWBLOCK_MEMORY_CUT") != "0" {
		if _, err = p.CaptureVolume(ctx, "v1"); err != nil {
			t.Fatal(err)
		}
	}
	anon, _ = memoryAnonBytes()
	t.Logf("after first cut anon=%d MiB", anon>>20)
}
