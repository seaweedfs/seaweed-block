package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Cut-stage crash recovery (cells P5.4, P6.4, P8.7, P9.4, P10.4, P11.3):
// the holder dies (kill -9) in the middle of a cut, at each
// stage, and restarts. Expected: HEAD is the old image (or, once the CAS
// applied, the new one), never a mix; every synced local write reads back
// after the restart; the next cut succeeds and publishes the volume as it
// is. The crash is modelled as a process death: the store's file is closed
// without any cleanup and every later remote mutation from the dead
// process is lost (crashRemote), so the remote keeps exactly what the dead
// cut had sent.

var errProcessDead = errors.New("injected: the process is dead")

// crashRemote forwards to the fake until killed; after that every remote
// call of the dead process fails without reaching the fake: a dead process
// neither writes nor reads.
type crashRemote struct {
	*fakeChunkRemote
	mu        sync.Mutex
	dead      bool
	puts      int
	onPut     func(n int) // after the n-th successful Put
	onCAS     func(applied bool)
	casSeen   bool
	beforeCAS bool // kill before the CAS is sent
}

func (r *crashRemote) isDead() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.dead }

func (r *crashRemote) kill() { r.mu.Lock(); r.dead = true; r.mu.Unlock() }

func (r *crashRemote) Put(ctx context.Context, key string, data []byte) error {
	if r.isDead() {
		return errProcessDead
	}
	if err := r.fakeChunkRemote.Put(ctx, key, data); err != nil {
		return err
	}
	r.mu.Lock()
	r.puts++
	n, hook := r.puts, r.onPut
	r.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	return nil
}

func (r *crashRemote) CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error) {
	if r.isDead() {
		return 0, errProcessDead
	}
	r.mu.Lock()
	before, hook := r.beforeCAS, r.onCAS
	r.mu.Unlock()
	if before && hook != nil {
		hook(false)
		return 0, errProcessDead
	}
	gen, err := r.fakeChunkRemote.CASHead(ctx, expected, next)
	if err == nil && hook != nil {
		hook(true)
		return 0, errProcessDead // the reply never reaches the dead process
	}
	return gen, err
}

func (r *crashRemote) Delete(ctx context.Context, key string) error {
	if r.isDead() {
		return errProcessDead
	}
	return r.fakeChunkRemote.Delete(ctx, key)
}

func (r *crashRemote) PutIntent(ctx context.Context, in chunkIntent) (string, error) {
	if r.isDead() {
		return "", errProcessDead
	}
	return r.fakeChunkRemote.PutIntent(ctx, in)
}

func (r *crashRemote) Get(ctx context.Context, key string) ([]byte, error) {
	if r.isDead() {
		return nil, errProcessDead
	}
	return r.fakeChunkRemote.Get(ctx, key)
}

func (r *crashRemote) GetHead(ctx context.Context) (chunkHead, error) {
	if r.isDead() {
		return chunkHead{}, errProcessDead
	}
	return r.fakeChunkRemote.GetHead(ctx)
}

func (r *crashRemote) ListIntents(ctx context.Context) ([]chunkIntent, error) {
	if r.isDead() {
		return nil, errProcessDead
	}
	return r.fakeChunkRemote.ListIntents(ctx)
}

func (r *crashRemote) DeleteIntent(ctx context.Context, id string) error {
	if r.isDead() {
		return errProcessDead
	}
	return r.fakeChunkRemote.DeleteIntent(ctx, id)
}

// cutCrashImage is the volume's content as a byte image.
func cutCrashImage(t *testing.T, s *WALStore) []byte {
	t.Helper()
	img := make([]byte, 0, ctNumBlocks*ctBlockSize)
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		b, err := s.Read(lba)
		if err != nil {
			t.Fatalf("read LBA %d: %v", lba, err)
		}
		img = append(img, b...)
	}
	return img
}

func TestChunkCutCrashAtEachStage(t *testing.T) {
	stages := []struct {
		name string
		// arm installs the kill at the stage; applied says whether the
		// stage is past the HEAD CAS.
		arm     func(l *chunkLayerImpl, r *crashRemote, die func())
		applied bool
	}{
		{"after admission", func(l *chunkLayerImpl, r *crashRemote, die func()) { l.testAfterAdmitHook = die }, false},
		{"at the barrier, after Sync fixed H", func(l *chunkLayerImpl, r *crashRemote, die func()) {
			l.testAfterBarrierSyncHook = func(uint64) { die() }
		}, false},
		{"during the upload, after the first chunk", func(l *chunkLayerImpl, r *crashRemote, die func()) {
			r.onPut = func(n int) {
				if n == 1 {
					die()
				}
			}
		}, false},
		{"before the HEAD CAS is sent", func(l *chunkLayerImpl, r *crashRemote, die func()) {
			r.beforeCAS = true
			r.onCAS = func(bool) { die() }
		}, false},
		{"after the HEAD CAS applied, before its reply", func(l *chunkLayerImpl, r *crashRemote, die func()) {
			r.onCAS = func(applied bool) {
				if applied {
					die()
				}
			}
		}, true},
	}
	for _, st := range stages {
		t.Run(st.name, func(t *testing.T) {
			ctx := ctTimeout(t)
			s, path, crashStore := crashableStore(t)
			f := newFakeChunkRemote()
			r := &crashRemote{fakeChunkRemote: f}
			cfg := ctConfig(1)
			l := mustAttach(t, s, r, cfg).(*chunkLayerImpl)
			for lba := uint32(0); lba < ctNumBlocks; lba++ {
				if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(0x10+lba))); err != nil {
					t.Fatal(err)
				}
			}
			mustSyncFlush(t, s)
			if _, err := l.Capture(ctx); err != nil {
				t.Fatal(err)
			}
			old, _ := f.GetHead(ctx)
			oldImage := f.imageOf(t, old)

			// New writes, then the cut that dies.
			for lba := uint32(4); lba < 12; lba++ { // chunks 1 and 2
				if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(0xC0+lba))); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			want := cutCrashImage(t, s)
			die := func() {
				r.kill()
				crashStore()
			}
			r.mu.Lock()
			r.puts = 0 // count this cut's uploads only
			r.mu.Unlock()
			st.arm(l, r, die)
			// The dying cut gets its own short context: after the death its
			// remote calls fail and it may retry them until its deadline (a
			// real dead process does not); the restarted process below runs
			// on fresh contexts.
			dyingCtx, cancelDying := context.WithTimeout(context.Background(), 5*time.Second)
			_, cutErr := l.Capture(dyingCtx)
			cancelDying()
			if !r.isDead() {
				t.Fatalf("the stage %q was never reached (puts %d, cut error %v)", st.name, r.puts, cutErr)
			}
			if cutErr == nil {
				t.Fatal("the cut succeeded although the process died during it")
			}
			if s.v3 != nil && s.v3.ownedMap != nil { // the dead process's map file
				_ = s.v3.ownedMap.Close()
				s.v3.ownedMap = nil
			}

			// Restart: the store recovers its WAL; HEAD is old or new, whole.
			ctx = ctTimeout(t)
			s2, _ := reopen(t, path)
			head, _ := f.GetHead(ctx)
			got := f.imageOf(t, head)
			switch {
			case st.applied && !bytes.Equal(got, want):
				t.Fatalf("the CAS applied but HEAD (gen %d) is not the new image", head.Gen)
			case !st.applied && (head.Gen != old.Gen || !bytes.Equal(got, oldImage)):
				t.Fatalf("a cut that died before its CAS changed HEAD: gen %d -> %d", old.Gen, head.Gen)
			}
			if local := cutCrashImage(t, s2); !bytes.Equal(local, want) {
				t.Fatal("a synced local write is lost after the crash and restart")
			}
			l2 := mustAttach(t, s2, f, cfg)
			if _, err := l2.Capture(ctx); err != nil {
				t.Fatalf("the first cut after the restart: %v", err)
			}
			final, _ := f.GetHead(ctx)
			if !bytes.Equal(f.imageOf(t, final), want) {
				t.Fatalf("the cut after the restart (gen %d) does not publish the volume as it is", final.Gen)
			}
			if intents, err := f.ListIntents(ctx); err == nil {
				t.Logf("stage %q: HEAD gen %d -> crash -> %d -> %d after the next cut; %d intent(s) left", st.name, old.Gen, head.Gen, final.Gen, len(intents))
			}
		})
	}
}
