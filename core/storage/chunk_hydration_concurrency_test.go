package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type hydrationResult struct {
	data []byte
	err  error
}

func hydrationFixture(t *testing.T, workers, depth int) (*WALStore, *chunkLayerImpl, *fakeChunkRemote, <-chan uint64) {
	t.Helper()
	image := make([]byte, ctNumBlocks*ctBlockSize)
	for i := uint32(0); i < ctNumBlocks; i++ {
		copy(image[int(i)*ctBlockSize:], makeBlock(ctBlockSize, byte(0x40+i)))
	}
	remote := newFakeChunkRemote()
	remote.seedHead(t, image, 50, 1)
	s, _ := newChunkTestStore(t)
	if err := s.SeedChunkFrontier(50); err != nil {
		t.Fatal(err)
	}
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	cfg.HydrationWorkers = workers
	cfg.HydrationQueueDepth = depth
	layer := mustAttach(t, s, remote, cfg).(*chunkLayerImpl)
	registered := make(chan uint64, 128)
	layer.testHydrationWait = func(idx uint64) { registered <- idx }
	return s, layer, remote, registered
}

func startHydrationRead(t *testing.T, s *WALStore, parent context.Context, lba uint32) <-chan hydrationResult {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	result := make(chan hydrationResult, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(ctLocalBound):
			t.Error("hydration reader failed to settle")
		}
	})
	go func() {
		defer close(joined)
		data, err := hydrationReadContext(s, ctx, lba)
		result <- hydrationResult{data, err}
	}()
	return result
}
func waitHydrationRegistration(t *testing.T, c <-chan uint64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-c:
		case <-time.After(ctLocalBound):
			t.Fatal("hydration waiter was not registered")
		}
	}
}
func hydrationOutcome(t *testing.T, c <-chan hydrationResult) hydrationResult {
	t.Helper()
	select {
	case r := <-c:
		return r
	case <-time.After(ctLocalBound):
		t.Fatal("hydration read did not finish")
		return hydrationResult{}
	}
}

func TestChunkHydration_ConcurrentReadersShareOneGet(t *testing.T) {
	s, _, remote, registered := hydrationFixture(t, 2, 64)
	entered, release := holdGets(t, remote)
	defer release()
	const readers = 12
	results := make([]<-chan hydrationResult, readers)
	for i := range results {
		results[i] = startHydrationRead(t, s, context.Background(), 8)
	}
	waitHydrationRegistration(t, registered, readers)
	waitKey(t, entered, "shared hydration")
	if got := remote.stats().gets; got != 1 {
		t.Fatalf("same-chunk readers started %d Gets", got)
	}
	release()
	for _, c := range results {
		r := hydrationOutcome(t, c)
		if r.err != nil || !bytes.Equal(r.data, makeBlock(ctBlockSize, 0x48)) {
			t.Fatalf("shared fill bytes: %v", r.err)
		}
	}
	if got := remote.stats().gets; got != 1 {
		t.Fatalf("shared fill restarted: Gets=%d", got)
	}
}

func TestChunkHydration_QueueAndWorkerLimitsAreBounded(t *testing.T) {
	s, layer, remote, registered := hydrationFixture(t, 1, 2)
	entered, release := holdGets(t, remote)
	defer release()
	first := startHydrationRead(t, s, context.Background(), 8)
	waitKey(t, entered, "first worker")
	waitHydrationRegistration(t, registered, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	second := startHydrationRead(t, s, ctx, 12)
	waitHydrationRegistration(t, registered, 1)
	if r := hydrationOutcome(t, second); !errors.Is(r.err, ErrChunkHydrationPending) {
		t.Fatalf("queued timeout: %v", r.err)
	}
	_, err := hydrationReadContext(s, context.Background(), 16)
	if !errors.Is(err, ErrChunkHydrationPending) {
		t.Fatalf("full queue did not refuse: %v", err)
	}
	if got := remote.stats().gets; got != 1 {
		t.Fatalf("worker bound exceeded: Gets=%d", got)
	}
	stats := layer.Stats().Hydration
	if stats.WorkerLimit != 1 || stats.QueueDepth != 2 || stats.Active != 1 || stats.Queued != 1 || stats.WaitRefusals != 2 || stats.Fetches != 1 {
		t.Fatalf("hydration status does not reflect bounded pressure: %+v", stats)
	}
	release()
	if r := hydrationOutcome(t, first); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestChunkHydration_ActiveDemandPrecedesExpiredWaiter(t *testing.T) {
	s, _, remote, registered := hydrationFixture(t, 1, 3)
	entered, release := holdGets(t, remote)
	defer release()
	first := startHydrationRead(t, s, context.Background(), 8)
	waitKey(t, entered, "occupied worker")
	waitHydrationRegistration(t, registered, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	expired := startHydrationRead(t, s, ctx, 12)
	waitHydrationRegistration(t, registered, 1)
	if r := hydrationOutcome(t, expired); !errors.Is(r.err, ErrChunkHydrationPending) {
		t.Fatalf("expired queued read: %v", r.err)
	}
	demand := startHydrationRead(t, s, context.Background(), 16)
	waitHydrationRegistration(t, registered, 1)
	release()
	if r := hydrationOutcome(t, first); r.err != nil {
		t.Fatal(r.err)
	}
	if r := hydrationOutcome(t, demand); r.err != nil || !bytes.Equal(r.data, makeBlock(ctBlockSize, 0x50)) {
		t.Fatalf("demand: %v", r.err)
	}
	var gets []string
	for _, op := range remote.stats().ops {
		if strings.HasPrefix(op, "get:") {
			gets = append(gets, strings.TrimPrefix(op, "get:"))
		}
	}
	if want := remote.stats().head.Chunks[4].Key; len(gets) < 2 || gets[1] != want {
		t.Fatalf("fetch order %v, want demand %q second", gets, want)
	}
}

func TestChunkHydration_CloseSettlesQueuedAndRunningReaders(t *testing.T) {
	s, layer, remote, registered := hydrationFixture(t, 1, 2)
	entered, release := holdGets(t, remote)
	defer release()
	first := startHydrationRead(t, s, context.Background(), 8)
	waitKey(t, entered, "running Get")
	second := startHydrationRead(t, s, context.Background(), 12)
	waitHydrationRegistration(t, registered, 2)
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []<-chan hydrationResult{first, second} {
		if r := hydrationOutcome(t, c); !errors.Is(r.err, errChunkLayerClosed) {
			t.Fatalf("close outcome: %v", r.err)
		}
	}
	if got := remote.stats().gets; got != 1 {
		t.Fatalf("queued work started after Close: Gets=%d", got)
	}
}

func TestChunkHydration_ConfiguredWorkersBoundConcurrentGets(t *testing.T) {
	s, _, remote, registered := hydrationFixture(t, 2, 3)
	entered, release := holdGets(t, remote)
	defer release()
	a := startHydrationRead(t, s, context.Background(), 8)
	b := startHydrationRead(t, s, context.Background(), 12)
	waitKey(t, entered, "worker1")
	waitKey(t, entered, "worker2")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c := startHydrationRead(t, s, ctx, 16)
	waitHydrationRegistration(t, registered, 3)
	if r := hydrationOutcome(t, c); !errors.Is(r.err, ErrChunkHydrationPending) {
		t.Fatalf("queued read: %v", r.err)
	}
	if got := remote.stats().gets; got != 2 {
		t.Fatalf("configured2 workers started%d Gets", got)
	}
	release()
	if r := hydrationOutcome(t, a); r.err != nil {
		t.Fatal(r.err)
	}
	if r := hydrationOutcome(t, b); r.err != nil {
		t.Fatal(r.err)
	}
}

type uninterruptibleHydrationRemote struct {
	*fakeChunkRemote
	entered, release chan struct{}
	once             sync.Once
}

func (r *uninterruptibleHydrationRemote) Get(_ context.Context, key string) ([]byte, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return r.fakeChunkRemote.Get(context.Background(), key)
}

func TestChunkHydration_CloseWakesCallerAndJoinsUninterruptibleGet(t *testing.T) {
	image := make([]byte, ctNumBlocks*ctBlockSize)
	copy(image[8*ctBlockSize:], makeBlock(ctBlockSize, 0x88))
	f := newFakeChunkRemote()
	f.seedHead(t, image, 50, 1)
	r := &uninterruptibleHydrationRemote{fakeChunkRemote: f, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(r.release) }) }
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, r, cfg)
	t.Cleanup(release)
	result := startHydrationRead(t, s, context.Background(), 8)
	select {
	case <-r.entered:
	case <-time.After(ctLocalBound):
		t.Fatal("Get not entered")
	}
	closed := make(chan struct{})
	t.Cleanup(func() {
		release()
		select {
		case <-closed:
		case <-time.After(ctLocalBound):
			t.Error("Close did not join Get")
		}
	})
	go func() { _ = layer.Close(); close(closed) }()
	select {
	case got := <-result:
		if !errors.Is(got.err, errChunkLayerClosed) {
			t.Fatalf("Close lost closed cause: %v", got.err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close did not wake hydration caller")
	}
	select {
	case <-closed:
		t.Fatal("Close abandoned uninterruptible Get")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case <-closed:
	case <-time.After(ctLocalBound):
		t.Fatal("Close did not settle after Get release")
	}
}

// Only the old generation ignores cancellation; a spare worker can serve the new one.
type staleHydrationRemote struct {
	*fakeChunkRemote
	oldKey           string
	entered, release chan struct{}
	once             sync.Once
}

func (r *staleHydrationRemote) Get(ctx context.Context, key string) ([]byte, error) {
	if key == r.oldKey {
		r.once.Do(func() { close(r.entered) })
		<-r.release
		return r.fakeChunkRemote.Get(context.Background(), key)
	}
	return r.fakeChunkRemote.Get(ctx, key)
}
func TestChunkHydration_NewGenerationDoesNotJoinStaleGet(t *testing.T) {
	s, layer, remote, registered := hydrationFixture(t, 2, 64)
	layer.stMu.Lock()
	oldRef, newRef := layer.head.Chunks[2], layer.head.Chunks[3]
	layer.stMu.Unlock()
	r := &staleHydrationRemote{fakeChunkRemote: remote, oldKey: oldRef.Key, entered: make(chan struct{}), release: make(chan struct{})}
	layer.remote = r
	var once sync.Once
	release := func() { once.Do(func() { close(r.release) }) }
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	old := startHydrationRead(t, s, ctx, 8)
	waitHydrationRegistration(t, registered, 1)
	select {
	case <-r.entered:
	case <-time.After(ctLocalBound):
		t.Fatal("old Get not entered")
	}
	layer.stMu.Lock()
	layer.head.Chunks[2] = newRef
	layer.absentGen[2]++
	layer.stMu.Unlock()
	newer := startHydrationRead(t, s, ctx, 8)
	waitHydrationRegistration(t, registered, 1)
	select {
	case got := <-newer:
		if got.err != nil || !bytes.Equal(got.data, makeBlock(ctBlockSize, 0x4c)) {
			t.Fatalf("new generation blocked by stale Get or wrong bytes: %v", got.err)
		}
	case <-time.After(500 * time.Millisecond):
		release()
		t.Fatal("new generation joined cancellation-ignoring old Get")
	}
	release()
	if got := hydrationOutcome(t, old); got.err != nil || !bytes.Equal(got.data, makeBlock(ctBlockSize, 0x4c)) {
		t.Fatalf("old waiter failed to follow new generation: %v", got.err)
	}
}
