package durable_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
)

func TestPrecopyRollingUploadReopensTentativeAdmission(t *testing.T) {
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	entered, release := make(chan struct{}), make(chan struct{})
	var upload sync.Once
	fixture.Blobs.PutHook = func(ctx context.Context) error {
		upload.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p := precopyBProvider(t, fixture, &parkView{stubView: v1View()}, nil)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 4096, block(0x5b)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	var once sync.Once
	unpark := func() { once.Do(func() { close(release) }) }
	done := make(chan struct{})
	var cutErr error
	t.Cleanup(func() {
		unpark()
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("cut did not join")
		}
	})
	go func() { defer close(done); _, _, cutErr = p.DetachVolume(ctx, "v1") }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("cold rolling cut never uploaded")
	}
	// This chunk is outside the held cut's COW candidate set. Missing cost
	// samples must reopen admission before the ordinary upload can complete.
	write, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	if _, err := b.Write(write, 80*4096, block(0x6d)); err != nil {
		t.Fatal("rolling upload held guest admission:", err)
	}
	unpark()
	select {
	case <-done:
		if cutErr != nil {
			t.Fatal(cutErr)
		}
	case <-ctx.Done():
		t.Fatal("rolling rounds did not converge")
	}
	if layoutHeadBlock(t, fixture, 80)[0] != 0x6d {
		t.Fatal("later admitted write missing from final image")
	}
}
