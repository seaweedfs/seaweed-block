package durable_test

// The scheduled-snapshot loop through the real provider.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type snapCalls struct {
	mu    sync.Mutex
	heads []storage.ChunkHead
	fail  error
}

func (c *snapCalls) take(ctx context.Context, _ string, _ time.Time, cut func(context.Context) (storage.ChunkHead, error)) error {
	c.mu.Lock()
	fail := c.fail
	c.mu.Unlock()
	if fail != nil {
		return fail
	}
	h, err := cut(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.heads = append(c.heads, h)
	c.mu.Unlock()
	return nil
}

func (c *snapCalls) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.heads)
}

func snapScheduleProvider(t *testing.T, remote storage.ChunkRemote, period time.Duration, calls *snapCalls) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkSnapshotPeriod:    period,
		ChunkScheduledSnapshot: calls.take,
		ChunkRemoteFor:         func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func scheduledStatus(t *testing.T, p *durable.DurableProvider) *durable.ScheduledSnapshotStatus {
	t.Helper()
	for _, st := range p.DurableStatuses() {
		if st.VolumeID == "v1" && st.Chunk != nil {
			return st.Chunk.ScheduledSnapshot
		}
	}
	return nil
}

func waitFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s not reached within %v", what, d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// With a schedule, the attached volume's snapshot is taken at its slot by
// one cut that publishes the dirty data, the snapshot callback gets that
// cut's HEAD, and the status shows the success and the next slot. A failure
// is visible in the status with its code.
func TestWALStoreChunk_ScheduledSnapshotCutsAndReports(t *testing.T) {
	const period = 2 * time.Second
	remote := storage.NewMemChunkRemote()
	calls := &snapCalls{}
	p := snapScheduleProvider(t, remote, period, calls)
	s := openRecovered(t, p)
	if _, err := s.Write(1, block(0x5A)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*period, "the first scheduled snapshot", func() bool { return calls.count() >= 1 })
	calls.mu.Lock()
	h := calls.heads[0]
	calls.mu.Unlock()
	if remoteHead := headGen(t, remote); h.Gen == 0 || h.Gen > remoteHead.Gen || len(h.Chunks) == 0 {
		t.Fatalf("snapshot HEAD gen %d with %d chunks (remote gen %d); want the cut that published the write", h.Gen, len(h.Chunks), remoteHead.Gen)
	}
	waitFor(t, time.Second, "the success in the status", func() bool {
		st := scheduledStatus(t, p)
		return st != nil && st.LastOkNs > 0
	})
	st := scheduledStatus(t, p)
	if st.PeriodNs != int64(period) || st.NextDueNs <= st.LastOkNs || st.Overdue || st.LastFailure != nil {
		t.Fatalf("status %+v; want period %v, next slot after the success, not overdue, no failure", st, period)
	}

	calls.mu.Lock()
	calls.fail = &durable.SnapshotFailure{Code: "remote_unavailable", Err: errors.New("filer down")}
	calls.mu.Unlock()
	waitFor(t, 3*period, "the failure in the status", func() bool {
		st := scheduledStatus(t, p)
		return st != nil && st.LastFailure != nil
	})
	if st := scheduledStatus(t, p); st.LastFailure.Code != "remote_unavailable" || st.Attempts < 1 {
		t.Fatalf("status %+v; want the typed failure and its attempt count", st)
	}
}

// With the schedule off (period zero), no snapshot is attempted and the
// status has no scheduled-snapshot section.
func TestWALStoreChunk_ScheduledSnapshotOffTakesNone(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	calls := &snapCalls{}
	p := snapScheduleProvider(t, remote, 0, calls)
	s := openRecovered(t, p)
	if _, err := s.Write(1, block(0x01)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if calls.count() != 0 || scheduledStatus(t, p) != nil {
			t.Fatal("a scheduled snapshot ran with the schedule off")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A snapshot period without its callback is refused.
func TestWALStoreChunk_ScheduledSnapshotNeedsItsCallback(t *testing.T) {
	_, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks, ChunkSnapshotPeriod: time.Hour,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil },
	}, v1View())
	if err == nil {
		t.Fatal("a snapshot period without its callback was accepted")
	}
}

// A volume still fetching the chunks its HEAD names (a fresh host starting
// from the remote HEAD) is restoring: its scheduled attempts are skipped,
// counted by reason, and no snapshot is taken.
func TestWALStoreChunk_ScheduledSnapshotSkipsARestoringVolume(t *testing.T) {
	const period = 2 * time.Second
	remote := storage.NewMemChunkRemote()
	view := v1View()
	first := chunkProvider(t, t.TempDir(), remote, view, 0)
	s := openRecovered(t, first)
	if _, err := s.Write(33, block(0x33)); err != nil {
		t.Fatal(err)
	}
	if err := first.MakeVolumeRemoteDurable(context.Background(), "v1", 1); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	calls := &snapCalls{}
	fresh, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkSnapshotPeriod: period, ChunkScheduledSnapshot: calls.take,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fresh.Close() })
	openRecovered(t, fresh)
	waitFor(t, 3*period, "a restoring skip", func() bool {
		st := scheduledStatus(t, fresh)
		return st != nil && st.Skipped[durable.SnapshotSkipRestoring] >= 1
	})
	if st := scheduledStatus(t, fresh); calls.count() != 0 || st.LastFailure != nil || st.Attempts != 0 {
		t.Fatalf("restoring volume: %d snapshots, status %+v; want none taken and no failure", calls.count(), st)
	}
}
