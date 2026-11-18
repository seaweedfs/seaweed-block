package replication

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

type writeLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *writeLogBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *writeLogBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// Issue #204: at RF=1 every local write logged one INFO line on the write
// path ("replication: OnLocalWrite volume=... lba=... lsn=... peers=0"). Under
// sustained writes that filled the volume's log (162 MB in 13 minutes) and
// buried the useful lines. Per-write detail belongs in the counters (Stats),
// not the log.
func TestReplicationVolume_LocalWritesDoNotLogPerWrite(t *testing.T) {
	buf := &writeLogBuffer{}
	prev, flags := log.Writer(), log.Flags()
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(prev); log.SetFlags(flags) })

	store := storage.NewBlockStore(8, 4096)
	v := NewReplicationVolume("rf1", store)
	t.Cleanup(func() { _ = v.Close() })

	const writes = 100
	data := make([]byte, 4096)
	for i := 0; i < writes; i++ {
		lsn, err := store.Write(uint32(i%8), data)
		if err != nil {
			t.Fatal(err)
		}
		if err := v.OnLocalWrite(context.Background(), LocalWrite{LBA: uint32(i % 8), LSN: lsn, Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(buf.String(), "OnLocalWrite"); n != 0 {
		t.Fatalf("%d local writes produced %d per-write log lines; first: %q", writes, n, firstLine(buf.String()))
	}
	if got := v.Stats().WriteOps; got != writes {
		t.Fatalf("Stats().WriteOps = %d, want %d (the counters keep per-write evidence)", got, writes)
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
