package durable_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// waitLog waits until the log holds want, then returns the log.
func waitLog(t *testing.T, l *lockedLog, want string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(l.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("log never held %q:\n%s", want, l.String())
		}
		time.Sleep(time.Millisecond)
	}
	return l.String()
}

// inOrder checks that every line holding a want[i] fragment set appears,
// each after the previous one.
func inOrder(t *testing.T, logs string, want ...[]string) {
	t.Helper()
	lines := strings.Split(logs, "\n")
	at := 0
next:
	for _, frags := range want {
		for ; at < len(lines); at++ {
			ok := true
			for _, f := range frags {
				ok = ok && strings.Contains(lines[at], f)
			}
			if ok {
				at++
				continue next
			}
		}
		t.Fatalf("no line with %q after the previous expected line:\n%s", frags, logs)
	}
}

// The deferred attach logs its wait (with the projection fields that
// fail), the first attach attempt, the HEAD claim and how the wait ended.
func TestChunkAuthorityWaitLogsTheWaitTheClaimAndTheEnd(t *testing.T) {
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
	p := chunkProviderForAssignment(t, v)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	v.active.Store(true)
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "waits for master authority")
	close(v.assigned)
	got := waitLog(t, logs, "chunk authority wait for volume=v1 ended after")
	inOrder(t, got,
		[]string{"volume=v1 waits for master authority (up to 30s): projection lacks healthy=false epoch=0"},
		[]string{"volume=v1: master authority after", `replica="r1" epoch=3 endpoint=1`, "first attach attempt"},
		[]string{"claiming HEAD for volume=v1: gen=", `-> epoch=3 holder="r1"`},
		[]string{"HEAD claim for volume=v1 done in ", `gen=0 epoch=0 holder="", no HEAD to claim (new lineage)`},
		[]string{"chunk authority wait for volume=v1 ended after", ": attached after 1 attempt(s)"},
	)
	if strings.Count(got, "first attach attempt") != 1 {
		t.Fatalf("first attach attempt logged more than once:\n%s", got)
	}
}

// A wait that reaches its bound logs the assignment-unavailable transition
// with the fields still missing; a wait ended by Close says so.
func TestChunkAuthorityWaitLogsDeadlineAndClose(t *testing.T) {
	for _, mode := range []string{"limit", "close"} {
		t.Run(mode, func(t *testing.T) {
			logs := &lockedLog{}
			captureWriteLogs(t, logs)
			v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
			limit := 40 * time.Millisecond
			if mode == "close" {
				limit = time.Minute
			}
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
				ChunkAssignmentTimeout: limit,
				ChunkRemoteFor:         func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil },
			}, v)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			v.active.Store(true)
			if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatal(err)
			}
			want := "ended after"
			if mode == "close" {
				waitLog(t, logs, "waits for master authority")
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
			}
			got := waitLog(t, logs, want)
			end := ": assignment unavailable, not serving (projection lacks healthy=false epoch=0)"
			if mode == "close" {
				end = ": provider closed"
			}
			inOrder(t, got,
				[]string{"volume=v1 waits for master authority (up to " + limit.String() + "): projection lacks healthy=false epoch=0"},
				[]string{"chunk authority wait for volume=v1 ended after", end},
			)
			if strings.Contains(got, "first attach attempt") || strings.Contains(got, "claiming HEAD") {
				t.Fatalf("attach logged without authority:\n%s", got)
			}
		})
	}
}
