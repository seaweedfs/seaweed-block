package durable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A scheduled snapshot refused because an earlier HEAD commit's outcome is
// unresolved says so in the log, by name, with the cause and the cure, once
// per attempt. Any other failure keeps its ordinary line.
func TestAScheduledSnapshotRefusedAsPendingUnresolvedIsLoud(t *testing.T) {
	stuck := fmt.Errorf("storage: capture: earlier CAS outcome still unresolved (layout: look up pool/24/247664-ab: rpc error: code = DeadlineExceeded): HEAD unchanged: %w", storage.ErrChunkCASOutcomeUnknown)
	for _, tc := range []struct {
		name  string
		err   error
		loud  bool
		cause string
	}{
		{"pending unresolved", stuck, true, "pool/24/247664-ab"},
		{"ordinary failure", &SnapshotFailure{Code: "remote_unavailable"}, false, "remote_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out syncBuffer
			prior := log.Writer()
			log.SetOutput(&out)
			t.Cleanup(func() { log.SetOutput(prior) })
			synctest.Test(t, func(t *testing.T) {
				_, start, stop := snapLoop(func(context.Context) error { return tc.err })
				start()
				defer func() {
					if !stop() {
						t.Error("the loop was not joined")
					}
				}()
				time.Sleep(24 * time.Hour) // the first attempt, at the volume's offset, and its first retry
				synctest.Wait()
			})
			var lines []string
			for _, l := range strings.Split(out.String(), "\n") {
				if strings.Contains(l, "scheduled snapshot volume=v1") {
					lines = append(lines, l)
				}
			}
			if len(lines) == 0 {
				t.Fatalf("no scheduled-snapshot line logged; log:\n%s", out.String())
			}
			for _, l := range lines {
				if !strings.Contains(l, tc.cause) {
					t.Fatalf("line %q does not carry the cause %q", l, tc.cause)
				}
				loud := strings.Contains(l, "REFUSED: PendingUnresolved") && strings.Contains(l, "every cut, snapshot and detach") && strings.Contains(l, "restart the volume")
				if loud != tc.loud {
					t.Fatalf("line %q: loud=%t, want %t", l, loud, tc.loud)
				}
			}
			if !errors.Is(tc.err, storage.ErrChunkCASOutcomeUnknown) && strings.Contains(out.String(), "PendingUnresolved") {
				t.Fatal("an ordinary failure was reported as PendingUnresolved")
			}
			t.Logf("%d attempt line(s), e.g. %s", len(lines), lines[0])
		})
	}
}
