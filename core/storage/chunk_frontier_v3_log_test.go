package storage

import (
	"bytes"
	"log"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type seedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *seedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *seedLog) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range strings.Split(l.buf.String(), "\n") {
		if strings.Contains(line, "storage: seed frontier") {
			out = append(out, line)
		}
	}
	return out
}

// Nonparallel: the standard logger is global.
func captureSeedLog(t *testing.T) *seedLog {
	t.Helper()
	l := &seedLog{}
	old := log.Writer()
	log.SetOutput(l)
	t.Cleanup(func() { log.SetOutput(old) })
	return l
}

var seedTimesLine = regexp.MustCompile(`storage: seed frontier 7: capture\+lifecycle lock wait [0-9.]+[a-zµ]+s, store mutex\+validation [0-9.]+[a-zµ]+s, data sync \(incl\. sync serialization\) [0-9.]+[a-zµ]+s, checkpoint publish [0-9.]+[a-zµ]+s, err=`)

// A v3 seed logs its lock wait, data fsync and checkpoint publication
// times once per seed that did I/O, with its outcome; a no-op repeat and a
// refusal log nothing.
func TestWALStoreV3SeedLogsItsStageTimes(t *testing.T) {
	t.Run("seeded", func(t *testing.T) {
		logs := captureSeedLog(t)
		s, _ := newMappedCheckpointStoreV3(t)
		if err := s.SeedChunkFrontier(7); err != nil {
			t.Fatal(err)
		}
		if err := s.SeedChunkFrontier(7); err != nil { // already seeded: no I/O
			t.Fatal(err)
		}
		got := logs.lines()
		if len(got) != 1 || !seedTimesLine.MatchString(got[0]) || !strings.HasSuffix(got[0], "err=<nil>") {
			t.Fatalf("seed log lines %q; want one line with the three stage times and err=<nil>", got)
		}
	})
	t.Run("publish failed", func(t *testing.T) {
		logs := captureSeedLog(t)
		s, _ := newMappedCheckpointStoreV3(t)
		fired := false
		s.v3.testMapPrepared = func() {
			if !fired {
				fired = true
				s.v3.recoverGen.Add(2)
			}
		}
		if err := s.SeedChunkFrontier(7); err == nil {
			t.Fatal("seed across a lifetime change succeeded")
		}
		got := logs.lines()
		if len(got) != 1 || !seedTimesLine.MatchString(got[0]) || !strings.Contains(got[0], ErrMapCoverageStale.Error()) {
			t.Fatalf("seed log lines %q; want one line with the stage times and the coverage error", got)
		}
	})
	t.Run("refused", func(t *testing.T) {
		logs := captureSeedLog(t)
		s, _ := newMappedCheckpointStoreV3(t)
		writeSynced(t, s, 1, 1)
		if err := s.SeedChunkFrontier(7); err == nil {
			t.Fatal("seed over local writes succeeded")
		}
		if got := logs.lines(); len(got) != 0 {
			t.Fatalf("a refused seed logged %q", got)
		}
	})
}

// blockingLog blocks its first write until released.
type blockingLog struct {
	entered, release chan struct{}
	once             sync.Once
}

func (b *blockingLog) Write(p []byte) (int, error) {
	first := false
	b.once.Do(func() { first = true })
	if first {
		close(b.entered)
		<-b.release
	}
	return len(p), nil
}

// The seed's log line is written after the capture and lifecycle locks are
// released: while the log output is blocked, both are free.
func TestWALStoreV3SeedLogsOutsideTheStoreLocks(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	b := &blockingLog{entered: make(chan struct{}), release: make(chan struct{})}
	old := log.Writer()
	log.SetOutput(b)
	t.Cleanup(func() { log.SetOutput(old) })
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(b.release) }) }
	done := make(chan error, 1)
	joined := false
	t.Cleanup(func() { // release the blocked writer, then join the seed, bounded
		release()
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("seed not joined")
		}
	})
	go func() { done <- s.SeedChunkFrontier(7) }()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the seed never logged")
	}
	if !s.captureMu.TryLock() {
		t.Fatal("captureMu held while the seed log output is blocked")
	}
	s.captureMu.Unlock()
	if !s.lifecycleMu.TryLock() {
		t.Fatal("lifecycleMu held while the seed log output is blocked")
	}
	s.lifecycleMu.Unlock()
	release()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the seed did not finish after the log output was released")
	}
}
