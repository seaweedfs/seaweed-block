package storage

import (
	"bytes"
	"log"
	"regexp"
	"strings"
	"sync"
	"testing"
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

var seedTimes = regexp.MustCompile(`storage: seed frontier 7: lock wait [0-9.]+[a-zµ]+s, data fsync [0-9.]+[a-zµ]+s, checkpoint publish [0-9.]+[a-zµ]+s, err=`)

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
		if len(got) != 1 || !seedTimes.MatchString(got[0]) || !strings.HasSuffix(got[0], "err=<nil>") {
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
		if len(got) != 1 || !seedTimes.MatchString(got[0]) || !strings.Contains(got[0], ErrMapCoverageStale.Error()) {
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
