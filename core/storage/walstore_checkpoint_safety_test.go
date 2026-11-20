package storage

import (
	"time"
)

func replaceFlusherForTest(s *WALStore, interval time.Duration) {
	s.flusher.Stop()
	s.flusher = newFlusher(s, flusherConfig{Interval: interval})
	go s.flusher.run()
}
