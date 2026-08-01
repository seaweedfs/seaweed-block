//go:build swblock_testtools

package storage

// ResetFlusherForMeasurement restarts the default flusher from a known tick
// origin. It is available only to fixed-work test binaries built with the
// swblock_testtools tag and is never part of a shipped product binary.
func (s *WALStore) ResetFlusherForMeasurement() error {
	if err := s.flusher.Stop(); err != nil {
		return err
	}
	s.flusher = newFlusher(s, flusherConfig{})
	started := make(chan struct{})
	go s.flusher.runWithStartSignal(started)
	<-started
	return nil
}
