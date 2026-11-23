package storage

import "testing"

// crashRecoveryHardeningStore drops the store without a clean close: the
// committer stops, the owned map and the file descriptor close, nothing is
// flushed. Tests reopen the file afterwards to observe crash recovery.
func crashRecoveryHardeningStore(t *testing.T, s *WALStore) {
	t.Helper()
	s.committer.Stop()
	if s.v3 != nil && s.v3.ownedMap != nil {
		if err := s.v3.ownedMap.Close(); err != nil {
			t.Fatal(err)
		}
		s.v3.ownedMap = nil
	}
	if err := s.fd.Close(); err != nil {
		t.Fatal(err)
	}
}
