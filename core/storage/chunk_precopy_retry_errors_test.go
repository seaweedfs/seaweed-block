package storage

import (
	"errors"
	"testing"
)

// TestPrecopyRoundLoopRetriesOnlyNamedErrors pins that a failed rolling
// pre-copy round starts another round only after a COW abort, an unavailable
// remote or a HEAD precondition failure; any other error ends the loop at once.
func TestPrecopyRoundLoopRetriesOnlyNamedErrors(t *testing.T) {
	errNotPrimary := errors.New("test: not primary")
	for _, tc := range []struct {
		name   string
		first  error
		rounds int
	}{
		{"other error", errNotPrimary, 1},
		{"COW aborted", errCutCOWAborted, 2},
		{"remote unavailable", errChunkRemoteUnavailable, 2},
		{"HEAD precondition", errChunkHeadPrecondition, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, l, _ := precopyLayer(t, 1024, 0)
			// Every round fails at capture admission: the first with the error
			// under test, any later one with an error that is never retried.
			calls := 0
			l.cfg.CaptureAllowed = func() error {
				calls++
				if calls == 1 {
					return tc.first
				}
				return errNotPrimary
			}
			r, err := l.PrecopyDetach(ctTimeout(t), func() bool {
				t.Error("refused rounds reached the final stop")
				return true
			})
			if !errors.Is(err, errNotPrimary) {
				t.Fatalf("err=%v, want the non-retried refusal", err)
			}
			if calls != tc.rounds || len(r.Precopy.Rounds) != tc.rounds {
				t.Fatalf("rounds: admission calls=%d reported=%d, want %d", calls, len(r.Precopy.Rounds), tc.rounds)
			}
		})
	}
}
