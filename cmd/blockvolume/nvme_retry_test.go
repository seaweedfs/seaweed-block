package main_test

import (
	"errors"
	"fmt"
	"testing"
)

// nvmeHostMaxRetries is the Linux NVMe host's default retry count for a
// command whose status has Do Not Retry clear (nvme_core max_retries).
const nvmeHostMaxRetries = 5

// nvmeStatusError is a completion with a non-zero status. Bit 15 of the
// status field is Do Not Retry.
type nvmeStatusError struct {
	op     string
	status uint16
}

func (e *nvmeStatusError) Error() string { return fmt.Sprintf("nvme %s status 0x%04x", e.op, e.status) }

func (e *nvmeStatusError) retryable() bool { return e.status&0x8000 == 0 }

// retryLikeHost runs op as the Linux NVMe host runs a command: a completion
// whose status has Do Not Retry clear is retried, at most max times; it
// returns the number of such refusals. When max retries are used up the
// error is returned, as the host returns EIO then. A status with Do Not
// Retry set, or any other error (transport, lost or unexpected bytes), is
// returned at once.
func retryLikeHost(max int, op func() error) (refused int, err error) {
	for {
		err = op()
		var se *nvmeStatusError
		if err == nil || !errors.As(err, &se) || !se.retryable() {
			return refused, err
		}
		refused++
		if refused > max {
			return refused, fmt.Errorf("%w: refused %d times, %d retries used up", err, refused, max)
		}
	}
}

func TestRetryLikeHostRetriesOnlyDNRClearStatusFiveTimes(t *testing.T) {
	busy := &nvmeStatusError{op: "Write", status: 0x0104}      // namespace not ready, DNR clear
	permanent := &nvmeStatusError{op: "Write", status: 0x8004} // DNR set
	script := func(errs ...error) (func() error, *int) {
		calls := 0
		return func() error {
			calls++
			if calls <= len(errs) {
				return errs[calls-1]
			}
			return nil
		}, &calls
	}
	repeat := func(e error, n int) []error {
		out := make([]error, n)
		for i := range out {
			out[i] = e
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		max     int
		errs    []error
		refused int
		calls   int
		fails   bool
	}{
		{"five refusals then success", nvmeHostMaxRetries, repeat(busy, 5), 5, 6, false},
		{"six refusals use up the retries", nvmeHostMaxRetries, repeat(busy, 6), 6, 6, true},
		{"retries off fails on the first refusal", 0, repeat(busy, 1), 1, 1, true},
		{"do not retry set fails at once", nvmeHostMaxRetries, []error{permanent}, 0, 1, true},
		{"a transport error fails at once", nvmeHostMaxRetries, []error{errors.New("nvme Write: want R2T: EOF")}, 0, 1, true},
		{"success", nvmeHostMaxRetries, nil, 0, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op, calls := script(tc.errs...)
			refused, err := retryLikeHost(tc.max, op)
			if refused != tc.refused || *calls != tc.calls || (err != nil) != tc.fails {
				t.Fatalf("refused %d calls %d err %v; want refused %d calls %d fails %v", refused, *calls, err, tc.refused, tc.calls, tc.fails)
			}
		})
	}
}
