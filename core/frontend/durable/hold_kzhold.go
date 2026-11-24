//go:build kzhold

package durable

import (
	"fmt"
	"os"
	"time"
)

// HoldBeforeChunkFreshRetireForTest exists only in a binary built with the
// kzhold tag, which is never released. At the attach's boundary after
// presence and before the fresh marker is retired (the attach commit), the
// provider writes reached and then waits until release exists (at most ten
// minutes), so a test can stop the process at that exact point.
func (p *DurableProvider) HoldBeforeChunkFreshRetireForTest(reached, release string) {
	p.testBeforeChunkFreshRetire = func() error {
		if err := os.WriteFile(reached, []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
			return fmt.Errorf("kzhold: write %s: %w", reached, err)
		}
		end := time.Now().Add(10 * time.Minute)
		for time.Now().Before(end) {
			if _, err := os.Stat(release); err == nil {
				return nil
			}
			time.Sleep(20 * time.Millisecond)
		}
		return fmt.Errorf("kzhold: %s never appeared", release)
	}
}
