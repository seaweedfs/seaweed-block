package authority

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// #242: a renewal refused for lack of host validity formats its error from
// the lease view; that read must happen under leaseMu, because the extender
// (refreshLeaseViewLocked) rewrites the view under leaseMu at any time.
// Run with -race: renewals that take the refusal run while another goroutine
// rewrites the view, as the extender does.
func TestWorkspaceLeaseRefusalReadsTheViewUnderTheLock(t *testing.T) {
	disk, _ := newTempStore(t)
	c := newTestClock()
	p := newLeasePublisher(t, disk, 3*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	// Force the no-host-validity refusal: the horizon just ahead, a skew
	// bound far larger than what is left of it; the granted deadline later.
	p.leaseMu.Lock()
	lv := p.leaseViews["v"]
	lv.d.horizon = c.now().Add(time.Millisecond).UnixNano()
	lv.d.maxSkewNS = int64(time.Hour)
	lv.deadline = c.now().Add(time.Hour)
	p.leaseMu.Unlock()

	const rounds = 200
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // the extender's republish: a write of the view under leaseMu
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			p.leaseMu.Lock()
			d := lv.d
			lv.d = d
			p.leaseMu.Unlock()
		}
	}()
	errs := make(chan error, 1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err == nil || !strings.Contains(err.Error(), "leaves no host validity") {
				select {
				case errs <- err:
				default:
				}
				return
			}
		}
	}()
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatalf("renewal with no host validity: %v; want the refusal", err)
	default:
	}
}
