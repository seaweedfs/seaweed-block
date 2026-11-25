package authority

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrWorkspaceDeleteReserved = errors.New("workspace: deletion is committed")

func (p *Publisher) beforeWorkspaceGrantLock(site string) {
	if p.testBeforeGrantLock != nil {
		p.testBeforeGrantLock(site)
	}
}

// ReserveWorkspaceDeletion persists the existing lifecycle reservation as
// committed, then closes all grant writers under mu -> leaseMu. persist must
// not reenter Publisher. A replay does no I/O. On startup, call this for every
// reloaded committed reservation before serving requests (persist is then nil).
func (p *Publisher) ReserveWorkspaceDeletion(ctx context.Context, volume string, persist func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if volume == "" {
		return errors.New("workspace delete: volume is required")
	}
	if p.deleteReserved[volume] {
		return nil
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	if p.deleteReserved == nil {
		p.deleteReserved = map[string]bool{}
	}
	p.deleteReserved[volume] = true
	return nil
}

// NoLiveWorkspaceLease checks the deletion's stable lease premise.
func (p *Publisher) NoLiveWorkspaceLease(ctx context.Context, volume string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.deleteReserved[volume] {
		return errors.New("workspace delete: committed reservation required")
	}
	// A skipped authority record is not evidence that its lease never existed.
	// Until recovery identifies it, do not certify absence from a partial load.
	if len(p.loadErrs) != 0 {
		return errors.New("workspace delete: authority records were skipped during recovery; lease absence is unknown")
	}
	if p.workspaceUncertain[volume] {
		return errors.New("workspace delete: lease persistence uncertain; restart required")
	}
	if s := p.workspace[volume]; s != nil && s.Lease != nil {
		until, _, err := p.forcedMoveBarrierLocked(volume, s.Lease)
		if err != nil {
			return fmt.Errorf("workspace delete: live until %s: %w", time.Unix(0, until).UTC().Format(time.RFC3339Nano), err)
		}
	}
	return nil
}
