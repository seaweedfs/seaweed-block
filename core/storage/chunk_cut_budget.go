package storage

import "context"

// Budget is the host limiter seam. Admit runs before staging or the cut
// barrier and reserves the configured payload ceiling. ReserveCOW accounts
// each saved chunk until Release; it must be nonblocking and perform no I/O.
// The limiter may invoke abort on any admitted ticket (for example, the
// largest holder). Callbacks must not call back into the store. Release runs
// only after all local savers and remote workers for the ticket have joined.
// A nil Budget uses only the per-volume payload and side-file caps.
type Budget interface {
	Admit(ctx context.Context, payloadBytes int64, abort func(error)) (ticket uint64, err error)
	ReserveCOW(ticket uint64, bytes int64) error
	Release(ticket uint64)
}
