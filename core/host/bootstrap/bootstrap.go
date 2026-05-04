package bootstrap

import (
	"fmt"
	"log"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// DurableAuthorityBootstrap is the live wiring of the P14 S5
// durable-authority route: AcquireStoreLock → NewFileAuthorityStore
// → NewPublisher(WithStore). Running this in non-test code is
// the closure requirement for S5 — before this, the store / lock /
// reload capability existed only in unit tests.
//
// Startup order matches sketch §7:
//   1. acquire the single-owner store lock (refuse to start if held)
//   2. open the file-per-volume store under the same directory
//   3. construct the Publisher with WithStore(store), which runs
//      reload synchronously — surviving records populate
//      Publisher state, corrupt records go to LoadErrors()
//   4. log any LoadErrors() at startup so operators see
//      reload-corruption events rather than having them stranded
//      inside the Publisher
//
// The caller owns lifecycle: invoke Close() on process exit (or
// via defer) to release the lock and close the store cleanly.
type DurableAuthorityBootstrap struct {
	Lock      *authority.StoreLock
	Store     authority.AuthorityStore
	Publisher *authority.Publisher

	// ReloadedRecords is the count of records successfully
	// reloaded from the store at construction. Exposed for
	// startup diagnostics / tests.
	ReloadedRecords int

	// ReloadSkips is the per-record skip errors observed during
	// reload (per-volume fail-closed corruption). Surfaced via
	// startup log and retained here for diagnosis.
	ReloadSkips []error
}

// Bootstrap performs the durable-authority startup sequence and
// returns a *DurableAuthorityBootstrap that owns the live store,
// lock, and Publisher. Fails closed if the lock cannot be
// acquired or the store directory cannot be enumerated.
//
// Startup logs:
//   "durable authority lock acquired"
//   "durable authority reload: N records"
//   "durable authority reload skip: %v" (one per skip)
//
// The logs are the promised operator/startup visibility of
// LoadErrors() — architect round-5 finding #3.
func Bootstrap(storeDir string, directive authority.Directive) (*DurableAuthorityBootstrap, error) {
	if storeDir == "" {
		return nil, fmt.Errorf("sparrow: --authority-store is required for durable bootstrap")
	}

	lock, err := authority.AcquireStoreLock(storeDir)
	if err != nil {
		return nil, fmt.Errorf("durable authority lock: %w", err)
	}
	log.Printf("durable authority lock acquired (store=%q)", storeDir)

	store, err := authority.NewFileAuthorityStore(storeDir)
	if err != nil {
		_ = lock.Release()
		return nil, fmt.Errorf("durable authority store: %w", err)
	}

	return bootstrapFromStoreLocked(storeDir, lock, store, directive)
}

// bootstrapFromStoreLocked performs the durable-authority bring-
// up steps that run AFTER the store lock is acquired and the
// store is open: the index pre-check, the Publisher construction
// (with its internal reload), and the LoadErrors startup logging.
//
// Exposed (as a package-private helper and via BootstrapFromStore
// below) so the tests can drive the pre-check branch with a
// mocked store that produces an index-level failure on any
// platform — a filesystem-permission-based approach is not
// portable enough to reliably exercise the branch.
func bootstrapFromStoreLocked(storeDir string, lock *authority.StoreLock, store authority.AuthorityStore, directive authority.Directive) (*DurableAuthorityBootstrap, error) {
	// Index-level pre-check BEFORE constructing the Publisher.
	// NewPublisher with a store panics on index-level reload
	// failure; without this pre-check Bootstrap would leave the
	// lock acquired and the store open when that panic escaped.
	// Doing the check here converts index-level failure into a
	// clean fails-closed path that releases the lock and closes
	// the store, honoring Bootstrap's documented contract.
	records, skips, loadErr := store.LoadWithSkips()
	if loadErr != nil {
		_ = store.Close()
		_ = lock.Release()
		return nil, fmt.Errorf("durable authority index check: %w", loadErr)
	}

	// NewPublisher is now safe: its internal reload will see the
	// same healthy index we just verified. Per-record corrupt
	// skips are still returned via Publisher.LoadErrors() and
	// logged below.
	pub := authority.NewPublisher(directive, authority.WithStore(store))

	// Publisher's LoadErrors is the canonical source; prefer it
	// when both available so we surface exactly what the
	// Publisher saw. Falls back to the probe above.
	publisherSkips := pub.LoadErrors()
	if len(publisherSkips) > 0 {
		skips = publisherSkips
	}

	log.Printf("durable authority reload: %d records", len(records))
	for _, e := range skips {
		log.Printf("durable authority reload skip: %v", e)
	}

	return &DurableAuthorityBootstrap{
		Lock:            lock,
		Store:           store,
		Publisher:       pub,
		ReloadedRecords: len(records),
		ReloadSkips:     skips,
	}, nil
}

// BootstrapFromStore is the seam tests use to exercise the
// bring-up sequence with a caller-supplied (possibly mocked)
// lock+store pair. Production code calls Bootstrap, which wraps
// this helper with AcquireStoreLock + NewFileAuthorityStore.
//
// The cleanup contract is the same: if the pre-check fails, the
// lock is released and the store is closed before the error
// returns.
func BootstrapFromStore(lock *authority.StoreLock, store authority.AuthorityStore, directive authority.Directive) (*DurableAuthorityBootstrap, error) {
	return bootstrapFromStoreLocked("", lock, store, directive)
}

// Close releases the lock and closes the store. Idempotent.
// Safe to call via defer at process startup.
func (b *DurableAuthorityBootstrap) Close() error {
	var firstErr error
	if b.Store != nil {
		if err := b.Store.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close store: %w", err)
		}
	}
	if b.Lock != nil {
		if err := b.Lock.Release(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("release lock: %w", err)
		}
	}
	return firstErr
}
