package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// runS5Bootstrap is the live, non-test entrypoint exercising the
// durable authority bootstrap path:
//
//   1. AcquireStoreLock — single-owner OS lock on the store dir
//   2. NewFileAuthorityStore — open file-per-volume backend
//   3. NewPublisher(..., WithStore(store)) — reload synchronously
//   4. log LoadErrors() at startup so reload-corruption is visible
//
// The mode is intentionally narrow: it proves the durable route
// is wired end-to-end under a real flag, and produces machine-
// readable output so CI (or a real operator) can verify the
// reload count and skip list.
//
// Without --authority-store, sparrow continues to run in-memory
// (existing behavior preserved). With --authority-store AND
// --s5-bootstrap, sparrow performs the durable bootstrap and
// exits 0 on success. Running twice in a row against the same
// store directory exercises the reload path.
func runS5Bootstrap(opts options) int {
	if opts.authorityStore == "" {
		fmt.Fprintln(os.Stderr, "sparrow: --s5-bootstrap requires --authority-store <dir>")
		return 2
	}

	directive := authority.NewStaticDirective(nil)
	boot, err := Bootstrap(opts.authorityStore, directive)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sparrow:", err)
		return 1
	}
	defer func() {
		if cerr := boot.Close(); cerr != nil {
			log.Printf("sparrow: close durable bootstrap: %v", cerr)
		}
	}()

	// Let the Publisher's Run goroutine start so the bootstrapped
	// state is observably "live" — not just constructed. A short
	// context cancel returns promptly; the bootstrap's own logs
	// already recorded the reload outcome.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	go func() { _ = boot.Publisher.Run(ctx) }()
	<-ctx.Done()

	if opts.json {
		report := map[string]any{
			"store":              opts.authorityStore,
			"reloaded_records":   boot.ReloadedRecords,
			"reload_skip_count":  len(boot.ReloadSkips),
			"reload_skip_reasons": summarizeSkips(boot.ReloadSkips),
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
	} else {
		fmt.Printf("sparrow s5-bootstrap: store=%q reloaded=%d skips=%d\n",
			opts.authorityStore, boot.ReloadedRecords, len(boot.ReloadSkips))
	}
	return 0
}

func summarizeSkips(skips []error) []string {
	out := make([]string, 0, len(skips))
	for _, e := range skips {
		out = append(out, e.Error())
	}
	return out
}
