package durable_test

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func ownEpochProvider(t *testing.T, root string, remote storage.ChunkRemote, holder string, epoch uint64) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: root,
		BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkHolder:    holder,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, viewAs("r1", epoch))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// An own-epoch HEAD is a record that this holder already claimed the volume.
// An empty replacement root cannot prove it still has the acknowledged suffix.
// Refuse before seeding or claiming, even when the last remote image is empty.
func TestOwnEpochEmptyLocalRootRefusesBeforeClaim(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty_remote_image", true: "published_remote_image"}[published], func(t *testing.T) {
			ctx := context.Background()
			remote := storage.NewMemChunkRemote()
			first := ownEpochProvider(t, t.TempDir(), remote, "s1/r1", 3)
			openRecovered(t, first)
			backend := first.Backend("v1")
			if published {
				if n, err := backend.Write(ctx, 4096, block('A')); err != nil || n != 4096 {
					t.Fatalf("write A: n=%d err=%v", n, err)
				}
				if _, err := first.CaptureVolume(ctx, "v1"); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := backend.Write(ctx, 2*4096, block('B')); err != nil || n != 4096 {
				t.Fatalf("write B: n=%d err=%v", n, err)
			}
			if err := backend.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			localLSN, _, _ := first.LogicalStorage("v1").(*storage.WALStore).Boundaries()
			before := headHolder(t, remote)
			if before.Holder != "s1/r1" || before.Epoch != 3 || localLSN <= before.RemoteLSN {
				t.Fatalf("fixture lacks acknowledged suffix: HEAD=%+v local=%d", before, localLSN)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			observed := &casObservedRemote{ChunkRemote: remote}
			emptyRoot := t.TempDir()
			replacement := ownEpochProvider(t, emptyRoot, observed, "s1/r1", 3)
			store, err := replacement.EnsureStorage("v1")
			if err != nil {
				t.Fatal(err)
			}
			_, err = replacement.RecoverVolume(ctx, "v1")
			if err == nil {
				got := make([]byte, 4096)
				n, readErr := replacement.Backend("v1").Read(ctx, 2*4096, got)
				t.Fatalf("empty root at its own epoch served without a move: HEAD R=%d acknowledged L=%d; guest read n=%d err=%v lost_B=%t", before.RemoteLSN, localLSN, n, readErr, !bytes.Equal(got, block('B')))
			}
			if !errors.Is(err, durable.ErrChunkMissingLocalRoot) || !strings.Contains(err.Error(), "wrong or unmounted data directory") || !strings.Contains(err.Error(), "forced move (also to the same host)") {
				t.Fatalf("missing typed cause or recovery cure: %v", err)
			}
			gen, ended := replacement.ChunkStartupOutcome("v1")
			if !ended || replacement.ChunkStartupSuperseded("v1", gen) || replacement.ChunkStartupCure("v1", gen) != durable.ChunkMissingLocalRootCure {
				t.Fatal("missing-root refusal lost its cure or invented a foreign holder")
			}
			if observed.cass != 0 || !reflect.DeepEqual(before, headHolder(t, remote)) {
				t.Fatal("refused empty root changed HEAD")
			}
			if err := replacement.Backend("v1").AdmitsIO(); err == nil {
				t.Fatal("refused empty root admits guest I/O")
			}
			if local, _, _ := store.(*storage.WALStore).Boundaries(); local != 0 {
				t.Fatalf("refusal seeded the empty local root to %d", local)
			}
			if err := replacement.Close(); err != nil {
				t.Fatal(err)
			}
			// Creating the refused store must not let the next process bypass
			// the rule merely because the volume file now exists.
			retry := ownEpochProvider(t, emptyRoot, observed, "s1/r1", 3)
			if _, err := retry.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := retry.RecoverVolume(ctx, "v1"); !errors.Is(err, durable.ErrChunkMissingLocalRoot) {
				t.Fatalf("reopening the fresh marker bypassed the refusal: %v", err)
			}
			if observed.cass != 0 || !reflect.DeepEqual(before, headHolder(t, remote)) {
				t.Fatal("retry changed HEAD")
			}
		})
	}
}

func TestOwnEpochExistingLocalRootKeepsAcknowledgedSuffix(t *testing.T) {
	ctx := context.Background()
	remote, root := storage.NewMemChunkRemote(), t.TempDir()
	p := ownEpochProvider(t, root, remote, "s1/r1", 3)
	openRecovered(t, p)
	if _, err := p.Backend("v1").Write(ctx, 4096, block('A')); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Backend("v1").Write(ctx, 2*4096, block('B')); err != nil {
		t.Fatal(err)
	}
	if err := p.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := ownEpochProvider(t, root, remote, "s1/r1", 3)
	openRecovered(t, reopened)
	got := make([]byte, 4096)
	if n, err := reopened.Backend("v1").Read(ctx, 2*4096, got); err != nil || n != len(got) || !bytes.Equal(got, block('B')) {
		t.Fatalf("same-root restart lost acknowledged suffix: n=%d err=%v", n, err)
	}
}

func TestFreshRootAtHigherIssuedEpochRestoresRemoteImage(t *testing.T) {
	for _, holder := range []string{"s1/r1", "s2/r1"} {
		t.Run(holder, func(t *testing.T) {
			ctx := context.Background()
			remote := storage.NewMemChunkRemote()
			p := ownEpochProvider(t, t.TempDir(), remote, "s1/r1", 3)
			openRecovered(t, p)
			if _, err := p.Backend("v1").Write(ctx, 4096, block('A')); err != nil {
				t.Fatal(err)
			}
			if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			// The authority tests separately require the forced move's
			// durable lost-range record before this higher epoch is issued.
			next := ownEpochProvider(t, t.TempDir(), remote, holder, 4)
			openRecovered(t, next)
			got := make([]byte, 4096)
			if n, err := next.Backend("v1").Read(ctx, 4096, got); err != nil || n != len(got) || !bytes.Equal(got, block('A')) {
				t.Fatalf("higher-epoch restore failed: n=%d err=%v", n, err)
			}
		})
	}
}

// New production callers need explicit missing-root admission coverage.
func TestFreshRootClaimHasOnlyOneProductionCaller(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	var callers []string
	for _, tree := range []string{"core", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "ClaimChunkHead" {
					relative, _ := filepath.Rel(root, path)
					callers = append(callers, filepath.ToSlash(relative))
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(callers, []string{"core/frontend/durable/provider_chunk.go"}) {
		t.Fatalf("review missing-root guard coverage for changed production HEAD-claim sites: %v", callers)
	}
}
