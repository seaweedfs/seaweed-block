package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
)

type cliFilerCheck struct {
	chunkstore.Filer
	checks, closes int
}

func (f *cliFilerCheck) CheckSingleFiler(context.Context) error {
	f.checks++
	return errors.New("different metadata point")
}
func TestChunkCatalogRefusesMultipleFilersBeforeMutation(t *testing.T) {
	f := &cliFilerCheck{Filer: chunkstore.NewMemFiler()}
	old := dialChunkFiler
	t.Cleanup(func() { dialChunkFiler = old })
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) {
		return f, func() error { f.closes++; return nil }, nil
	}
	var out, stderr bytes.Buffer
	cat, closeFn, exit := chunkCatalog("fork", "filer", defaultChunkPathCLI, &out, &stderr)
	if closeFn != nil {
		_ = closeFn()
	}
	if cat != nil || exit < 0 || f.checks != 1 || f.closes != 1 {
		t.Fatalf("unchecked CLI catalog: cat=%v exit=%d checks=%d closes=%d", cat, exit, f.checks, f.closes)
	}
}

func TestChunkGCRefusesMultipleFilersBeforeEffects(t *testing.T) {
	f := &cliFilerCheck{Filer: chunkstore.NewMemFiler()}
	old := dialChunkFiler
	t.Cleanup(func() { dialChunkFiler = old })
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) {
		return f, func() error { f.closes++; return nil }, nil
	}
	code, _, _ := runChunk("gc", "--chunk-filer=filer", "--master=master", "--apply")
	if code == 0 || f.checks != 1 || f.closes != 1 {
		t.Fatalf("GC skipped topology admission: code=%d checks=%d closes=%d", code, f.checks, f.closes)
	}
}
