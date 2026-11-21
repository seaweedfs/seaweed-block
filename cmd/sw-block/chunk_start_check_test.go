package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
)

// O-ENG-4: every ops chunk command that writes chunk metadata runs the
// single-filer start check before it reads or writes anything, and refuses
// when the filer is not the only one of its group; read-only commands (and
// read-only modes) do not run it.
func TestOpsChunk_WritingCommandsRunTheSingleFilerCheck(t *testing.T) {
	mem := chunkstore.NewMemFiler()
	prevDial, prevCheck := dialChunkFiler, checkSingleFiler
	t.Cleanup(func() { dialChunkFiler, checkSingleFiler = prevDial, prevCheck })
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	checks := 0
	checkSingleFiler = func(context.Context, chunkstore.Filer) error {
		checks++
		return chunkstore.ErrNotSingleFiler
	}
	filer := []string{"--chunk-filer", "filer:18888"}
	writing := map[string][]string{
		"snapshot-delete": append([]string{"ops", "chunk", "snapshot-delete", "--volume", "v1", "--name", "s1"}, filer...),
		"fork":            append([]string{"ops", "chunk", "fork", "--source", "v1", "--snapshot", "s1", "--target", "v2", "--request-id", "r"}, filer...),
		"retain":          append([]string{"ops", "chunk", "retain", "--volume", "v1"}, filer...),
		"volume-delete":   append([]string{"ops", "chunk", "volume-delete", "--volume", "v1", "--master", "m:1", "--request-id", "r"}, filer...),
		"gc --apply":      append([]string{"ops", "chunk", "gc", "--master", "m:1", "--apply"}, filer...),
	}
	for name, args := range writing {
		before := checks
		var out, errb bytes.Buffer
		code := run(args, &out, &errb)
		if checks != before+1 {
			t.Errorf("%s: start check ran %d times; want once", name, checks-before)
		}
		if code == 0 || !strings.Contains(out.String()+errb.String(), "exactly one filer") {
			t.Errorf("%s: exit %d, out %q err %q; want the single-filer refusal", name, code, out.String(), errb.String())
		}
	}
	readOnly := map[string][]string{
		"snapshots":        append([]string{"ops", "chunk", "snapshots", "--volume", "v1"}, filer...),
		"show":             append([]string{"ops", "chunk", "show", "--volume", "v1"}, filer...),
		"usage":            append([]string{"ops", "chunk", "usage", "--volume", "v1"}, filer...),
		"retain --dry-run": append([]string{"ops", "chunk", "retain", "--volume", "v1", "--dry-run"}, filer...),
		"gc (read-only)":   append([]string{"ops", "chunk", "gc", "--master", "m:1"}, filer...),
	}
	for name, args := range readOnly {
		before := checks
		var out, errb bytes.Buffer
		run(args, &out, &errb)
		if checks != before {
			t.Errorf("%s: a read-only command ran the start check", name)
		}
	}
}
