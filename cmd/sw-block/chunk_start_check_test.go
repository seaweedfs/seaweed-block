package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
)

// Every ops chunk command that writes chunk metadata runs the
// single-filer start check before it reads or writes anything, and refuses
// when the filer is not the only one of its group; read-only commands (and
// read-only modes) do not run it.
func TestOpsChunk_WritingCommandsRunTheSingleFilerCheck(t *testing.T) {
	mem := chunkstore.NewMemFiler()
	prevDial, prevCheck := dialChunkFiler, checkSingleFiler
	t.Cleanup(func() { dialChunkFiler, checkSingleFiler = prevDial, prevCheck })
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) {
		return mem, func() error { return nil }, nil
	}
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

// The start check runs within the command's own --timeout: a check stalled
// on its filer or master (here one that blocks until its context ends) ends
// the command at the timeout, as a retryable refusal.
func TestOpsChunk_StartCheckIsBoundedByTheTimeout(t *testing.T) {
	mem := chunkstore.NewMemFiler()
	prevDial, prevCheck := dialChunkFiler, checkSingleFiler
	t.Cleanup(func() { dialChunkFiler, checkSingleFiler = prevDial, prevCheck })
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) {
		return mem, func() error { return nil }, nil
	}
	checkSingleFiler = func(ctx context.Context, _ chunkstore.Filer) error {
		<-ctx.Done()
		return ctx.Err()
	}
	filer := []string{"--chunk-filer", "filer:18888", "--timeout", "200ms"}
	for name, args := range map[string][]string{
		"snapshot-delete": append([]string{"ops", "chunk", "snapshot-delete", "--volume", "v1", "--name", "s1"}, filer...),
		"gc --apply":      append([]string{"ops", "chunk", "gc", "--master", "m:1", "--apply"}, filer...),
	} {
		done := make(chan struct{})
		var out, errb bytes.Buffer
		var code int
		go func() { code = run(args, &out, &errb); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: a stalled start check held the command past its 200ms --timeout", name)
		}
		if code == 0 || !strings.Contains(out.String(), "remote_unavailable") || !strings.Contains(out.String(), `"retryable":true`) {
			t.Errorf("%s: exit %d, out %q; want a retryable remote_unavailable refusal", name, code, out.String())
		}
	}
}

// A writing command also runs the chunk path storage-rule check on its
// --chunk-path, and a refused rule refuses the command.
func TestOpsChunk_WritingCommandsRunThePathRuleCheck(t *testing.T) {
	mem := chunkstore.NewMemFiler()
	prevDial, prevCheck, prevRule := dialChunkFiler, checkSingleFiler, checkPathRule
	t.Cleanup(func() { dialChunkFiler, checkSingleFiler, checkPathRule = prevDial, prevCheck, prevRule })
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) {
		return mem, func() error { return nil }, nil
	}
	checkSingleFiler = func(context.Context, chunkstore.Filer) error { return nil }
	var bases []string
	checkPathRule = func(_ context.Context, _ chunkstore.Filer, base string) error {
		bases = append(bases, base)
		return chunkstore.ErrPathRule
	}
	place := []string{"--chunk-filer", "filer:18888", "--chunk-path", "/rule/blocks"}
	for name, args := range map[string][]string{
		"snapshot-delete": {"ops", "chunk", "snapshot-delete", "--volume", "v1", "--name", "s1"},
		"fork":            {"ops", "chunk", "fork", "--source", "v1", "--snapshot", "s1", "--target", "v2", "--request-id", "r"},
		"retain":          {"ops", "chunk", "retain", "--volume", "v1"},
		"volume-delete":   {"ops", "chunk", "volume-delete", "--volume", "v1", "--master", "m:1", "--request-id", "r"},
		"restore":         {"ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", "v3", "--request-id", "r"},
		"gc --apply":      {"ops", "chunk", "gc", "--master", "m:1", "--apply"},
	} {
		before := len(bases)
		var out, errb bytes.Buffer
		code := run(append(args, place...), &out, &errb)
		if len(bases) != before+1 || bases[before] != "/rule/blocks" {
			t.Errorf("%s: rule check calls %v; want one on /rule/blocks", name, bases[before:])
		}
		if code == 0 {
			t.Errorf("%s: a refused rule did not refuse the command: %s", name, out.String())
		}
	}
}
