package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// Every ops chunk command that writes chunk metadata runs the
// single-filer start check before it reads or writes anything, and refuses
// when the filer is not the only one of its group; read-only commands (and
// read-only modes) do not run it.
func TestOpsChunk_WritingCommandsRunTheSingleFilerCheck(t *testing.T) {
	mem := newMemChunkStore()
	prevDial, prevCheck := dialChunkFiler, checkSingleFiler
	t.Cleanup(func() { dialChunkFiler, checkSingleFiler = prevDial, prevCheck })
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	prevResolve := resolveVolumeID
	t.Cleanup(func() { resolveVolumeID = prevResolve })
	resolveVolumeID = func(context.Context, string, string) (layout.VolumeID, error) { return layout.VolumeID{1}, nil }
	id := []string{"--master", "m:1"}
	src := []string{"--source", "v1", "--master", "m:1"}
	tgt := []string{"--target", "v2"}
	checks := 0
	checkSingleFiler = func(context.Context, chunkstore.Filer) error {
		checks++
		return chunkstore.ErrNotSingleFiler
	}
	filer := []string{"--chunk-filer", "filer:18888"}
	writing := map[string][]string{
		"snapshot-delete": append(append([]string{"ops", "chunk", "snapshot-delete", "--volume", "v1", "--name", "s1"}, id...), filer...),
		"release":         append(append(append([]string{"ops", "chunk", "release"}, src...), tgt...), filer...),
		"retain":          append(append([]string{"ops", "chunk", "retain", "--volume", "v1"}, id...), filer...),
		"volume-delete":   append(append([]string{"ops", "chunk", "volume-delete", "--volume", "v1", "--master", "m:1", "--request-id", "r"}, id...), filer...),
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
		"snapshots":        append(append([]string{"ops", "chunk", "snapshots", "--volume", "v1"}, id...), filer...),
		"show":             append(append([]string{"ops", "chunk", "show", "--volume", "v1"}, id...), filer...),
		"usage":            append(append([]string{"ops", "chunk", "usage", "--volume", "v1"}, id...), filer...),
		"retain --dry-run": append(append([]string{"ops", "chunk", "retain", "--volume", "v1", "--dry-run"}, id...), filer...),
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

// Restore has one master authority call. The master owns the filer check
// and target creation; the CLI must never write the filer independently.
func TestOpsChunkRestoreUsesOnlyMasterCreate(t *testing.T) {
	prevDial, prevRestore := dialChunkFiler, restoreCreateRPC
	t.Cleanup(func() { dialChunkFiler, restoreCreateRPC = prevDial, prevRestore })
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) {
		t.Fatal("restore CLI contacted the filer")
		return nil, nil, nil
	}
	calls := 0
	var firstID string
	restoreCreateRPC = func(_ context.Context, master string, req *control.CreateVolumeRequest) (*control.CreateVolumeResponse, error) {
		calls++
		if master != "m:1" || req.GetVolumeId() != "v2" || req.GetSourceVolume() != "v1" || req.GetSourceSnapshot() != "s1" || req.GetCreateRequestId() == "" {
			t.Fatalf("restore RPC: master=%q req=%+v", master, req)
		}
		if firstID == "" {
			firstID = req.GetCreateRequestId()
		}
		if req.GetCreateRequestId() != firstID {
			t.Fatal("retry changed request id")
		}
		return &control.CreateVolumeResponse{VolumeId: "v2", LayoutId: (layout.VolumeID{1}).String(), StoreReady: true}, nil
	}
	args := []string{"ops", "chunk", "restore", "--master", "m:1", "--source", "v1", "--target", "v2", "--snapshot", "s1"}
	for i := 0; i < 2; i++ {
		var out, errs bytes.Buffer
		if code := run(args, &out, &errs); code != 0 {
			t.Fatalf("restore replay: code=%d out=%q err=%q", code, out.String(), errs.String())
		}
	}
	if calls != 2 {
		t.Fatalf("restore made %d RPCs; want one per invocation", calls)
	}
}

// The start check runs within the command's own --timeout: a check stalled
// on its filer or master (here one that blocks until its context ends) ends
// the command at the timeout, as a retryable refusal.
func TestOpsChunk_StartCheckIsBoundedByTheTimeout(t *testing.T) {
	mem := chunkstore.NewMemFiler()
	prevDial, prevCheck, prevResolve := dialChunkFiler, checkSingleFiler, resolveVolumeID
	t.Cleanup(func() { dialChunkFiler, checkSingleFiler, resolveVolumeID = prevDial, prevCheck, prevResolve })
	resolveVolumeID = func(context.Context, string, string) (layout.VolumeID, error) { return layout.VolumeID{1}, nil }
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	checkSingleFiler = func(ctx context.Context, _ chunkstore.Filer) error {
		<-ctx.Done()
		return ctx.Err()
	}
	filer := []string{"--chunk-filer", "filer:18888", "--timeout", "200ms"}
	for name, args := range map[string][]string{
		"snapshot-delete": append([]string{"ops", "chunk", "snapshot-delete", "--volume", "v1", "--master", "m:1", "--name", "s1"}, filer...),
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
