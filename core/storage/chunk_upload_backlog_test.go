package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestChunkStatsUploadBacklogDuringCut(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 0, 0x41)
	entered, release := holdPuts(t, f)
	done := startCapture(t, layer)
	waitKey(t, entered, "upload")
	// The exported stats schema is the read-side diagnostic seam.
	b, err := json.Marshal(layer.Stats())
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	if err = json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	if st["UploadPending"] != float64(1) {
		release()
		t.Fatalf("held upload missing from backlog: %s", b)
	}
	release()
	if err := waitErr(t, done, "cut"); err != nil {
		t.Fatal(err)
	}
	b, _ = json.Marshal(layer.Stats())
	_ = json.Unmarshal(b, &st)
	if st["UploadPending"] != float64(0) {
		t.Fatalf("settled upload remains queued: %s", b)
	}
}

func TestChunkStatsCancelledUploadBacklogSettlesAfterJoin(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 0, 0x41)
	entered, release := holdPuts(t, f)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	// Cleanup was installed before the worker: always cancel and join it,
	// including a failure to enter the held remote.
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-finished:
		case <-time.After(ctLocalBound):
			t.Error("cancelled cut not joined")
		}
	})
	go func() { defer close(finished); _, err := layer.Cut(ctx); done <- err }()
	waitKey(t, entered, "upload")
	cancel()
	if err := waitErr(t, done, "cancelled cut"); err == nil {
		t.Fatal("cancelled cut succeeded")
	}
	b, _ := json.Marshal(layer.Stats())
	var st map[string]any
	_ = json.Unmarshal(b, &st)
	if st["UploadPending"] != float64(0) {
		t.Fatalf("cancelled workers remain queued: %s", b)
	}
	if f.stats().cas != 0 {
		t.Fatal("cancelled upload published HEAD")
	}
}
