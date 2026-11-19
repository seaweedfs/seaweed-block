package storage

import (
	"encoding/json"
	"testing"
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
