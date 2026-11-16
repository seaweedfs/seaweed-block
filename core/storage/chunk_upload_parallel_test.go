package storage

import (
	"context"
	"testing"
	"time"
)

func TestChunkUploads_DefaultOverlapsBeforeCAS(t *testing.T) {
	_, f, l, w := precopyLayer(t, 0, 3)
	f.mu.Lock()
	f.putGate = make(chan struct{})
	gate := f.putGate
	f.putEntered = make(chan string, 8)
	entered := f.putEntered
	f.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, err := l.Cut(context.Background()); done <- err }()
	defer func() {
		close(gate)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("only %d upload calls entered before release; default uploads must overlap", i)
		}
	}
	f.mu.Lock()
	if len(f.intents) == 0 || f.casCalls != 0 {
		t.Error("overlapping uploads require a durable intent and no root CAS")
	}
	f.mu.Unlock()
	_ = w
}
