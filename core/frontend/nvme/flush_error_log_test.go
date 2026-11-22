package nvme

import (
	"bytes"
	"context"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"log"
	"strings"
	"syscall"
	"testing"
)

type flushErrorBackend struct {
	frontend.Backend
	syncErr error
}

func (b *flushErrorBackend) Sync(context.Context) error { return b.syncErr }

func TestSessionFlushLogsBackendReason(t *testing.T) {
	var logs bytes.Buffer
	b := &flushErrorBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"}), syncErr: fmt.Errorf("injected Sync failure: extent EIO: %w", syscall.EIO)}
	s := &Session{handler: NewIOHandler(HandlerConfig{Backend: b}), logger: log.New(&logs, "", 0), qid: 7, respCh: make(chan *response, 1), done: make(chan struct{})}
	req := &Request{}
	req.capsule.OpCode = ioFlush
	req.capsule.NSID = 1
	s.dispatchIO(context.Background(), req)
	resp := <-s.respCh
	if resp.resp.Status != MakeStatusField(SCTGeneric, SCInternalError, true) {
		t.Fatalf("status=%#x", resp.resp.Status)
	}
	for _, want := range []string{"Flush failed", "qid=7", "nsid=1", "backend error on flush: injected Sync failure: extent EIO"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q missing %q", logs.String(), want)
		}
	}
	if strings.Count(logs.String(), "Flush failed") != 1 {
		t.Fatalf("duplicate failure log: %q", logs.String())
	}
	logs.Reset()
	b.syncErr = nil
	s.dispatchIO(context.Background(), &Request{capsule: req.capsule})
	resp = <-s.respCh
	if resp.resp.Status != 0 {
		t.Fatalf("success status=%#x", resp.resp.Status)
	}
	if logs.Len() != 0 {
		t.Fatalf("successful Flush logged a failure: %q", logs.String())
	}
}
