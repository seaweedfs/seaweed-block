package nvme

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type readyReadBackend struct {
	frontend.Backend
	err error
}

func (b *readyReadBackend) Read(context.Context, int64, []byte) (int, error) { return 0, b.err }

func TestSessionReadNotReadyIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		sct, sc uint8
		dnr     bool
	}{
		{"attach-pending", fmt.Errorf("awaiting HEAD attach: %w", frontend.ErrNotReady), SCTGeneric, SCNamespaceNotReady, false},
		{"not-ready-closed", errors.Join(frontend.ErrNotReady, frontend.ErrBackendClosed), SCTPathRelated, SCPathAsymAccessInaccessible, false},
		{"not-ready-stale", errors.Join(frontend.ErrNotReady, frontend.ErrStalePrimary), SCTPathRelated, SCPathAsymAccessTransition, false},
		{"physical-fault", errors.New("physical read failure"), SCTGeneric, SCInternalError, true},
		{"closed", frontend.ErrBackendClosed, SCTPathRelated, SCPathAsymAccessInaccessible, false},
		{"stale", frontend.ErrStalePrimary, SCTPathRelated, SCPathAsymAccessTransition, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &readyReadBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"}), err: tc.err}
			s := &Session{handler: NewIOHandler(HandlerConfig{Backend: backend}), respCh: make(chan *response, 1), done: make(chan struct{})}
			req := &Request{}
			req.capsule.OpCode = ioRead
			req.capsule.NSID = 1
			s.handleRead(context.Background(), req)
			resp := <-s.respCh
			if want := MakeStatusField(tc.sct, tc.sc, tc.dnr); resp.resp.Status != want {
				t.Fatalf("wire status=%#x want=%#x", resp.resp.Status, want)
			}
		})
	}
}

func TestReadinessRetryDelayIsNotAdvertised(t *testing.T) {
	s := &Session{target: &Target{cfg: TargetConfig{VolumeID: "v1"}}, handler: &IOHandler{}, ctrl: &adminController{}, respCh: make(chan *response, 1), done: make(chan struct{})}
	controller := s.buildIdentifyController()
	for offset := 128; offset < 134; offset++ {
		if controller[offset] != 0 {
			t.Fatalf("CRDT byte%d=%d; host-policy control assumes no retry delay", offset, controller[offset])
		}
	}
	req := &Request{}
	req.capsule.D10 = 0x16 // Host Behavior Support, including ACRE.
	if err := s.handleSetFeatures(req); err != nil {
		t.Fatal(err)
	}
	if got := (<-s.respCh).resp.Status; got != MakeStatusField(SCTGeneric, SCInvalidField, true) {
		t.Fatalf("unsupported Host Behavior status=%#x", got)
	}
}
