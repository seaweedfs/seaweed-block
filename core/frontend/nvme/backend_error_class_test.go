package nvme

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"syscall"
	"testing"
	"time"
)

type retryClassBackend struct {
	frontend.Backend
	err error
}

func (b retryClassBackend) Read(context.Context, int64, []byte) (int, error)  { return 0, b.err }
func (b retryClassBackend) Write(context.Context, int64, []byte) (int, error) { return 0, b.err }
func (b retryClassBackend) Sync(context.Context) error                        { return b.err }

// INV-FE-ERROR-RETRY-CLASS-001: validate the completion encoding, not only the handler's status code.
func TestBackendRetryClassClearsWireDNR(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		dnr  bool
	}{
		{"not-ready", frontend.ErrNotReady, false},
		{"unknown", errors.New("unrecognized backend cause"), true},
		{"busy", frontend.ErrBackendBusy, false},
		// Path statuses let multipath fail over: never DNR.
		{"stale-ana-transition", frontend.ErrStalePrimary, false},
		{"closed-ana-inaccessible", frontend.ErrBackendClosed, false},
		{"stale-with-media-error", errors.Join(frontend.ErrStalePrimary, syscall.EIO), true},
		{"permanent", syscall.EIO, true},
		{"deadline-busy", errors.Join(context.DeadlineExceeded, frontend.ErrBackendBusy), false},
		{"deadline-not-ready", errors.Join(context.DeadlineExceeded, frontend.ErrNotReady), false},
		{"deadline-busy-physical", errors.Join(context.DeadlineExceeded, frontend.ErrBackendBusy, syscall.EIO), true},
		{"deadline-not-ready-physical", errors.Join(context.DeadlineExceeded, frontend.ErrNotReady, syscall.EIO), true},
	} {
		for _, op := range []uint8{ioRead, ioWrite, ioFlush} {
			t.Run(tc.name+string(rune('0'+op)), func(t *testing.T) {
				h := NewIOHandler(HandlerConfig{Backend: retryClassBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v"}), err: tc.err}})
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				result := h.Handle(ctx, IOCommand{Opcode: op, NSID: 1, NLB: 1, Data: make([]byte, 512)})
				if dnr := result.completionStatus()&(1<<15) != 0; dnr != tc.dnr {
					t.Fatalf("DNR=%t want %t, result=%+v", dnr, tc.dnr, result)
				}
			})
		}
	}
}

type durabilityClassBackend struct {
	frontend.Backend
	err           error
	writes, syncs int
}

func (b *durabilityClassBackend) Write(_ context.Context, _ int64, p []byte) (int, error) {
	b.writes++
	return len(p), nil
}
func (b *durabilityClassBackend) Sync(context.Context) error {
	b.syncs++
	return b.err
}

// INV-FE-ERROR-RETRY-CLASS-001: a failed durability
// operation's latched permanent cause wins even beneath a deliberate busy marker.
func TestSyncAndFUALatchedFaultOverridesBusy(t *testing.T) {
	for _, fua := range []bool{false, true} {
		for _, fault := range []struct {
			name string
			err  error
		}{
			{name: "no-latch"},
			{name: "fsync-sentinel", err: storage.ErrFsyncFailed},
			{name: "arbitrary-substrate-latch", err: storage.NewSubstrateIOFailure(errors.New("map persist failed"), "store")},
			{name: "arbitrary-integrity-latch", err: storage.NewWALIntegrityFailure(errors.New("descriptor corrupt"), "store")},
			{name: "counter-exhausted", err: storage.ErrCounterExhausted},
		} {
			t.Run(fmt.Sprintf("fua=%t/%s", fua, fault.name), func(t *testing.T) {
				cause := errors.Join(frontend.ErrBackendBusy, errors.New("untyped durability cause"))
				if fault.err != nil {
					cause = errors.Join(cause, fault.err)
				}
				b := &durabilityClassBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v"}), err: cause}
				cmd := IOCommand{Opcode: ioFlush, NSID: 1}
				if fua {
					cmd = IOCommand{Opcode: ioWrite, NSID: 1, NLB: 1, Data: make([]byte, 512), FUA: true}
				}
				got := NewIOHandler(HandlerConfig{Backend: b}).Handle(context.Background(), cmd)
				want := MakeStatusField(SCTGeneric, SCNamespaceNotReady, false)
				if fault.err != nil {
					want = MakeStatusField(SCTGeneric, SCInternalError, true)
				}
				if got.completionStatus() != want {
					t.Fatalf("completion=%#x want %#x", got.completionStatus(), want)
				}
				writes := 0
				if fua {
					writes = 1
				}
				if b.writes != writes || b.syncs != 1 {
					t.Fatalf("durability seam not reached once: writes=%d syncs=%d", b.writes, b.syncs)
				}
			})
		}
	}
}

type nonOptimizedANA struct{}

func (nonOptimizedANA) ANAState() ANAState     { return ANANonOptimized }
func (nonOptimizedANA) ANAGroupID() uint32     { return 1 }
func (nonOptimizedANA) ANAChangeCount() uint64 { return 1 }

// A metadata-only (non-optimized) path refuses data commands as ANA
// inaccessible. Without DNR, so the host fails them over to the optimized
// path instead of failing them.
func TestMetadataOnlyPathRefusalClearsWireDNR(t *testing.T) {
	h := NewIOHandler(HandlerConfig{
		Backend:      testback.NewRecordingBackend(frontend.Identity{VolumeID: "v"}),
		ANA:          nonOptimizedANA{},
		MetadataOnly: true,
	})
	for _, op := range []uint8{ioRead, ioWrite, ioFlush} {
		result := h.Handle(context.Background(), IOCommand{Opcode: op, NSID: 1, NLB: 1, Data: make([]byte, 512)})
		if result.SCT != SCTPathRelated || result.SC != SCPathAsymAccessInaccessible {
			t.Fatalf("op %d: status (%d,0x%02x), want path/inaccessible", op, result.SCT, result.SC)
		}
		if result.completionStatus()&(1<<15) != 0 {
			t.Fatalf("op %d: a path status went out with DNR set", op)
		}
	}
}
