package nvme_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type fuaBackend struct {
	*testback.RecordingBackend
	entered  chan struct{}
	release  chan struct{}
	syncErr  error
	writeErr error
	syncs    atomic.Int32
}

func (b *fuaBackend) Write(ctx context.Context, off int64, p []byte) (int, error) {
	if b.writeErr != nil {
		return 0, b.writeErr
	}
	return b.RecordingBackend.Write(ctx, off, p)
}
func (b *fuaBackend) Sync(ctx context.Context) error {
	b.syncs.Add(1)
	close(b.entered)
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.syncErr
}

// Exercise the real TCP capsule decoder, write dispatcher and completion writer.
func TestNVMeWriteFUACompletion(t *testing.T) {
	for _, tc := range []struct {
		name              string
		fua               bool
		syncErr, writeErr error
		wantSC            uint8
	}{
		{name: "FUA waits for Sync", fua: true},
		{name: "Sync failure is not success", fua: true, syncErr: errors.New("sync failed"), wantSC: nvme.SCInternalError},
		{name: "ordinary write does not Sync"},
		{name: "failed write does not Sync", fua: true, writeErr: errors.New("write failed"), wantSC: nvme.SCInternalError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &fuaBackend{RecordingBackend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}), entered: make(chan struct{}), release: make(chan struct{}), syncErr: tc.syncErr, writeErr: tc.writeErr}
			target := nvme.NewTarget(nvme.TargetConfig{Listen: "127.0.0.1:0", SubsysNQN: "nqn.2026-04.example.v3:subsys", VolumeID: "v1", Provider: testback.NewStaticProvider(b)})
			addr, err := target.Start()
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			defer close(b.release)
			cli := dialAndConnect(t, addr)
			defer cli.close()
			if err := cli.io.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			cmd := nvme.CapsuleCommand{OpCode: 1, CID: 77, NSID: 1}
			if tc.fua {
				cmd.D12 = 1 << 30
			}
			if err := cli.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
				t.Fatal(err)
			}
			ch, err := cli.ioR.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			if ch.Type != 0x9 {
				t.Fatalf("expected R2T, got %x", ch.Type)
			}
			var r2t nvme.R2THeader
			if err := cli.ioR.Receive(&r2t); err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, nvme.DefaultBlockSize)
			h2c := nvme.H2CDataHeader{CCCID: 77, TAG: r2t.TAG, DATAL: uint32(len(payload))}
			if err := cli.ioW.SendWithData(0x6, 0, &h2c, 16, payload); err != nil {
				t.Fatal(err)
			}
			type completion struct {
				resp nvme.CapsuleResponse
				err  error
			}
			completed := make(chan completion, 1)
			go func() {
				var c completion
				h, e := cli.ioR.Dequeue()
				c.err = e
				if e == nil {
					if h.Type != 0x5 {
						c.err = errors.New("not a completion")
					} else {
						c.err = cli.ioR.Receive(&c.resp)
					}
				}
				completed <- c
			}()
			if tc.fua && tc.writeErr == nil {
				select {
				case c := <-completed:
					t.Fatalf("FUA completed before backend Sync: status=%x error=%v", c.resp.Status, c.err)
				case <-b.entered:
				case <-time.After(2 * time.Second):
					t.Fatal("neither Sync nor completion")
				}
				if got := b.WriteCount(); got != 1 {
					t.Fatalf("Sync before Write: writes=%d", got)
				}
				select {
				case c := <-completed:
					t.Fatalf("completion while Sync blocked: %+v", c)
				case <-time.After(30 * time.Millisecond):
				}
				b.release <- struct{}{}
			}
			select {
			case c := <-completed:
				if c.err != nil {
					t.Fatal(c.err)
				}
				if c.resp.CID != 77 || c.resp.Status != nvme.MakeStatusField(nvme.SCTGeneric, tc.wantSC, tc.wantSC != 0) {
					t.Fatalf("completion=%+v want SC=%d", c.resp, tc.wantSC)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("no completion")
			}
			wantSync := int32(0)
			if tc.fua && tc.writeErr == nil {
				wantSync = 1
			}
			if n := b.syncs.Load(); n != wantSync {
				t.Fatalf("Sync count=%d want %d", n, wantSync)
			}
		})
	}
}
