// Ownership: QA (from sketch-approved test spec v3-phase-15-t2-test-spec.md for T2 First Real Frontends).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t2-test-spec.md
// Maps to ledger rows: INV-FRONTEND-NVME-001, INV-FRONTEND-PROTOCOL-001,
//                      INV-FRONTEND-PROTOCOL-002.WRITE, INV-FRONTEND-PROTOCOL-002.READ
//
// Test layer: Component (L1)
// Protocol: NVMe/TCP
// Bounded fate (mirror of iSCSI L1):
//   - In-process NVMe/TCP target + StaticProvider(RecordingBackend)
//     serves real wire frames (TCP + capsules + R2T/H2CData/C2HData).
//     Write + Read round-trip the bytes.
//   - Swap to StaleRejectingBackend → fresh Connect's IO Write
//     and Read return ANA Transition (SCT=3 SC=3) at protocol
//     level.
package nvme_test

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// swappableProvider mirrors the iSCSI L1 helper: hands out one
// backend, lets the test swap to a stale-rejecting one mid-flight.
type swappableProvider struct {
	mu     sync.Mutex
	active frontend.Backend
}

func newSwappableProvider(initial frontend.Backend) *swappableProvider {
	return &swappableProvider{active: initial}
}

func (p *swappableProvider) Open(_ context.Context, _ string) (frontend.Backend, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active, nil
}

func (p *swappableProvider) swap(b frontend.Backend) {
	p.mu.Lock()
	p.active = b
	p.mu.Unlock()
}

// T2.L1.n1 — in-process NVMe target + RecordingBackend; Write
// then Read round-trip the payload bytes over the real wire.
func TestT2Route_NVMe_AttachWriteRead_Component(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	prov := testback.NewStaticProvider(rec)

	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndConnect(t, addr)
	defer cli.close()

	payload := make([]byte, nvme.DefaultBlockSize)
	copy(payload, []byte("l1-nvme-roundtrip"))

	status := cli.writeCmd(t, 0, 1, payload)
	expectStatusSuccess(t, status, "Write")

	if rec.WriteCount() != 1 {
		t.Fatalf("backend WriteCount=%d want 1", rec.WriteCount())
	}
	w := rec.WriteAt(0)
	if w.Offset != 0 || !bytes.Equal(w.Data, payload) {
		t.Fatalf("backend write mismatch")
	}

	status, data := cli.readCmd(t, 0, 1, int(nvme.DefaultBlockSize))
	expectStatusSuccess(t, status, "Read")
	if !bytes.Equal(data, payload) {
		t.Fatalf("Read data mismatch")
	}
}

// T2.L1.n2 — projection advance → stale Write returns ANA Transition.
func TestT2Route_NVMe_EndpointVersionMove_RejectsOldWrite(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	prov := newSwappableProvider(rec)

	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	// Pre-swap session: write succeeds.
	cli := dialAndConnect(t, addr)
	payload := make([]byte, nvme.DefaultBlockSize)
	status := cli.writeCmd(t, 0, 1, payload)
	expectStatusSuccess(t, status, "Write pre-swap")
	cli.close()

	// Swap to stale-rejecting and reconnect — same caveat as
	// iSCSI L1: the in-flight session captured the previous
	// backend; the swap takes effect on the next Open.
	prov.swap(testback.NewStaleRejectingBackend())

	cli2 := dialAndConnect(t, addr)
	defer cli2.close()
	status = cli2.writeCmd(t, 0, 1, payload)
	expectStatusANATransition(t, status, "Write post-swap")
}

// T2.L1.n3 — projection advance → stale Read returns ANA Transition,
// no payload bytes leak.
func TestT2Route_NVMe_EndpointVersionMove_RejectsOldRead(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	prov := newSwappableProvider(rec)

	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	// Populate the recording backend with a known pattern then
	// swap to stale before opening the test session.
	ctx := context.Background()
	known := make([]byte, nvme.DefaultBlockSize)
	copy(known, []byte("secret-bytes-must-not-leak"))
	if _, err := rec.Write(ctx, 0, known); err != nil {
		t.Fatalf("pre-populate: %v", err)
	}
	prov.swap(testback.NewStaleRejectingBackend())

	cli := dialAndConnect(t, addr)
	defer cli.close()
	status, data := cli.readCmd(t, 0, 1, int(nvme.DefaultBlockSize))
	expectStatusANATransition(t, status, "Read post-swap")
	// Critical: stale path MUST NOT return the known bytes.
	if bytes.Contains(data, []byte("secret-bytes-must-not-leak")) {
		t.Fatal("stale Read leaked the known payload bytes to initiator")
	}
}
