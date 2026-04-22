// Ownership: sw unit tests for Batch 11b Async Event Request
// (AER) parking — QA constraint #1.
//
// The serial session loop cannot block; AER handler MUST return
// without emitting a CapsuleResp for the first AER (single-slot
// parking). A second AER while the slot is parked returns
// AsyncEventRequestLimitExceeded. T2 never produces an event,
// so parked AERs never complete; they're released on admin
// session close.
package nvme_test

import (
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func newAERHarness(t *testing.T) (*nvme.Target, *nvmeClient) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
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
		t.Fatalf("Start: %v", err)
	}
	cli := dialAndConnectOpts(t, addr, connectOptions{SkipIOQueue: true})
	return tg, cli
}

// TestT2Batch11b_AER_Parks verifies the first AER does NOT
// produce a CapsuleResp — the session loop keeps reading.
// We prove this by sending an AER then a KeepAlive and observing
// that the next CapsuleResp carries the KeepAlive's CID (not
// the AER's). If AER produced a response, we'd see its CID first.
func TestT2Batch11b_AER_Parks(t *testing.T) {
	tg, cli := newAERHarness(t)
	defer tg.Close()
	defer cli.close()

	aerCID := cli.adminAER(t)
	// Set a short read deadline so we don't hang the test if
	// the server (incorrectly) sent a response.
	_ = cli.admin.(*net.TCPConn).SetReadDeadline(time.Now().Add(500 * time.Millisecond))

	// Follow with KeepAlive — this should succeed. If AER had
	// emitted a CapsuleResp, recvCapsuleResp below would see
	// the AER's CID first and we'd CID-mismatch.
	_ = cli.adminKeepAlive(t)

	// Reset deadline for the session's normal lifecycle.
	_ = cli.admin.(*net.TCPConn).SetReadDeadline(time.Time{})
	_ = aerCID // not observed in wire — the whole point
}

// TestT2Batch11b_AER_LimitExceeded pins the single-slot rule:
// the second AER while one is parked MUST return a status
// error. Server MUST NOT block or queue.
func TestT2Batch11b_AER_LimitExceeded(t *testing.T) {
	tg, cli := newAERHarness(t)
	defer tg.Close()
	defer cli.close()

	// First AER: parks without response.
	_ = cli.adminAER(t)
	_ = cli.admin.(*net.TCPConn).SetReadDeadline(time.Now().Add(2 * time.Second))

	// Second AER: must receive a CapsuleResp with non-zero status.
	status := cli.adminAERExpectResponse(t)
	if status == 0 {
		t.Error("second AER succeeded — must return AsyncEventRequestLimitExceeded")
	}

	// Verify session still alive: KeepAlive still succeeds.
	_ = cli.admin.(*net.TCPConn).SetReadDeadline(time.Time{})
	if s := cli.adminKeepAlive(t); s != 0 {
		t.Errorf("KeepAlive after AER limit status=0x%04x — session should stay alive", s)
	}
}

// TestT2Batch11b_AER_SessionCloseReleasesSlot indirectly checks
// that admin-session close clears the parked AER so a second
// dial can park again. Each session is independent so this
// reduces to "two sessions in sequence each park successfully
// with no leak" — if clearPendingAER didn't run, a second
// session would still park on a fresh controller (different
// slot) and the test would pass vacuously. The real leak it
// guards against is Target-wide controller retention; the
// cleanup path goes through releaseAdminController. We assert
// the two sessions each see their own fresh parking slot.
func TestT2Batch11b_AER_TwoSessionsBothPark(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
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
		t.Fatalf("Start: %v", err)
	}
	defer tg.Close()

	// Session 1: park + close.
	cli1 := dialAndConnectOpts(t, addr, connectOptions{SkipIOQueue: true})
	_ = cli1.adminAER(t)
	// Second AER on session 1 must fail (slot full).
	_ = cli1.admin.(*net.TCPConn).SetReadDeadline(time.Now().Add(2 * time.Second))
	if s := cli1.adminAERExpectResponse(t); s == 0 {
		t.Fatal("session1: second AER should fail (slot full)")
	}
	cli1.close()

	// Session 2: new controller, fresh slot.
	cli2 := dialAndConnectOpts(t, addr, connectOptions{SkipIOQueue: true})
	defer cli2.close()
	_ = cli2.adminAER(t) // should park (new controller)
	// KeepAlive still works.
	_ = cli2.admin.(*net.TCPConn).SetReadDeadline(time.Time{})
	if s := cli2.adminKeepAlive(t); s != 0 {
		t.Errorf("session2 KeepAlive status=0x%04x — fresh session should be healthy", s)
	}
}
