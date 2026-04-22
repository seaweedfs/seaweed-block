// Ownership: sw BUG-003 fix test — pins that admin Connect CDW12
// KATO value is plumbed into the controller and round-trips via
// Get Features 0x0F.
//
// V2 reference: weed/storage/blockvol/nvme/fabric.go:41 extracts
// KATO via connectKATO() at Connect time. V3 previously relied
// solely on Set Features 0x0F, which typical Linux hosts do not
// call — so KATO was effectively 0 on target side.
//
// Per QA constraint #2 (11b testing directive): KATO is stored
// only, no timer armed. This test verifies storage + round-trip,
// NOT timer enforcement.
package nvme_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// TestT2V2Port_NVMe_Connect_KATO_CDW12_Persisted pins the BUG-003
// fix: Connect CDW12 value must be stored in the controller and
// returned by Get Features 0x0F.
func TestT2V2Port_NVMe_Connect_KATO_CDW12_Persisted(t *testing.T) {
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

	const wantKATO uint32 = 12000 // 12s in V2/V3 store units (ms)
	cli := dialAndConnectOpts(t, addr, connectOptions{
		SkipIOQueue: true,
		KATO:        wantKATO,
	})
	defer cli.close()

	// Issue Get Features 0x0F. Target must return the same value
	// the host wrote in Connect CDW12.
	status, got := cli.adminGetFeatures(t, 0x0F, 0)
	expectStatusSuccess(t, status, "GetFeatures(KATO)")
	if got != wantKATO {
		t.Errorf("Get Features KATO=%d want %d — Connect CDW12 not plumbed (BUG-003)",
			got, wantKATO)
	}
}

// TestT2V2Port_NVMe_Connect_KATO_Zero_Accepted pins that KATO=0
// in Connect CDW12 is acceptable (= no keep-alive timeout) and
// round-trips as 0. Some hosts don't set KATO at Connect time
// and rely on later Set Features; this test ensures we don't
// reject the zero value.
func TestT2V2Port_NVMe_Connect_KATO_Zero_Accepted(t *testing.T) {
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

	cli := dialAndConnectOpts(t, addr, connectOptions{
		SkipIOQueue: true,
		KATO:        0,
	})
	defer cli.close()

	status, got := cli.adminGetFeatures(t, 0x0F, 0)
	expectStatusSuccess(t, status, "GetFeatures(KATO)")
	if got != 0 {
		t.Errorf("Get Features KATO=%d want 0 (Connect CDW12 was 0)", got)
	}
}

// TestT2V2Port_NVMe_Connect_KATO_OverriddenBySetFeatures pins
// precedence: if the host calls Connect with KATO=X and later
// Set Features 0x0F with Y, Get Features returns Y (last-write
// wins). V2 has the same precedence (fabric.go stores kato at
// Connect; admin.go Set Features 0x0F overwrites).
func TestT2V2Port_NVMe_Connect_KATO_OverriddenBySetFeatures(t *testing.T) {
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

	const initialKATO uint32 = 5000
	const newKATO uint32 = 30000
	cli := dialAndConnectOpts(t, addr, connectOptions{
		SkipIOQueue: true,
		KATO:        initialKATO,
	})
	defer cli.close()

	// Confirm Connect-time value stored.
	_, got := cli.adminGetFeatures(t, 0x0F, 0)
	if got != initialKATO {
		t.Fatalf("initial KATO=%d want %d", got, initialKATO)
	}

	// Set Features overwrite.
	if s, _ := cli.adminSetFeatures(t, 0x0F, newKATO); s != 0 {
		t.Fatalf("SetFeatures(KATO) status=0x%04x", s)
	}

	// Get Features returns the new value.
	_, got = cli.adminGetFeatures(t, 0x0F, 0)
	if got != newKATO {
		t.Errorf("after SetFeatures, KATO=%d want %d", got, newKATO)
	}
}
