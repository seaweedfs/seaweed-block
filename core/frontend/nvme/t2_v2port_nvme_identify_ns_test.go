// Ownership: QA (Batch 11 A-tier Phase 3 per
// sw-block/design/v3-phase-15-t2-batch-11-test-skeleton.md §A11.1).
// sw may NOT modify this file without architect approval via §8B.4
// Discovery Bridge.
//
// Maps to ledger rows:
//   PCDD-NVME-IDENTIFY-NS-NGUID-DETERMINISM-001
//
// Pins port plan R1+ invariants on NGUID/EUI-64 derivation from
// (SubsysNQN, VolumeID). sw's identify_test.go covers single-volume
// byte-shape + SWF00001-stub regression; this file covers the
// multi-dimensional determinism + collision invariants that prevent
// the Batch 10.5 Serial-default pattern from recurring in NVMe.
//
// Test layer: Unit (library-level; Target + NVMeClient in-process).

package nvme_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// targetForNVMe builds a Target + client pair bound to the given
// (SubsysNQN, VolumeID) pair. Mirrors sw's newIdentifyHarness shape
// but exposes both inputs for determinism/collision tests.
func targetForNVMe(t *testing.T, subsysNQN, volumeID string) (*nvme.Target, *nvmeClient) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: volumeID, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	prov := testback.NewStaticProvider(rec)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: subsysNQN,
		VolumeID:  volumeID,
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}
	cli := dialAndConnectOpts(t, addr, connectOptions{
		SubNQN:      subsysNQN,
		SkipIOQueue: true, // NGUID tests need admin only
	})
	t.Cleanup(func() {
		cli.close()
		_ = tg.Close()
	})
	return tg, cli
}

// identifyNS issues Identify Namespace CNS=0x00 NSID=1 and returns
// the 4096-byte payload. Fatals on any non-success or short payload.
func identifyNS(t *testing.T, cli *nvmeClient) []byte {
	t.Helper()
	status, data := cli.adminIdentify(t, 0x00 /* Namespace */, 1)
	if status != 0 {
		t.Fatalf("Identify Namespace non-success: status=0x%04x", status)
	}
	if len(data) != idNSPayloadSize {
		t.Fatalf("Identify Namespace len=%d want %d", len(data), idNSPayloadSize)
	}
	return data
}

// --- QA A11.1 — NGUID/EUI-64 derivation invariants (port plan R1+) ---

func TestT2V2Port_NVMe_IdentifyNS_NGUIDDerivationDeterministic(t *testing.T) {
	t.Run("SameSubsysSameVolumeProducesSameNGUID", func(t *testing.T) {
		_, c1 := targetForNVMe(t, "nqn.2026-04.m01:sub1", "v1")
		_, c2 := targetForNVMe(t, "nqn.2026-04.m01:sub1", "v1")
		n1 := extractNGUID(t, identifyNS(t, c1))
		n2 := extractNGUID(t, identifyNS(t, c2))
		if n1 != n2 {
			t.Fatalf("NGUID not stable across handlers for same (subsys, volume): %x vs %x (R1 determinism broken)", n1, n2)
		}
	})

	t.Run("DifferentVolumeIDsProduceDifferentNGUID", func(t *testing.T) {
		sub := "nqn.2026-04.m01:sub1"
		cases := []string{
			"v1", "v2", "vol-alpha", "vol-beta",
			"iqn.2026-04.example:ns1", "iqn.2026-04.example:ns2",
			// Adversarial similar pair — catches truncation bugs.
			"vol-aaaa", "vol-aaab",
		}
		seen := make(map[[16]byte]string, len(cases))
		for _, vid := range cases {
			_, cli := targetForNVMe(t, sub, vid)
			naa := extractNGUID(t, identifyNS(t, cli))
			if prior, dup := seen[naa]; dup {
				t.Fatalf("NGUID collision: volumes %q and %q both produce NGUID=%x (R1 collision guard broken)", prior, vid, naa)
			}
			seen[naa] = vid
		}
	})

	t.Run("DifferentSubsysNQNsProduceDifferentNGUID", func(t *testing.T) {
		// Same VolumeID across 3 subsystems — proves multi-tenant
		// isolation (e.g., same "v1" label in two customers'
		// subsystems must not collide at the NGUID level).
		volume := "v1"
		subsystems := []string{
			"nqn.2026-04.m01:sub1",
			"nqn.2026-04.m01:sub2",
			"nqn.2026-04.customer-alpha:tenant-9",
		}
		seen := make(map[[16]byte]string, len(subsystems))
		for _, sub := range subsystems {
			_, cli := targetForNVMe(t, sub, volume)
			naa := extractNGUID(t, identifyNS(t, cli))
			if prior, dup := seen[naa]; dup {
				t.Fatalf("NGUID collision across subsystems: %q and %q (same VolumeID %q) produce NGUID=%x (R1 cross-subsys isolation broken)",
					prior, sub, volume, naa)
			}
			seen[naa] = sub
		}
	})

	t.Run("EUI64IsNGUIDFirstEightBytes", func(t *testing.T) {
		_, cli := targetForNVMe(t, "nqn.2026-04.m01:sub1", "v1")
		data := identifyNS(t, cli)
		nguid := extractNGUID(t, data)
		eui64 := extractEUI64(t, data)

		// NVMe spec: when both NGUID and EUI-64 are non-zero,
		// EUI-64 must be the first 8 bytes of NGUID (consistency
		// rule; hosts should not need to disambiguate which ID
		// is authoritative).
		var wantEUI [8]byte
		copy(wantEUI[:], nguid[:8])
		if eui64 != wantEUI {
			t.Fatalf("EUI-64 mismatch: got %x, NGUID[:8]=%x (R1 consistency broken)", eui64, wantEUI)
		}
	})

	t.Run("EmptyVolumeIDStillProducesNonZeroNGUID", func(t *testing.T) {
		// Defense: empty VolumeID must not produce all-zero NGUID
		// (all-zero is the spec-reserved "not present" sentinel;
		// returning it would confuse udev / multipath).
		_, cli := targetForNVMe(t, "nqn.2026-04.m01:sub1", "")
		nguid := extractNGUID(t, identifyNS(t, cli))
		if isAllZero(nguid[:]) {
			t.Fatal("empty VolumeID produced all-zero NGUID (violates NVMe 'not present' sentinel rule)")
		}
	})

	t.Run("NSANAGRPIDIsZero", func(t *testing.T) {
		// Paired with A11.2 ANA-zero: per-namespace ANAGRPID MUST
		// be 0 when ANA is not implemented (port plan 11a/11b).
		// sw's TestT2Batch11a_IdentifyController_ANAFieldsAllZero
		// checks controller-level ANA fields; this is the namespace
		// counterpart.
		_, cli := targetForNVMe(t, "nqn.2026-04.m01:sub1", "v1")
		grpID := extractNSANAGRPID(t, identifyNS(t, cli))
		if grpID != 0 {
			t.Fatalf("NS ANAGRPID=%d; want 0 (ANA not implemented in 11a/11b)", grpID)
		}
	})
}
