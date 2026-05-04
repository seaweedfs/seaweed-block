// Ownership: sw — L2-A sign-prep YAML-scripted smoke for T2B-NVMe-
// product-ready. Reuses the existing L2 harness (real blockmaster
// + blockvolume subprocesses) and drives it with a declarative
// scenario from testdata/t2-nvme-smoke.yaml.
//
// Value over hand-written L2 tests:
//   - Scenario is reviewable YAML; test code is just a replay engine
//   - New scenarios can be added without Go code changes
//   - Read-after-write byte-level integrity is asserted per step
//
// Scope:
//   - Real blockmaster + blockvolume subprocess pair (via
//     startL2NvmeHarness)
//   - Go initiator runs the scripted steps through the in-package
//     nvmeClient test helpers (dialAndConnect / writeCmd / readCmd)
//   - YAML ops: write (seed-derived payload), read (asserts
//     seed-derived expected bytes)

package nvme_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// smokeScenario mirrors the YAML file shape.
type smokeScenario struct {
	Name        string      `yaml:"name"`
	SubsysNQN   string      `yaml:"subsys_nqn"`
	Description string      `yaml:"description"`
	Steps       []smokeStep `yaml:"steps"`
}

// smokeStep is a single op. Only one of write / read fields is
// meaningful per step (selected by Op).
type smokeStep struct {
	Op         string `yaml:"op"` // "write" | "read"
	SLBA       uint64 `yaml:"slba"`
	NLB        uint16 `yaml:"nlb"`
	Seed       int    `yaml:"seed"`
	ExpectSeed int    `yaml:"expect_seed"`
	Note       string `yaml:"note"`
}

// makeSeedPayload deterministically builds a payload from seed.
// Matches what a corresponding read step will expect.
func makeSeedPayload(seed int, bytes int) []byte {
	p := make([]byte, bytes)
	for i := range p {
		p[i] = byte((seed + i) & 0xFF)
	}
	return p
}

// TestT2A_L2_NvmeSmokeReplay reads the scenario YAML, spins up the
// real L2 subprocess harness, and replays each step as an NVMe/TCP
// operation. Each read is byte-verified against seed-derived data.
func TestT2A_L2_NvmeSmokeReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; skipped under -short")
	}

	scenarioPath := filepath.Join("testdata", "t2-nvme-smoke.yaml")
	raw, err := os.ReadFile(scenarioPath)
	if err != nil {
		t.Fatalf("read scenario: %v", err)
	}
	var scenario smokeScenario
	if err := yaml.Unmarshal(raw, &scenario); err != nil {
		t.Fatalf("parse scenario: %v", err)
	}
	if len(scenario.Steps) == 0 {
		t.Fatalf("scenario has no steps (file: %s)", scenarioPath)
	}

	t.Logf("Scenario: %s (%d steps)", scenario.Name, len(scenario.Steps))

	h := startL2NvmeHarness(t)
	defer h.Close()

	cli := dialAndConnect(t, h.nvmeAddr)
	defer cli.close()

	blockSize := 512 // advertised LBADS=9
	for i, step := range scenario.Steps {
		i, step := i, step
		// Subtest per step for localized failure reporting.
		t.Run(fmt.Sprintf("step_%02d_%s_slba%d_nlb%d", i, step.Op, step.SLBA, step.NLB), func(t *testing.T) {
			totalBytes := int(step.NLB) * blockSize
			if step.Note != "" {
				t.Logf("step: %s", step.Note)
			}
			switch step.Op {
			case "write":
				payload := makeSeedPayload(step.Seed, totalBytes)
				status := cli.writeCmd(t, step.SLBA, step.NLB, payload)
				if status != 0 {
					t.Fatalf("Write status=0x%04x", status)
				}
			case "read":
				status, data := cli.readCmd(t, step.SLBA, step.NLB, totalBytes)
				if status != 0 {
					t.Fatalf("Read status=0x%04x", status)
				}
				if len(data) != totalBytes {
					t.Fatalf("Read returned %d bytes, want %d", len(data), totalBytes)
				}
				expected := makeSeedPayload(step.ExpectSeed, totalBytes)
				if !bytesEqual(data, expected) {
					t.Fatalf("Read data mismatch at slba=%d nlb=%d seed=%d: first diff at byte %d (got 0x%02x want 0x%02x)",
						step.SLBA, step.NLB, step.ExpectSeed,
						firstDiff(data, expected),
						data[firstDiff(data, expected)],
						expected[firstDiff(data, expected)])
				}
			default:
				t.Fatalf("unknown op %q", step.Op)
			}
		})
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
