package nvmerdma

import "testing"

func TestValidateSubsystemNQN(t *testing.T) {
	for _, nqn := range []string{
		"",
		".",
		"..",
		"../escape",
		`..\escape`,
		"parent/child",
	} {
		if err := validateSubsystemNQN(nqn); err == nil {
			t.Fatalf("validateSubsystemNQN(%q) succeeded", nqn)
		}
	}
	if err := validateSubsystemNQN("nqn.2026-07.io.seaweedfs:volume-1"); err != nil {
		t.Fatalf("valid NQN rejected: %v", err)
	}
}
