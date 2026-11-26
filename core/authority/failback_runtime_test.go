package authority

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

func TestFailbackAuthorityRuntime_ReassignsThroughPublisher(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID:  "v1",
		ReplicaID: "r2",
		DataAddr:  "data-r2",
		CtrlAddr:  "ctrl-r2",
		Intent:    IntentBind,
	}); err != nil {
		t.Fatalf("bind current: %v", err)
	}
	result, err := (FailbackAuthorityRuntime{Publisher: pub, Gate: allowGate, TargetEndpoint: r1Endpoint}).ExecuteFailback(context.Background(), validAuthorityFailbackRequest())
	if err != nil {
		t.Fatalf("execute failback: %v", err)
	}
	if !result.FailbackStarted ||
		!result.AuthorityEpochAdvanced ||
		!result.SinglePrimaryAfterFailback ||
		!result.PublishTargetSwappedAfterFailback ||
		!result.NoStorageMutation ||
		!result.NoCrossVolumeIdentityChange {
		t.Fatalf("result=%+v", result)
	}
	line, ok := pub.VolumeAuthorityLine("v1")
	if !ok {
		t.Fatal("missing authority line")
	}
	if line.ReplicaID != "r1" || line.Epoch != 2 || line.EndpointVersion != 1 ||
		line.DataAddr != "data-r1" || line.CtrlAddr != "ctrl-r1" {
		t.Fatalf("line=%+v", line)
	}
}

func TestFailbackAuthorityRuntime_RejectsStaleExpectedCurrent(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r2", DataAddr: "data-r2", CtrlAddr: "ctrl-r2", Intent: IntentBind}); err != nil {
		t.Fatalf("bind current: %v", err)
	}
	req := validAuthorityFailbackRequest()
	req.ExpectedCurrentEpoch = 99
	_, err := (FailbackAuthorityRuntime{Publisher: pub, Gate: allowGate, TargetEndpoint: r1Endpoint}).ExecuteFailback(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "stale expected current line") {
		t.Fatalf("err=%v", err)
	}
	line, _ := pub.VolumeAuthorityLine("v1")
	if line.ReplicaID != "r2" || line.Epoch != 1 {
		t.Fatalf("stale request must not move authority: %+v", line)
	}
}

func TestFailbackAuthorityRuntime_RequiresTerminalPreconditions(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	req := validAuthorityFailbackRequest()
	req.AckEligible = false
	_, err := (FailbackAuthorityRuntime{Publisher: pub, Gate: allowGate, TargetEndpoint: r1Endpoint}).ExecuteFailback(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "terminal failback preconditions") {
		t.Fatalf("err=%v", err)
	}
	if _, ok := pub.VolumeAuthorityLine("v1"); ok {
		t.Fatal("precondition failure must not mint authority")
	}
}

func validAuthorityFailbackRequest() FailbackRuntimeRequest {
	return FailbackRuntimeRequest{
		VolumeID:                     "v1",
		ReplicaID:                    "r1",
		TargetDataAddr:               "data-r1",
		TargetCtrlAddr:               "ctrl-r1",
		ExpectedCurrentReplicaID:     "r2",
		ExpectedCurrentEpoch:         1,
		AckEligible:                  true,
		FrontendFencedBeforeFailback: true,
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
		EvidenceRefs:                 []string{"failback-target"},
	}
}

// allowGate stands in for the master's planned-move decision: the target is
// allowed with the probe round's evidence.
func allowGate(volumeID, target string) (adapter.PrimaryEvidence, error) {
	return adapter.PrimaryEvidence{Kind: adapter.PrimaryKindPromotion, DurableLSN: 1, Basis: adapter.BasisPlanned}, nil
}

// A failback is a primary change: with no gate, or a gate that refuses (the
// target not in every acknowledgement set, not caught up), nothing is
// published.
func TestFailbackAuthorityRuntime_PublishesNothingWithoutTheGatesEvidence(t *testing.T) {
	for name, rt := range map[string]func(*Publisher) FailbackAuthorityRuntime{
		"no gate": func(p *Publisher) FailbackAuthorityRuntime { return FailbackAuthorityRuntime{Publisher: p} },
		"gate refuses": func(p *Publisher) FailbackAuthorityRuntime {
			return FailbackAuthorityRuntime{Publisher: p, TargetEndpoint: r1Endpoint, Gate: func(string, string) (adapter.PrimaryEvidence, error) {
				return adapter.PrimaryEvidence{}, errors.New("not caught up")
			}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			pub := NewPublisher(NewStaticDirective(nil))
			if err := pub.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r2", DataAddr: "data-r2", CtrlAddr: "ctrl-r2", Intent: IntentBind}); err != nil {
				t.Fatal(err)
			}
			before, _ := pub.VolumeAuthorityLine("v1")
			if _, err := rt(pub).ExecuteFailback(context.Background(), validAuthorityFailbackRequest()); err == nil {
				t.Fatal("a failback without the gate's evidence was executed")
			}
			if after, _ := pub.VolumeAuthorityLine("v1"); after != before {
				t.Fatalf("the line moved without the gate's evidence: %+v -> %+v", before, after)
			}
		})
	}
}

// r1Endpoint is the master's record of r1's addresses, as the valid request
// names them.
func r1Endpoint(_, replica string) (string, string, bool) {
	if replica == "r1" {
		return "data-r1", "ctrl-r1", true
	}
	return "", "", false
}

// The line is published at the master's record of the target's addresses:
// a request naming other addresses, a target with no record, or a runtime
// without the record is refused with ErrFailbackTargetEndpoint, and nothing
// is published.
func TestFailbackAuthorityRuntime_RefusesAddressesThatAreNotTheMastersRecord(t *testing.T) {
	for name, c := range map[string]struct {
		endpoint func(string, string) (string, string, bool)
		data     string
		ctrl     string
	}{
		"wrong data address":            {r1Endpoint, "data-elsewhere", "ctrl-r1"},
		"wrong control address":         {r1Endpoint, "data-r1", "ctrl-elsewhere"},
		"no record of the target":       {func(string, string) (string, string, bool) { return "", "", false }, "data-r1", "ctrl-r1"},
		"no endpoint record configured": {nil, "data-r1", "ctrl-r1"},
	} {
		t.Run(name, func(t *testing.T) {
			pub := NewPublisher(NewStaticDirective(nil))
			if err := pub.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r2", DataAddr: "data-r2", CtrlAddr: "ctrl-r2", Intent: IntentBind}); err != nil {
				t.Fatal(err)
			}
			before, _ := pub.VolumeAuthorityLine("v1")
			req := validAuthorityFailbackRequest()
			req.TargetDataAddr, req.TargetCtrlAddr = c.data, c.ctrl
			gated := false
			rt := FailbackAuthorityRuntime{Publisher: pub, TargetEndpoint: c.endpoint, Gate: func(v, r string) (adapter.PrimaryEvidence, error) {
				gated = true
				return allowGate(v, r)
			}}
			if _, err := rt.ExecuteFailback(context.Background(), req); !errors.Is(err, ErrFailbackTargetEndpoint) {
				t.Fatalf("err=%v; want ErrFailbackTargetEndpoint", err)
			}
			if gated {
				t.Fatal("the gate ran (a fencing round may have fenced survivors) for a request refused on its addresses")
			}
			if after, _ := pub.VolumeAuthorityLine("v1"); after != before {
				t.Fatalf("line=%+v; want %+v unchanged", after, before)
			}
		})
	}
}
