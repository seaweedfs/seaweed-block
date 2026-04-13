// Package schema defines the conformance case format for the V3 mini engine.
// Cases are declarative: an ordered list of events and the expected outcomes.
package schema

// ConformanceCase is one replay scenario for the mini engine.
// It describes an ordered event stream and what the engine should produce.
type ConformanceCase struct {
	Name        string       `yaml:"name"`
	Description string       `yaml:"description,omitempty"`
	Events      []EventInput `yaml:"events"`
	Expect      Expectation  `yaml:"expect"`
}

// EventInput is one event in a conformance case.
// The Kind field selects the event type; the remaining fields
// are populated based on kind.
type EventInput struct {
	Kind string `yaml:"kind"`

	// Identity
	VolumeID        string `yaml:"volume_id,omitempty"`
	ReplicaID       string `yaml:"replica_id,omitempty"`
	Epoch           uint64 `yaml:"epoch,omitempty"`
	EndpointVersion uint64 `yaml:"endpoint_version,omitempty"`
	DataAddr        string `yaml:"data_addr,omitempty"`
	CtrlAddr        string `yaml:"ctrl_addr,omitempty"`

	// Recovery facts
	R uint64 `yaml:"r,omitempty"`
	S uint64 `yaml:"s,omitempty"`
	H uint64 `yaml:"h,omitempty"`

	// Session
	SessionID   uint64 `yaml:"session_id,omitempty"`
	SessionKind string `yaml:"session_kind,omitempty"`
	TargetLSN   uint64 `yaml:"target_lsn,omitempty"`
	AchievedLSN uint64 `yaml:"achieved_lsn,omitempty"`

	// Failure
	Reason          string `yaml:"reason,omitempty"`
	TransportEpoch  uint64 `yaml:"transport_epoch,omitempty"`
}

// Expectation defines what the engine should produce after all events.
type Expectation struct {
	// Final projection fields.
	Mode             string `yaml:"mode,omitempty"`
	RecoveryDecision string `yaml:"recovery_decision,omitempty"`
	SessionPhase     string `yaml:"session_phase,omitempty"`

	// Commands that must appear (by kind name).
	MustEmitCommands []string `yaml:"must_emit_commands,omitempty"`

	// Commands that must NOT appear.
	MustNotEmitCommands []string `yaml:"must_not_emit_commands,omitempty"`

	// Trace steps that must appear.
	MustHaveTraceSteps []string `yaml:"must_have_trace_steps,omitempty"`
}

// CaseSuite is a collection of conformance cases.
type CaseSuite struct {
	Version string            `yaml:"version"`
	Cases   []ConformanceCase `yaml:"cases"`
}
