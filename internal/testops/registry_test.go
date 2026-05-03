package testops

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRunRequestRoundTripAndValidate(t *testing.T) {
	raw := `{
	  "schema_version": "1.0",
	  "scenario": "g15b-manifest",
	  "source": {"repo":"seaweed_block","commit":"5375add"},
	  "binaries": {"build": false, "bin_dir": "V:\\share\\v3-debug\\bin\\5375add"},
	  "scenario_params": {"focus":"manifest"},
	  "artifact_dir": "V:\\share\\v3-debug\\runs\\r1",
	  "run_id": "r1",
	  "timeout_s": 30
	}`
	req, err := DecodeRunRequest(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("DecodeRunRequest: %v", err)
	}
	if req.Scenario != "g15b-manifest" || req.Source.Commit != "5375add" {
		t.Fatalf("decoded req=%+v", req)
	}
	if req.Timeout().Seconds() != 30 {
		t.Fatalf("timeout=%v", req.Timeout())
	}

	var buf bytes.Buffer
	if err := EncodeResult(&buf, NewResult(req, StatusPass, "ok")); err != nil {
		t.Fatalf("EncodeResult: %v", err)
	}
	var res Result
	if err := json.Unmarshal(buf.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if res.SchemaVersion != SchemaVersion || res.Status != StatusPass || res.SourceCommit != "5375add" {
		t.Fatalf("result=%+v", res)
	}
}

func TestRegistryRunsRegisteredScenario(t *testing.T) {
	req := RunRequest{
		SchemaVersion: SchemaVersion,
		Scenario:      "fake",
		Source:        SourceSpec{Repo: "seaweed_block", Commit: "abc123"},
		ArtifactDir:   "/tmp/art",
		RunID:         "run-1",
	}
	reg := NewRegistry()
	if err := reg.Register("fake", DriverFunc(func(ctx context.Context, req RunRequest) (Result, error) {
		return Result{
			Status:       StatusPass,
			Summary:      "fake pass",
			PhaseResults: []PhaseResult{{Name: "phase-1", Status: StatusPass}},
			NonClaims:    []string{"no product behavior"},
		}, nil
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	res, err := reg.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.RunID != "run-1" || res.Scenario != "fake" || res.SourceCommit != "abc123" {
		t.Fatalf("registry did not fill result identity: %+v", res)
	}
	if res.Status != StatusPass || len(res.PhaseResults) != 1 {
		t.Fatalf("result=%+v", res)
	}
}

func TestRegistryRejectsUnknownAndDuplicate(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("x", DriverFunc(func(context.Context, RunRequest) (Result, error) {
		return Result{}, nil
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := reg.Register("x", DriverFunc(func(context.Context, RunRequest) (Result, error) {
		return Result{}, nil
	})); err == nil {
		t.Fatal("duplicate register succeeded")
	}
	_, err := reg.Run(context.Background(), RunRequest{
		SchemaVersion: SchemaVersion,
		Scenario:      "missing",
		Source:        SourceSpec{Repo: "seaweed_block", Commit: "abc123"},
		ArtifactDir:   "/tmp/art",
		RunID:         "run-1",
	})
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("Run missing err=%v", err)
	}
}

type DriverFunc func(context.Context, RunRequest) (Result, error)

func (f DriverFunc) Run(ctx context.Context, req RunRequest) (Result, error) {
	return f(ctx, req)
}
