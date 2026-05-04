// Package testops defines the stable V3 TestOps contract.
//
// It is intentionally small: V3 exposes run requests, results, and a
// pluggable driver registry. Bash, go-test, Kubernetes, privileged-host,
// and future YAML testrunner backends should plug in as drivers instead of
// forcing V3 to adopt one runner's object model.
package testops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const SchemaVersion = "1.0"

type RunRequest struct {
	SchemaVersion  string            `json:"schema_version"`
	Scenario       string            `json:"scenario"`
	Source         SourceSpec        `json:"source"`
	Binaries       BinarySpec        `json:"binaries"`
	ScenarioParams map[string]string `json:"scenario_params,omitempty"`
	ArtifactDir    string            `json:"artifact_dir"`
	RunID          string            `json:"run_id"`
	TimeoutSeconds int               `json:"timeout_s"`
}

type SourceSpec struct {
	Repo    string `json:"repo"`
	Commit  string `json:"commit"`
	TarPath string `json:"tar_path,omitempty"`
}

type BinarySpec struct {
	Build  bool   `json:"build"`
	BinDir string `json:"bin_dir,omitempty"`
}

type Result struct {
	SchemaVersion string            `json:"schema_version"`
	RunID         string            `json:"run_id"`
	Scenario      string            `json:"scenario"`
	SourceCommit  string            `json:"source_commit"`
	Status        Status            `json:"status"`
	Summary       string            `json:"summary"`
	WallClock     float64           `json:"wall_clock_s"`
	PhaseResults  []PhaseResult     `json:"phase_results,omitempty"`
	ArtifactDir   string            `json:"artifact_dir"`
	Artifacts     map[string]string `json:"artifacts,omitempty"`
	Checksums     map[string]string `json:"checksums,omitempty"`
	NonClaims     []string          `json:"non_claims,omitempty"`
}

type Status string

const (
	StatusPass  Status = "pass"
	StatusFail  Status = "fail"
	StatusError Status = "error"
)

type PhaseResult struct {
	Name     string  `json:"name"`
	Status   Status  `json:"status"`
	Duration float64 `json:"duration_s,omitempty"`
}

type Driver interface {
	Run(ctx context.Context, req RunRequest) (Result, error)
}

func DecodeRunRequest(r io.Reader) (RunRequest, error) {
	var req RunRequest
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		return RunRequest{}, err
	}
	return req, req.Validate()
}

func EncodeResult(w io.Writer, res Result) error {
	if res.SchemaVersion == "" {
		res.SchemaVersion = SchemaVersion
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(res)
}

func (r RunRequest) Validate() error {
	if r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("testops: schema_version %q unsupported", r.SchemaVersion)
	}
	if r.Scenario == "" {
		return fmt.Errorf("testops: scenario is required")
	}
	if r.Source.Repo == "" {
		return fmt.Errorf("testops: source.repo is required")
	}
	if r.Source.Commit == "" {
		return fmt.Errorf("testops: source.commit is required")
	}
	if r.ArtifactDir == "" {
		return fmt.Errorf("testops: artifact_dir is required")
	}
	if r.RunID == "" {
		return fmt.Errorf("testops: run_id is required")
	}
	return nil
}

func (r RunRequest) Timeout() time.Duration {
	if r.TimeoutSeconds <= 0 {
		return 0
	}
	return time.Duration(r.TimeoutSeconds) * time.Second
}

func NewResult(req RunRequest, status Status, summary string) Result {
	return Result{
		SchemaVersion: SchemaVersion,
		RunID:         req.RunID,
		Scenario:      req.Scenario,
		SourceCommit:  req.Source.Commit,
		Status:        status,
		Summary:       summary,
		ArtifactDir:   req.ArtifactDir,
	}
}
