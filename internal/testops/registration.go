package testops

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
)

type Registration struct {
	SchemaVersion         string            `json:"schema_version"`
	Scenario              string            `json:"scenario"`
	Gate                  string            `json:"gate,omitempty"`
	Layer                 string            `json:"layer,omitempty"`
	Driver                DriverSpec        `json:"driver"`
	DefaultTimeoutSeconds int               `json:"default_timeout_s,omitempty"`
	RequiredCapabilities  []string          `json:"required_capabilities,omitempty"`
	RequiredImages        []string          `json:"required_images,omitempty"`
	QAInstruction         string            `json:"qa_instruction,omitempty"`
	KnownGreenCommit      string            `json:"known_green_commit,omitempty"`
	Artifacts             []string          `json:"artifacts,omitempty"`
	NonClaims             []string          `json:"non_claims,omitempty"`
	ScenarioDefaultParams map[string]string `json:"scenario_default_params,omitempty"`
}

type DriverSpec struct {
	Type    string   `json:"type"`
	Path    string   `json:"path,omitempty"`
	Package string   `json:"package,omitempty"`
	Run     string   `json:"run,omitempty"`
	Args    []string `json:"args,omitempty"`
	Env     []string `json:"env,omitempty"`
}

func DecodeRegistration(r io.Reader) (Registration, error) {
	var reg Registration
	if err := json.NewDecoder(r).Decode(&reg); err != nil {
		return Registration{}, err
	}
	return reg, reg.Validate()
}

func (r Registration) Validate() error {
	if r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("testops: registration schema_version %q unsupported", r.SchemaVersion)
	}
	if r.Scenario == "" {
		return fmt.Errorf("testops: registration scenario is required")
	}
	switch r.Driver.Type {
	case "go-test":
		if r.Driver.Package == "" {
			return fmt.Errorf("testops: go-test registration %q requires driver.package", r.Scenario)
		}
	case "shell":
		if r.Driver.Path == "" {
			return fmt.Errorf("testops: shell registration %q requires driver.path", r.Scenario)
		}
	default:
		return fmt.Errorf("testops: driver type %q unsupported", r.Driver.Type)
	}
	return nil
}

func (r Registration) NewDriver(repoRoot string) (Driver, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	switch r.Driver.Type {
	case "go-test":
		return GoTestDriver{
			WorkDir:  repoRoot,
			Package:  r.Driver.Package,
			RunRegex: r.Driver.Run,
			Args:     r.Driver.Args,
			Env:      r.Driver.Env,
		}, nil
	case "shell":
		path := r.Driver.Path
		if repoRoot != "" && !filepath.IsAbs(path) {
			path = filepath.Join(repoRoot, path)
		}
		return ShellDriver{Path: path, Env: r.Driver.Env}, nil
	default:
		return nil, fmt.Errorf("testops: driver type %q unsupported", r.Driver.Type)
	}
}

func (r Registration) RegisterInto(registry *Registry, repoRoot string) error {
	driver, err := r.NewDriver(repoRoot)
	if err != nil {
		return err
	}
	return registry.Register(r.Scenario, driver)
}
