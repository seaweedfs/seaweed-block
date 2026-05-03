package testops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type GoTestDriver struct {
	WorkDir  string
	Package  string
	RunRegex string
	Args     []string
	Env      []string
}

func (d GoTestDriver) Run(ctx context.Context, req RunRequest) (Result, error) {
	if d.Package == "" {
		return NewResult(req, StatusError, "go-test driver package is empty"), fmt.Errorf("testops: go-test driver package is empty")
	}
	if err := os.MkdirAll(req.ArtifactDir, 0o755); err != nil {
		return NewResult(req, StatusError, "create artifact dir failed"), fmt.Errorf("testops: create artifact dir: %w", err)
	}
	if err := writeJSON(filepath.Join(req.ArtifactDir, "run-request.json"), req); err != nil {
		return NewResult(req, StatusError, "write normalized request failed"), err
	}

	args := []string{"test", d.Package}
	if d.RunRegex != "" {
		args = append(args, "-run", d.RunRegex)
	}
	args = append(args, d.Args...)

	start := time.Now()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if d.WorkDir != "" {
		cmd.Dir = d.WorkDir
	}
	cmd.Env = append(os.Environ(), d.Env...)
	err := cmd.Run()
	duration := time.Since(start).Seconds()

	artifacts := make(map[string]string)
	if stdout.Len() > 0 {
		path := filepath.Join(req.ArtifactDir, "test-stdout.log")
		_ = os.WriteFile(path, stdout.Bytes(), 0o644)
		artifacts["test-stdout.log"] = path
	}
	if stderr.Len() > 0 {
		path := filepath.Join(req.ArtifactDir, "test-stderr.log")
		_ = os.WriteFile(path, stderr.Bytes(), 0o644)
		artifacts["test-stderr.log"] = path
	}

	status := StatusPass
	summary := "go test passed"
	if err != nil {
		status = StatusFail
		summary = fmt.Sprintf("go test failed: %v", err)
		if ctx.Err() != nil || !isExitError(err) {
			status = StatusError
		}
	}

	res := NewResult(req, status, summary)
	res.WallClock = duration
	res.Artifacts = artifacts
	res.PhaseResults = []PhaseResult{{
		Name:     "go-test",
		Status:   status,
		Duration: duration,
	}}
	if writeErr := writeJSON(filepath.Join(req.ArtifactDir, "result.json"), res); writeErr != nil && err == nil {
		return res, writeErr
	}
	return res, err
}

func isExitError(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr)
}
