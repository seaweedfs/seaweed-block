package testops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
)

type ShellDriver struct {
	Path string
	Env  []string
}

func (d ShellDriver) Run(ctx context.Context, req RunRequest) (Result, error) {
	if d.Path == "" {
		return NewResult(req, StatusError, "shell driver path is empty"), fmt.Errorf("testops: shell driver path is empty")
	}
	if err := os.MkdirAll(req.ArtifactDir, 0o755); err != nil {
		return NewResult(req, StatusError, "create artifact dir failed"), fmt.Errorf("testops: create artifact dir: %w", err)
	}

	reqPath := filepath.Join(req.ArtifactDir, "run-request.json")
	if err := writeJSON(reqPath, req); err != nil {
		return NewResult(req, StatusError, "write normalized request failed"), err
	}

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, d.Path, reqPath)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = shellEnv(d.Env, req.ScenarioParams)
	err := cmd.Run()

	if stdout.Len() > 0 {
		_ = os.WriteFile(filepath.Join(req.ArtifactDir, "driver-stdout.log"), stdout.Bytes(), 0o644)
	}
	if stderr.Len() > 0 {
		_ = os.WriteFile(filepath.Join(req.ArtifactDir, "driver-stderr.log"), stderr.Bytes(), 0o644)
	}

	resultPath := filepath.Join(req.ArtifactDir, "result.json")
	if b, readErr := os.ReadFile(resultPath); readErr == nil {
		var res Result
		if decodeErr := json.Unmarshal(b, &res); decodeErr != nil {
			return NewResult(req, StatusError, "driver wrote invalid result.json"), decodeErr
		}
		if err != nil && res.Status == "" {
			res.Status = StatusError
		}
		return res, err
	}

	if err != nil {
		return NewResult(req, StatusError, fmt.Sprintf("driver failed before writing result.json: %v", err)), err
	}
	return NewResult(req, StatusError, "driver did not write result.json"), fmt.Errorf("testops: shell driver %s did not write result.json", d.Path)
}

func shellEnv(driverEnv []string, params map[string]string) []string {
	env := append([]string{}, os.Environ()...)
	env = append(env, driverEnv...)
	if len(params) == 0 {
		return env
	}
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		env = append(env, key+"="+params[key])
	}
	return env
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
