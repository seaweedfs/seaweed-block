package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/internal/testops"
)

type options struct {
	repoRoot    string
	registryDir string
	scenario    string
	artifactDir string
	runID       string
	commit      string
	controlDir  string
	params      paramFlag
	list        bool
	controlList bool
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	var opts options
	fs := flag.NewFlagSet("sw-testops", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&opts.repoRoot, "repo-root", ".", "seaweed_block repository root")
	fs.StringVar(&opts.registryDir, "registry-dir", "internal/testops/registry", "scenario registry directory")
	fs.StringVar(&opts.scenario, "scenario", "", "scenario name to run")
	fs.StringVar(&opts.artifactDir, "artifact-dir", "", "artifact output directory")
	fs.StringVar(&opts.runID, "run-id", "", "stable run id")
	fs.StringVar(&opts.commit, "commit", "", "source commit label; defaults to git HEAD")
	fs.StringVar(&opts.controlDir, "control-dir", "", "testops control directory for active/history/lock records")
	fs.Var(&opts.params, "param", "scenario parameter KEY=VALUE; may be repeated")
	fs.BoolVar(&opts.list, "list", false, "list registered scenarios")
	fs.BoolVar(&opts.controlList, "control-list", false, "list testops control records from --control-dir")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := opts.normalize(); err != nil {
		fmt.Fprintln(stderr, "sw-testops:", err)
		return 2
	}
	if opts.controlList {
		return listControlRecords(opts.controlDir, stdout, stderr)
	}
	registrations, err := loadRegistrations(opts.repoRoot, opts.registryDir)
	if err != nil {
		fmt.Fprintln(stderr, "sw-testops:", err)
		return 2
	}
	if opts.list {
		for _, reg := range registrations {
			fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\n", reg.Scenario, reg.Gate, reg.Layer, reg.Driver.Type, strings.Join(reg.Resources.Exclusive, ","))
		}
		return 0
	}
	registration, ok := findRegistration(registrations, opts.scenario)
	if !ok {
		fmt.Fprintf(stderr, "sw-testops: scenario %q not found\n", opts.scenario)
		return 2
	}
	registry := testops.NewRegistry()
	if err := registration.RegisterInto(registry, opts.repoRoot); err != nil {
		fmt.Fprintln(stderr, "sw-testops:", err)
		return 2
	}
	req := testops.RunRequest{
		SchemaVersion:  testops.SchemaVersion,
		Scenario:       registration.Scenario,
		Source:         testops.SourceSpec{Repo: "seaweed_block", Commit: opts.commit},
		Binaries:       testops.BinarySpec{Build: false},
		ScenarioParams: mergedParams(registration.ScenarioDefaultParams, opts.params),
		ArtifactDir:    opts.artifactDir,
		RunID:          opts.runID,
		TimeoutSeconds: registration.DefaultTimeoutSeconds,
	}
	var control testops.ControlManager
	var controlRec testops.ControlRecord
	if opts.controlDir != "" {
		control = testops.NewControlManager(opts.controlDir)
		var startErr error
		controlRec, startErr = control.Start(req, registration.Resources)
		if startErr != nil {
			fmt.Fprintf(stderr, "sw-testops: control start failed: %v\n", startErr)
			return 2
		}
	}
	res, err := registry.Run(context.Background(), req)
	if writeErr := writeResult(opts.artifactDir, res); writeErr != nil && err == nil {
		err = writeErr
		res.Status = testops.StatusError
		res.Summary = fmt.Sprintf("write result failed: %v", writeErr)
	}
	if opts.controlDir != "" {
		if completeErr := control.Complete(controlRec, res.Status, err); completeErr != nil && err == nil {
			err = completeErr
			res.Status = testops.StatusError
			res.Summary = fmt.Sprintf("control complete failed: %v", completeErr)
			_ = writeResult(opts.artifactDir, res)
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "sw-testops: %s failed: %v\n", opts.scenario, err)
		if res.Status == testops.StatusFail {
			return 1
		}
		return 2
	}
	fmt.Fprintf(stdout, "%s\t%s\t%s\n", res.Status, res.Scenario, res.ArtifactDir)
	if res.Status == testops.StatusFail {
		return 1
	}
	if res.Status == testops.StatusError {
		return 2
	}
	return 0
}

type paramFlag map[string]string

func (p *paramFlag) String() string {
	if p == nil || len(*p) == 0 {
		return ""
	}
	keys := make([]string, 0, len(*p))
	for key := range *p {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+(*p)[key])
	}
	return strings.Join(parts, ",")
}

func (p *paramFlag) Set(value string) error {
	key, val, ok := strings.Cut(value, "=")
	if !ok || key == "" {
		return fmt.Errorf("expected KEY=VALUE")
	}
	if *p == nil {
		*p = make(map[string]string)
	}
	(*p)[key] = val
	return nil
}

func mergedParams(defaults map[string]string, overrides map[string]string) map[string]string {
	if len(defaults) == 0 && len(overrides) == 0 {
		return nil
	}
	out := make(map[string]string, len(defaults)+len(overrides))
	for key, value := range defaults {
		out[key] = value
	}
	for key, value := range overrides {
		out[key] = value
	}
	return out
}

func (o *options) normalize() error {
	root, err := filepath.Abs(o.repoRoot)
	if err != nil {
		return err
	}
	o.repoRoot = root
	if !filepath.IsAbs(o.registryDir) {
		o.registryDir = filepath.Join(o.repoRoot, o.registryDir)
	}
	if o.list || o.controlList {
		return nil
	}
	if o.scenario == "" {
		return fmt.Errorf("--scenario is required unless --list is set")
	}
	if o.runID == "" {
		o.runID = o.scenario + "-" + time.Now().UTC().Format("20060102T150405Z")
	}
	if o.artifactDir == "" {
		o.artifactDir = filepath.Join(o.repoRoot, "testops", "runs", o.runID)
	}
	if o.commit == "" {
		o.commit = gitHead(o.repoRoot)
	}
	return nil
}

func listControlRecords(controlDir string, stdout, stderr io.Writer) int {
	if controlDir == "" {
		fmt.Fprintln(stderr, "sw-testops: --control-dir is required with --control-list")
		return 2
	}
	records, err := testops.NewControlManager(controlDir).List()
	if err != nil {
		fmt.Fprintf(stderr, "sw-testops: control list failed: %v\n", err)
		return 2
	}
	for _, rec := range records {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			rec.State,
			rec.RunID,
			rec.Scenario,
			rec.SourceCommit,
			rec.UpdatedAt.UTC().Format(time.RFC3339),
			strings.Join(rec.Locks, ","),
			rec.ArtifactDir)
	}
	return 0
}

func loadRegistrations(repoRoot, registryDir string) ([]testops.Registration, error) {
	entries, err := os.ReadDir(registryDir)
	if err != nil {
		return nil, fmt.Errorf("read registry dir %q: %w", registryDir, err)
	}
	var out []testops.Registration
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(registryDir, entry.Name())
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		reg, decodeErr := testops.DecodeRegistration(f)
		closeErr := f.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("decode %s: %w", path, decodeErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if reg.QAInstruction != "" && !filepath.IsAbs(reg.QAInstruction) {
			reg.QAInstruction = filepath.Clean(reg.QAInstruction)
		}
		_ = repoRoot
		out = append(out, reg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Scenario < out[j].Scenario })
	return out, nil
}

func findRegistration(registrations []testops.Registration, scenario string) (testops.Registration, bool) {
	for _, reg := range registrations {
		if reg.Scenario == scenario {
			return reg, true
		}
	}
	return testops.Registration{}, false
}

func gitHead(repoRoot string) string {
	cmd := exec.Command("git", "-C", repoRoot, "rev-parse", "--short", "HEAD")
	raw, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(raw))
}

func writeResult(artifactDir string, res testops.Result) error {
	if artifactDir == "" {
		return nil
	}
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(artifactDir, "result.json")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return testops.EncodeResult(f, res)
}
