package testops

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const ControlSchemaVersion = "1.0"

var errControlLockExists = errors.New("resource is locked")

type ControlRecord struct {
	SchemaVersion string       `json:"schema_version"`
	RunID         string       `json:"run_id"`
	Scenario      string       `json:"scenario"`
	State         string       `json:"state"`
	SourceCommit  string       `json:"source_commit"`
	ArtifactDir   string       `json:"artifact_dir"`
	Resources     ResourceSpec `json:"resources,omitempty"`
	Locks         []string     `json:"locks,omitempty"`
	StartedAt     time.Time    `json:"started_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
	EndedAt       *time.Time   `json:"ended_at,omitempty"`
	ErrorSummary  string       `json:"error_summary,omitempty"`
}

type ControlManager struct {
	Root      func() string
	Now       func() time.Time
	WriteLock func(path string, data []byte) error
}

func NewControlManager(root string) ControlManager {
	return ControlManager{
		Root: func() string { return root },
		Now:  time.Now,
	}
}

func (m ControlManager) Start(req RunRequest, resources ResourceSpec) (ControlRecord, error) {
	root := m.root()
	if root == "" {
		return ControlRecord{}, fmt.Errorf("testops control: root is required")
	}
	if req.RunID == "" {
		return ControlRecord{}, fmt.Errorf("testops control: run_id is required")
	}
	if err := os.MkdirAll(filepath.Join(root, "active"), 0o755); err != nil {
		return ControlRecord{}, err
	}
	if err := os.MkdirAll(filepath.Join(root, "history"), 0o755); err != nil {
		return ControlRecord{}, err
	}
	if err := os.MkdirAll(filepath.Join(root, "locks"), 0o755); err != nil {
		return ControlRecord{}, err
	}
	locks := lockNames(resources)
	for _, lock := range locks {
		if err := m.acquireLock(req, lock); err != nil {
			m.releaseLocks(locks[:indexOf(locks, lock)])
			return ControlRecord{}, err
		}
	}
	now := m.now()
	rec := ControlRecord{
		SchemaVersion: ControlSchemaVersion,
		RunID:         req.RunID,
		Scenario:      req.Scenario,
		State:         "running",
		SourceCommit:  req.Source.Commit,
		ArtifactDir:   req.ArtifactDir,
		Resources:     resources,
		Locks:         locks,
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if err := writeJSONFile(filepath.Join(root, "active", req.RunID+".json"), rec); err != nil {
		m.releaseLocks(locks)
		return ControlRecord{}, err
	}
	_ = m.appendEvent("start", rec)
	return rec, nil
}

func (m ControlManager) Complete(rec ControlRecord, status Status, err error) error {
	root := m.root()
	now := m.now()
	defer m.releaseLocks(rec.Locks)
	rec.State = string(status)
	if rec.State == "" {
		rec.State = "error"
	}
	rec.UpdatedAt = now
	rec.EndedAt = &now
	if err != nil {
		rec.ErrorSummary = err.Error()
	}
	historyPath := filepath.Join(root, "history", rec.RunID+".json")
	if writeErr := writeJSONFile(historyPath, rec); writeErr != nil {
		rec.State = string(StatusError)
		rec.ErrorSummary = fmt.Sprintf("control complete failed: %v", writeErr)
		rec.UpdatedAt = m.now()
		_ = writeJSONFile(filepath.Join(root, "active", rec.RunID+".json"), rec)
		_ = m.appendEvent("complete-error", rec)
		return writeErr
	}
	_ = os.Remove(filepath.Join(root, "active", rec.RunID+".json"))
	_ = m.appendEvent("complete", rec)
	return nil
}

func (m ControlManager) List() ([]ControlRecord, error) {
	root := m.root()
	var out []ControlRecord
	for _, dir := range []string{"active", "history"} {
		records, err := readRecords(filepath.Join(root, dir))
		if err != nil {
			return nil, err
		}
		out = append(out, records...)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

func (m ControlManager) acquireLock(req RunRequest, name string) error {
	path := filepath.Join(m.root(), "locks", sanitizeLockName(name)+".lock")
	payload := map[string]string{
		"run_id":    req.RunID,
		"scenario":  req.Scenario,
		"lock":      name,
		"commit":    req.Source.Commit,
		"artifact":  req.ArtifactDir,
		"timestamp": m.now().UTC().Format(time.RFC3339),
	}
	raw, _ := json.MarshalIndent(payload, "", "  ")
	if err := m.writeLock(path, append(raw, '\n')); err != nil {
		if errors.Is(err, errControlLockExists) {
			return fmt.Errorf("testops control: resource %q is locked by %s", name, path)
		}
		_ = os.Remove(path)
		return err
	}
	return nil
}

func (m ControlManager) writeLock(path string, data []byte) error {
	if m.WriteLock != nil {
		return m.WriteLock(path, data)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("%w: %s", errControlLockExists, path)
		}
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(path)
		}
	}()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func (m ControlManager) releaseLocks(locks []string) {
	for _, lock := range locks {
		_ = os.Remove(filepath.Join(m.root(), "locks", sanitizeLockName(lock)+".lock"))
	}
}

func (m ControlManager) appendEvent(kind string, rec ControlRecord) error {
	event := map[string]any{
		"event":      kind,
		"run_id":     rec.RunID,
		"scenario":   rec.Scenario,
		"state":      rec.State,
		"updated_at": rec.UpdatedAt.UTC().Format(time.RFC3339),
	}
	raw, _ := json.Marshal(event)
	f, err := os.OpenFile(filepath.Join(m.root(), "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(raw, '\n'))
	return err
}

func (m ControlManager) root() string {
	if m.Root == nil {
		return ""
	}
	return m.Root()
}

func (m ControlManager) now() time.Time {
	if m.Now == nil {
		return time.Now()
	}
	return m.Now()
}

func lockNames(resources ResourceSpec) []string {
	seen := map[string]bool{}
	var out []string
	if resources.Group != "" {
		out = append(out, "group:"+resources.Group)
		seen["group:"+resources.Group] = true
	}
	for _, item := range resources.Exclusive {
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	for _, port := range resources.Ports {
		name := fmt.Sprintf("port:%d", port)
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func sanitizeLockName(name string) string {
	replacer := strings.NewReplacer(":", "_", "/", "_", "\\", "_", " ", "_")
	return replacer.Replace(name)
}

func indexOf(items []string, want string) int {
	for i, item := range items {
		if item == want {
			return i
		}
	}
	return len(items)
}

func readRecords(dir string) ([]ControlRecord, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []ControlRecord
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var rec ControlRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func writeJSONFile(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}
