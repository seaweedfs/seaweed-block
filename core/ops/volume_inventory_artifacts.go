package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	VolumeInventoryArtifact        = "volume-inventory.json"
	VolumeInventorySummaryArtifact = "volume-inventory-summary.txt"
	OpsInventoryBundleArtifact     = "ops-inventory-bundle.json"
)

type VolumeInventoryCollector interface {
	Collect(context.Context) (VolumeInventory, error)
}

type VolumeInventoryCollectorFunc func(context.Context) (VolumeInventory, error)

func (f VolumeInventoryCollectorFunc) Collect(ctx context.Context) (VolumeInventory, error) {
	return f(ctx)
}

func StaticVolumeInventoryCollector(in VolumeInventoryInput) VolumeInventoryCollector {
	return VolumeInventoryCollectorFunc(func(context.Context) (VolumeInventory, error) {
		return BuildVolumeInventory(in), nil
	})
}

type OpsInventoryBundle struct {
	SchemaVersion    string                       `json:"schema_version"`
	Command          string                       `json:"command"`
	CapturedAt       string                       `json:"captured_at"`
	ProductRevision  string                       `json:"product_revision"`
	RunnerRevision   string                       `json:"runner_revision,omitempty"`
	ExitCode         int                          `json:"exit_code"`
	Status           string                       `json:"status"`
	VolumeCount      int                          `json:"volume_count"`
	Artifacts        []OpsStatusBundleArtifactRef `json:"artifacts"`
	CollectionErrors []string                     `json:"collection_errors"`
	NonClaims        []string                     `json:"non_claims"`
}

func WriteVolumeInventoryArtifacts(ctx context.Context, dir string, collector VolumeInventoryCollector) (VolumeInventory, int, error) {
	if dir == "" {
		return VolumeInventory{}, VolumeStatusExitInvalid, fmt.Errorf("artifact dir is required")
	}
	if collector == nil {
		return VolumeInventory{}, VolumeStatusExitInvalid, fmt.Errorf("inventory collector is required")
	}
	if err := ensureArtifactDir(dir); err != nil {
		return VolumeInventory{}, VolumeStatusExitInvalid, err
	}

	inventory, collectErr := collector.Collect(ctx)
	if collectErr != nil {
		inventory.CollectionErrors = append(inventory.CollectionErrors, splitErrorMessages(collectErr)...)
	}
	classification := ClassifyVolumeInventory(inventory)
	inventory.Status = inventoryExitLabel(classification)

	raw, err := json.MarshalIndent(inventory, "", "  ")
	if err != nil {
		return inventory, VolumeStatusExitInvalid, fmt.Errorf("marshal volume inventory: %w", err)
	}
	if err := writeFileViaTemp(filepath.Join(dir, VolumeInventoryArtifact), append(raw, '\n'), 0o644); err != nil {
		return inventory, VolumeStatusExitInvalid, fmt.Errorf("write %s: %w", VolumeInventoryArtifact, err)
	}
	if err := writeFileViaTemp(filepath.Join(dir, VolumeInventorySummaryArtifact), []byte(RenderVolumeInventorySummary(inventory)), 0o644); err != nil {
		return inventory, VolumeStatusExitInvalid, fmt.Errorf("write %s: %w", VolumeInventorySummaryArtifact, err)
	}
	bundleRaw, err := json.MarshalIndent(BuildOpsInventoryBundle(inventory, classification), "", "  ")
	if err != nil {
		return inventory, VolumeStatusExitInvalid, fmt.Errorf("marshal ops inventory bundle: %w", err)
	}
	if err := writeFileViaTemp(filepath.Join(dir, OpsInventoryBundleArtifact), append(bundleRaw, '\n'), 0o644); err != nil {
		return inventory, VolumeStatusExitInvalid, fmt.Errorf("write %s: %w", OpsInventoryBundleArtifact, err)
	}
	if collectErr != nil {
		return inventory, classification, collectErr
	}
	return inventory, classification, nil
}

func BuildOpsInventoryBundle(inventory VolumeInventory, exitCode int) OpsInventoryBundle {
	return OpsInventoryBundle{
		SchemaVersion:    VolumeInventorySchemaVersion,
		Command:          "sw-block ops inventory",
		CapturedAt:       inventory.CapturedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
		ProductRevision:  inventory.ProductRevision,
		RunnerRevision:   inventory.RunnerRevision,
		ExitCode:         exitCode,
		Status:           inventoryExitLabel(exitCode),
		VolumeCount:      len(inventory.Volumes),
		Artifacts:        opsInventoryBundleArtifacts(),
		CollectionErrors: copyStringSlice(inventory.CollectionErrors),
		NonClaims:        copyStringSlice(inventory.NonClaims),
	}
}

func opsInventoryBundleArtifacts() []OpsStatusBundleArtifactRef {
	return []OpsStatusBundleArtifactRef{
		{Name: VolumeInventoryArtifact, Description: "machine-readable multi-volume inventory evidence"},
		{Name: VolumeInventorySummaryArtifact, Description: "human-readable inventory summary"},
		{Name: OpsInventoryBundleArtifact, Description: "self-describing inventory support bundle manifest"},
	}
}

func ensureArtifactDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create artifact dir: %w", err)
	}
	return nil
}
