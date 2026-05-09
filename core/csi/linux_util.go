package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func NewDefaultNodeServer(nodeID, iqnPrefix string) *NodeServer {
	return NewNodeServer(NodeConfig{
		NodeID:    nodeID,
		IQNPrefix: iqnPrefix,
		ISCSIUtil: &realISCSIUtil{},
		NVMeUtil:  &realNVMeUtil{},
		MountUtil: &realMountUtil{},
	})
}

type realISCSIUtil struct{}

func (r *realISCSIUtil) Discovery(ctx context.Context, portal string) error {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "discovery", "-t", "sendtargets", "-p", portal)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("iscsiadm discovery: %s: %w", string(out), err)
	}
	return nil
}

func (r *realISCSIUtil) ConfigureCHAP(ctx context.Context, iqn, portal string, auth ISCSIAuth) error {
	updates := []struct {
		key   string
		value string
	}{
		{key: "node.session.auth.authmethod", value: "CHAP"},
		{key: "node.session.auth.username", value: auth.Username},
		{key: "node.session.auth.password", value: auth.Secret},
	}
	for _, update := range updates {
		cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--op=update", "-n", update.key, "-v", update.value)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("iscsiadm configure CHAP %s: %s: %w", update.key, string(out), err)
		}
	}
	return nil
}

func (r *realISCSIUtil) Login(ctx context.Context, iqn, portal string) error {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--login")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("iscsiadm login: %s: %w", string(out), err)
	}
	return nil
}

func (r *realISCSIUtil) Logout(ctx context.Context, iqn string) error {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "--logout")
	out, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "No matching sessions") {
			return nil
		}
		return fmt.Errorf("iscsiadm logout: %s: %w", string(out), err)
	}
	return nil
}

func (r *realISCSIUtil) GetDeviceByIQN(ctx context.Context, iqn string) (string, error) {
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("timeout waiting for device for IQN %s", iqn)
		case <-ticker.C:
			matches, err := filepath.Glob(fmt.Sprintf("/dev/disk/by-path/*%s*", iqn))
			if err != nil {
				continue
			}
			for _, match := range matches {
				if strings.Contains(match, "-part") {
					continue
				}
				dev, err := filepath.EvalSymlinks(match)
				if err != nil {
					continue
				}
				return dev, nil
			}
		}
	}
}

func (r *realISCSIUtil) IsLoggedIn(ctx context.Context, iqn string) (bool, error) {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "session")
	out, err := cmd.CombinedOutput()
	if err != nil {
		outStr := string(out)
		if strings.Contains(outStr, "No active sessions") {
			return false, nil
		}
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 21 {
			return false, nil
		}
		return false, fmt.Errorf("iscsiadm session: %s: %w", outStr, err)
	}
	return strings.Contains(string(out), iqn), nil
}

func (r *realISCSIUtil) RescanDevice(ctx context.Context, iqn string) error {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "--rescan")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("iscsiadm rescan: %s: %w", string(out), err)
	}
	return nil
}

type realNVMeUtil struct{}

func (r *realNVMeUtil) Connect(ctx context.Context, addr, nqn string) error {
	host, port, err := splitHostPort(addr)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "nvme", "connect", "-t", "tcp", "-a", host, "-s", port, "-n", nqn)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvme connect: %s: %w", string(out), err)
	}
	return nil
}

func (r *realNVMeUtil) Disconnect(ctx context.Context, nqn string) error {
	cmd := exec.CommandContext(ctx, "nvme", "disconnect", "-n", nqn)
	out, err := cmd.CombinedOutput()
	if err != nil {
		outStr := string(out)
		if strings.Contains(outStr, "No such") || strings.Contains(outStr, "not found") {
			return nil
		}
		return fmt.Errorf("nvme disconnect: %s: %w", outStr, err)
	}
	return nil
}

func (r *realNVMeUtil) IsConnected(ctx context.Context, nqn string) (bool, error) {
	doc, err := nvmeListSubsystems(ctx)
	if err != nil {
		return false, err
	}
	return nvmeSubsystemPathCount(doc, nqn) > 0, nil
}

func (r *realNVMeUtil) GetDeviceByNQN(ctx context.Context, nqn string) (string, error) {
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("timeout waiting for NVMe namespace for NQN %s", nqn)
		case <-ticker.C:
			devices, err := nvmeNamespaceDevices(nqn)
			if err != nil {
				continue
			}
			if len(devices) > 0 {
				return devices[0], nil
			}
		}
	}
}

func splitHostPort(addr string) (string, string, error) {
	parts := strings.LastIndex(addr, ":")
	if parts <= 0 || parts == len(addr)-1 {
		return "", "", fmt.Errorf("nvme address %q must be host:port", addr)
	}
	return addr[:parts], addr[parts+1:], nil
}

func nvmeListSubsystems(ctx context.Context) (any, error) {
	cmd := exec.CommandContext(ctx, "nvme", "list-subsys", "-o", "json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("nvme list-subsys: %s: %w", string(out), err)
	}
	var doc any
	if err := json.Unmarshal(out, &doc); err != nil {
		return nil, fmt.Errorf("parse nvme list-subsys: %w", err)
	}
	return doc, nil
}

func nvmeSubsystemPathCount(doc any, nqn string) int {
	total := 0
	for _, sub := range iterNVMeSubsystems(doc) {
		if got, _ := sub["NQN"].(string); got != nqn {
			continue
		}
		if paths, ok := sub["Paths"].([]any); ok {
			total += len(paths)
		}
	}
	return total
}

func iterNVMeSubsystems(node any) []map[string]any {
	var out []map[string]any
	switch v := node.(type) {
	case map[string]any:
		if _, hasNQN := v["NQN"]; hasNQN {
			if _, hasPaths := v["Paths"]; hasPaths {
				out = append(out, v)
			}
		}
		if subs, ok := v["Subsystems"].([]any); ok {
			for _, sub := range subs {
				out = append(out, iterNVMeSubsystems(sub)...)
			}
		}
	case []any:
		for _, item := range v {
			out = append(out, iterNVMeSubsystems(item)...)
		}
	}
	return out
}

func nvmeNamespaceDevices(nqn string) ([]string, error) {
	nqnFiles, err := filepath.Glob("/sys/class/nvme-subsystem/*/subsysnqn")
	if err != nil {
		return nil, err
	}
	nsPattern := regexp.MustCompile(`^nvme[0-9]+n[0-9]+$`)
	var devices []string
	for _, nqnFile := range nqnFiles {
		raw, err := os.ReadFile(nqnFile)
		if err != nil || strings.TrimSpace(string(raw)) != nqn {
			continue
		}
		entries, err := os.ReadDir(filepath.Dir(nqnFile))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			if nsPattern.MatchString(name) {
				devices = append(devices, filepath.Join("/dev", name))
			}
		}
	}
	return devices, nil
}

type realMountUtil struct{}

func (r *realMountUtil) FormatAndMount(ctx context.Context, device, target, fsType string) error {
	formatted, err := r.isFormatted(ctx, device)
	if err != nil {
		return err
	}
	if !formatted {
		cmd := exec.CommandContext(ctx, "mkfs."+fsType, device)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("mkfs.%s: %s: %w", fsType, string(out), err)
		}
	}
	cmd := exec.CommandContext(ctx, "mount", "-t", fsType, device, target)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount: %s: %w", string(out), err)
	}
	return nil
}

func (r *realMountUtil) BindMount(ctx context.Context, source, target string, readOnly bool) error {
	cmd := exec.CommandContext(ctx, "mount", "--bind", source, target)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bind mount: %s: %w", string(out), err)
	}
	if readOnly {
		cmd = exec.CommandContext(ctx, "mount", "-o", "remount,bind,ro", target)
		out, err = cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("remount ro: %s: %w", string(out), err)
		}
	}
	return nil
}

func (r *realMountUtil) Unmount(ctx context.Context, target string) error {
	cmd := exec.CommandContext(ctx, "umount", target)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "not mounted") {
			return nil
		}
		return fmt.Errorf("umount: %s: %w", string(out), err)
	}
	return nil
}

func (r *realMountUtil) IsMounted(ctx context.Context, target string) (bool, error) {
	cmd := exec.CommandContext(ctx, "mountpoint", "-q", target)
	if err := cmd.Run(); err != nil {
		return false, nil
	}
	return true, nil
}

func (r *realMountUtil) isFormatted(ctx context.Context, device string) (bool, error) {
	cmd := exec.CommandContext(ctx, "blkid", "-p", device)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == 2 {
			return false, nil
		}
		return false, fmt.Errorf("blkid: %s: %w", string(out), err)
	}
	return strings.Contains(string(out), "TYPE="), nil
}
