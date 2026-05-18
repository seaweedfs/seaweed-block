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
	return NewDefaultNodeServerWithLookup(nodeID, iqnPrefix, nil)
}

func NewDefaultNodeServerWithLookup(nodeID, iqnPrefix string, lookup PublishTargetLookup) *NodeServer {
	return NewDefaultNodeServerWithLookupAndEventReporter(nodeID, iqnPrefix, lookup, nil)
}

func NewDefaultNodeServerWithLookupAndEventReporter(nodeID, iqnPrefix string, lookup PublishTargetLookup, reporter EventReporter) *NodeServer {
	return NewNodeServer(NodeConfig{
		NodeID:        nodeID,
		IQNPrefix:     iqnPrefix,
		ISCSIUtil:     &realISCSIUtil{},
		NVMeUtil:      &realNVMeUtil{},
		MountUtil:     &realMountUtil{},
		Lookup:        lookup,
		EventReporter: reporter,
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

func (r *realISCSIUtil) GetDeviceByIQN(ctx context.Context, iqn, portal string) (string, error) {
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("timeout waiting for device for IQN %s portal %s", iqn, portal)
		case <-ticker.C:
			matches, err := filepath.Glob(fmt.Sprintf("/dev/disk/by-path/*%s*", iqn))
			if err != nil {
				continue
			}
			for _, match := range matches {
				if strings.Contains(match, "-part") {
					continue
				}
				if portal != "" && !iscsiByPathMatchesPortal(match, portal) {
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

func (r *realISCSIUtil) GetMultipathDeviceByIQN(ctx context.Context, iqn string, minPaths int) (string, error) {
	if minPaths < 2 {
		return "", fmt.Errorf("multipath requires at least two paths, got %d", minPaths)
	}
	deadline := time.After(20 * time.Second)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("timeout waiting for multipath device for IQN %s paths >= %d", iqn, minPaths)
		case <-ticker.C:
			_ = refreshMultipathMaps(ctx)
			dev, paths, err := iscsiMultipathDeviceForIQN(ctx, iqn)
			if err != nil || dev == "" || paths < minPaths {
				continue
			}
			return dev, nil
		}
	}
}

func refreshMultipathMaps(ctx context.Context) error {
	cmdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, "multipath", "-r")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("multipath -r: %s: %w", string(out), err)
	}
	return nil
}

func iscsiMultipathDeviceForIQN(ctx context.Context, iqn string) (string, int, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, "multipath", "-ll")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", 0, fmt.Errorf("multipath -ll: %s: %w", string(out), err)
	}
	return parseISCSIMultipathDeviceForIQN(string(out), iqn, iscsiRawDevicesForIQN(iqn))
}

func iscsiRawDevicesForIQN(iqn string) map[string]struct{} {
	out := map[string]struct{}{}
	matches, err := filepath.Glob(fmt.Sprintf("/dev/disk/by-path/*%s*", iqn))
	if err != nil {
		return out
	}
	for _, match := range matches {
		if strings.Contains(match, "-part") {
			continue
		}
		dev, err := filepath.EvalSymlinks(match)
		if err != nil {
			continue
		}
		base := filepath.Base(dev)
		if base != "" {
			out[base] = struct{}{}
		}
	}
	return out
}

func parseISCSIMultipathDeviceForIQN(out, iqn string, rawDevices map[string]struct{}) (string, int, error) {
	lines := strings.Split(out, "\n")
	for i, line := range lines {
		if !multipathLooksLikeMapHeader(line) {
			continue
		}
		if !strings.Contains(line, iqn) && !multipathBlockContainsRawDevice(lines, i+1, rawDevices) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		device := "/dev/mapper/" + fields[0]
		paths := 0
		for j := i + 1; j < len(lines); j++ {
			next := lines[j]
			if strings.TrimSpace(next) == "" {
				break
			}
			if multipathLooksLikeMapHeader(next) {
				break
			}
			if strings.Contains(next, " active") || strings.Contains(next, " ready") || strings.Contains(next, " running") {
				paths++
			}
		}
		if paths == 0 {
			paths = 1
		}
		return device, paths, nil
	}
	return "", 0, nil
}

func multipathLooksLikeMapHeader(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "|") || strings.HasPrefix(trimmed, "`") {
		return false
	}
	return strings.Contains(trimmed, "(") && strings.Contains(trimmed, ")")
}

func multipathBlockContainsRawDevice(lines []string, start int, rawDevices map[string]struct{}) bool {
	if len(rawDevices) == 0 {
		return false
	}
	for j := start; j < len(lines); j++ {
		next := lines[j]
		if strings.TrimSpace(next) == "" {
			break
		}
		if multipathLooksLikeMapHeader(next) {
			break
		}
		for dev := range rawDevices {
			if multipathLineContainsDevice(next, dev) {
				return true
			}
		}
	}
	return false
}

func multipathLineContainsDevice(line, dev string) bool {
	for _, field := range strings.Fields(line) {
		if field == dev {
			return true
		}
	}
	return false
}

func (r *realISCSIUtil) IsLoggedIn(ctx context.Context, iqn, portal string) (bool, error) {
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
	return iscsiSessionContainsTarget(string(out), iqn, portal), nil
}

func iscsiSessionContainsTarget(out, iqn, portal string) bool {
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, iqn) {
			continue
		}
		if portal == "" || strings.Contains(line, portal) || strings.Contains(line, iscsiPortalHostPortForByPath(portal)) {
			return true
		}
	}
	return false
}

func iscsiByPathMatchesPortal(path, portal string) bool {
	if portal == "" {
		return true
	}
	return strings.Contains(path, portal) || strings.Contains(path, iscsiPortalHostPortForByPath(portal))
}

func iscsiPortalHostPortForByPath(portal string) string {
	return strings.ReplaceAll(portal, ":", "-")
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
