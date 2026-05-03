package csi

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func NewDefaultNodeServer(nodeID, iqnPrefix string) *NodeServer {
	return NewNodeServer(NodeConfig{
		NodeID:    nodeID,
		IQNPrefix: iqnPrefix,
		ISCSIUtil: &realISCSIUtil{},
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
