//go:build linux

package nvmerdma

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend/nbd"
)

const defaultConfigFSRoot = "/sys/kernel/config/nvmet"

func Implemented() bool { return true }

type linuxTarget struct {
	nbd       *nbd.Device
	configfs  *configFSTarget
	closeOnce sync.Once
	closeErr  error
}

func startPlatformTarget(cfg TargetConfig, backend nbd.Backend) (platformTarget, string, error) {
	if os.Geteuid() != 0 {
		return nil, "", errors.New("nvmerdma: kernel target requires root")
	}
	host, service, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return nil, "", fmt.Errorf("nvmerdma: invalid listen address %q: %w", cfg.Listen, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() || ip.IsLoopback() {
		return nil, "", fmt.Errorf("nvmerdma: RDMA listen address must use a non-loopback IP: %q", host)
	}
	port, err := strconv.Atoi(service)
	if err != nil || port < 1 || port > 65535 {
		return nil, "", fmt.Errorf("nvmerdma: invalid service port %q", service)
	}
	if cfg.SubsysNQN == "" || filepath.Base(cfg.SubsysNQN) != cfg.SubsysNQN {
		return nil, "", fmt.Errorf("nvmerdma: invalid subsystem NQN %q", cfg.SubsysNQN)
	}
	if cfg.NSID == 0 {
		return nil, "", errors.New("nvmerdma: NSID must be non-zero")
	}
	if err := modprobe("nvmet-rdma"); err != nil {
		return nil, "", err
	}
	if _, err := os.Stat(defaultConfigFSRoot); err != nil {
		return nil, "", fmt.Errorf("nvmerdma: configfs target unavailable: %w", err)
	}

	device, err := nbd.Start(nbd.Config{
		Backend:   backend,
		BlockSize: cfg.BlockSize,
		Size:      cfg.VolumeSize,
	})
	if err != nil {
		return nil, "", fmt.Errorf("nvmerdma: start backend bridge: %w", err)
	}
	configTarget := &configFSTarget{
		root:       defaultConfigFSRoot,
		nqn:        cfg.SubsysNQN,
		nsid:       cfg.NSID,
		portID:     port,
		address:    host,
		service:    service,
		devicePath: device.Path(),
	}
	if err := configTarget.Start(); err != nil {
		_ = device.Close()
		return nil, "", fmt.Errorf("nvmerdma: configure kernel target: %w", err)
	}
	return &linuxTarget{nbd: device, configfs: configTarget}, cfg.Listen, nil
}

func (t *linuxTarget) Close() error {
	t.closeOnce.Do(func() {
		if err := t.configfs.Close(); err != nil {
			t.closeErr = err
		}
		if err := t.nbd.Close(); err != nil && t.closeErr == nil {
			t.closeErr = err
		}
	})
	return t.closeErr
}

func modprobe(module string) error {
	out, err := exec.Command("modprobe", module).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvmerdma: modprobe %s: %w: %s", module, err, strings.TrimSpace(string(out)))
	}
	return nil
}

type configFSTarget struct {
	root       string
	nqn        string
	nsid       uint32
	portID     int
	address    string
	service    string
	devicePath string

	started bool
}

func (t *configFSTarget) Start() (err error) {
	subsystem := filepath.Join(t.root, "subsystems", t.nqn)
	namespace := filepath.Join(subsystem, "namespaces", strconv.FormatUint(uint64(t.nsid), 10))
	port := filepath.Join(t.root, "ports", strconv.Itoa(t.portID))
	link := filepath.Join(port, "subsystems", t.nqn)
	if _, err := os.Stat(subsystem); err == nil {
		return fmt.Errorf("subsystem already exists: %s", t.nqn)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := os.Stat(port); err == nil {
		return fmt.Errorf("port ID already exists: %d", t.portID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	defer func() {
		if err != nil {
			_ = t.cleanup()
		}
	}()

	if err = os.Mkdir(subsystem, 0o755); err != nil {
		return err
	}
	if err = writeConfig(filepath.Join(subsystem, "attr_allow_any_host"), "1"); err != nil {
		return err
	}
	if err = os.MkdirAll(namespace, 0o755); err != nil {
		return err
	}
	if err = writeConfig(filepath.Join(namespace, "device_path"), t.devicePath); err != nil {
		return err
	}
	if err = writeConfig(filepath.Join(namespace, "enable"), "1"); err != nil {
		return err
	}
	if err = os.Mkdir(port, 0o755); err != nil {
		return err
	}
	for _, attr := range []struct{ name, value string }{
		{name: "addr_traddr", value: t.address},
		{name: "addr_trtype", value: "rdma"},
		{name: "addr_trsvcid", value: t.service},
		{name: "addr_adrfam", value: "ipv4"},
	} {
		if err = writeConfig(filepath.Join(port, attr.name), attr.value); err != nil {
			return err
		}
	}
	if err = os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		return err
	}
	if err = os.Symlink(subsystem, link); err != nil {
		return err
	}
	t.started = true
	return nil
}

func (t *configFSTarget) Close() error {
	if !t.started {
		return nil
	}
	t.started = false
	return t.cleanup()
}

func (t *configFSTarget) cleanup() error {
	subsystem := filepath.Join(t.root, "subsystems", t.nqn)
	namespace := filepath.Join(subsystem, "namespaces", strconv.FormatUint(uint64(t.nsid), 10))
	port := filepath.Join(t.root, "ports", strconv.Itoa(t.portID))
	link := filepath.Join(port, "subsystems", t.nqn)
	var errs []error
	if err := os.Remove(link); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	if err := writeConfig(filepath.Join(namespace, "enable"), "0"); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	for _, path := range []string{namespace, port, subsystem} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func writeConfig(path, value string) error {
	return os.WriteFile(path, []byte(value), 0o644)
}
