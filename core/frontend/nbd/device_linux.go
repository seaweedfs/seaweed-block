//go:build linux

package nbd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	nbdSetSock       = uintptr(0xab00)
	nbdSetBlockSize  = uintptr(0xab01)
	nbdDoIt          = uintptr(0xab03)
	nbdClearSock     = uintptr(0xab04)
	nbdClearQueue    = uintptr(0xab05)
	nbdSetSizeBlocks = uintptr(0xab07)
	nbdDisconnect    = uintptr(0xab08)
	nbdSetFlags      = uintptr(0xab0a)

	nbdFlagHasFlags  = uintptr(1 << 0)
	nbdFlagSendFlush = uintptr(1 << 2)
	nbdFlagSendFUA   = uintptr(1 << 3)
)

type Config struct {
	Backend   Backend
	BlockSize uint32
	Size      uint64
	Device    string
}

type Device struct {
	path       string
	fd         int
	serverFile *os.File
	cancel     context.CancelFunc
	doItDone   chan error
	serveDone  chan error
	closeOnce  sync.Once
	closeErr   error
}

func Start(cfg Config) (*Device, error) {
	if cfg.Backend == nil {
		return nil, errors.New("nbd: backend required")
	}
	if cfg.BlockSize == 0 || cfg.Size == 0 || cfg.Size%uint64(cfg.BlockSize) != 0 {
		return nil, fmt.Errorf("nbd: invalid geometry blockSize=%d size=%d", cfg.BlockSize, cfg.Size)
	}
	if err := runModprobe("nbd", "max_part=0", "nbds_max=16"); err != nil {
		return nil, err
	}

	path, fd, err := openDevice(cfg.Device)
	if err != nil {
		return nil, err
	}
	cleanupFD := true
	defer func() {
		if cleanupFD {
			_ = unix.Close(fd)
		}
	}()

	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("nbd: socketpair: %w", err)
	}
	kernelFD, serverFD := sockets[0], sockets[1]
	defer func() {
		if kernelFD >= 0 {
			_ = unix.Close(kernelFD)
		}
		if serverFD >= 0 {
			_ = unix.Close(serverFD)
		}
	}()

	if err := ioctlArg(fd, nbdSetBlockSize, uintptr(cfg.BlockSize)); err != nil {
		return nil, fmt.Errorf("nbd: set block size: %w", err)
	}
	if err := ioctlArg(fd, nbdSetSizeBlocks, uintptr(cfg.Size/uint64(cfg.BlockSize))); err != nil {
		return nil, fmt.Errorf("nbd: set size blocks: %w", err)
	}
	if err := ioctlArg(fd, nbdSetFlags, nbdFlagHasFlags|nbdFlagSendFlush|nbdFlagSendFUA); err != nil {
		return nil, fmt.Errorf("nbd: set flags: %w", err)
	}
	if err := ioctlArg(fd, nbdSetSock, uintptr(kernelFD)); err != nil {
		return nil, fmt.Errorf("nbd: set socket: %w", err)
	}
	_ = unix.Close(kernelFD)
	kernelFD = -1

	serverFile := os.NewFile(uintptr(serverFD), "sw-block-nbd")
	serverFD = -1
	ctx, cancel := context.WithCancel(context.Background())
	d := &Device{
		path:       path,
		fd:         fd,
		serverFile: serverFile,
		cancel:     cancel,
		doItDone:   make(chan error, 1),
		serveDone:  make(chan error, 1),
	}
	go func() {
		d.serveDone <- protocolServer{backend: cfg.Backend, size: cfg.Size}.serve(ctx, serverFile)
	}()
	go func() {
		d.doItDone <- ioctlArg(fd, nbdDoIt, 0)
	}()
	if err := waitActive(path, 2*time.Second); err != nil {
		_ = d.Close()
		return nil, err
	}

	cleanupFD = false
	return d, nil
}

func (d *Device) Path() string {
	if d == nil {
		return ""
	}
	return d.path
}

func (d *Device) Close() error {
	if d == nil {
		return nil
	}
	d.closeOnce.Do(func() {
		d.cancel()
		_ = ioctlArg(d.fd, nbdDisconnect, 0)
		_ = d.serverFile.Close()
		d.waitDone(d.doItDone)
		d.waitDone(d.serveDone)
		_ = ioctlArg(d.fd, nbdClearQueue, 0)
		_ = ioctlArg(d.fd, nbdClearSock, 0)
		if err := unix.Close(d.fd); err != nil && d.closeErr == nil {
			d.closeErr = err
		}
	})
	return d.closeErr
}

func (d *Device) waitDone(ch <-chan error) {
	select {
	case err := <-ch:
		if err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.EPIPE) && !errors.Is(err, io.EOF) && d.closeErr == nil {
			d.closeErr = err
		}
	case <-time.After(5 * time.Second):
		if d.closeErr == nil {
			d.closeErr = errors.New("nbd: timed out stopping device")
		}
	}
}

func runModprobe(module string, args ...string) error {
	argv := append([]string{module}, args...)
	out, err := exec.Command("modprobe", argv...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nbd: modprobe %s: %w: %s", module, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func openDevice(requested string) (string, int, error) {
	if requested != "" {
		fd, err := unix.Open(requested, unix.O_RDWR|unix.O_EXCL|unix.O_CLOEXEC, 0)
		if err != nil {
			return "", -1, fmt.Errorf("nbd: open %s: %w", requested, err)
		}
		return requested, fd, nil
	}

	paths, err := filepath.Glob("/dev/nbd*")
	if err != nil {
		return "", -1, fmt.Errorf("nbd: list devices: %w", err)
	}
	sort.Slice(paths, func(i, j int) bool { return nbdNumber(paths[i]) < nbdNumber(paths[j]) })
	for _, path := range paths {
		fd, err := unix.Open(path, unix.O_RDWR|unix.O_EXCL|unix.O_CLOEXEC, 0)
		if err == nil {
			return path, fd, nil
		}
		if !errors.Is(err, syscall.EBUSY) {
			return "", -1, fmt.Errorf("nbd: open %s: %w", path, err)
		}
	}
	return "", -1, errors.New("nbd: no free /dev/nbd device")
}

func nbdNumber(path string) int {
	n, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(path), "nbd"))
	if err != nil {
		return int(^uint(0) >> 1)
	}
	return n
}

func waitActive(path string, timeout time.Duration) error {
	pidPath := filepath.Join("/sys/block", filepath.Base(path), "pid")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(pidPath)
		if err == nil && strings.TrimSpace(string(data)) != "" {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("nbd: device %s did not become active", path)
}

func ioctlArg(fd int, request, arg uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), request, arg)
	if errno != 0 {
		return errno
	}
	return nil
}
