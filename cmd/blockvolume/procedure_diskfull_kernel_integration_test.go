//go:build subprocess && chunkstore_integration && linux

package main_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// What a real Linux guest sees when the local disk under a volume's WAL fills
// (V1-SPEC r2.60, issue 484): the kernel NVMe/TCP initiator connects to the
// volume's own target on this host, the durable root's disk fills, and dd
// writes with O_DIRECT. The test records, for the operator guide, whether dd
// stalls or fails and with what, what the kernel logs, and what happens once
// the space is freed. It records; it asserts only that the kernel connects
// and the namespace appears (data survival is TestProcedureLocalDiskFull's).
//
// Environment: as TestProcedureLocalDiskFull, plus SWBLOCK_DISKFULL_KERNEL=1
// (root, the nvme-tcp module, a disposable host).
func TestProcedureLocalDiskFullKernelInitiator(t *testing.T) {
	if os.Getenv("SWBLOCK_DISKFULL_KERNEL") != "1" {
		t.Skip("requires SWBLOCK_DISKFULL_KERNEL=1 on a disposable root host")
	}
	root := os.Getenv("SWBLOCK_DISKFULL_ROOT")
	if root == "" {
		t.Fatal("SWBLOCK_DISKFULL_ROOT must be set")
	}
	env := startHandoffEnvWith(t, 1, nil)
	if d := os.Getenv("SWBLOCK_REPLAY_LOGS"); d != "" {
		t.Cleanup(func() { copyLogs(t, env.h.dir, d) })
	}
	h := env.hosts["s1"]
	h.root = filepath.Join(root, "vol")
	env.hosts["s1"] = h
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")

	sh := func(cmd string) string {
		out, err := exec.Command("bash", "-c", cmd).CombinedOutput()
		if err != nil {
			t.Logf("KERNEL: %q: %v", cmd, err)
		}
		return strings.TrimSpace(string(out))
	}
	if out := sh("modprobe nvme-tcp && grep -c . /dev/nvme-fabrics >/dev/null; echo ok"); !strings.HasSuffix(out, "ok") {
		t.Fatalf("nvme-tcp: %s", out)
	}
	sh("dmesg -C")
	// The namespace device is the one /sys/block entry the connect adds (a
	// multipath head nvmeXnY or a plain one; per-path nvmeXcYnZ is hidden).
	blocks := func() map[string]bool {
		m := map[string]bool{}
		ns, _ := filepath.Glob("/sys/block/nvme*n*")
		for _, n := range ns {
			if b := filepath.Base(n); !strings.Contains(strings.TrimPrefix(b, "nvme"), "c") {
				m[b] = true
			}
		}
		return m
	}
	before := blocks()
	host, port, _ := strings.Cut(env.hosts["s1"].nvme, ":")
	connect := fmt.Sprintf("transport=tcp,traddr=%s,trsvcid=%s,nqn=%s,hostnqn=nqn.2026-11.io.seaweedfs:diskfull-kernel", host, port, plannedHandoffNQN)
	if out := sh("echo " + connect + " > /dev/nvme-fabrics && echo connected"); !strings.Contains(out, "connected") {
		t.Fatalf("kernel connect: %s", out)
	}
	var ctrl, dev string
	for end := time.Now().Add(30 * time.Second); dev == "" || ctrl == ""; time.Sleep(500 * time.Millisecond) {
		ctrls, _ := filepath.Glob("/sys/class/nvme/nvme*")
		for _, c := range ctrls {
			if b, _ := os.ReadFile(filepath.Join(c, "subsysnqn")); strings.TrimSpace(string(b)) == plannedHandoffNQN {
				ctrl = c
			}
		}
		for b := range blocks() {
			if !before[b] {
				dev = "/dev/" + b
			}
		}
		if time.Now().After(end) {
			t.Fatalf("no namespace device for %s (controller %q, device %q)", plannedHandoffNQN, ctrl, dev)
		}
	}
	t.Cleanup(func() { _ = os.WriteFile(filepath.Join(ctrl, "delete_controller"), []byte("1"), 0o200) })
	t.Logf("KERNEL: %s connected as %s (%s); multipath=%s io_timeout=%ss max_retries=%s", plannedHandoffNQN, dev, ctrl,
		sh("cat /sys/module/nvme_core/parameters/multipath"), sh("cat /sys/module/nvme_core/parameters/io_timeout"),
		sh("cat /sys/module/nvme_core/parameters/max_retries"))

	fill := fillDisk(t, root, 2<<20)
	// 128 MiB of O_DIRECT 4 KiB writes, more than the 2 MiB left and the WAL.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	dd := exec.CommandContext(ctx, "dd", "if=/dev/urandom", "of="+dev, "bs=4k", "count=32768", "oflag=direct", "status=progress")
	var ddOut strings.Builder
	dd.Stdout, dd.Stderr = &ddOut, &ddOut
	start := time.Now()
	if err := dd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- dd.Wait() }()
	var ddErr error
	finished := false
	select {
	case ddErr = <-done:
		finished = true
		t.Logf("KERNEL: with the disk full, dd ended after %s: err=%v\n%s", time.Since(start).Round(time.Second), ddErr, ddOut.String())
	case <-time.After(4 * time.Minute):
		t.Logf("KERNEL: with the disk full, dd is still running after 4m (the guest stalls); its progress:\n%s", ddOut.String())
	}
	t.Logf("KERNEL: dmesg while full:\n%s", sh("dmesg -T | grep -iE 'nvme|blk_update|I/O error|Buffer I/O' | tail -40"))
	fill.free(t)
	freed := time.Now()
	if !finished {
		select {
		case ddErr = <-done:
			t.Logf("KERNEL: after freeing the space, dd finished %s later: err=%v\n%s", time.Since(freed).Round(time.Second), ddErr, ddOut.String())
		case <-time.After(4 * time.Minute):
			t.Logf("KERNEL: dd still running 4m after the space was freed")
			cancel()
			<-done
		}
	} else {
		// After a failure: does a new write succeed once the space is free?
		t.Logf("KERNEL: after freeing, a new O_DIRECT write: %s", sh("dd if=/dev/urandom of="+dev+" bs=4k count=256 oflag=direct 2>&1 | tail -1"))
	}
	t.Logf("KERNEL: dmesg at the end:\n%s", sh("dmesg -T | grep -iE 'nvme|blk_update|I/O error|Buffer I/O' | tail -40"))
	t.Logf("KERNEL: holder log ENOSPC lines: %d; frontend ready: %v", strings.Count(readFile(holder.log), "no space left on device"), env.frontendReadyAt("s1"))
	fmt.Println("DISKFULL_KERNEL_RECORDED")
}

func readFile(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}
