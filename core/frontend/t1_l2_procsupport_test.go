// Ownership: sw test-support (per test-spec §9: non-test helper
// files are sw-owned). Process wrappers used by
// t1_l2_subprocess_test.go. Intentionally self-contained — this
// test binary does not link against core/host's test package.
package frontend_test

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// testProc wraps one subprocess: stdout is tee'd into an
// in-memory buffer + a log file so tests can probe ready lines
// and post-mortems on failure.
type testProc struct {
	label string
	cmd   *exec.Cmd

	mu        sync.Mutex
	stdoutBuf strings.Builder
	addr      string
	statusAddr string
}

func newTestProc(t *testing.T, label, bin string, args ...string) *testProc {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Stderr = os.Stderr
	return &testProc{label: label, cmd: cmd}
}

func (p *testProc) start(t *testing.T) {
	t.Helper()
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("%s: stdout pipe: %v", p.label, err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("%s: start: %v", p.label, err)
	}
	go p.pump(stdout)
}

// pump drains stdout into the shared buffer and also scans for
// JSON ready lines to populate addr / statusAddr.
func (p *testProc) pump(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 4096), 64*1024)
	for sc.Scan() {
		line := sc.Text()
		p.mu.Lock()
		p.stdoutBuf.WriteString(line + "\n")
		p.mu.Unlock()

		// Attempt JSON decode; ignore non-JSON lines silently.
		var probe struct {
			Component  string `json:"component"`
			Phase      string `json:"phase"`
			Addr       string `json:"addr"`
			StatusAddr string `json:"status_addr"`
		}
		if err := json.Unmarshal([]byte(line), &probe); err == nil {
			p.mu.Lock()
			if probe.Addr != "" {
				p.addr = probe.Addr
			}
			if probe.StatusAddr != "" {
				p.statusAddr = probe.StatusAddr
			}
			p.mu.Unlock()
		}
	}
}

func (p *testProc) waitAddr(t *testing.T, expectedPhase string, deadline time.Duration) string {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		p.mu.Lock()
		addr := p.addr
		p.mu.Unlock()
		if addr != "" {
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s did not emit %q ready line within %v; stdout=%q", p.label, expectedPhase, deadline, p.stdoutSnapshot())
	return ""
}

func (p *testProc) readStatusAddr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statusAddr
}

func (p *testProc) stdoutSnapshot() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stdoutBuf.String()
}

func (p *testProc) stop() {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = p.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
}

// runtimeGOOS is a tiny indirection so the main test file can
// stay free of a runtime import (readable, not essential).
func runtimeGOOS() string { return runtime.GOOS }
