//go:build chunkstore_integration

package chunkstore

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// On a real filer (stock weed): a pool upload cut short leaves no entry or a
// complete one. A child process uploads pool objects through the production
// path (layout.EnsureObject over LayoutStore: blob upload, then the exclusive
// entry create) and is killed (SIGKILL) at a random point of a random
// upload, round after round. After every kill each object the child began is
// either absent or present with exactly the bytes its name hashes, and a new
// session's EnsureObject then completes it. An entry naming partial bytes
// would be reused by every later upload of that chunk (EnsureObject reuses
// an existing entry), so this is checked at the store, before any reader.
//
//	CHUNKSTORE_WEED=/path/to/weed go test -tags chunkstore_integration -run CutUpload -v ./core/frontend/chunkstore/
//
// CHUNKSTORE_CUT_ROUNDS (default 40) sets the number of kills.

const cutHelperEnv = "CHUNKSTORE_CUT_HELPER"

// cutObject is upload i of a round: 2 MiB of bytes fixed by (seed, i).
func cutObject(seed int64, i int) (string, []byte) {
	data := make([]byte, 2<<20)
	rand.New(rand.NewSource(seed*1000003 + int64(i))).Read(data)
	return layout.NameChunk(uint64(i), data).Path(), data
}

// TestIntegration_CutUploadHelper is the child: it uploads objects until it
// is killed, announcing each before it starts. It does nothing unless run as
// the child.
func TestIntegration_CutUploadHelper(t *testing.T) {
	spec := os.Getenv(cutHelperEnv)
	if spec == "" {
		t.Skip("child of TestIntegration_CutUploadLeavesNoEntryOrACompleteOne only")
	}
	parts := strings.Split(spec, "|")
	if len(parts) != 5 {
		t.Fatalf("bad %s", cutHelperEnv)
	}
	filerGRPC, root, session := parts[0], parts[1], parts[4]
	id, err := layout.ParseVolumeID(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	seed, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := DialSWFS(SWFSConfig{FilerGRPC: filerGRPC, Collection: "swblock", AssignPath: root + "/"})
	if err != nil {
		t.Fatal(err)
	}
	f, err := NewLayoutStore(cl, cl, root, id)
	if err != nil {
		t.Fatal(err)
	}
	out := bufio.NewWriter(os.Stdout)
	for i := 0; ; i++ {
		rel, data := cutObject(seed, i)
		fmt.Fprintf(out, "CUT-BEGIN %d\n", i)
		out.Flush()
		if err := layout.EnsureObject(context.Background(), f, rel, data, session); err != nil {
			fmt.Fprintf(out, "CUT-ERR %d %v\n", i, err)
			out.Flush()
			os.Exit(3)
		}
	}
}

func TestIntegration_CutUploadLeavesNoEntryOrACompleteOne(t *testing.T) {
	c := startWeed(t)
	rounds := 40
	if v, err := strconv.Atoi(os.Getenv("CHUNKSTORE_CUT_ROUNDS")); err == nil && v > 0 {
		rounds = v
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rounds)*30*time.Second+2*time.Minute)
	defer cancel()
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	root := "/swblock/objs/" + layout.VolumeDir("v1", id)
	cl := c.client(t, root+"/")
	f, err := NewLayoutStore(cl, cl, root, id)
	if err != nil {
		t.Fatal(err)
	}
	pick := rand.New(rand.NewSource(time.Now().UnixNano()))
	var absent, complete int
	for round := 0; round < rounds; round++ {
		seed := int64(round + 1)
		target := pick.Intn(4)                                  // the upload to cut
		delay := time.Duration(pick.Intn(40)) * time.Millisecond // into that upload
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIntegration_CutUploadHelper$", "-test.v")
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s|%s|%s|%d|child-%d", cutHelperEnv, c.filerGRPC, root, id, seed, round))
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		// Every line the child writes is read until its stdout closes, so an
		// upload it began during the delay or the kill is counted, and a
		// CUT-ERR at any point fails the round.
		lines := make(chan string, 64)
		go func() {
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				lines <- sc.Text()
			}
			close(lines)
		}()
		began, cutErr := -1, ""
		note := func(line string) {
			if strings.HasPrefix(line, "CUT-ERR") && cutErr == "" {
				cutErr = line
			}
			if n, ok := strings.CutPrefix(line, "CUT-BEGIN "); ok {
				if i, err := strconv.Atoi(n); err == nil && i > began {
					began = i
				}
			}
		}
		for line := range lines {
			note(line)
			if began >= target || cutErr != "" {
				break
			}
		}
		time.Sleep(delay)
		_ = cmd.Process.Kill()
		for line := range lines { // to EOF: the child is gone
			note(line)
		}
		waitErr := cmd.Wait()
		if cutErr != "" {
			t.Fatalf("round %d: the child's upload failed on its own, not by the cut: %s", round, cutErr)
		}
		ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
			t.Fatalf("round %d: the child did not die by the cut (SIGKILL): %v; stderr: %s", round, waitErr, stderr.String())
		}
		if began < 0 {
			t.Fatalf("round %d: the child began no upload: %s", round, stderr.String())
		}
		// Objects 0..last: every one the child began is absent (only the last,
		// the one in flight) or exactly the bytes its name hashes; one it never
		// began is absent. At least the first four are checked every round.
		last := began
		if last < 3 {
			last = 3
		}
		for i := 0; i <= last; i++ {
			rel, data := cutObject(seed, i)
			_, found, err := f.Stat(ctx, rel)
			if err != nil {
				t.Fatalf("round %d object %d: stat after the cut: %v", round, i, err)
			}
			if !found {
				if i < began {
					t.Fatalf("round %d object %d: a completed upload left no entry", round, i)
				}
				if i == began {
					absent++
				}
				continue
			}
			if i > began {
				t.Fatalf("round %d object %d: an entry exists for an upload the child never began", round, i)
			}
			raw, ok, err := f.Fetch(ctx, path.Join(root, rel))
			if err != nil || !ok {
				t.Fatalf("round %d object %d: entry present but its bytes do not read: %v %v", round, i, ok, err)
			}
			if !bytes.Equal(raw, data) || layout.Sum(raw) != layout.NameChunk(uint64(i), data).Hash {
				t.Fatalf("round %d object %d: the cut left an entry with %d bytes that do not hash to its name (want %d)", round, i, len(raw), len(data))
			}
			if i == began {
				complete++
			}
		}
		// A new session completes every object 0..last, whatever the cut left.
		for i := 0; i <= last; i++ {
			rel, data := cutObject(seed, i)
			if err := layout.EnsureObject(ctx, f, rel, data, fmt.Sprintf("recover-%d", round)); err != nil {
				t.Fatalf("round %d object %d: recovery upload: %v", round, i, err)
			}
			raw, ok, err := f.Fetch(ctx, path.Join(root, rel))
			if err != nil || !ok || !bytes.Equal(raw, data) {
				t.Fatalf("round %d object %d: after recovery the entry does not read back its bytes: %v %v", round, i, ok, err)
			}
		}
	}
	t.Logf("%d cuts: the cut upload left no entry %d times and a complete entry %d times", rounds, absent, complete)
	if absent == 0 {
		t.Error("no cut landed before the entry was created: the cut points did not reach mid-upload")
	}
}
