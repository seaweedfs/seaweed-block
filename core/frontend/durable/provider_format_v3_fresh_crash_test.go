//go:build swblock_format_v3_test

package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func freshCrashFixture(t *testing.T, path string) *formatV3Fixture {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var img formatV3Image
	if err = json.Unmarshal(raw, &img); err != nil {
		t.Fatal(err)
	}
	return restoreFormatV3Image(t, img)
}

// The child exits after presence is durable but before the fresh marker is
// removed. No Close or provider cleanup runs on the successful crash path.
func TestDurableProviderV3_FreshMarkerCrashKeepsCompletedBASE(t *testing.T) {
	freshMarkerCrashKeepsCompletedBASE(t, "attach")
}

func TestDurableProviderV3_FreshMarkerCompletionCrashKeepsCompletedBASE(t *testing.T) {
	freshMarkerCrashKeepsCompletedBASE(t, "complete")
}

func TestDurableProviderV3_FreshMarkerCaptureCrashKeepsCompletedBASE(t *testing.T) {
	freshMarkerCrashKeepsCompletedBASE(t, "capture")
}

func freshMarkerCrashKeepsCompletedBASE(t *testing.T, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if root := os.Getenv("SWBLOCK_V3_FRESH_CRASH_ROOT"); root != "" {
		fixture := freshCrashFixture(t, filepath.Join(root, "layout.json"))
		p := mappedProviderV3(t, root, fixture, formatV3TakeoverView{})
		if os.Getenv("SWBLOCK_V3_FRESH_CRASH_MODE") == "capture" {
			p.cfg.ChunkCaptureInterval = time.Hour
		}
		st, err := p.EnsureStorage("v1")
		if err != nil {
			t.Fatal(err)
		}
		ws := st.(*storage.WALStore)
		if _, err = ws.Recover(); err != nil {
			t.Fatal(err)
		}
		var h storage.InstallHandle
		install := func() {
			var id [16]byte
			id[0] = 23
			h, err = storage.BeginInstall(ws, 7, id)
			if err != nil {
				t.Fatal(err)
			}
			data := bytes.Repeat([]byte{0x78}, 4096)
			if err = h.WriteBase(5, data); err != nil {
				t.Fatal(err)
			}
			if err = h.WriteBase(9, make([]byte, 4096)); err != nil {
				t.Fatal(err)
			}
			if err = h.ApplyInstallEntry(5, data, 8); err != nil {
				t.Fatal(err)
			}
			if err = h.MarkBaseComplete(); err != nil {
				t.Fatal(err)
			}
			if _, err = ws.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = storage.FlushMappedWALStoreV3Test(ws); err != nil {
				t.Fatal(err)
			}
		}
		mode = os.Getenv("SWBLOCK_V3_FRESH_CRASH_MODE")
		if mode != "complete" {
			install()
		}
		p.testBeforeChunkFreshRetire = func() error {
			if mode == "complete" {
				install()
			}
			if id, fresh, ok := ws.LayoutIdentity(); !ok || !fresh || id == "" {
				t.Fatalf("local layout identity was not durably fresh: id=%q fresh=%v ok=%v", id, fresh, ok)
			}
			if err := h.ApplyInstallEntry(5, bytes.Repeat([]byte{0x79}, 4096), 9); err != nil {
				t.Fatal(err)
			}
			if _, err := ws.Sync(); err != nil {
				t.Fatal(err)
			}
			if mode == "capture" {
				beforeBytes, err := json.Marshal(fixture.image(t))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := p.CaptureVolume(ctx, "v1"); !errors.Is(err, ErrNotChunkVolume) {
					t.Fatal("precommit CaptureVolume published before durable owner commit")
				}
				if _, err := p.CutVolume(ctx, "v1", 9); !errors.Is(err, ErrNotChunkVolume) {
					t.Fatal("precommit CutVolume was admitted")
				}
				if err := p.MakeVolumeRemoteDurable(ctx, "v1", 9); !errors.Is(err, ErrNotChunkVolume) {
					t.Fatal("precommit MakeVolumeRemoteDurable was admitted")
				}
				afterBytes, err := json.Marshal(fixture.image(t))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(beforeBytes, afterBytes) || fixture.head(t).Activity != (layout.Activity{Kind: layout.ActIdle}) {
					t.Fatal("refused precommit publication changed remote state")
				}
				p.mu.Lock()
				handle := p.volumes["v1"]
				exposed := handle.chunk != nil || handle.chunkStop != nil || handle.chunkDone != nil
				p.mu.Unlock()
				if exposed {
					t.Fatal("precommit layer or scheduler was exposed")
				}
				fmt.Println("FRESH_CAPTURE_REFUSED no_remote_effect=true scheduler_hidden=true")
			}
			fmt.Println("FRESH_CRASH_READY presence_saved=true descriptor_fresh=true ack=9 no_Close=true")
			os.Exit(0)
			return nil
		}
		if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
			t.Fatal(err)
		}
		t.Fatal("crash boundary not reached")
	}
	root := t.TempDir()
	fixture := newFormatV3Fixture(t)
	source := mappedProviderV3(t, t.TempDir(), fixture, formatV3View{})
	if _, err := source.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	for _, lba := range []int64{5, 9} {
		if n, err := source.Backend("v1").Write(ctx, lba*4096, bytes.Repeat([]byte{0x11}, 4096)); err != nil || n != 4096 {
			t.Fatal(n, err)
		}
	}
	if err := source.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(fixture.image(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "layout.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDurableProviderV3_FreshMarkerCrashKeepsCompletedBASE$", "-test.v")
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = append(os.Environ(), "SWBLOCK_V3_FRESH_CRASH_ROOT="+root, "SWBLOCK_V3_FRESH_CRASH_MODE="+mode)
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("FRESH_CRASH_READY presence_saved=true descriptor_fresh=true ack=9 no_Close=true")) {
		t.Fatal("child missed crash boundary")
	}
	p := mappedProviderV3(t, root, freshCrashFixture(t, filepath.Join(root, "layout.json")), formatV3TakeoverView{})
	st, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if id, fresh, ok := st.(*storage.WALStore).LayoutIdentity(); !ok || !fresh || id == "" {
		t.Fatalf("crash lost fresh descriptor: id=%q fresh=%v ok=%v", id, fresh, ok)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("post-presence/pre-marker crash lost completed BASE recovery: %v", err)
	}
	for lba, want := range map[int64][]byte{5: bytes.Repeat([]byte{0x79}, 4096), 9: make([]byte, 4096)} {
		got := make([]byte, 4096)
		if n, err := p.Backend("v1").Read(ctx, lba*4096, got); err != nil || n != 4096 || !bytes.Equal(got, want) {
			t.Fatalf("crash BASE bytes lba=%d n=%d err=%v equal=%v", lba, n, err, bytes.Equal(got, want))
		}
	}
}

// A failed owner commit keeps the layer private without leaking or trying
// to attach it twice. Success publishes it and starts its scheduler once.
func TestDurableProviderV3_AttachCommitRetryKeepsLayerPrivate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := mappedProviderV3(t, t.TempDir(), newFormatV3Fixture(t), formatV3View{})
	p.cfg.ChunkCaptureInterval = time.Hour
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("hold durable owner commit")
	p.testBeforeChunkFreshRetire = func() error { return injected }
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	p.mu.Lock()
	h := p.volumes["v1"]
	pending := h.chunkPending
	exposed := h.chunk != nil || h.chunkStop != nil || h.chunkDone != nil
	p.mu.Unlock()
	if pending == nil || exposed {
		t.Fatal("failed commit exposed or lost pending layer")
	}
	if _, err := p.CaptureVolume(ctx, "v1"); !errors.Is(err, ErrNotChunkVolume) {
		t.Fatal("failed commit capture", err)
	}
	if id, fresh, ok := p.volumes["v1"].storage.(*storage.WALStore).LayoutIdentity(); !ok || !fresh || id == "" {
		t.Fatalf("failed owner commit lost fresh descriptor: id=%q fresh=%v ok=%v", id, fresh, ok)
	}
	p.testBeforeChunkFreshRetire = nil
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal("commit retry", err)
	}
	p.mu.Lock()
	correct := h.chunk == pending && h.chunkPending == nil && h.chunkStop != nil && h.chunkDone != nil
	p.mu.Unlock()
	if !correct {
		t.Fatal("commit did not publish the same pending layer and scheduler")
	}
	if id, fresh, ok := p.volumes["v1"].storage.(*storage.WALStore).LayoutIdentity(); !ok || fresh || id == "" {
		t.Fatalf("successful commit retained fresh descriptor: id=%q fresh=%v ok=%v", id, fresh, ok)
	}
	if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal("postcommit capture", err)
	}
}
