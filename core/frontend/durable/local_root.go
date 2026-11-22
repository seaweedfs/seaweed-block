package durable

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/holder"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// localRoot is a chunk volume's local root identity (V1-SPEC r2.38,
// INV-WS-LOCAL-ROOT-IDENTITY-001): the root's nonce, written into HEAD's
// HolderRoot by every publication of this root, and the published-generation
// high-water, recorded durably before each HEAD CAS this root proposes. It
// lives next to the volume file as <volume>.root.
type localRoot struct {
	path string

	mu        sync.Mutex
	Nonce     string `json:"nonce"`
	HighWater uint64 `json:"high_water"`
	stored    bool   // the file exists: this root has recorded at least once
}

const localRootSuffix = ".root"

// openLocalRoot reads the root identity next to volPath. A fresh root (a new
// volume that attaches from HEAD) gets a new nonce and ignores any old file;
// an existing root without a file (written before root identities) gets a
// nonce that is stored only by its first record. Nothing is written here.
func openLocalRoot(volPath string, fresh bool) (*localRoot, error) {
	r := &localRoot{path: volPath + localRootSuffix}
	if !fresh {
		b, err := os.ReadFile(r.path)
		switch {
		case err == nil:
			if err := json.Unmarshal(b, r); err != nil || r.Nonce == "" {
				return nil, fmt.Errorf("durable: local root identity %s is unreadable (%v): quarantine the local root to attach fresh from HEAD", r.path, err)
			}
			r.stored = true
			return r, nil
		case !errors.Is(err, os.ErrNotExist):
			return nil, fmt.Errorf("durable: read local root identity %s: %w", r.path, err)
		}
	}
	var nb [8]byte
	if _, err := rand.Read(nb[:]); err != nil {
		return nil, err
	}
	r.Nonce = hex.EncodeToString(nb[:])
	return r, nil
}

// record durably raises the high-water to gen (file and directory synced)
// before the caller's CAS may install gen. It never lowers it.
func (r *localRoot) record(gen uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stored && gen <= r.HighWater {
		return nil
	}
	hw := r.HighWater
	if gen > hw {
		hw = gen
	}
	b, err := json.Marshal(struct {
		Nonce     string `json:"nonce"`
		HighWater uint64 `json:"high_water"`
	}{r.Nonce, hw})
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.path); err != nil {
		return err
	}
	if err := syncParentDir(r.path); err != nil {
		return err
	}
	r.HighWater, r.stored = hw, true
	return nil
}

// beforeHeadCAS is the layer's storage.ChunkLayerConfig.BeforeHeadCAS: a CAS
// that publishes this holder's root records its generation first.
func (r *localRoot) beforeHeadCAS(holderID string) func(storage.ChunkHead, uint64) error {
	return func(next storage.ChunkHead, gen uint64) error {
		if !holder.Same(next.Holder, next.HolderRoot, holderID, r.Nonce) {
			return nil
		}
		return r.record(gen)
	}
}

// lineageOf decides whether an existing root may attach under head (HEAD of
// this volume, already known to name this holder's server and replica): it
// returns "" when head is this root's lineage, else why it is not.
//   - HEAD carries no root (a pre-release HEAD): adopted once by a root that
//     has not recorded yet, or by this root up to its own record (a crash
//     between the record and the claim CAS); the next claim stamps the root.
//   - HEAD carries a root: it must be this root's, at a generation not above
//     this root's high-water. Another root, a root that never recorded (a
//     copy of a pre-conversion root), or a HEAD above the high-water (a
//     rolled-back copy of this root) is refused.
func (r *localRoot) lineageOf(head storage.ChunkHead) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case head.Gen == 0:
		return ""
	case head.HolderRoot == "":
		if !r.stored || head.Gen <= r.HighWater {
			return ""
		}
		return fmt.Sprintf("HEAD gen %d carries no local root and this root already recorded high-water %d", head.Gen, r.HighWater)
	case !r.stored:
		return fmt.Sprintf("HEAD gen %d was published by local root %s and this root has no identity record (a copy of a root taken before its conversion)", head.Gen, head.HolderRoot)
	case head.HolderRoot != r.Nonce:
		return fmt.Sprintf("HEAD gen %d was published by another local root %s of this holder (this root is %s)", head.Gen, head.HolderRoot, r.Nonce)
	case head.Gen > r.HighWater:
		return fmt.Sprintf("HEAD gen %d is above this root's published high-water %d (a rolled-back copy of this root)", head.Gen, r.HighWater)
	}
	return ""
}
