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
	// Epoch is the epoch of the publication the high-water was recorded for.
	Epoch  uint64 `json:"epoch"`
	stored bool   // the file exists: this root has recorded at least once
	// synced: this process has made the visible record durable (file and
	// parent directory synced). A record read on open is visible, not proven
	// durable: an earlier process may have renamed it into place and then
	// failed its directory sync. Until synced, nothing skips the barrier.
	synced bool
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

// localRootSyncDir makes a rename in a record's directory durable; tests
// replace it to inject a failed barrier.
var localRootSyncDir = syncParentDir

// record durably raises the high-water to gen, with the epoch of that
// publication (file and directory synced), before the caller's CAS may
// install gen. It never lowers either. A record that already covers gen and
// epoch returns at once only once this process has made it durable; a record
// read on open is first synced again (file, then directory): no CAS passes
// a barrier that has not succeeded.
func (r *localRoot) record(gen, epoch uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stored && gen <= r.HighWater && epoch <= r.Epoch {
		if r.synced {
			return nil
		}
		if err := syncLocalRootFile(r.path); err != nil {
			return fmt.Errorf("durable: make local root record %s durable: %w", r.path, err)
		}
		if err := localRootSyncDir(r.path); err != nil {
			return fmt.Errorf("durable: make local root record %s durable: %w", r.path, err)
		}
		r.synced = true
		return nil
	}
	hw, ep := r.HighWater, r.Epoch
	if gen > hw {
		hw = gen
	}
	if epoch > ep {
		ep = epoch
	}
	b, err := json.Marshal(struct {
		Nonce     string `json:"nonce"`
		HighWater uint64 `json:"high_water"`
		Epoch     uint64 `json:"epoch"`
	}{r.Nonce, hw, ep})
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
	// The rename is visible from here on; durable only once this succeeds.
	r.synced = false
	if err := localRootSyncDir(r.path); err != nil {
		return err
	}
	r.HighWater, r.Epoch, r.stored, r.synced = hw, ep, true, true
	return nil
}

// syncLocalRootFile fsyncs a visible record file.
func syncLocalRootFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	serr := f.Sync()
	cerr := f.Close()
	return errors.Join(serr, cerr)
}

// beforeHeadCAS is the layer's storage.ChunkLayerConfig.BeforeHeadCAS: a CAS
// that publishes this holder's root records its generation first.
func (r *localRoot) beforeHeadCAS(holderID string) func(storage.ChunkHead, uint64) error {
	return func(next storage.ChunkHead, gen uint64) error {
		if !holder.Same(next.Holder, next.HolderRoot, holderID, r.Nonce) {
			return nil
		}
		return r.record(gen, next.Epoch)
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
	case rootGen(head) > r.HighWater:
		return fmt.Sprintf("HEAD names this root at its generation %d, above this root's published high-water %d (a rolled-back copy of this root)", rootGen(head), r.HighWater)
	}
	return ""
}

// rolledBack: HEAD names this very root above its high-water (a rolled-back
// copy of this root). Never call with mu held.
func (r *localRoot) rolledBack(head storage.ChunkHead) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stored && head.HolderRoot != "" && head.HolderRoot == r.Nonce && rootGen(head) > r.HighWater
}

// rootGen is the generation HEAD's holder root last installed itself:
// HolderRootGen (catalog marks copy it), or Gen on a HEAD written before it.
func rootGen(head storage.ChunkHead) uint64 {
	if head.HolderRootGen != 0 {
		return head.HolderRootGen
	}
	return head.Gen
}

// highWater is the root's recorded published-generation high-water.
func (r *localRoot) highWater() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.HighWater
}

// recordedEpoch is the epoch of the root's last recorded publication.
func (r *localRoot) recordedEpoch() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Epoch
}

// supersededBefore is the early stale-root check (V1-SPEC r2.38, TERM-STOP-
// STALE-ROOT-001): an existing root that has recorded a publication is
// superseded when HEAD names another holder (server, replica, root; a
// pre-release HEAD without a root is still ours) at an epoch at or above the
// root's recorded epoch, i.e. someone published over this root's own last
// line. It returns why, or "" (a lower-epoch foreign HEAD, an empty HEAD or a
// root without a record takes the normal path).
func (r *localRoot) supersededBefore(head storage.ChunkHead, holderID string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stored || head.Gen == 0 || head.Holder == "" || holder.Claimable(head.Holder, head.HolderRoot, holderID, r.Nonce) || head.Epoch < r.Epoch {
		return ""
	}
	return fmt.Sprintf("HEAD gen %d epoch %d was published by holder %q root %q over this root's recorded epoch %d (this holder %q, root %s)",
		head.Gen, head.Epoch, head.Holder, head.HolderRoot, r.Epoch, holderID, r.Nonce)
}

// LocalRootNonce is the open volume's local root nonce as recorded ("" for a
// root that has not recorded yet: a pre-release root). Sealed-proof checks
// compare it (V1-SPEC r2.38).
func (p *DurableProvider) LocalRootNonce(volumeID string) (string, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	p.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrVolumeNotOpen, volumeID)
	}
	r, err := openLocalRoot(h.path, false)
	if err != nil {
		return "", err
	}
	if !r.stored {
		return "", nil
	}
	return r.Nonce, nil
}
