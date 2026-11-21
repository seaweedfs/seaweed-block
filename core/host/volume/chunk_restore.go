package volume

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Independent restore driver (M6.Restore). A restored volume serves at once;
// the driver then makes every chunk local (Restoring>Hydrating), re-uploads
// the chunks whose HEAD keys name another volume under the volume's own keys
// in bounded batch cuts (Hydrating>Owning, Owning>Owning; the walker and the
// batches may overlap), and writes the independence marker once HEAD holds
// no foreign ref (Owning>Independent). Every decision is read from the
// target's own facts (its VOLUME, HEAD and marker), never from the source, so
// a rerun after a crash resumes where the remote state is, and a deleted
// source snapshot or volume is a normal state.

// Restore quantities (invariants.md Part E).
const (
	DefaultRestoreBatchChunks    = 64
	DefaultRestoreBytesPerSecond = 64 << 20
)

// RestoreProvider is the chunk layer's restore surface for an attached volume.
type RestoreProvider interface {
	HydrateVolume(ctx context.Context, volumeID string, bytesPerSecond int64) (storage.HydrateProgress, error)
	OwnVolumeBatch(ctx context.Context, volumeID string, n int, foreign func(string) bool) (storage.OwnProgress, error)
}

// RestoreConfig bounds one restore run; zero fields take the defaults.
type RestoreConfig struct {
	BatchChunks    int
	BytesPerSecond int64
}

// Restore phases.
const (
	RestoreHydrating   = "hydrating"
	RestoreOwning      = "owning"
	RestoreIndependent = "independent"
	RestoreFailed      = "failed"
)

// RestoreStatus is the body of /chunk/restore.
type RestoreStatus struct {
	VolumeID  string `json:"volumeID"`
	Phase     string `json:"phase"`
	Running   bool   `json:"running"`
	Filled    int    `json:"filled"`    // chunks the walker made local
	Owned     int    `json:"owned"`     // chunks re-uploaded under own keys
	Foreign   int    `json:"foreign"`   // HEAD chunks still naming another volume
	NotLocal  int    `json:"notLocal"`  // of those, not local yet
	Batches   int    `json:"batches"`   // batch cuts run
	MarkedGen uint64 `json:"markedGen"` // HEAD gen the marker names (independent)
	ErrorCode string `json:"errorCode,omitempty"`
	ErrorText string `json:"error,omitempty"`
}

// foreignKey reports whether key names another volume's object: an
// owner-qualified key whose owner is not vol.
func foreignKey(vol string) func(string) bool {
	own := "@" + vol + "/"
	return func(key string) bool { return strings.HasPrefix(key, "@") && !strings.HasPrefix(key, own) }
}

var (
	errRestoreStuck = errors.New("restore: foreign chunks remain but none is local and the walker fills none")
	errNotRestore   = errors.New("restore: not a restore")
)

// runRestore drives one restore of vol to independence, reporting progress.
func runRestore(ctx context.Context, cat *chunkstore.Catalog, p RestoreProvider, vol string, cfg RestoreConfig, report func(RestoreStatus)) (RestoreStatus, error) {
	if cfg.BatchChunks <= 0 {
		cfg.BatchChunks = DefaultRestoreBatchChunks
	}
	if cfg.BytesPerSecond <= 0 {
		cfg.BytesPerSecond = DefaultRestoreBytesPerSecond
	}
	st := RestoreStatus{VolumeID: vol, Phase: RestoreHydrating, Running: true}
	if m, ok, err := cat.ReadIndependence(ctx, vol); err != nil {
		return st, err
	} else if ok {
		st.Phase, st.MarkedGen = RestoreIndependent, m.HeadGen
		return st, nil
	}
	v, ok, err := cat.ReadVolume(ctx, vol)
	if err != nil {
		return st, err
	}
	if !ok || v.Origin != chunkstore.OriginRestore {
		return st, fmt.Errorf("volume %s was not made by restore: %w", vol, errNotRestore)
	}
	foreign := foreignKey(vol)
	for stalls := 0; ; {
		h, err := p.HydrateVolume(ctx, vol, cfg.BytesPerSecond)
		st.Filled += h.Filled
		report(st)
		if err != nil {
			return st, err
		}
		st.Phase = RestoreOwning
		progressed := h.Filled > 0
		for {
			o, err := p.OwnVolumeBatch(ctx, vol, cfg.BatchChunks, foreign)
			st.Owned, st.Foreign, st.NotLocal = st.Owned+o.Owned, o.Remaining, o.NotLocal
			if o.Owned > 0 {
				st.Batches++
				progressed = true
			}
			report(st)
			if err != nil {
				return st, err
			}
			if o.Remaining == 0 {
				m, err := cat.MarkIndependent(ctx, vol)
				if err != nil {
					return st, err
				}
				st.Phase, st.MarkedGen, st.Running = RestoreIndependent, m.HeadGen, false
				report(st)
				return st, nil
			}
			if o.Owned == 0 {
				break // what is left is not local: walk again
			}
		}
		if !progressed {
			if stalls++; stalls > 2 {
				return st, errRestoreStuck
			}
		} else {
			stalls = 0
		}
	}
}

// restoreRuns holds at most one running restore per volume and the last
// status of each.
type restoreRuns struct {
	mu   sync.Mutex
	runs map[string]*RestoreStatus
}

// Restore reports vol's restore status and, with start, starts a run unless
// one is running (POST /chunk/restore). A run outlives the request but not
// the serving provider (RestoreOwner); it ends independent, or failed with
// the error (a later start resumes from the target's remote facts).
func (c *chunkControl) Restore(ctx context.Context, vol string, start bool) (RestoreStatus, error) {
	p, ok := c.p.(RestoreProvider)
	owner, owned := c.p.(RestoreOwner)
	if !ok || !owned {
		return RestoreStatus{}, fmt.Errorf("restore: provider has no restore surface: %w", errNotRestore)
	}
	c.restores.mu.Lock()
	defer c.restores.mu.Unlock()
	if c.restores.runs == nil {
		c.restores.runs = map[string]*RestoreStatus{}
	}
	cur := c.restores.runs[vol]
	if cur != nil && (cur.Running || !start) {
		return *cur, nil
	}
	if !start {
		return RestoreStatus{VolumeID: vol}, nil
	}
	st := &RestoreStatus{VolumeID: vol, Phase: RestoreHydrating, Running: true}
	c.restores.runs[vol] = st
	report := func(s RestoreStatus) {
		c.restores.mu.Lock()
		*st = s
		c.restores.mu.Unlock()
	}
	run := func(ctx context.Context) {
		final, err := runRestore(ctx, c.cat, p, vol, RestoreConfig{}, report)
		final.Running = false
		if err != nil {
			_, code, _ := chunkErrorCode(err)
			final.Phase, final.ErrorCode, final.ErrorText = RestoreFailed, code, err.Error()
		}
		report(final)
	}
	if err := owner.GoOwned(run); err != nil {
		delete(c.restores.runs, vol)
		return RestoreStatus{}, err
	}
	return *st, nil
}

// RestoreOwner runs the driver inside the serving volume's lifetime: the
// driver's ctx ends when the owner closes, and the owner waits for it
// (DurableProvider.GoOwned).
type RestoreOwner interface {
	GoOwned(fn func(ctx context.Context)) error
}

// handleChunkRestore serves GET /chunk/restore?volume=V (status) and
// POST /chunk/restore {"volumeID": V} (start or join a run; with
// "statusOnly": true, status only). Both answer 200 with the status.
func (s *StatusServer) handleChunkRestore(w http.ResponseWriter, r *http.Request) {
	method := http.MethodGet
	if r.Method == http.MethodPost {
		method = http.MethodPost
	}
	if !s.chunkAdmit(w, r, method) {
		return
	}
	vol := r.URL.Query().Get("volume")
	if method == http.MethodPost {
		var req struct {
			VolumeID   string `json:"volumeID"`
			StatusOnly bool   `json:"statusOnly"`
			TimeoutMs  int64  `json:"timeoutMs"`
		}
		if !decodeChunkBody(w, r, &req) {
			return
		}
		vol = req.VolumeID
		if req.StatusOnly {
			method = http.MethodGet
		}
	}
	ctl := s.chunkTarget(w, vol)
	if ctl == nil {
		return
	}
	rc, ok := ctl.(interface {
		Restore(context.Context, string, bool) (RestoreStatus, error)
	})
	if !ok {
		writeChunkErr(w, http.StatusNotFound, codeNotFound, false, "restore is not served here")
		return
	}
	st, err := rc.Restore(r.Context(), vol, method == http.MethodPost)
	if err != nil {
		writeChunkFailure(w, err)
		return
	}
	writeChunkJSON(w, http.StatusOK, st)
}
