package lifecycle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// WorkspaceBinding is prepared endpoint ownership, not assignment/readiness.
// Retired entries remain as a per-volume marker: explicit volumes never fall
// back to node-level addresses, even after all their bindings are retired.
type WorkspaceBinding struct {
	VolumeID       string `json:"volume_id"`
	ServerID       string `json:"server_id"`
	DataAddr       string `json:"data_addr"`
	CtrlAddr       string `json:"ctrl_addr"`
	Retired        bool   `json:"retired,omitempty"`
	RetiredEpoch   uint64 `json:"retired_epoch,omitempty"`   // idempotent authority retirement event
	RetirementOnly bool   `json:"retirement_only,omitempty"` // history before first explicit preparation
}

type WorkspaceBindingStore struct {
	mu        sync.Mutex
	dir       string
	records   map[string][]WorkspaceBinding
	uncertain bool
}

func OpenWorkspaceBindingStore(dir string) (*WorkspaceBindingStore, error) {
	return openWorkspaceBindingStore(dir, syncBindingParent)
}

func syncBindingParent(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func openWorkspaceBindingStore(dir string, syncParent func(string) error) (*WorkspaceBindingStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	// Persist the directory entry before any binding can be acknowledged.
	if err := syncParent(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	s := &WorkspaceBindingStore{dir: dir, records: map[string][]WorkspaceBinding{}}
	b, err := os.ReadFile(filepath.Join(dir, "bindings.json"))
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, &s.records); err != nil {
		return nil, err
	}
	if s.records == nil {
		return nil, fmt.Errorf("workspace bindings: null state")
	}
	for v, list := range s.records {
		if err = validateVolumeID(v); err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		for _, r := range list {
			if r.VolumeID != v || seen[r.ServerID] {
				return nil, fmt.Errorf("workspace bindings: inconsistent record")
			}
			seen[r.ServerID] = true
			if r.RetirementOnly {
				if !r.Retired || r.RetiredEpoch == 0 || r.DataAddr != "" || r.CtrlAddr != "" {
					return nil, fmt.Errorf("workspace bindings: invalid retirement watermark")
				}
				if err = validateServerID(r.ServerID); err != nil {
					return nil, err
				}
				continue
			}
			if err = validateWorkspaceBinding(r); err != nil {
				return nil, err
			}
			if !r.Retired {
				if err = bindingCollision(s.records, r); err != nil {
					return nil, err
				}
			}
		}
	}
	return s, nil
}

func validateWorkspaceBinding(b WorkspaceBinding) error {
	if err := validateVolumeID(b.VolumeID); err != nil {
		return err
	}
	if err := validateServerID(b.ServerID); err != nil {
		return err
	}
	for _, a := range []string{b.DataAddr, b.CtrlAddr} {
		h, p, e := net.SplitHostPort(a)
		n, e2 := strconv.Atoi(p)
		if e != nil || e2 != nil || h == "" || n < 1 || n > 65535 {
			return fmt.Errorf("workspace binding: invalid endpoint %q", a)
		}
	}
	if workspaceEndpointKey(b.DataAddr) == workspaceEndpointKey(b.CtrlAddr) {
		return fmt.Errorf("workspace binding: endpoints must differ")
	}
	return nil
}

// Compare numeric ports and equivalent IP spellings without DNS/network I/O.
func workspaceEndpointKey(addr string) string {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		return addr
	}
	if ip := net.ParseIP(h); ip != nil {
		h = ip.String()
	} else {
		h = strings.ToLower(h)
	}
	return net.JoinHostPort(h, strconv.Itoa(n))
}

func (b WorkspaceBinding) EndpointsOverlap(other WorkspaceBinding) bool {
	d, c := workspaceEndpointKey(b.DataAddr), workspaceEndpointKey(b.CtrlAddr)
	od, oc := workspaceEndpointKey(other.DataAddr), workspaceEndpointKey(other.CtrlAddr)
	return d == od || d == oc || c == od || c == oc
}

func bindingCollision(all map[string][]WorkspaceBinding, b WorkspaceBinding) error {
	for _, list := range all {
		for _, r := range list {
			if r.Retired || (r.VolumeID == b.VolumeID && r.ServerID == b.ServerID) {
				continue
			}
			if b.EndpointsOverlap(r) {
				return fmt.Errorf("workspace binding: endpoint reserved by %s on %s", r.VolumeID, r.ServerID)
			}
		}
	}
	return nil
}

func (s *WorkspaceBindingStore) Get(volume string) ([]WorkspaceBinding, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain {
		return nil, false, fmt.Errorf("workspace bindings uncertain; restart required")
	}
	var list []WorkspaceBinding
	for _, b := range s.records[volume] {
		if !b.RetirementOnly {
			list = append(list, b)
		}
	}
	return list, len(list) > 0, nil
}

func (s *WorkspaceBindingStore) CheckAvailable(b WorkspaceBinding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain {
		return fmt.Errorf("workspace bindings uncertain; restart required")
	}
	return bindingCollision(s.records, b)
}

func (s *WorkspaceBindingStore) Register(b WorkspaceBinding) error {
	if b.Retired {
		return fmt.Errorf("workspace binding: retired is server-owned")
	}
	if err := validateWorkspaceBinding(b); err != nil {
		return err
	}
	return s.update(func(all map[string][]WorkspaceBinding) error {
		if err := bindingCollision(all, b); err != nil {
			return err
		}
		list := all[b.VolumeID]
		for i, r := range list {
			if r.ServerID == b.ServerID {
				if !r.Retired && r.DataAddr == b.DataAddr && r.CtrlAddr == b.CtrlAddr {
					return nil
				}
				if !r.Retired {
					return fmt.Errorf("workspace binding: live binding cannot change")
				}
				list[i] = b
				list[i].RetiredEpoch = r.RetiredEpoch
				all[b.VolumeID] = list
				return nil
			}
		}
		all[b.VolumeID] = append(list, b)
		return nil
	})
}

func (s *WorkspaceBindingStore) Retire(volume, server string, epoch uint64) error {
	return s.update(func(all map[string][]WorkspaceBinding) error {
		found := false
		for i := range all[volume] {
			if all[volume][i].ServerID != server {
				continue
			}
			found = true
			if epoch > all[volume][i].RetiredEpoch {
				all[volume][i].Retired = true
				all[volume][i].RetiredEpoch = epoch
			}
		}
		if !found {
			all[volume] = append(all[volume], WorkspaceBinding{VolumeID: volume, ServerID: server, Retired: true, RetiredEpoch: epoch, RetirementOnly: true})
		}
		return nil
	})
}

func (s *WorkspaceBindingStore) DeleteVolume(volume string) error {
	return s.update(func(all map[string][]WorkspaceBinding) error { delete(all, volume); return nil })
}

// Atomic snapshot includes collision ownership across volumes. No in-memory
// mutation is committed before durable publication. An ambiguous filesystem
// error latches the store fail-closed until reload, including read admission.
func (s *WorkspaceBindingStore) update(change func(map[string][]WorkspaceBinding) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain {
		return fmt.Errorf("workspace bindings uncertain; restart required")
	}
	all := make(map[string][]WorkspaceBinding, len(s.records))
	for v, l := range s.records {
		all[v] = append([]WorkspaceBinding(nil), l...)
	}
	if err := change(all); err != nil {
		return err
	}
	b, err := json.Marshal(all)
	if err != nil {
		return err
	}
	old, _ := json.Marshal(s.records)
	if bytes.Equal(b, old) {
		return nil
	}
	f, err := os.CreateTemp(s.dir, "bindings-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(s.dir, "bindings.json")); err != nil {
		s.uncertain = true
		return err
	}
	d, err := os.Open(s.dir)
	if err == nil {
		err = d.Sync()
		_ = d.Close()
	}
	if err != nil {
		s.uncertain = true
		return err
	}
	s.records = all
	return nil
}
