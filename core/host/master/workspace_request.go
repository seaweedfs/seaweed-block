package master

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"sync"
)

// Per-volume admission may span remote HEAD reads, never the global workspaceMu.
// The short receipt mutex protects only local durable request/result writes.
// Lock order: volume admission -> workspaceMu -> receipt mutex. Receipt reads
// release the receipt mutex before invoking a handler or taking workspaceMu.
// The authority directory's existing exclusive process lock owns these receipts.
type workspaceRequestStore struct {
	mu        sync.Mutex
	volumes   sync.Map
	records   map[string]workspaceReceipt
	uncertain bool
}
type workspaceReceipt struct {
	Volume  string
	Pending bool
	Method  string
	Body    []byte
	Reply   []byte
	Done    bool
}

func (h *Host) workspaceMutation(ctx context.Context, q any, info *grpc.UnaryServerInfo, call grpc.UnaryHandler) (any, error) {
	var id, volume string
	var reply proto.Message
	switch v := q.(type) {
	case *control.WorkspaceBinding:
		id, volume = v.RequestId, v.VolumeId
		reply = &control.WorkspaceBinding{}
	case *control.WorkspacePlacement:
		id, volume = v.RequestId, v.VolumeId
		reply = &control.WorkspaceEmpty{}
	case *control.DeleteVolumeRequest:
		volume = v.VolumeId
	case *control.WorkspaceMove:
		id, volume = v.RequestId, v.VolumeId
		reply = &control.WorkspaceGrant{}
	default:
		// Lease Renew/Revoke/ACK remain independent of endpoint preparation.
		// Their existing Publisher epoch/expiry/move checks are authoritative.
		return call(ctx, q)
	}
	// Older RPC callers remain compatible; the workspace CLI requires an ID.

	if len(id) > 256 || volume == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id <=256 bytes and volume required")
	}
	v, _ := h.workspaceRequests.volumes.LoadOrStore(volume, make(chan struct{}, 1))
	gate := v.(chan struct{})
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	body, e := (proto.MarshalOptions{Deterministic: true}).Marshal(q.(proto.Message))
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	key := sha256.Sum256([]byte(id))
	name := hex.EncodeToString(key[:])
	store := &h.workspaceRequests
	store.mu.Lock()
	if store.uncertain {
		store.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "request store durability uncertain; master restart required")
	}
	if store.records == nil {
		store.records = map[string]workspaceReceipt{}
		data, err := os.ReadFile(filepath.Join(h.cfg.AuthorityStoreDir, "workspace-requests.json"))
		if err != nil && !os.IsNotExist(err) {
			store.records = nil
			store.mu.Unlock()
			return nil, status.Error(codes.Internal, err.Error())
		}
		if len(data) > 0 {
			if err = json.Unmarshal(data, &store.records); err != nil {
				store.records = nil
				store.mu.Unlock()
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
	}
	if store.records == nil {
		store.mu.Unlock()
		return nil, status.Error(codes.Internal, "invalid null request receipt store")
	}
	for k, r := range store.records {
		if r.Volume == volume && r.Pending && (id == "" || k != name) {
			store.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "resolve the pending request_id for this volume before another mutation")
		}
	}
	if id == "" {
		store.mu.Unlock()
		return call(ctx, q)
	}
	old, found := store.records[name]
	if found && (old.Method != info.FullMethod || string(old.Body) != string(body)) {
		store.mu.Unlock()
		return nil, status.Error(codes.AlreadyExists, "request_id already belongs to a different mutation body")
	}
	if found && old.Done {
		store.mu.Unlock()
		if e = proto.Unmarshal(old.Reply, reply); e != nil {
			return nil, status.Error(codes.Internal, e.Error())
		}
		return reply, nil
	}
	if !found {
		old = workspaceReceipt{Method: info.FullMethod, Body: body, Volume: volume}
	}
	old.Pending = true
	store.records[name] = old
	if e = h.persistWorkspaceRequests(); e != nil {
		store.uncertain = true
		store.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "request intent durability unknown: "+e.Error())
	}
	store.mu.Unlock()
	completed := false
	save := func(result proto.Message) error {
		bytes, err := proto.Marshal(result)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		old.Reply = bytes
		old.Done = true
		old.Pending = false
		store.records[name] = old
		if err = h.persistWorkspaceRequests(); err != nil {
			store.uncertain = true
			old.Done = false
			old.Pending = true
			old.Reply = nil
			store.records[name] = old
			return status.Error(codes.Unavailable, "mutation applied; receipt durability unknown; retry same request_id after restart: "+err.Error())
		}
		completed = true
		return nil
	}
	ctx = context.WithValue(ctx, workspaceResultKey{}, save)
	result, e := call(ctx, q)
	if e != nil {
		// A definite refusal may be retried later (e.g. after lease expiry), but
		// its ID remains bound to the same body. Ambiguous failures keep admission
		// blocked until this same request resolves the durable pending intent.
		switch status.Code(e) {
		case codes.InvalidArgument, codes.FailedPrecondition, codes.AlreadyExists, codes.NotFound, codes.PermissionDenied, codes.Unauthenticated:
			store.mu.Lock()
			old.Pending = false
			store.records[name] = old
			if err := h.persistWorkspaceRequests(); err != nil {
				store.uncertain = true
			}
			uncertain := store.uncertain
			store.mu.Unlock()
			if uncertain {
				return nil, status.Error(codes.Unavailable, "request refusal receipt durability unknown")
			}
		}
		return nil, e
	}
	if !completed {
		if e = save(result.(proto.Message)); e != nil {
			return nil, e
		}
	}
	return result, nil
}

type workspaceResultKey struct{}

// Called while the handler still owns workspaceMu: persist the result before
// another ACK/delete/move can change the state whose result we are returning.
func finishWorkspaceMutation[T proto.Message](ctx context.Context, result T) (T, error) {
	if save, ok := ctx.Value(workspaceResultKey{}).(func(proto.Message) error); ok {
		if err := save(result); err != nil {
			var zero T
			return zero, err
		}
	}
	return result, nil
}

// Atomic replacement, file sync and directory sync before any result is sent.
// An intent is durable before calling the idempotent binding/placement stores or
// Publisher's existing request-ID move journal. A retry rolls a pending intent
// forward using those stores; a completed result never re-runs the mutation.
func (h *Host) persistWorkspaceRequests() error {
	data, e := json.Marshal(h.workspaceRequests.records)
	if e != nil {
		return e
	}
	dir := h.cfg.AuthorityStoreDir
	f, e := os.CreateTemp(dir, ".workspace-requests-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.Write(data); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(name, filepath.Join(dir, "workspace-requests.json")); e != nil {
		return e
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	if e = d.Sync(); e != nil {
		return fmt.Errorf("request directory sync: %w", e)
	}
	return nil
}
