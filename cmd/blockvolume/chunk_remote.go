package main

// --durable-impl=walstore-chunk: walstore with the chunk layer publishing
// the volume to SeaweedFS (opt-in proof of concept). Each volume's HEAD and
// chunk key bindings live under <--chunk-path>/<volume-id> on the filer
// given by --chunk-filer; chunk bytes are needles on its volume servers.

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
)

const (
	defaultChunkPath = "/swblock/chunks"
	// Periodic HEAD publication bounds the host-loss recovery point. Scheduled
	// named snapshots are set by --chunk-snapshot-schedule.
	defaultChunkCaptureInterval     = 0
	defaultChunkMaintenanceInterval = time.Hour
	defaultChunkCutTimeout          = 5 * time.Minute
	defaultChunkMaintenanceTimeout  = time.Minute
	defaultChunkGCGrace             = time.Hour
	defaultChunkIntentTTL           = time.Hour
)

type chunkFlags struct {
	detachPrecopy       bool
	hostLimit           hostlimit.Config
	filer               string
	path                string
	collection          string
	replication         string
	captureInterval     time.Duration
	maintenanceInterval time.Duration
	snapshotSchedule    snapshotSchedule
	automaticGC         bool
	uploadConcurrency   int
	hydrationWorkers    int
	hydrationQueueDepth int
	// controlEndpoint mounts the /chunk/* routes on the status listener.
	controlEndpoint bool
}

func validateChunkFlags(f flags) error {
	c := f.chunk
	if c.detachPrecopy {
		return errors.New("--chunk-detach-precopy is not supported in this release")
	}
	if c.hostLimit.Directory != "" {
		if err := c.hostLimit.Validate(); err != nil {
			return err
		}
	} else if c.hostLimit != (hostlimit.Config{}) && c.hostLimit != hostlimit.DefaultConfig("") {
		return errors.New("--host-limit-* settings require --host-limit-dir")
	}
	if f.durableImpl != "walstore-chunk" {
		if c.filer != "" || c.collection != "" || c.replication != "" || c.path != defaultChunkPath ||
			c.captureInterval != defaultChunkCaptureInterval || c.maintenanceInterval != defaultChunkMaintenanceInterval || c.snapshotSchedule != defaultSnapshotSchedule || c.automaticGC ||
			c.uploadConcurrency != 8 || c.hydrationWorkers != 2 || c.hydrationQueueDepth != 64 || c.controlEndpoint || c.detachPrecopy || c.hostLimit.Directory != "" {
			return errors.New("--chunk-* flags require --durable-impl=walstore-chunk")
		}
		return nil
	}
	if c.controlEndpoint && f.statusAddr == "" {
		return errors.New("--chunk-control-endpoint requires --status-addr")
	}
	if f.replicationAck != "" && f.replicationAck != "best-effort" {
		return fmt.Errorf("--durable-impl=walstore-chunk is single-replica: --replication-ack=%q needs replica peers (%s)", f.replicationAck, chunkRF1Reason)
	}
	if f.durableRoot == "" {
		return errors.New("--durable-impl=walstore-chunk requires --durable-root")
	}
	if c.filer == "" {
		return errors.New("--durable-impl=walstore-chunk requires --chunk-filer")
	}
	if !path.IsAbs(c.path) || path.Clean(c.path) != c.path || c.path == "/" {
		return fmt.Errorf("--chunk-path=%q must be a clean absolute filer directory other than /", c.path)
	}
	if c.maintenanceInterval < 0 {
		return errors.New("--chunk-maintenance-interval must not be negative")
	}
	if c.captureInterval > 0 && c.captureInterval < durable.MinCaptureInterval {
		return fmt.Errorf("--chunk-capture-interval must be 0 or at least %s", durable.MinCaptureInterval)
	}
	if c.captureInterval < 0 {
		return fmt.Errorf("--chunk-capture-interval=%s must not be negative", c.captureInterval)
	}
	if c.uploadConcurrency < 1 || c.uploadConcurrency > 64 {
		return errors.New("--chunk-upload-concurrency must be between 1 and 64")
	}
	if c.hydrationWorkers < 1 || c.hydrationWorkers > 64 {
		return errors.New("--chunk-hydration-workers must be between 1 and 64")
	}
	if c.hydrationQueueDepth < 1 || c.hydrationQueueDepth > 65536 {
		return errors.New("--chunk-hydration-queue-depth must be between 1 and 65536")
	}
	return nil
}

// chunkRF1Reason is why a walstore-chunk volume refuses replication, at start
// and on every assignment with replica slots: a chunk cut takes its LSN from the
// local WAL head, which no replica is guaranteed to hold, so a published HEAD
// could be ahead of every replica. The master's workspace RPCs enforce RF1; this
// is the same rule on the volume server, for a direct start.
const chunkRF1Reason = "chunk snapshots take the cut LSN from the local WAL, which replicas may not hold; walstore-chunk is RF1 only"

// chunkWiring is what --durable-impl=walstore-chunk dials once: the
// provider's per-volume remotes, the catalog under the same base (VOLUME
// entries, snapshots) and the client's closer.
// snapshotSchedule is --chunk-snapshot-schedule: "daily:N", "weekly:N"
// (one scheduled snapshot per period at a per-volume offset, the newest N
// kept) or "off".
type snapshotSchedule struct {
	period time.Duration
	keep   int
}

var defaultSnapshotSchedule = snapshotSchedule{period: 24 * time.Hour, keep: 7}

func (s *snapshotSchedule) String() string {
	switch s.period {
	case 0:
		return "off"
	case 24 * time.Hour:
		return fmt.Sprintf("daily:%d", s.keep)
	case 7 * 24 * time.Hour:
		return fmt.Sprintf("weekly:%d", s.keep)
	}
	return fmt.Sprintf("%s:%d", s.period, s.keep)
}

func (s *snapshotSchedule) Set(v string) error {
	if v == "off" {
		*s = snapshotSchedule{}
		return nil
	}
	name, count, ok := strings.Cut(v, ":")
	periods := map[string]time.Duration{"daily": 24 * time.Hour, "weekly": 7 * 24 * time.Hour}
	keep, err := strconv.Atoi(count)
	if !ok || periods[name] == 0 || err != nil || keep < 1 || keep > 1000 {
		return fmt.Errorf("%q: want daily:N, weekly:N (N from 1 to 1000) or off", v)
	}
	*s = snapshotSchedule{period: periods[name], keep: keep}
	return nil
}

type chunkWiring struct {
	remoteFor func(volumeID string) (storage.ChunkRemote, error)
	catalog   *chunkstore.Catalog
	bCatalog  *chunkstore.BCatalog
	close     func() error
}

// newChunkWiring dials the filer and refuses unless it is the only filer of
// its group (chunkstore.CheckSingleFiler).
func newChunkWiring(c chunkFlags, masterTLS grpc.DialOption, master ...string) (chunkWiring, error) {
	client, err := chunkstore.DialSWFS(chunkstore.SWFSConfig{
		FilerGRPC:   c.filer,
		Collection:  c.collection,
		Replication: c.replication,
		AssignPath:  c.path,
	})
	if err != nil {
		return chunkWiring{}, err
	}
	// Deployment rule: block metadata goes through exactly one filer.
	if err := client.CheckSingleFiler(context.Background()); err != nil {
		_ = client.Close()
		return chunkWiring{}, err
	}
	cat, err := chunkstore.NewCatalog(client, c.path)
	if err != nil {
		_ = client.Close()
		return chunkWiring{}, err
	}
	bcat, err := chunkstore.NewBCatalog(client, client, c.path)
	if err != nil {
		_ = client.Close()
		return chunkWiring{}, err
	}
	return chunkWiring{
		remoteFor: func(volumeID string) (storage.ChunkRemote, error) {
			root, err := chunkVolumeRoot(c.path, volumeID)
			if err != nil {
				return nil, err
			}
			return chunkstore.NewSWFSChunkRemote(client, root)
		},
		catalog:  cat,
		bCatalog: bcat,
		close:    client.Close,
	}, nil
}

// chunkAttachCheck creates only the master's identified volume. Geometry is
// local/master configuration, not a second catalog record.
func chunkAttachCheck(cat *chunkstore.BCatalog, ref chunkstore.VolumeRef) func(context.Context, string, durable.ChunkGeometry) error {
	return func(ctx context.Context, name string, _ durable.ChunkGeometry) error {
		if cat == nil || name != ref.Name || ref.ID.IsZero() {
			return layout.ErrIdentity
		}
		_, err := cat.CreateVolume(ctx, ref)
		return err
	}
}

// exitGeometryRefused ends the process with no restart (the shipped units'
// RestartPreventExitStatus=78): a geometry flag that disagrees with the
// volume record stays wrong until the operator changes the flag.
const exitGeometryRefused = 78

// geometryFlagsRefusal compares the geometry flags with the volume record
// before any local store is created, so a wrong flag never leaves a store of
// the wrong size behind. It returns the refusal line and its exit status:
// exitGeometryRefused (78) when the record disagrees or is there but does not
// decode; exitWorkspaceRetry (75) when the filer could not be read (an
// unknown, never taken as "absent"); ("", 0) when the record agrees, does
// not exist yet (the attach
// check creates it from the flags), or no store is being created (numBlocks
// 0: an existing store opens with its own geometry).
func geometryFlagsRefusal(ctx context.Context, cat *chunkstore.Catalog, volumeID string, numBlocks uint64, blockSize uint32) (string, int) {
	if numBlocks == 0 {
		return "", 0 // an existing store: the flags are not used
	}
	v, ok, err := cat.ReadVolume(ctx, volumeID)
	if err != nil && chunkstore.CodeOf(err) == chunkstore.CodeRemoteUnavailable {
		return fmt.Sprintf("blockvolume: the volume record of %s could not be read (%v); no local store was created; retrying", volumeID, err), exitWorkspaceRetry
	}
	if err != nil {
		// A record that is there but does not decode stays that way: a
		// permanent refusal, not a retry loop.
		return fmt.Sprintf("blockvolume: the volume record of %s is unreadable (corrupt: %v); no local store was created; restore or recreate the record", volumeID, err), exitGeometryRefused
	}
	if !ok || (v.NumBlocks == numBlocks && v.BlockSize == blockSize) {
		return "", 0
	}
	return fmt.Sprintf("blockvolume: geometry_mismatch: --durable-blocks %d --durable-blocksize %d disagree with the volume record of %s (%d blocks of %d bytes); no local store was created; start it with the record's geometry",
		numBlocks, blockSize, volumeID, v.NumBlocks, v.BlockSize), exitGeometryRefused
}

// chunkHolder is how this host names itself in HEAD: <server-id>/<replica-id>
// (B9 W2 appendix). The replica id alone is not enough:
// two hosts that both serve replica r1 would look like the same holder
// returning, and the later one would not be fenced.
func chunkHolder(f flags) string { return f.serverID + "/" + f.replicaID }

// chunkVolumeRoot is <base>/<volumeID>; the volume ID must be one plain
// path element so no volume can reach another volume's directory.
func chunkVolumeRoot(base, volumeID string) (string, error) {
	if volumeID == "" || volumeID == "." || volumeID == ".." || strings.ContainsAny(volumeID, "/\\") {
		return "", fmt.Errorf("blockvolume: volume id %q is not a plain path element", volumeID)
	}
	return path.Join(base, volumeID), nil
}
