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
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	defaultChunkPath = "/swblock/chunks"
	// The remote holds snapshots, not live data: cuts are published at
	// snapshots, at detach/stop and on explicit request. The timer is off
	// by default (an operator may set one, e.g. 15m, as a host-loss safety
	// net); the dirty threshold only bounds how large one cut can grow.
	defaultChunkCaptureInterval = 0
	defaultChunkDirtyThreshold  = 512 // chunks, 2 GiB at 4 MiB chunks
)

type chunkFlags struct {
	hostLimit           hostlimit.Config
	filer               string
	path                string
	collection          string
	replication         string
	captureInterval     time.Duration
	dirtyThreshold      int
	uploadConcurrency   int
	hydrationWorkers    int
	hydrationQueueDepth int
	// controlEndpoint mounts the /chunk/* routes on the status listener.
	controlEndpoint bool
}

func validateChunkFlags(f flags) error {
	c := f.chunk
	if f.durableImpl != "walstore-chunk" {
		if c.filer != "" || c.collection != "" || c.replication != "" || c.path != defaultChunkPath ||
			c.captureInterval != defaultChunkCaptureInterval || c.dirtyThreshold != defaultChunkDirtyThreshold ||
			c.uploadConcurrency != 8 || c.hydrationWorkers != 2 || c.hydrationQueueDepth != 64 || c.controlEndpoint || c.hostLimit.Directory != "" {
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
	if c.dirtyThreshold < 0 {
		return fmt.Errorf("--chunk-dirty-threshold=%d must not be negative", c.dirtyThreshold)
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
type chunkWiring struct {
	remoteFor func(volumeID string) (storage.ChunkRemote, error)
	catalog   *chunkstore.Catalog
	close     func() error
}

// newChunkWiring dials the filer (lazily).
func newChunkWiring(c chunkFlags) (chunkWiring, error) {
	client, err := chunkstore.DialSWFS(chunkstore.SWFSConfig{
		FilerGRPC:   c.filer,
		Collection:  c.collection,
		Replication: c.replication,
		AssignPath:  c.path,
	})
	if err != nil {
		return chunkWiring{}, err
	}
	cat, err := chunkstore.NewCatalog(client, c.path)
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
		catalog: cat,
		close:   client.Close,
	}, nil
}

// chunkAttachCheck applies the VOLUME entry's attach rules before the chunk
// layer attaches (B9 W2 §4.2): create VOLUME from the local geometry when
// it is missing, refuse geometry_mismatch, and refuse fork_incomplete for a
// fork whose HEAD is not written yet.
func chunkAttachCheck(cat *chunkstore.Catalog) func(context.Context, string, durable.ChunkGeometry) error {
	return func(ctx context.Context, volumeID string, g durable.ChunkGeometry) error {
		_, err := cat.EnsureVolume(ctx, volumeID, chunkstore.Geometry{NumBlocks: g.NumBlocks, BlockSize: g.BlockSize, ChunkBlocks: g.ChunkBlocks})
		return err
	}
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
