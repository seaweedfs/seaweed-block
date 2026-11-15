package main

// --durable-impl=walstore-chunk: walstore with the chunk layer publishing
// the volume to SeaweedFS (opt-in proof of concept). Each volume's HEAD and
// chunk key bindings live under <--chunk-path>/<volume-id> on the filer
// given by --chunk-filer; chunk bytes are needles on its volume servers.

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const defaultChunkPath = "/swblock/chunks"

type chunkFlags struct {
	filer           string
	path            string
	collection      string
	replication     string
	captureInterval time.Duration
}

func validateChunkFlags(f flags) error {
	c := f.chunk
	if f.durableImpl != "walstore-chunk" {
		if c.filer != "" || c.collection != "" || c.replication != "" || c.captureInterval != 0 || c.path != defaultChunkPath {
			return errors.New("--chunk-* flags require --durable-impl=walstore-chunk")
		}
		return nil
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
	return nil
}

// newChunkRemoteFor dials the filer (lazily) and returns the provider's
// ChunkRemoteFor plus the client's closer.
func newChunkRemoteFor(c chunkFlags) (func(volumeID string) (storage.ChunkRemote, error), func() error, error) {
	client, err := chunkstore.DialSWFS(chunkstore.SWFSConfig{
		FilerGRPC:   c.filer,
		Collection:  c.collection,
		Replication: c.replication,
		AssignPath:  c.path,
	})
	if err != nil {
		return nil, nil, err
	}
	return func(volumeID string) (storage.ChunkRemote, error) {
		root, err := chunkVolumeRoot(c.path, volumeID)
		if err != nil {
			return nil, err
		}
		return chunkstore.NewSWFSChunkRemote(client, root)
	}, client.Close, nil
}

// chunkVolumeRoot is <base>/<volumeID>; the volume ID must be one plain
// path element so no volume can reach another volume's directory.
func chunkVolumeRoot(base, volumeID string) (string, error) {
	if volumeID == "" || volumeID == "." || volumeID == ".." || strings.ContainsAny(volumeID, "/\\") {
		return "", fmt.Errorf("blockvolume: volume id %q is not a plain path element", volumeID)
	}
	return path.Join(base, volumeID), nil
}
