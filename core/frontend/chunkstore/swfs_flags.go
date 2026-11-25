package chunkstore

import "flag"

// DefaultChunkPath is the filer directory shared by master creation and
// blockvolume attachment. Both commands register the same storage flags.
const DefaultChunkPath = "/swblock/chunks"

func RegisterSWFSFlags(fs *flag.FlagSet, filer, path, collection, replication *string) {
	fs.StringVar(filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(path, "chunk-path", DefaultChunkPath, "filer directory holding one directory per volume")
	fs.StringVar(collection, "chunk-collection", "", "collection for chunk needles (empty = filer default / storage rule)")
	fs.StringVar(replication, "chunk-replication", "", "replication for chunk needles (empty = filer default / storage rule)")
}
