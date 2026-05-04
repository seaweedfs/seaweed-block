package frontend

import "errors"

// ErrStalePrimary is returned by Backend.Read / Backend.Write
// when the current projection lineage no longer matches the
// backend's Identity (sketch §7). Every operation re-reads the
// projection; open-time validation alone is insufficient.
var ErrStalePrimary = errors.New("frontend: stale primary lineage")

// ErrNotReady is returned by Provider.Open when the projection
// never reports healthy before the context deadline.
var ErrNotReady = errors.New("frontend: volume not ready")

// ErrBackendClosed is returned by Backend.Read / Backend.Write
// after Backend.Close has been invoked.
var ErrBackendClosed = errors.New("frontend: backend closed")
