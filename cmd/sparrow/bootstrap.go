package main

// The durable-authority bootstrap logic now lives in
// core/host/bootstrap so that both cmd/sparrow and cmd/v3master
// can share it. This file keeps the prior sparrow-internal names
// as thin aliases so existing S5 / S7 tests compile unchanged.

import (
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/bootstrap"
)

// DurableAuthorityBootstrap is re-exported from core/host/bootstrap
// so existing sparrow code and tests can keep using the short name.
type DurableAuthorityBootstrap = bootstrap.DurableAuthorityBootstrap

// Bootstrap and BootstrapFromStore are forwarded to the shared
// library. Signatures are preserved verbatim.
func Bootstrap(storeDir string, directive authority.Directive) (*DurableAuthorityBootstrap, error) {
	return bootstrap.Bootstrap(storeDir, directive)
}

func BootstrapFromStore(lock *authority.StoreLock, store authority.AuthorityStore, directive authority.Directive) (*DurableAuthorityBootstrap, error) {
	return bootstrap.BootstrapFromStore(lock, store, directive)
}
