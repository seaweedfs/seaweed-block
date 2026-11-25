//go:build !kzhold

package main

import "github.com/seaweedfs/seaweed-block/core/frontend/durable"

// applyBuildTagHold does nothing in a normal build.
func applyBuildTagHold(*durable.DurableProvider) {}
