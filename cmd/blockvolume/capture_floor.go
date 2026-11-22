//go:build !swb_enterprise

package main

import "time"

// minChunkCaptureInterval is the shortest --chunk-capture-interval this
// build accepts; 0 turns the timer off. A build with the swb_enterprise tag
// supplies its own value.
const minChunkCaptureInterval = 5 * time.Minute
