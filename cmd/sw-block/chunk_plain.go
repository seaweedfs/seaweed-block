package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
)

// M5 PlainStop -> Quiesce, INV-CUT-PLAIN-DETACH-001. Keep stdout JSON
// unchanged; the separate human readout identifies the seal to the operator.
func chunkPlainDetachPost(addr, volume string, timeout time.Duration, body map[string]any, stdout, stderr io.Writer) int {
	var out bytes.Buffer
	code := chunkPostLoop("detach", addr, "/chunk/detach", timeout, body, &out, stderr)
	if code != ops.VolumeStatusExitOK {
		if err := writeAll(stdout, out.Bytes()); err != nil {
			fmt.Fprintf(stderr, "sw-block ops chunk detach: write result: %v\n", err)
		}
		return code
	}
	var seal struct {
		VolumeID string `json:"volumeID"`
		HeadGen  uint64 `json:"headGen"`
	}
	if err := json.Unmarshal(out.Bytes(), &seal); err != nil || seal.VolumeID != volume || seal.HeadGen == 0 {
		return chunkFail("detach", stdout, stderr, chunkExitFailed, codeInternalCLI, "plain detach outcome unknown: reply has no matching volume and sealed HEAD generation", false)
	}
	if err := writeAll(stdout, out.Bytes()); err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk detach: write result: %v\n", err)
		return chunkExitFailed
	}
	fmt.Fprintf(stderr, "sealed HEAD generation: %d\n", seal.HeadGen)
	return ops.VolumeStatusExitOK
}
