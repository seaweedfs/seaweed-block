//go:build !linux

package nvmerdma

import (
	"errors"

	"github.com/seaweedfs/seaweed-block/core/frontend/nbd"
)

func Implemented() bool { return false }

func startPlatformTarget(TargetConfig, nbd.Backend) (platformTarget, string, error) {
	return nil, "", errors.New("nvmerdma: Linux required")
}
