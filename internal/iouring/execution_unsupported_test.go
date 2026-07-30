//go:build !linux || !amd64

package iouring

import (
	"errors"
	"testing"
)

func TestExecutorIsExplicitlyUnsupported(t *testing.T) {
	executor, err := New(8)
	if executor != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("New=(%v,%v) want=(nil,ErrUnsupported)", executor, err)
	}
}
