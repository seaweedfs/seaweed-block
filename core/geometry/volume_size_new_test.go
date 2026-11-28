package geometry

import (
	"errors"
	"testing"
)

func TestValidateNewVolumeSizeAtAndAboveTheMaximum(t *testing.T) {
	if MaxNewVolumeBytes > MaxVolumeBytes {
		t.Fatalf("MaxNewVolumeBytes %d is above MaxVolumeBytes %d", MaxNewVolumeBytes, MaxVolumeBytes)
	}
	if err := ValidateNewVolumeSize(MaxNewVolumeBytes); err != nil {
		t.Fatalf("at the maximum: %v", err)
	}
	if err := ValidateNewVolumeSize(MaxNewVolumeBytes + 4<<20); !errors.Is(err, ErrVolumeTooLarge) {
		t.Fatalf("one chunk above: %v, want ErrVolumeTooLarge", err)
	}
}
