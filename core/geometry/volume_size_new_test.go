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
	for _, by := range []uint64{4096, 4 << 20} {
		if err := ValidateNewVolumeSize(MaxNewVolumeBytes + by); !errors.Is(err, ErrVolumeTooLarge) {
			t.Fatalf("%d bytes above: %v, want ErrVolumeTooLarge", by, err)
		}
	}
}
