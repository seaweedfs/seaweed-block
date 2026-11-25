package storage

import (
	"errors"
	"testing"
)

// A restart loses in-memory cost samples, even when its one acknowledged
// write has already reached HEAD. Retry must make a decision without a new
// guest write; it cannot rely on wall-clock scheduling to create a sample.
func TestPrecopyColdPublishedWriteRetryConverges(t *testing.T) {
	_, _, layer, _ := precopyLayer(t, 1024, 1)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	layer.precopySamples = nil // inject the cold estimator state after restart
	for attempt := 1; attempt <= 3; attempt++ {
		stopped := 0
		report, err := layer.PrecopyDetach(ctTimeout(t), func() bool { stopped++; return true })
		t.Logf("attempt=%d err=%v reason=%s rounds=%d rate_samples=%d stopped=%d samples_after=%d", attempt, err, report.Precopy.Reason, len(report.Precopy.Rounds), report.Precopy.RateSamples, stopped, len(layer.precopySamples))
		if err == nil {
			if stopped != 1 || !report.Precopy.Frozen {
				t.Fatalf("successful detach did not freeze: %+v", report.Precopy)
			}
			return
		}
		if !errors.Is(err, ErrPrecopyUnknown) || stopped != 0 {
			t.Fatalf("unexpected refusal: err=%v stopped=%d", err, stopped)
		}
	}
	t.Fatal("cold detach did not converge after three bounded retries without new writes")
}
