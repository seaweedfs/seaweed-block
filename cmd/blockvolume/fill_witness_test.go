package main_test

import (
	"errors"
	"fmt"
	"testing"
)

// fillProgress is what a test knows about a running fill: the absent chunk
// count when its reader started, the count now, and the reader's state.
type fillProgress struct {
	baseline, absent int
	reads            int64
	readerErr        error
	finished         bool
}

// fillWitness is nil only while a fill is observably running: the reader
// has completed a read without error and has not finished, the absent
// count fell below its value when the reader started, and work remains.
// Remaining work alone does not show a fill is running.
func fillWitness(p fillProgress) error {
	switch {
	case p.readerErr != nil:
		return fmt.Errorf("the reader failed: %w", p.readerErr)
	case p.reads == 0:
		return errors.New("the reader has completed no read")
	case p.finished:
		return errors.New("the reader finished: no fill left to interrupt")
	case p.absent >= p.baseline:
		return fmt.Errorf("no chunk was made local since the reader started (%d absent, %d at its start)", p.absent, p.baseline)
	case p.absent == 0:
		return errors.New("every chunk is local: no fill left to interrupt")
	}
	return nil
}

func TestFillWitnessNeedsAnObservedRunningFill(t *testing.T) {
	running := fillProgress{baseline: 16, absent: 12, reads: 3}
	if err := fillWitness(running); err != nil {
		t.Fatalf("a running fill: %v", err)
	}
	for name, p := range map[string]fillProgress{
		"reader never started": {baseline: 16, absent: 16},
		"reader failed":        {baseline: 16, absent: 12, reads: 3, readerErr: errors.New("read: i/o timeout")},
		"reader finished":      {baseline: 16, absent: 12, reads: 8, finished: true},
		"no progress":          {baseline: 16, absent: 16, reads: 3},
		"nothing left":         {baseline: 16, absent: 0, reads: 16},
	} {
		if err := fillWitness(p); err == nil {
			t.Errorf("%s: %+v accepted as a running fill", name, p)
		}
	}
}
