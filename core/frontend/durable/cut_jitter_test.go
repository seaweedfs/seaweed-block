package durable

import (
	"testing"
	"time"
)

func TestCutInitialJitterAndExplicitReset(t *testing.T) {
	pol := testPolicy()
	draws := 0
	draw := func(n int64) int64 {
		draws++
		if n != int64(30*time.Second) {
			t.Fatalf("jitter bound %d", n)
		}
		return int64(10 * time.Second)
	}
	s := initialCutSchedule(pol.interval, t0, draw)
	if draws != 1 {
		t.Fatalf("random draws %d", draws)
	}
	if _, due := pol.due(s, at(39*time.Second), false); due {
		t.Fatal("initial jitter ignored")
	}
	if _, due := pol.due(s, at(40*time.Second), false); !due {
		t.Fatal("initial cut never due")
	}
	// Explicit cut at five seconds resets to a normal interval; it must not
	// inherit the original startup delay or get another random draw.
	s = s.observe(at(5 * time.Second).UnixNano())
	if _, due := pol.due(s, at(34*time.Second), false); due {
		t.Fatal("early after explicit cut")
	}
	if _, due := pol.due(s, at(35*time.Second), false); !due {
		t.Fatal("startup delay survived explicit cut")
	}
	if draws != 1 {
		t.Fatal("explicit cut redrew jitter")
	}
	off := initialCutSchedule(0, t0, func(int64) int64 { t.Fatal("disabled timer sampled jitter"); return 0 })
	pol.interval = 0
	if _, due := pol.due(off, at(24*time.Hour), false); due {
		t.Fatal("disabled timer cut")
	}
}
