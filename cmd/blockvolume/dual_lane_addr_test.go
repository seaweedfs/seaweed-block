package main

import "testing"

// TestDeriveDualLaneAddr pins the port+1 deployment convention
// (docs/recovery-wiring-plan.md §4 Option A). Production daemons MUST
// agree on this offset until AssignmentFact carries an explicit
// dual-lane field.
func TestDeriveDualLaneAddr(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"127.0.0.1:9220", "127.0.0.1:9221", false},
		{"127.0.0.1:0", "127.0.0.1:1", false},
		{"[::1]:9220", "[::1]:9221", false},
		{"192.168.1.10:65534", "192.168.1.10:65535", false},
		{"192.168.1.10:65535", "", true}, // overflow
		{"no-port-here", "", true},
		{"127.0.0.1:notanumber", "", true},
	}
	for _, tc := range cases {
		got, err := deriveDualLaneAddr(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("deriveDualLaneAddr(%q): want error, got %q", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("deriveDualLaneAddr(%q): unexpected err %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("deriveDualLaneAddr(%q): got %q want %q", tc.in, got, tc.want)
		}
	}
}
