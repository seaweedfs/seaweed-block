package main

import "testing"

// The volume this command runs adopts no primary line that names no
// process: the master stamps every line with the process it was issued
// for, and an unnamed one is re-issued for this process on its first
// report. Off, a restarted process could take an unnamed line and reuse a
// dead process's sessions.
func TestHostConfigAdoptsOnlyLinesNamingThisProcess(t *testing.T) {
	if !hostConfig(flags{}, nil).LinesNameTheirProcess {
		t.Fatal("the shipping command adopts primary lines that name no process")
	}
}
