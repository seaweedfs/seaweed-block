// Package iouring contains the Linux-native execution primitive shared by the
// capability gate and the opt-in storage experiment.
package iouring

import "strings"

type Report struct {
	Platform             string
	KernelRelease        string
	Supported            bool
	RefusalReason        string
	QueueDepth           uint32
	WriteOpcodeSupported bool
	FsyncOpcodeSupported bool
	SubmittedOps         int
	SubmitSyscalls       int
	WriteCompletions     int
	FsyncCompletions     int
	CompletionCount      int
	VerifiedBytes        int
}

func oneLine(value string) string {
	return strings.Join(strings.Fields(value), "_")
}
