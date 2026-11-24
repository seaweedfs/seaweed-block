//go:build subprocess && chunkstore_integration

package main_test

import (
	"testing"
	"time"
)

// TestProcedureRetireTemplateSource runs the template-holder procedure:
// fork the seed, publish the template on the holder, fork a consumer, then
// seal/revoke/ACK and delete the original source. No GC grace is advanced;
// the final ordinary pass proves its command and reads, not reclamation.
func TestProcedureRetireTemplateSource(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"template-holder", "consumer"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	source := env.start(t, "blockvolume-source", "s1")
	e := env.adopt(t, source, "s1")
	const template = "template-holder"
	want := map[uint64][]byte{5: pattern("template-seed!!")}
	env.writeBlock(t, "s1", 5, want[5])
	env.snapshot(t, "s1", "v1", "seed")
	for _, step := range []struct{ source, snapshot, target string }{
		{"v1", "seed", template}, {template, "template-v1", "consumer"},
	} {
		env.h.command(t, "ops", "chunk", "fork", "--source", step.source, "--snapshot", step.snapshot,
			"--target", step.target, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath,
			"--request-id", "fork-"+step.target)
		env.startTarget(t, step.target, "blockvolume-"+step.target)
		env.requireBlocks(t, "n-"+step.target, 0, 16, want)
		env.requireEdges(t, "n-"+step.target)
		if step.target == template {
			env.snapshot(t, "n-"+template, template, "template-v1")
		}
	}
	g := env.seal(t, "s1", e)
	env.revoke(t, e, g)
	// The process must ACK and exit naturally; a signal would skip the ACK.
	if err := source.wait(30 * time.Second); err != nil {
		t.Fatalf("source did not exit naturally after revocation: %v", err)
	}
	env.waitAck(t, e, g)
	env.deleteSnapshot(t, "v1", "seed")
	out, stderr, code := env.volumeDelete(t, "delete-template-source")
	requireDeleteCompleted(t, out, stderr, code, false)
	env.requireDeletedForGood(t, "delete-template-source")
	check := func() {
		for _, vol := range []string{template, "consumer"} {
			env.h.command(t, "ops", "chunk", "verify", "--volume", vol, "--all-snapshots", "--depth", "data",
				"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
			env.requireBlocks(t, "n-"+vol, 0, 16, want)
			env.requireEdges(t, "n-"+vol)
		}
		env.requireSnapshotNames(t, template, "template-v1")
	}
	check()
	env.h.command(t, "ops", "chunk", "gc", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath,
		"--master", env.h.masterAddr, "--apply")
	check()
	t.Logf("template source retired; %s/template-v1 and consumer read unchanged after a default-grace GC pass", template)
}
