package durable

// SetTestAfterChunkResolve installs the test-only hook CutVolume runs right
// after it resolves the volume's handle and layer.
func SetTestAfterChunkResolve(p *DurableProvider, f func()) { p.testAfterChunkResolve = f }
