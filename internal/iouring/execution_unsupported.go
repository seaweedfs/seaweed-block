//go:build !linux || !amd64

package iouring

type Executor struct{}

func New(uint32) (*Executor, error) {
	return nil, ErrUnsupported
}

func (*Executor) SubmitAndWait([]Operation) ([]Completion, error) {
	return nil, ErrUnsupported
}

func (*Executor) Stats() ExecutionStats {
	return ExecutionStats{}
}

func (*Executor) Close() error {
	return nil
}
