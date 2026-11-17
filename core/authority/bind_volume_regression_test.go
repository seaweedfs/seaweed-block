package authority

import (
	"context"
	"testing"
	"time"
)

type bindRegressionDirective struct {
	asks      *StaticDirective
	processed chan struct{}
	calls     int
}

func (d *bindRegressionDirective) Next(ctx context.Context) (AssignmentAsk, error) {
	d.calls++
	if d.calls == 3 {
		// Run requests another ask only after it settles the preceding apply.
		close(d.processed)
		<-ctx.Done()
		return AssignmentAsk{}, ctx.Err()
	}
	return d.asks.Next(ctx)
}

func TestPublisher_RunRejectsSecondSlotGenesisBind(t *testing.T) {
	d := &bindRegressionDirective{processed: make(chan struct{}), asks: NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind},
		{VolumeID: "v", ReplicaID: "r2", DataAddr: "d2", CtrlAddr: "c2", Intent: IntentBind},
	})}
	pub := NewPublisher(d)
	first, cancelFirst := pub.Subscribe("v", "r1")
	defer cancelFirst()
	second, cancelSecond := pub.Subscribe("v", "r2")
	defer cancelSecond()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pub.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("publisher did not stop")
		}
	})
	select {
	case <-first:
	case <-time.After(2 * time.Second):
		t.Fatal("first Bind did not publish")
	}
	select {
	case <-d.processed:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher did not settle both asks")
	}
	select {
	case got := <-second:
		t.Fatalf("second slot received genesis authority: %+v; first volume Bind must exclude a second genesis", got)
	default:
	}
}
