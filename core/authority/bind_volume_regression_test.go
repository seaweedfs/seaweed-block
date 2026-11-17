package authority

import (
	"context"
	"testing"
	"time"
)

func TestPublisher_RunRejectsSecondSlotGenesisBind(t *testing.T) {
	pub := NewPublisher(NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind},
		{VolumeID: "v", ReplicaID: "r2", DataAddr: "d2", CtrlAddr: "c2", Intent: IntentBind},
	}))
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
	// A rejected second Bind leaves this subscriber empty. The current defect
	// publishes the second genesis immediately; the bound also catches stalls.
	select {
	case got := <-second:
		t.Fatalf("second slot received genesis authority: %+v; first volume Bind must exclude a second genesis", got)
	case <-time.After(100 * time.Millisecond):
	}
}
