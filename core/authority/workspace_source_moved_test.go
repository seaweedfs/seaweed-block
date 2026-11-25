package authority

import "testing"

// A known issued source is permanent only while another known server owns the
// current line. Unknown writes, a new waiting target, and a move back are not.
func TestWorkspaceSourceMovedProof(t *testing.T) {
	for _, tc := range []struct {
		name            string
		edit            func(*Publisher)
		server, replica string
		want            bool
	}{
		{"issued source", func(*Publisher) {}, "node-a", "r1", true},
		{"new waiting target", func(*Publisher) {}, "node-c", "r1", false},
		{"current target", func(*Publisher) {}, "node-b", "r1", false},
		{"wrong replica", func(*Publisher) {}, "node-a", "r2", false},
		{"no server", func(*Publisher) {}, "", "r1", false},
		{"uncertain", func(p *Publisher) { p.workspaceUncertain["v"] = true }, "node-a", "r1", false},
		{"reserved only", func(p *Publisher) { p.workspace["v"].Moves[0].Issued = false }, "node-a", "r1", false},
		{"no issued history", func(p *Publisher) { p.workspace["v"].Moves = nil }, "node-a", "r1", false},
		{"no line", func(p *Publisher) { delete(p.leaseViews, "v") }, "node-a", "r1", false},
		{"unknown owner", func(p *Publisher) { p.leaseViews["v"].d.issuedHost = "" }, "node-a", "r1", false},
		{"moved back", func(p *Publisher) { p.leaseViews["v"].d.issuedHost = "node-a" }, "node-a", "r1", false},
		{"line not newer", func(p *Publisher) { p.leaseViews["v"].d.epoch = 1 }, "node-a", "r1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPublisher(nil)
			p.leaseViews["v"] = &leaseView{d: leaseDerived{bound: true, replica: "r1", epoch: 8, issuedHost: "node-b", ownerFromMove: true}}
			p.workspace["v"] = &WorkspaceState{Moves: []WorkspaceMoveRecord{{Issued: true, OldHost: "node-a", OldReplicaID: "r1", OldEpoch: 1, NewHost: "node-b", NewEpoch: 8}}}
			tc.edit(p)
			line, moved := p.WorkspaceSourceMoved("v", tc.replica, tc.server)
			if moved != tc.want || moved && (line.Epoch != 8 || line.IssuedHost != "node-b") {
				t.Fatalf("line=%+v moved=%v; want %v", line, moved, tc.want)
			}
		})
	}
}
