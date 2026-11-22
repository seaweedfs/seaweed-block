// Package holder decides whether two HEAD publication owners are the same
// (V1-SPEC r2.38, INV-WS-LOCAL-ROOT-IDENTITY-001). A HEAD names its owner by
// two typed fields: Holder, "<server-id>/<replica-id>", and HolderRoot, the
// nonce of the local root that published it (empty on pre-release HEADs).
// Every HEAD-holder comparison goes through this package; none compares the
// strings directly.
package holder

// Format is the Holder of server's replica.
func Format(server, replica string) string { return server + "/" + replica }

// HeldBy reports whether holder is server's replica. It ignores the root:
// for callers that know only the assignment (the master's move admission).
func HeldBy(holder, server, replica string) bool { return holder == Format(server, replica) }

// SameAssignment reports whether two Holder strings name the same server's
// replica, whatever their roots: where any root of that holder may act (a
// fresh root taking over its holder's HEAD, a sealed incarnation's proof).
func SameAssignment(holderA, holderB string) bool { return holderA == holderB }

// Same reports whether two owners are the same holder and the same local
// root. An empty root equals only an empty root.
func Same(holderA, rootA, holderB, rootB string) bool {
	return holderA == holderB && rootA == rootB
}

// Claimable reports whether a publisher (holder, root) may treat a HEAD
// owned by (headHolder, headRoot) as its own: the same owner, or the same
// holder on a pre-release HEAD that carries no root yet (adopted once: the
// next claim writes the root).
func Claimable(headHolder, headRoot, holder, root string) bool {
	return headHolder == holder && (headRoot == root || headRoot == "")
}
