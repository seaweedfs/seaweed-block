package nvme

// ANAState is the NVMe Asymmetric Namespace Access state reported in the ANA
// log page. Values follow NVMe 1.3 ANA state encoding.
type ANAState uint8

const (
	ANAOptimized      ANAState = 0x01
	ANANonOptimized   ANAState = 0x02
	ANAInaccessible   ANAState = 0x03
	ANAPersistentLoss ANAState = 0x04
	ANAChange         ANAState = 0x0F
)

// ANAProvider supplies host-visible path state. It is deliberately small:
// protocol code reports the facts, but authority and replica readiness stay in
// product state outside the NVMe package.
type ANAProvider interface {
	ANAState() ANAState
	ANAGroupID() uint32
	ANAChangeCount() uint64
}
