package iscsi

type dataInWriter struct {
	maxSegment uint32
}

func newDataInWriter(maxSegment int) *dataInWriter {
	if maxSegment <= 0 {
		maxSegment = DefaultNegotiableConfig().MaxRecvDataSegmentLength
	}
	return &dataInWriter{maxSegment: uint32(maxSegment)}
}

func (w *dataInWriter) build(req *PDU, result SCSIResult) []*PDU {
	data := result.Data
	if len(data) == 0 {
		p := w.basePDU(req)
		p.BHS[1] = FlagF | FlagS
		p.SetSCSIStatusByte(result.Status)
		p.SetResidualCount(req.ExpectedDataTransferLength())
		return []*PDU{p}
	}

	maxSeg := w.maxSegment
	if maxSeg == 0 {
		maxSeg = uint32(len(data))
	}

	pdus := make([]*PDU, 0, (uint32(len(data))+maxSeg-1)/maxSeg)
	var offset uint32
	var dataSN uint32
	for offset < uint32(len(data)) {
		segLen := maxSeg
		if offset+segLen > uint32(len(data)) {
			segLen = uint32(len(data)) - offset
		}
		final := offset+segLen == uint32(len(data))

		p := w.basePDU(req)
		p.SetDataSN(dataSN)
		p.SetBufferOffset(offset)
		p.DataSegment = data[offset : offset+segLen]
		if final {
			p.BHS[1] = FlagF | FlagS
			p.SetSCSIStatusByte(result.Status)
			p.SetResidualCount(residualCount(req.ExpectedDataTransferLength(), uint32(len(data))))
		}
		pdus = append(pdus, p)

		offset += segLen
		dataSN++
	}
	return pdus
}

func (w *dataInWriter) basePDU(req *PDU) *PDU {
	p := &PDU{}
	p.SetOpcode(OpSCSIDataIn)
	p.SetLUN(req.LUN())
	p.SetInitiatorTaskTag(req.InitiatorTaskTag())
	p.SetTargetTransferTag(0xFFFFFFFF)
	return p
}

func residualCount(expected, actual uint32) uint32 {
	if expected > actual {
		return expected - actual
	}
	if actual > expected {
		return actual - expected
	}
	return 0
}
