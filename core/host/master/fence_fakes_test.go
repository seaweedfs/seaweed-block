package master

import "github.com/seaweedfs/seaweed-block/core/authority"

// fenceObeyed is a fencing round in which every survivor obeys the fence: it
// makes the line's next epoch its durable floor and answers as it did in the
// plain round.
func fenceObeyed(current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	out := make([]PromotionCandidateEvidence, len(survivors))
	for i, s := range survivors {
		s.Floor = current.Epoch + 1
		s.TableValid = true // a replica that obeys reads its evidence
		out[i] = s
	}
	return PromotionProbeResult{FencedEpoch: current.Epoch + 1, Candidates: out}, nil
}

func (s staticPromotionProbe) FencePromotionCandidates(_ string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	if s.err != nil {
		return PromotionProbeResult{}, s.err
	}
	return fenceObeyed(current, survivors)
}

func (p *switchProbe) FencePromotionCandidates(_ string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	p.mu.Lock()
	silent, mismatch := p.fenceSilent, p.fenceMismatch
	p.fenceCalls++
	p.mu.Unlock()
	if silent {
		return PromotionProbeResult{FencedEpoch: current.Epoch + 1}, nil
	}
	r, err := fenceObeyed(current, survivors)
	r.ProfileMismatch = mismatch
	return r, err
}

func (p *swapPromotionProbe) FencePromotionCandidates(_ string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	return fenceObeyed(current, survivors)
}

func (p reviewHeldPromotion) FencePromotionCandidates(_ string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	return fenceObeyed(current, survivors)
}

func (p *workloadPlanPromotionEvidenceProvider) FencePromotionCandidates(_ string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	return fenceObeyed(current, survivors)
}
