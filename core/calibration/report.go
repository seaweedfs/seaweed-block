package calibration

// Report is the machine-readable artifact this package emits. It is the
// one canonical shape consumed by docs/calibration/ and by tester
// evidence review. Keeping the fields minimal and stable matters more
// than being rich — scenario-map.md and divergence-log.md both reference
// this shape.
type Report struct {
	Summary Summary  `json:"summary"`
	Results []Result `json:"results"`
}

type Summary struct {
	Total     int `json:"total"`
	Passed    int `json:"passed"`
	Failed    int `json:"failed"`
	Diverged  int `json:"diverged"`
	Errors    int `json:"errors"`
	AllPassed bool `json:"all_passed"`
}

func newReport(results []Result) Report {
	s := Summary{}
	for _, r := range results {
		s.Total++
		if r.Error != "" {
			s.Errors++
		}
		if r.Pass {
			s.Passed++
		} else {
			s.Failed++
			if r.Divergence != nil && r.Error == "" {
				s.Diverged++
			}
		}
	}
	s.AllPassed = s.Failed == 0
	return Report{Summary: s, Results: results}
}
