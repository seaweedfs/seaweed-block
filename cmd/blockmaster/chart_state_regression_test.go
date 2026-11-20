package main

import (
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChartDefaultAuthorityStateSurvivesPodReplacement(t *testing.T) {
	if os.Getenv("SEAWEED_KNOWN_BUGS") != "1" {
		t.Skip("known bug #187 (open): reproduction runs with SEAWEED_KNOWN_BUGS=1; the fix PR removes this skip")
	}
	var values struct {
		Blockmaster struct {
			StateHostPath  string `yaml:"stateHostPath"`
			AuthorityStore string `yaml:"authorityStore"`
		} `yaml:"blockmaster"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "seaweed-block", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	template, err := os.ReadFile(filepath.Join("..", "..", "charts", "seaweed-block", "templates", "blockmaster.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if values.Blockmaster.StateHostPath == "" && strings.Contains(string(template), "emptyDir: {}") {
		t.Fatalf("default authorityStore=%s uses the chart's emptyDir state branch; a replacement Pod receives an empty store (same-Pod container restart is unaffected)", values.Blockmaster.AuthorityStore)
	}
}
