package schema

import (
	"bytes"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadCaseSuite loads a conformance case suite from a YAML file.
func LoadCaseSuite(path string) (*CaseSuite, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("schema: read %s: %w", path, err)
	}
	return ParseCaseSuite(data)
}

// ParseCaseSuite parses a conformance case suite from YAML bytes.
// Uses strict decoding: unknown fields are rejected to prevent typos
// from silently weakening conformance expectations.
func ParseCaseSuite(data []byte) (*CaseSuite, error) {
	var suite CaseSuite
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // reject unknown fields
	if err := dec.Decode(&suite); err != nil {
		return nil, fmt.Errorf("schema: parse: %w", err)
	}
	if err := validateSuite(&suite); err != nil {
		return nil, err
	}
	return &suite, nil
}

func validateSuite(suite *CaseSuite) error {
	if suite.Version == "" {
		return fmt.Errorf("schema: missing version")
	}
	if len(suite.Cases) == 0 {
		return fmt.Errorf("schema: no cases defined")
	}
	for i, c := range suite.Cases {
		if c.Name == "" {
			return fmt.Errorf("schema: case %d missing name", i)
		}
		if len(c.Events) == 0 {
			return fmt.Errorf("schema: case %q has no events", c.Name)
		}
		for j, ev := range c.Events {
			if ev.Kind == "" {
				return fmt.Errorf("schema: case %q event %d missing kind", c.Name, j)
			}
		}
	}
	return nil
}
