package iscsi

// iSCSI text parameter encoding (RFC 7143 §6) +
// negotiation helpers (numeric / boolean per RFC 7143 §13).
//
// Ported from weed/storage/blockvol/iscsi/params.go per T2
// assignment §4.3 ckpt 9 allowlist (parameter negotiation
// mechanism only). Target-side CHAP auth is implemented in
// login.go; this file intentionally stays as generic text
// parameter parsing / negotiation helpers.

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrMalformedParam = errors.New("iscsi: malformed parameter (missing '=')")
	ErrEmptyKey       = errors.New("iscsi: empty parameter key")
	ErrDuplicateKey   = errors.New("iscsi: duplicate parameter key")
)

// Params is an ordered list of iSCSI key-value parameters.
// Order matters during negotiation (RFC 7143 §6.1), so we use
// a slice rather than a map.
type Params struct {
	items []paramItem
}

type paramItem struct {
	key   string
	value string
}

// NewParams returns an empty Params.
func NewParams() *Params { return &Params{} }

// ParseParams decodes "Key=Value\0Key=Value\0..." from raw bytes.
// Trailing nulls / empty segments are ignored. Returns
// ErrDuplicateKey if the same key appears more than once
// (negotiation rule — duplicates are a protocol error).
func ParseParams(data []byte) (*Params, error) {
	p := &Params{}
	if len(data) == 0 {
		return p, nil
	}
	seen := make(map[string]bool)
	parts := strings.Split(string(data), "\x00")
	for _, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.IndexByte(part, '=')
		if idx < 0 {
			return nil, fmt.Errorf("%w: %q", ErrMalformedParam, part)
		}
		key := part[:idx]
		if key == "" {
			return nil, ErrEmptyKey
		}
		value := part[idx+1:]
		if seen[key] {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateKey, key)
		}
		seen[key] = true
		p.items = append(p.items, paramItem{key: key, value: value})
	}
	return p, nil
}

// Encode serializes parameters to "Key=Value\0" format.
func (p *Params) Encode() []byte {
	if len(p.items) == 0 {
		return nil
	}
	var b strings.Builder
	for _, item := range p.items {
		b.WriteString(item.key)
		b.WriteByte('=')
		b.WriteString(item.value)
		b.WriteByte(0)
	}
	return []byte(b.String())
}

// Get returns the value for a key, or ("", false) if not present.
func (p *Params) Get(key string) (string, bool) {
	for _, item := range p.items {
		if item.key == key {
			return item.value, true
		}
	}
	return "", false
}

// Set adds or replaces a parameter.
func (p *Params) Set(key, value string) {
	for i, item := range p.items {
		if item.key == key {
			p.items[i].value = value
			return
		}
	}
	p.items = append(p.items, paramItem{key: key, value: value})
}

// Len returns the number of parameters.
func (p *Params) Len() int { return len(p.items) }

// Each iterates over key-value pairs in order.
func (p *Params) Each(fn func(key, value string)) {
	for _, item := range p.items {
		fn(item.key, item.value)
	}
}

// --- Negotiation helpers ---

// NegotiateNumber applies "smaller-of-two" semantics (RFC 7143
// §13 numeric negotiation). Clamps offered to [min, max] then
// returns min(offered, ours).
func NegotiateNumber(offered string, ours, min, max int) (int, error) {
	v, err := strconv.Atoi(offered)
	if err != nil {
		return 0, fmt.Errorf("iscsi: invalid numeric value %q: %w", offered, err)
	}
	if v < min {
		v = min
	}
	if v > max {
		v = max
	}
	if ours < v {
		return ours, nil
	}
	return v, nil
}

// NegotiateBool applies AND semantics (default per RFC 7143).
// Result is true only if both sides agree to true. Caller
// applies OR semantics for InitialR2T separately.
func NegotiateBool(offered string, ours bool) (bool, error) {
	switch offered {
	case "Yes":
		return ours, nil
	case "No":
		return false, nil
	default:
		return false, fmt.Errorf("iscsi: invalid boolean value %q", offered)
	}
}

// BoolStr renders Yes/No.
func BoolStr(v bool) string {
	if v {
		return "Yes"
	}
	return "No"
}
