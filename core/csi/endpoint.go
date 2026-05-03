package csi

import (
	"fmt"
	"net/url"
	"strings"
)

func ParseEndpoint(endpoint string) (network, address string, err error) {
	if strings.HasPrefix(endpoint, "unix://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return "", "", err
		}
		addr := u.Path
		if u.Host != "" {
			addr = u.Host + addr
		}
		if addr == "" {
			return "", "", fmt.Errorf("empty unix endpoint")
		}
		return "unix", addr, nil
	}
	if strings.HasPrefix(endpoint, "tcp://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return "", "", err
		}
		if u.Host == "" {
			return "", "", fmt.Errorf("empty tcp endpoint host")
		}
		return "tcp", u.Host, nil
	}
	return "", "", fmt.Errorf("unsupported endpoint scheme: %s", endpoint)
}
