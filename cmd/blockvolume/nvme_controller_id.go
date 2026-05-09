package main

import (
	"hash/fnv"
	"strconv"
)

const maxNVMeControllerID uint32 = 0xffef

func nvmeControllerIDFromReplicaID(replicaID string) uint16 {
	if n, ok := trailingDecimal(replicaID); ok {
		if n == 0 {
			return 1
		}
		if n > uint64(maxNVMeControllerID) {
			n = uint64(maxNVMeControllerID)
		}
		return uint16(n)
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(replicaID))
	return uint16(h.Sum32()%maxNVMeControllerID + 1)
}

func trailingDecimal(s string) (uint64, bool) {
	if s == "" {
		return 0, false
	}
	start := len(s)
	for start > 0 && s[start-1] >= '0' && s[start-1] <= '9' {
		start--
	}
	if start == len(s) {
		return 0, false
	}
	n, err := strconv.ParseUint(s[start:], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
