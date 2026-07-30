package replication

import (
	"context"
	"errors"
	"fmt"
)

func waitForPeerAcks(
	ctx context.Context,
	mode DurabilityMode,
	rf int,
	peerCount int,
	results <-chan peerWorkResult,
	operation string,
) error {
	if peerCount == 0 {
		return nil
	}
	requiredPeerSuccesses := peerCount
	if mode == DurabilitySyncQuorum {
		requiredPeerSuccesses = rf / 2
	}

	successes := 0
	failures := 0
	var firstErr error
	var terminalErr error
	for received := 0; received < peerCount; received++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-results:
			if result.err == nil && result.eligible {
				successes++
			} else {
				failures++
				if firstErr == nil {
					if result.err != nil {
						firstErr = result.err
					} else {
						firstErr = fmt.Errorf("replication: peer %s was not write-ack eligible", result.peerID)
					}
				}
				if terminalErr == nil && isPeerQueueTerminalError(result.err) {
					terminalErr = result.err
				}
			}
		}

		remaining := peerCount - received - 1
		if mode == DurabilitySyncQuorum {
			if successes >= requiredPeerSuccesses {
				return nil
			}
			if successes+remaining < requiredPeerSuccesses {
				return durabilityAckError(ErrDurabilityQuorumLost, operation, successes+1, rf/2+1, firstErr)
			}
		}
	}

	switch mode {
	case DurabilitySyncAll:
		if failures > 0 {
			return durabilityAckError(ErrDurabilityBarrierFailed, operation, successes+1, rf, firstErr)
		}
	case DurabilityBestEffort:
		if terminalErr != nil {
			return terminalErr
		}
	}
	return nil
}

func waitForSyncAcks(
	ctx context.Context,
	mode DurabilityMode,
	targetLSN uint64,
	peerCount int,
	localResult <-chan localSyncResult,
	results <-chan peerWorkResult,
) error {
	rf := peerCount + 1
	requiredPeerSuccesses := peerCount
	if mode == DurabilitySyncQuorum {
		requiredPeerSuccesses = rf / 2
	}

	localDone := false
	peerReceived := 0
	peerSuccesses := 0
	var firstPeerErr error
	for !localDone || peerReceived < peerCount {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-localResult:
			localDone = true
			if result.err != nil {
				return result.err
			}
		case result := <-results:
			peerReceived++
			if result.err == nil {
				peerSuccesses++
			} else if firstPeerErr == nil {
				firstPeerErr = result.err
			}
		}

		if mode == DurabilitySyncQuorum {
			if localDone && peerSuccesses >= requiredPeerSuccesses {
				return nil
			}
			remaining := peerCount - peerReceived
			if localDone && peerSuccesses+remaining < requiredPeerSuccesses {
				return durabilityAckError(
					ErrDurabilityQuorumLost,
					fmt.Sprintf("sync target LSN %d", targetLSN),
					peerSuccesses+1,
					rf/2+1,
					firstPeerErr,
				)
			}
		}
	}

	if mode == DurabilitySyncAll && firstPeerErr != nil {
		return durabilityAckError(
			ErrDurabilityBarrierFailed,
			fmt.Sprintf("sync target LSN %d", targetLSN),
			peerSuccesses+1,
			rf,
			firstPeerErr,
		)
	}
	return nil
}

func durabilityAckError(kind error, operation string, got, required int, cause error) error {
	if cause != nil {
		return fmt.Errorf("%w: %s acknowledgements %d/%d: %w", kind, operation, got, required, cause)
	}
	return fmt.Errorf("%w: %s acknowledgements %d/%d", kind, operation, got, required)
}

func isPeerQueueTerminalError(err error) bool {
	return errors.Is(err, ErrPeerQueueSaturated) || errors.Is(err, ErrPeerQueueClosed)
}
