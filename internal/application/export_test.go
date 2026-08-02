package application

import "context"

// TestOnlyEmitHeartbeat is the external-test hook for the unexported
// emitHeartbeat. Only *_test.go files see it (Go's file-suffix rule
// scopes this file to the test build), so production callers cannot
// reach past Run into the heartbeat directly.
func (s *MeteringService) TestOnlyEmitHeartbeat(ctx context.Context) {
	s.emitHeartbeat(ctx)
}
