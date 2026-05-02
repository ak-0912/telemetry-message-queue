package grpcsvc

import (
	"fmt"
	"testing"

	offsetstore "github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/offset"
	"github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapRPCError_nil(t *testing.T) {
	t.Parallel()
	if mapRPCError(nil) != nil {
		t.Fatal("nil in should be nil out")
	}
}

func TestMapRPCError_codes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err  error
		code codes.Code
	}{
		{fmt.Errorf("boom"), codes.Internal},
		{fmt.Errorf("topic %q: %w", "x", store.ErrUnknownTopic), codes.NotFound},
		{fmt.Errorf("partition %d: %w", 9, store.ErrInvalidPartition), codes.OutOfRange},
		{fmt.Errorf("regression: %w", offsetstore.ErrOffsetRegression), codes.InvalidArgument},
	}
	for _, tc := range tests {
		st, ok := status.FromError(mapRPCError(tc.err))
		if !ok {
			t.Fatalf("not a status: %v", tc.err)
		}
		if st.Code() != tc.code {
			t.Fatalf("%v: code %v want %v", tc.err, st.Code(), tc.code)
		}
	}
}
