package ags

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestNormalizeErrorPreservesDeadlineAndClassifiesMalformedWire(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code ErrorCode
	}{
		{"deadline", context.DeadlineExceeded, DeadlineExceeded},
		{"malformed_json", &json.SyntaxError{Offset: 1}, Protocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			normalized := normalizeError("Synthetic.Operation", tc.err)
			var sdk *Error
			if !errors.As(normalized, &sdk) || sdk.Code != tc.code || sdk.Operation != "Synthetic.Operation" || !errors.Is(normalized, tc.err) {
				t.Fatalf("normalized error = %#v", normalized)
			}
		})
	}
}
