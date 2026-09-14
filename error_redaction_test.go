package ags

import (
	"errors"
	"strings"
	"testing"
)

func TestErrorStringOmitsCauseAndResourceIdentifiers(t *testing.T) {
	err := &Error{
		Code:       Unavailable,
		Operation:  "Sandboxes.Create",
		Reason:     "READINESS_FAILED",
		RequestID:  "request-sensitive",
		InstanceID: "instance-sensitive",
		Cause:      errors.New("response-body-sensitive"),
	}
	formatted := err.Error()
	for _, secret := range []string{"request-sensitive", "instance-sensitive", "response-body-sensitive"} {
		if strings.Contains(formatted, secret) {
			t.Fatalf("public error string leaked %q", secret)
		}
	}
	if !strings.Contains(formatted, "UNAVAILABLE") || !strings.Contains(formatted, "READINESS_FAILED") {
		t.Fatalf("stable error semantics missing: %q", formatted)
	}
}

func TestErrorIdentityUsesStableCodeAndPreservesUnwrap(t *testing.T) {
	cause := errors.New("synthetic cause")
	err := &Error{Code: NotFound, Operation: "Sandboxes.Get", Cause: cause}
	if !errors.Is(err, &Error{Code: NotFound}) || errors.Is(err, &Error{Code: Conflict}) {
		t.Fatal("stable error code comparison changed")
	}
	if !errors.Is(err, cause) {
		t.Fatal("wrapped cause is no longer available to errors.Is")
	}
}
