package tape9

import (
	"regexp"
	"testing"
)

func TestNewIdempotencyKeyMatchesContract(t *testing.T) {
	got, err := NewIdempotencyKey()
	if err != nil {
		t.Fatalf("NewIdempotencyKey: %v", err)
	}
	if len(got) != idempotencyKeyLength {
		t.Fatalf("idempotency key length = %d, want %d", len(got), idempotencyKeyLength)
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{16}$`).MatchString(got) {
		t.Fatalf("idempotency key %q does not match contract", got)
	}
}
