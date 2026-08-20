package tape9

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

const idempotencyKeyLength = 16

// NewIdempotencyKey returns a freshly generated idempotency key.
//
// The returned key matches the server contract: [A-Za-z0-9_-]{16}.
func NewIdempotencyKey() (string, error) {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate idempotency key: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
