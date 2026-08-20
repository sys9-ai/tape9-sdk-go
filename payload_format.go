package tape9

import (
	"fmt"
	"strings"
)

type payloadFormat string

const (
	payloadFormatIdentity payloadFormat = "identity"

	payloadFormatFramedZstdV1 payloadFormat = "framed-zstd-v1"
)

func (f payloadFormat) isValid() bool {
	switch f {
	case payloadFormatIdentity, payloadFormatFramedZstdV1:
		return true
	default:
		return false
	}
}

func (f payloadFormat) orDefault() payloadFormat {
	if f == "" {
		return payloadFormatIdentity
	}
	return f
}

func storedPayloadFormat(raw string) (payloadFormat, error) {
	format := payloadFormat(strings.ToLower(strings.TrimSpace(raw)))
	if format == "" {
		return payloadFormatIdentity, nil
	}
	if !format.isValid() {
		return "", fmt.Errorf("invalid payload_format: %q", raw)
	}
	return format, nil
}
