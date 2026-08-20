package tape9

import (
	"fmt"
	"strings"
)

// Compression controls whether the SDK transforms bytes before storing them.
type Compression string

const (
	// CompressionNone stores payload bytes as-is.
	CompressionNone Compression = ""

	// CompressionZstd stores payload bytes as framed zstd records.
	CompressionZstd Compression = "zstd"
)

// IsValid reports whether c is a supported payload compression mode.
func (c Compression) IsValid() bool {
	switch c {
	case CompressionNone, CompressionZstd:
		return true
	default:
		return false
	}
}

// ParseCompression parses a CLI/SDK compression mode.
func ParseCompression(raw string) (Compression, error) {
	compression := Compression(strings.ToLower(strings.TrimSpace(raw)))
	if compression == "" {
		return CompressionNone, nil
	}
	if !compression.IsValid() {
		return "", fmt.Errorf("invalid compression: %q", raw)
	}
	return compression, nil
}

func (c Compression) payloadFormat() (payloadFormat, error) {
	switch c {
	case CompressionNone:
		return payloadFormatIdentity, nil
	case CompressionZstd:
		return payloadFormatFramedZstdV1, nil
	default:
		return "", fmt.Errorf("invalid compression: %q", c)
	}
}
