package tape9

import (
	"fmt"
	"strings"
)

// RetainMode controls which visible window a tape keeps once retention trims bytes.
type RetainMode string

const (
	// RetainModeMiddle keeps the configured head and tail windows.
	RetainModeMiddle RetainMode = "middle"

	// RetainModeHead keeps the earliest visible window.
	RetainModeHead RetainMode = "head"

	// RetainModeTail keeps the latest visible window.
	RetainModeTail RetainMode = "tail"
)

// ParseRetainMode parses a CLI/SDK retain mode.
func ParseRetainMode(raw string) (RetainMode, error) {
	mode := RetainMode(strings.ToLower(strings.TrimSpace(raw)))
	if mode == "" {
		return "", nil
	}
	if !mode.IsValid() {
		return "", fmt.Errorf("invalid retain mode: %q", raw)
	}
	return mode, nil
}

// IsValid reports whether m is a supported tape retention shape.
func (m RetainMode) IsValid() bool {
	switch m {
	case RetainModeMiddle, RetainModeHead, RetainModeTail:
		return true
	default:
		return false
	}
}
