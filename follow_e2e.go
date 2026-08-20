//go:build tape9_e2e

package tape9

import "time"

func init() {
	// Keep CLI e2e behavior coverage without paying the real 30s+30s follow waits.
	waitContentCallMaxWait = 300 * time.Millisecond
	waitContentCallTimeoutGrace = 100 * time.Millisecond
}
