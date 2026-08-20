package tape9

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const maxUsageScopes = 1024

// TapeUsage describes the current payload usage of one live tape.
// RetentionDroppedLogicalBytes is nil until retention tracking has a complete
// baseline; RetentionTrackingStartedAt identifies the beginning of that window.
type TapeUsage struct {
	// TapeID identifies the tape described by this usage result.
	TapeID string `json:"tape_id"`

	// LogicalBytes is the current decoded payload size, when known.
	LogicalBytes *int64 `json:"logical_bytes"`

	// StoredBytes is the current encoded payload size.
	StoredBytes int64 `json:"stored_bytes"`

	// RetainApplied reports whether retention has hidden stored bytes.
	RetainApplied bool `json:"retain_applied"`

	// DroppedBytes is the encoded byte count hidden by retention.
	DroppedBytes int64 `json:"dropped_bytes"`

	// RetentionDroppedLogicalBytes is the logical byte count hidden by retention, when known.
	RetentionDroppedLogicalBytes *int64 `json:"retention_dropped_logical_bytes"`

	// RetentionTrackingStartedAt is the beginning of the complete logical-retention window.
	RetentionTrackingStartedAt *time.Time `json:"retention_tracking_started_at"`
}

// UsageSummary aggregates payload usage for caller-owned usage scopes.
type UsageSummary struct {
	// TapeCount is the number of live tapes in the requested scopes.
	TapeCount int64 `json:"tape_count"`

	// LogicalBytes is the aggregate decoded payload size, when known.
	LogicalBytes *int64 `json:"logical_bytes"`

	// StoredBytes is the aggregate encoded payload size.
	StoredBytes int64 `json:"stored_bytes"`

	// RetentionDroppedLogicalBytes is the aggregate logical byte count hidden by retention, when known.
	RetentionDroppedLogicalBytes *int64 `json:"retention_dropped_logical_bytes"`

	// RetentionTrackingCompleteSince is the earliest complete logical-retention boundary for the aggregate.
	RetentionTrackingCompleteSince *time.Time `json:"retention_tracking_complete_since"`
}

// TapeUsage returns current logical and encoded payload usage for one tape.
func (c *Client) TapeUsage(ctx context.Context, spaceID, tapeID string) (TapeUsage, error) {
	if !IsValidID(spaceID) {
		return TapeUsage{}, fmt.Errorf("invalid space_id: %q", spaceID)
	}
	if !IsValidID(tapeID) {
		return TapeUsage{}, fmt.Errorf("invalid tape_id: %q", tapeID)
	}

	var usage TapeUsage
	err := c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(
			attemptCtx,
			http.MethodGet,
			c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "tapes", url.PathEscape(tapeID), "usage"),
			nil,
		)
		if err != nil {
			return err
		}
		c.applySpaceSecret(req)

		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return readResponseError(resp)
		}
		usage = TapeUsage{}
		return json.NewDecoder(resp.Body).Decode(&usage)
	})
	return usage, err
}

// UsageSummary returns one metadata-only aggregate for up to 1024 usage scopes.
func (c *Client) UsageSummary(ctx context.Context, spaceID string, usageScopes []string) (UsageSummary, error) {
	if !IsValidID(spaceID) {
		return UsageSummary{}, fmt.Errorf("invalid space_id: %q", spaceID)
	}
	if len(usageScopes) == 0 || len(usageScopes) > maxUsageScopes {
		return UsageSummary{}, fmt.Errorf("usage_scopes must contain between 1 and 1024 values")
	}
	for _, scope := range usageScopes {
		if !IsValidID(scope) {
			return UsageSummary{}, fmt.Errorf("invalid usage_scope: %q", scope)
		}
	}
	body, err := json.Marshal(struct {
		UsageScopes []string `json:"usage_scopes"`
	}{UsageScopes: usageScopes})
	if err != nil {
		return UsageSummary{}, err
	}

	var usage UsageSummary
	err = c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(
			attemptCtx,
			http.MethodPost,
			c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "usage"),
			bytes.NewReader(body),
		)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		c.applySpaceSecret(req)

		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return readResponseError(resp)
		}
		usage = UsageSummary{}
		return json.NewDecoder(resp.Body).Decode(&usage)
	})
	return usage, err
}
