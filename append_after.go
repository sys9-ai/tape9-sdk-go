package tape9

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// MaxAppendAfterBytes is the maximum raw payload of one conditional batch.
// Compression never expands this limit. Oversized batches are not split.
const MaxAppendAfterBytes = 32 << 20

const maxAppendAfterPayloadBytes = MaxAppendAfterBytes + (64 << 10)

// AppendAfterOptions defines one immutable batch and its required predecessor.
type AppendAfterOptions struct {
	// After is the previous successful AppendID. Empty starts the conditional
	// chain, even when the Tape already contains ordinary appended content.
	After string
	// AppendID is a unique, never-reused batch identity. Empty generates an ID
	// once per call, returned even on error. Persist it with After and payload
	// before sending when recovery must survive caller process replacement.
	AppendID string
	// RetainMode and Compression select the new Tape's storage policy. An
	// existing Tape retains its compression, just as with ordinary Append.
	RetainMode RetainMode
	// Compression selects the format when creating a Tape.
	Compression Compression
	// UsageScope associates the Tape with a caller-owned accounting scope.
	UsageScope string
}

// AppendAfterResult identifies the attempted batch, including when an error
// leaves its commit uncertain. Only a nil error acknowledges the append.
type AppendAfterResult struct {
	// TapeID is the requested Tape identity.
	TapeID string `json:"tape_id"`
	// AppendID is the stable identity to reuse if this call needs recovery.
	AppendID string `json:"append_id"`
	// LogicalBytes is the attempted raw payload size, not proof of acceptance.
	LogicalBytes int64 `json:"logical_bytes"`
}

// AppendConflictError reports the current tail when a conditional batch conflicts.
// It never authorizes resending the same content with a different predecessor.
type AppendConflictError struct {
	// TailID was the current batch at the server conflict boundary.
	TailID string
	cause  *HTTPError
}

// Error implements error.
func (e *AppendConflictError) Error() string {
	return fmt.Sprintf("conditional append conflict: current tail is %q", e.TailID)
}

// Unwrap preserves the underlying HTTP response for transport diagnostics.
func (e *AppendConflictError) Unwrap() error { return e.cause }

// AppendState identifies the last successful conditional append, independently of ordinary writes.
type AppendState struct {
	// TailID is empty until the first successful AppendAfter, even on a nonempty Tape.
	TailID string `json:"tail_id"`
}

// AppendState reads a Tape's tail without creating it or reading its content.
// Retention does not change this state. Deleted Tapes return an error.
func (c *Client) AppendState(ctx context.Context, spaceID, tapeID string) (AppendState, error) {
	if !IsValidID(spaceID) || !IsValidID(tapeID) {
		return AppendState{}, fmt.Errorf("invalid space_id or tape_id")
	}
	var state AppendState
	err := c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet,
			c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "tapes", url.PathEscape(tapeID), "append-state"), nil)
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
		var body struct {
			TailID *string `json:"tail_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return err
		}
		if body.TailID == nil || (*body.TailID != "" && !IsValidID(*body.TailID)) {
			return fmt.Errorf("invalid append state response")
		}
		state = AppendState{TailID: *body.TailID}
		return nil
	})
	return state, err
}

// AppendAfter atomically appends one finite batch after opts.After. A missing
// Tape is created when needed; existing content is preserved. Ordinary Append
// and Capture remain allowed and never check or change the conditional tail.
//
// Keep one unacknowledged batch per logical writer. An exact retry of the latest
// batch succeeds; older retries conflict. Empty content still advances the tail.
// All retries use the same ID, predecessor and encoded content. The SDK never
// follows a conflicting tail or splits a batch. Once any AppendAfter succeeds,
// the Tape ID cannot be reused after deletion; replacements need new Tape IDs.
// Until then, ordinary delete/recreate semantics still apply.
func (c *Client) AppendAfter(ctx context.Context, spaceID, tapeID string, payload []byte, opts AppendAfterOptions) (AppendAfterResult, error) {
	result := AppendAfterResult{TapeID: tapeID, AppendID: opts.AppendID, LogicalBytes: int64(len(payload))}
	if !IsValidID(spaceID) || !IsValidID(tapeID) {
		return result, fmt.Errorf("invalid space_id or tape_id")
	}
	if (opts.After != "" && !IsValidID(opts.After)) || (opts.AppendID != "" && !IsValidID(opts.AppendID)) || (opts.AppendID != "" && opts.AppendID == opts.After) {
		return result, fmt.Errorf("invalid append_id or predecessor")
	}
	if len(payload) > MaxAppendAfterBytes {
		return result, fmt.Errorf("conditional payload exceeds %d raw bytes", MaxAppendAfterBytes)
	}
	format, err := opts.Compression.payloadFormat()
	if err != nil {
		return result, err
	}
	if result.AppendID == "" {
		result.AppendID, err = NewIdempotencyKey()
		if err != nil {
			return result, err
		}
		// Stream retry keys allow a leading '-', but Tape batch IDs do not.
		result.AppendID = "batch_" + result.AppendID
	}
	_, format, err = c.createOrReuseAppendTape(ctx, spaceID, tapeID, createTapeOptions{
		RetainMode: opts.RetainMode, PayloadFormat: format, UsageScope: opts.UsageScope,
	})
	if err != nil {
		return result, err
	}
	var encoded []byte
	if format == payloadFormatFramedZstdV1 && len(payload) > 0 {
		encoder, err := newFramedZstdEncoder()
		if err != nil {
			return result, err
		}
		defer encoder.Close()
		// Preserve the existing read format's bounded frames, but send all of
		// them in one HTTP request and one commit, never separate appends.
		for start := 0; start < len(payload); start += defaultFramedZstdRawWindowBytes {
			end := min(start+defaultFramedZstdRawWindowBytes, len(payload))
			encoded = append(encoded, encodeFramedZstdChunk(payload[start:end], encoder)...)
		}
	} else {
		encoded = bytes.Clone(payload)
	}
	if len(encoded) > maxAppendAfterPayloadBytes {
		return result, fmt.Errorf("conditional payload exceeds %d stored bytes", maxAppendAfterPayloadBytes)
	}
	err = c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodPut,
			c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "tapes", url.PathEscape(tapeID), "appends", url.PathEscape(result.AppendID)), bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Tape9-After", opts.After)
		c.applySpaceSecret(req)
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return readResponseError(resp)
		}
		var body struct {
			AppendID string `json:"append_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return err
		}
		if body.AppendID != result.AppendID {
			return fmt.Errorf("append response does not identify this batch")
		}
		return nil
	})
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusConflict {
		var body struct {
			Code   string  `json:"code"`
			TailID *string `json:"tail_id"`
		}
		if json.Unmarshal([]byte(httpErr.Body), &body) == nil && body.Code == "append_conflict" && body.TailID != nil {
			err = &AppendConflictError{TailID: *body.TailID, cause: httpErr}
		}
	}
	return result, err
}
