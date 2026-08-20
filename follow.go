package tape9

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

var (
	waitContentCallMaxWait      = 30 * time.Second
	waitContentCallTimeoutGrace = 1 * time.Second
)

// FollowFrom selects where a new follow starts streaming from.
type FollowFrom string

const (
	// FollowFromStart writes the current visible tape content from its
	// beginning, then streams later visible bytes.
	FollowFromStart FollowFrom = "start"

	// FollowFromLatest skips the bytes already visible in the initial follow
	// response and streams only later visible bytes.
	FollowFromLatest FollowFrom = "latest"
)

// IsValid reports whether v is a supported follow start mode.
func (v FollowFrom) IsValid() bool {
	return v == FollowFromStart || v == FollowFromLatest
}

func (v FollowFrom) contentFrom() contentFrom {
	if v == FollowFromLatest {
		return contentFromLatest
	}
	return contentFromStart
}

// FollowOptions configures Client.Follow.
type FollowOptions struct {
	// From selects where follow starts.
	//
	// When empty, FollowFromStart is used.
	From FollowFrom

	// AllowMissing starts follow from an empty frontier when the space or tape
	// does not exist yet.
	AllowMissing bool

	// OnInitial reports the initial visible snapshot after Follow writes it.
	//
	// This callback runs only when From is FollowFromStart. FollowFromLatest
	// does not write the current visible content, so it does not report an
	// initial snapshot.
	OnInitial func(ReadResult)
}

// Follow streams tape content to w according to opts.From, then keeps waiting
// for later visible bytes and writes only the newly visible bytes until ctx is
// cancelled. When AllowMissing is true, a missing space or tape starts from an
// empty frontier and follow waits for later visible bytes instead of failing.
func (c *Client) Follow(ctx context.Context, spaceID string, tapeID string, w io.Writer, opts FollowOptions) error {
	if !IsValidID(spaceID) {
		return fmt.Errorf("invalid space_id: %q", spaceID)
	}
	if !IsValidID(tapeID) {
		return fmt.Errorf("invalid tape_id: %q", tapeID)
	}
	selectedFrom := opts.From
	if selectedFrom == "" {
		selectedFrom = FollowFromStart
	}
	if !selectedFrom.IsValid() {
		return fmt.Errorf("invalid follow from: %q", selectedFrom)
	}

	content, contentURL, contentDuration, err := c.content(ctx, spaceID, tapeID, contentOptions{
		from:         selectedFrom.contentFrom(),
		allowMissing: opts.AllowMissing,
	})
	if err != nil {
		return err
	}
	if selectedFrom == FollowFromLatest && len(content.Parts) != 0 {
		return fmt.Errorf("latest follow unexpectedly returned initial content")
	}

	resumeToken, err := c.writeContentParts(ctx, contentURL, content, selectedFrom == FollowFromStart, w, contentDuration, opts.OnInitial)
	if err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		content, contentURL, _, noChange, err := c.waitForContentChange(ctx, spaceID, tapeID, resumeToken, 0)
		if err != nil {
			return err
		}
		if noChange {
			continue
		}
		resumeToken, err = c.writeContentParts(ctx, contentURL, content, false, w, 0, nil)
		if err != nil {
			return err
		}
	}
}

func (c *Client) waitForContentChange(ctx context.Context, spaceID, tapeID, resumeToken string, wait time.Duration) (contentResponse, *url.URL, time.Duration, bool, error) {
	if wait < 0 {
		return contentResponse{}, nil, 0, false, fmt.Errorf("wait must be >= 0")
	}

	finiteWait := wait > 0
	remainingWait := wait
	var totalDuration time.Duration

	for {
		if err := ctx.Err(); err != nil {
			return contentResponse{}, nil, totalDuration, false, err
		}
		if finiteWait && remainingWait <= 0 {
			return contentResponse{}, nil, totalDuration, true, nil
		}

		reqWait := waitContentCallMaxWait
		if finiteWait && remainingWait < reqWait {
			reqWait = remainingWait
		}

		var (
			body       contentResponse
			contentURL *url.URL
			duration   time.Duration
			noChange   bool
		)
		if err := c.doWithRetry(ctx, func(attemptCtx context.Context) error {
			var err error
			body, contentURL, duration, noChange, err = c.waitForContentChangeOnce(attemptCtx, spaceID, tapeID, resumeToken, reqWait)
			return err
		}); err != nil {
			totalDuration += duration
			if finiteWait && remainingWait > reqWait && isTapeNotFoundHTTPError(err) {
				remainingWait -= reqWait
				continue
			}
			return contentResponse{}, nil, totalDuration, false, err
		}
		totalDuration += duration

		if !noChange {
			return body, contentURL, totalDuration, false, nil
		}
		if finiteWait {
			remainingWait -= reqWait
		}
	}
}

func (c *Client) waitForContentChangeOnce(ctx context.Context, spaceID, tapeID, resumeToken string, wait time.Duration) (contentResponse, *url.URL, time.Duration, bool, error) {
	timeout := wait + waitContentCallTimeoutGrace
	if wait == 0 {
		timeout = c.requestTimeout
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	startedAt := time.Now()
	req, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodGet,
		c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "tapes", url.PathEscape(tapeID), "content"),
		nil,
	)
	if err != nil {
		return contentResponse{}, nil, 0, false, err
	}
	c.applySpaceSecret(req)

	values := req.URL.Query()
	values.Set("resume_token", resumeToken)
	values.Set("wait", wait.String())
	req.URL.RawQuery = values.Encode()

	resp, err := c.http.Do(req)
	if err != nil {
		return contentResponse{}, nil, 0, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return contentResponse{}, resp.Request.URL, time.Since(startedAt), true, nil
	}
	if resp.StatusCode != http.StatusOK {
		return contentResponse{}, nil, time.Since(startedAt), false, readResponseError(resp)
	}

	var body contentResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return contentResponse{}, nil, 0, false, err
	}
	return body, resp.Request.URL, time.Since(startedAt), false, nil
}

func isTapeNotFoundHTTPError(err error) bool {
	message, ok := httpAPIErrorMessage(err, http.StatusNotFound)
	return ok && message == "tape not found"
}

func (c *Client) writeContentParts(ctx context.Context, contentURL *url.URL, content contentResponse, reportInitial bool, w io.Writer, contentDuration time.Duration, onInitial func(ReadResult)) (string, error) {
	pullResult, err := c.pullContent(ctx, contentURL, content, w, contentDuration)
	if err != nil {
		return "", err
	}

	if reportInitial && onInitial != nil {
		onInitial(ReadResult{
			RetainApplied: pullResult.RetainApplied,
			DroppedBytes:  pullResult.DroppedBytes,
			Metrics:       pullResult.Metrics,
		})
	}
	return pullResult.ResumeToken.raw, nil
}
