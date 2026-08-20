package tape9

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

// ResumeToken is an opaque read cursor returned by the tape9 content protocol.
//
// Callers may persist it and feed it back into Client.Pull, but must not
// inspect or construct it from tape internals.
type ResumeToken struct {
	raw string
}

// ParseResumeToken wraps one previously persisted opaque resume token.
func ParseResumeToken(raw string) (ResumeToken, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ResumeToken{}, fmt.Errorf("resume token is empty")
	}
	return ResumeToken{raw: raw}, nil
}

// IsZero reports whether t is the zero token, which means "start a new pull"
// instead of "resume from an existing cursor".
func (t ResumeToken) IsZero() bool {
	return t.raw == ""
}

// String returns the opaque token text for persistence or transport.
func (t ResumeToken) String() string {
	return t.raw
}

// MarshalText encodes the opaque token text.
func (t ResumeToken) MarshalText() ([]byte, error) {
	return []byte(t.raw), nil
}

// UnmarshalText decodes one previously persisted opaque token text.
func (t *ResumeToken) UnmarshalText(text []byte) error {
	if t == nil {
		return fmt.Errorf("nil ResumeToken")
	}
	t.raw = strings.TrimSpace(string(text))
	return nil
}

// MarshalJSON encodes the opaque token as a JSON string.
func (t ResumeToken) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.raw)
}

// UnmarshalJSON decodes the opaque token from a JSON string or null.
func (t *ResumeToken) UnmarshalJSON(body []byte) error {
	if t == nil {
		return fmt.Errorf("nil ResumeToken")
	}
	if string(body) == "null" {
		*t = ResumeToken{}
		return nil
	}
	var raw string
	if err := json.Unmarshal(body, &raw); err != nil {
		return err
	}
	t.raw = strings.TrimSpace(raw)
	return nil
}

// PullFrom selects where a new caller-managed pull starts.
type PullFrom string

const (
	// PullFromStart writes the current visible tape content first.
	PullFromStart PullFrom = "start"

	// PullFromLatest skips the bytes already visible in the initial snapshot.
	PullFromLatest PullFrom = "latest"
)

// IsValid reports whether v is a supported pull start mode.
func (v PullFrom) IsValid() bool {
	return v == PullFromStart || v == PullFromLatest
}

func (v PullFrom) contentFrom() contentFrom {
	if v == PullFromLatest {
		return contentFromLatest
	}
	return contentFromStart
}

// PullOptions configures one caller-managed pull request.
type PullOptions struct {
	// From selects the starting point for a new pull.
	//
	// It is used only when ResumeToken is zero. When empty,
	// PullFromStart is used.
	From PullFrom

	// ResumeToken resumes from one previous successful Pull result.
	//
	// When set, From must stay empty and Wait controls whether the SDK should
	// long-poll for later visible bytes or return immediately.
	ResumeToken ResumeToken

	// AllowMissing treats a missing space or tape as an empty starting frontier
	// for a new pull. It is only valid when ResumeToken is zero.
	AllowMissing bool

	// PrefixLogicalBytes limits a new PullFromStart request to the smallest
	// prefix of complete Segments whose decoded payload reaches this target. The
	// returned payload may exceed the target by the final Segment. Zero returns
	// the complete visible snapshot.
	//
	// It is only valid when ResumeToken is zero and From is empty or
	// PullFromStart.
	PrefixLogicalBytes int64

	// Wait is the optional long-poll budget for resumed pulls.
	//
	// It is only valid when ResumeToken is set. Zero means "check once
	// immediately". Positive values are split into repeated 30s protocol waits
	// internally until either later visible bytes arrive or the budget expires.
	Wait time.Duration
}

// PullResult describes one caller-managed pull result.
type PullResult struct {
	// ResumeToken is the opaque cursor that represents the visible content after
	// this pull result. Persist it and feed it into the next Pull call.
	ResumeToken ResumeToken

	// NoChange reports that a resumed pull observed no later visible bytes.
	//
	// In this case no payload bytes were written and ResumeToken stays equal to
	// the input cursor.
	NoChange bool

	// RetainApplied reports whether the tape retain policy hid bytes from the
	// returned snapshot or delta.
	RetainApplied bool

	// DroppedBytes is the number of stored payload bytes hidden by retention.
	DroppedBytes int64

	// Metrics reports content and part download timings for this pull.
	Metrics ReadMetrics
}

// Pull performs one caller-managed content pull.
//
// With a zero ResumeToken it returns the current visible content immediately
// according to From (`start` by default, or `latest` to establish a "from now"
// cursor without writing current bytes). When AllowMissing is true, a missing
// space or tape yields an empty frontier token instead of an error.
//
// With a non-zero ResumeToken it returns only bytes that became visible after
// that cursor. Wait=0 checks once immediately; Wait>0 long-polls up to that
// budget and returns NoChange=true on timeout with no later visible bytes.
func (c *Client) Pull(ctx context.Context, spaceID string, tapeID string, w io.Writer, opts PullOptions) (PullResult, error) {
	if !IsValidID(spaceID) {
		return PullResult{}, fmt.Errorf("invalid space_id: %q", spaceID)
	}
	if !IsValidID(tapeID) {
		return PullResult{}, fmt.Errorf("invalid tape_id: %q", tapeID)
	}
	if opts.Wait < 0 {
		return PullResult{}, fmt.Errorf("wait must be >= 0")
	}
	if opts.PrefixLogicalBytes < 0 {
		return PullResult{}, fmt.Errorf("prefix logical bytes must be >= 0")
	}

	if opts.ResumeToken.IsZero() {
		if opts.Wait != 0 {
			return PullResult{}, fmt.Errorf("wait requires resume token")
		}
		selectedFrom := opts.From
		if selectedFrom == "" {
			selectedFrom = PullFromStart
		}
		if !selectedFrom.IsValid() {
			return PullResult{}, fmt.Errorf("invalid pull from: %q", selectedFrom)
		}
		if selectedFrom == PullFromLatest && opts.PrefixLogicalBytes > 0 {
			return PullResult{}, fmt.Errorf("prefix logical bytes conflict with latest pull")
		}

		content, contentURL, contentDuration, err := c.content(ctx, spaceID, tapeID, contentOptions{
			from:               selectedFrom.contentFrom(),
			allowMissing:       opts.AllowMissing,
			prefixLogicalBytes: opts.PrefixLogicalBytes,
		})
		if err != nil {
			return PullResult{}, err
		}
		if selectedFrom == PullFromLatest && len(content.Parts) != 0 {
			return PullResult{}, fmt.Errorf("latest pull unexpectedly returned initial content")
		}
		result, err := c.pullContent(ctx, contentURL, content, w, contentDuration)
		if err != nil {
			return PullResult{}, err
		}
		if selectedFrom == PullFromLatest {
			result.RetainApplied = false
			result.DroppedBytes = 0
		}
		return result, nil
	}

	if opts.From != "" {
		return PullResult{}, fmt.Errorf("resume token conflicts with from")
	}
	if opts.AllowMissing {
		return PullResult{}, fmt.Errorf("resume token conflicts with allow_missing")
	}
	if opts.PrefixLogicalBytes > 0 {
		return PullResult{}, fmt.Errorf("resume token conflicts with prefix logical bytes")
	}

	if opts.Wait == 0 {
		var (
			content         contentResponse
			contentURL      *url.URL
			contentDuration time.Duration
			noChange        bool
		)
		if err := c.doWithRetry(ctx, func(attemptCtx context.Context) error {
			var err error
			content, contentURL, contentDuration, noChange, err = c.waitForContentChangeOnce(attemptCtx, spaceID, tapeID, opts.ResumeToken.raw, 0)
			return err
		}); err != nil {
			return PullResult{}, err
		}
		if noChange {
			return noChangePullResult(opts.ResumeToken, contentDuration), nil
		}
		return c.pullContent(ctx, contentURL, content, w, contentDuration)
	}

	content, contentURL, contentDuration, noChange, err := c.waitForContentChange(ctx, spaceID, tapeID, opts.ResumeToken.raw, opts.Wait)
	if err != nil {
		return PullResult{}, err
	}
	if noChange {
		return noChangePullResult(opts.ResumeToken, contentDuration), nil
	}
	return c.pullContent(ctx, contentURL, content, w, contentDuration)
}

func noChangePullResult(token ResumeToken, contentDuration time.Duration) PullResult {
	metrics := newReadMetrics(contentDuration, nil)
	metrics.Total = contentDuration
	return PullResult{
		ResumeToken: token,
		NoChange:    true,
		Metrics:     metrics,
	}
}

func (c *Client) pullContent(ctx context.Context, contentURL *url.URL, content contentResponse, w io.Writer, contentDuration time.Duration) (PullResult, error) {
	payloadFormat, targets, err := prepareContentDownload(contentURL, content)
	if err != nil {
		return PullResult{}, err
	}

	metrics := newReadMetrics(contentDuration, content.Parts)
	if len(content.Parts) != 0 {
		pullStartedAt := time.Now()
		if err := c.downloadSegments(ctx, targets, content.Parts, payloadFormat, w, &metrics); err != nil {
			return PullResult{}, err
		}
		metrics.Total = contentDuration + time.Since(pullStartedAt)
	} else {
		metrics.Total = contentDuration
	}

	return PullResult{
		ResumeToken:   ResumeToken{raw: content.ResumeToken},
		RetainApplied: content.RetainApplied,
		DroppedBytes:  content.DroppedBytes,
		Metrics:       metrics,
	}, nil
}
