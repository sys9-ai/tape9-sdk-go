package tape9

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is the public tape9 SDK client.
//
// It intentionally exposes CLI-like semantics instead of the raw HTTP API.
type Client struct {
	baseURL                       *url.URL
	http                          *http.Client
	secret                        string
	requestTimeout                time.Duration
	retryBase                     time.Duration
	retryMax                      time.Duration
	maxRetryTime                  time.Duration
	framedZstdUploadFlushInterval time.Duration
}

const (
	spaceSecretHeader = "X-Space-Secret"
)

// New creates a new SDK client bound to the given server endpoint.
func New(endpoint string, opts ...Option) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid endpoint: %q", endpoint)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid endpoint: must not contain query or fragment: %q", endpoint)
	}
	u.Path = strings.TrimRight(u.Path, "/")

	cfg := clientOptions{maxRetryTime: defaultMaxRetryTime}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.maxRetryTime <= 0 {
		return nil, fmt.Errorf("invalid max retry time: %s", cfg.maxRetryTime)
	}

	return &Client{
		baseURL:                       u,
		http:                          cloneHTTPClient(cfg.httpClient),
		secret:                        cfg.secret,
		requestTimeout:                defaultRequestTimeout,
		retryBase:                     defaultRetryBase,
		retryMax:                      defaultRetryMax,
		maxRetryTime:                  cfg.maxRetryTime,
		framedZstdUploadFlushInterval: defaultFramedZstdUploadFlushInterval,
	}, nil
}

func cloneHTTPClient(src *http.Client) *http.Client {
	redirect := func(req *http.Request, via []*http.Request) error {
		// Redirect targets must not receive the space secret.
		req.Header.Del(spaceSecretHeader)
		return nil
	}

	if src == nil {
		return &http.Client{CheckRedirect: redirect}
	}

	cloned := *src
	if src.CheckRedirect == nil {
		cloned.CheckRedirect = redirect
	} else {
		next := src.CheckRedirect
		cloned.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			req.Header.Del(spaceSecretHeader)
			return next(req, via)
		}
	}
	return &cloned
}

func (c *Client) endpointURL(parts ...string) string {
	return c.baseURL.JoinPath(parts...).String()
}

func (c *Client) applySpaceSecret(req *http.Request) {
	if c.secret != "" {
		req.Header.Set(spaceSecretHeader, c.secret)
	}
}

type createTapeOptions struct {
	Conditional   bool
	RetainMode    RetainMode
	PayloadFormat payloadFormat
	UsageScope    string
}

type createStreamOptions struct {
	IdempotencyKey string
}

func (c *Client) createTape(ctx context.Context, spaceID string, tapeID string, opts createTapeOptions) (string, error) {
	type reqBody struct {
		Conditional   bool   `json:"conditional,omitempty"`
		TapeID        string `json:"tape_id,omitempty"`
		RetainMode    string `json:"retain_mode,omitempty"`
		PayloadFormat string `json:"payload_format,omitempty"`
		UsageScope    string `json:"usage_scope,omitempty"`
	}
	type respBody struct {
		TapeID string `json:"tape_id"`
	}

	if opts.RetainMode != "" && !opts.RetainMode.IsValid() {
		return "", fmt.Errorf("invalid retain mode: %q", opts.RetainMode)
	}
	if opts.PayloadFormat != "" && !opts.PayloadFormat.isValid() {
		return "", fmt.Errorf("invalid payload format: %q", opts.PayloadFormat)
	}
	if opts.UsageScope != "" && !IsValidID(opts.UsageScope) {
		return "", fmt.Errorf("invalid usage_scope: %q", opts.UsageScope)
	}
	selectedFormat := opts.PayloadFormat.orDefault()

	var body respBody
	var bodyBytes []byte
	if opts.Conditional || tapeID != "" || opts.RetainMode != "" || selectedFormat != payloadFormatIdentity || opts.UsageScope != "" {
		b, err := json.Marshal(reqBody{
			Conditional:   opts.Conditional,
			TapeID:        tapeID,
			RetainMode:    string(opts.RetainMode),
			PayloadFormat: string(selectedFormat),
			UsageScope:    opts.UsageScope,
		})
		if err != nil {
			return "", err
		}
		bodyBytes = b
	}

	do := func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		var bodyReader io.Reader
		if bodyBytes != nil {
			bodyReader = bytes.NewReader(bodyBytes)
		}

		req, err := http.NewRequestWithContext(
			attemptCtx,
			http.MethodPost,
			c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "tapes"),
			bodyReader,
		)
		if err != nil {
			return err
		}
		if bodyReader != nil {
			req.Header.Set("Content-Type", "application/json")
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

		body = respBody{}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return err
		}
		if body.TapeID == "" {
			return fmt.Errorf("missing tape_id in response")
		}
		return nil
	}

	if tapeID == "" {
		if err := do(ctx); err != nil {
			return "", err
		}
		return body.TapeID, nil
	}
	if err := c.doWithRetry(ctx, do); err != nil {
		return "", err
	}
	return body.TapeID, nil
}

func (c *Client) createStream(ctx context.Context, spaceID string, tapeID string, opts ...createStreamOptions) (string, error) {
	type respBody struct {
		StreamID string `json:"stream_id"`
	}

	streamOpts := createStreamOptions{}
	if len(opts) > 0 {
		streamOpts = opts[0]
	}

	var bodyBytes []byte
	if streamOpts.IdempotencyKey != "" {
		b, err := json.Marshal(struct {
			IdempotencyKey string `json:"idempotency_key,omitempty"`
		}{
			IdempotencyKey: streamOpts.IdempotencyKey,
		})
		if err != nil {
			return "", err
		}
		bodyBytes = b
	}

	send := func(attemptCtx context.Context) (string, error) {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		var bodyReader io.Reader
		if bodyBytes != nil {
			bodyReader = bytes.NewReader(bodyBytes)
		}

		req, err := http.NewRequestWithContext(
			attemptCtx,
			http.MethodPost,
			c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "tapes", url.PathEscape(tapeID), "streams"),
			bodyReader,
		)
		if err != nil {
			return "", err
		}
		if bodyReader != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		c.applySpaceSecret(req)

		resp, err := c.http.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return "", readResponseError(resp)
		}

		var body respBody
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return "", err
		}
		if body.StreamID == "" {
			return "", fmt.Errorf("missing stream_id in response")
		}
		return body.StreamID, nil
	}

	if streamOpts.IdempotencyKey == "" {
		return send(ctx)
	}

	var streamID string
	if err := c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		var err error
		streamID, err = send(attemptCtx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return "", err
	}
	return streamID, nil
}

func (c *Client) putChunk(ctx context.Context, spaceID, tapeID, streamID string, seq int64, sha256Hex string, payload []byte) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		c.endpointURL(
			"v1",
			"spaces",
			url.PathEscape(spaceID),
			"tapes",
			url.PathEscape(tapeID),
			"streams",
			url.PathEscape(streamID),
			"chunks",
			strconv.FormatInt(seq, 10),
		),
		bytes.NewReader(payload),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Tape9-SHA256", sha256Hex)
	c.applySpaceSecret(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return readResponseError(resp)
}

func (c *Client) closeStream(ctx context.Context, spaceID, tapeID, streamID string) error {
	return c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(
			attemptCtx,
			http.MethodPost,
			c.endpointURL(
				"v1",
				"spaces",
				url.PathEscape(spaceID),
				"tapes",
				url.PathEscape(tapeID),
				"streams",
				url.PathEscape(streamID),
				"close",
			),
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

		if resp.StatusCode != http.StatusNoContent {
			return readResponseError(resp)
		}
		return nil
	})
}

const (
	maxErrorBodyBytes      = 4 * 1024
	maxErrorBodyDrainBytes = 256 * 1024
)

func readErrorBody(resp *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyDrainBytes))
	return strings.TrimSpace(string(b))
}

func readHTTPError(resp *http.Response) error {
	return &HTTPError{StatusCode: resp.StatusCode, Body: readErrorBody(resp)}
}

func readRetryableError(resp *http.Response) error {
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
	return &RetryableError{
		StatusCode: resp.StatusCode,
		RetryAfter: retryAfter,
		Body:       readErrorBody(resp),
	}
}

func readResponseError(resp *http.Response) error {
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		return readRetryableError(resp)
	}
	return readHTTPError(resp)
}

func parseRetryAfter(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	sec, err := strconv.Atoi(raw)
	if err != nil || sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}
