package tape9

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"
)

const defaultReadConcurrency = 32

// ReadMetrics describes the storage-side phases for one successful read or pull.
type ReadMetrics struct {
	Content             time.Duration
	SegmentFetch        time.Duration
	Total               time.Duration
	SegmentCount        int
	SegmentBytes        int64
	SegmentSourceCounts map[string]int
}

// ReadResult describes one successful read snapshot.
type ReadResult struct {
	// RetainApplied reports whether the tape retain policy has hidden bytes from
	// this snapshot.
	RetainApplied bool

	// DroppedBytes is the number of stored payload bytes hidden by retention.
	DroppedBytes int64

	// Metrics reports content and part download timings for this read.
	Metrics ReadMetrics
}

type contentPart struct {
	ByteCount int64  `json:"byte_count"`
	Source    string `json:"source"`
	URL       string `json:"url"`
}

type contentResponse struct {
	ResumeToken   string        `json:"resume_token"`
	PayloadFormat string        `json:"payload_format"`
	RetainApplied bool          `json:"retain_applied"`
	DroppedBytes  int64         `json:"dropped_bytes"`
	Parts         []contentPart `json:"parts"`
}

type contentFrom string

const (
	contentFromStart  contentFrom = "start"
	contentFromLatest contentFrom = "latest"
)

type contentOptions struct {
	from               contentFrom
	allowMissing       bool
	prefixLogicalBytes int64
}

type segmentTarget struct {
	url         *url.URL
	needsSecret bool
}

// Read reads the current tape content snapshot and writes payload bytes to w.
//
// It uses the content read protocol to fetch the current visible bytes through
// server-issued content-part URLs.
func (c *Client) Read(ctx context.Context, spaceID string, tapeID string, w io.Writer) (ReadResult, error) {
	if !IsValidID(spaceID) {
		return ReadResult{}, fmt.Errorf("invalid space_id: %q", spaceID)
	}
	if !IsValidID(tapeID) {
		return ReadResult{}, fmt.Errorf("invalid tape_id: %q", tapeID)
	}

	content, contentURL, contentDuration, err := c.content(ctx, spaceID, tapeID, contentOptions{from: contentFromStart})
	if err != nil {
		return ReadResult{}, err
	}
	pullResult, err := c.pullContent(ctx, contentURL, content, w, contentDuration)
	if err != nil {
		return ReadResult{}, err
	}

	return ReadResult{
		RetainApplied: pullResult.RetainApplied,
		DroppedBytes:  pullResult.DroppedBytes,
		Metrics:       pullResult.Metrics,
	}, nil
}

func (c *Client) downloadSegments(ctx context.Context, targets []segmentTarget, parts []contentPart, payloadFormat payloadFormat, w io.Writer, metrics *ReadMetrics) error {
	if len(parts) == 0 {
		return nil
	}
	if len(targets) != len(parts) {
		return fmt.Errorf("content target count mismatch: %d != %d", len(targets), len(parts))
	}
	var decoder interface {
		DecodeAll(input, dst []byte) ([]byte, error)
		Close()
	}
	if payloadFormat == payloadFormatFramedZstdV1 {
		zstdDecoder, err := newFramedZstdDecoder()
		if err != nil {
			return err
		}
		defer zstdDecoder.Close()
		decoder = zstdDecoder
	}

	downloadBaseCtx, stopDownloadBase := storageDownloadBaseContext(ctx)
	defer stopDownloadBase()

	deadline, hasDeadline := ctx.Deadline()
	var fetchDuration time.Duration
	var writeDuration time.Duration
	for start := 0; start < len(parts); start += defaultReadConcurrency {
		end := start + defaultReadConcurrency
		if end > len(parts) {
			end = len(parts)
		}
		batchCtx, cancelBatch := storageDownloadContext(downloadBaseCtx, deadline, hasDeadline, writeDuration)
		payloads, batchFetchDuration, err := c.downloadSegmentBatch(batchCtx, targets[start:end], parts[start:end])
		cancelBatch()
		if err != nil {
			return err
		}
		fetchDuration += batchFetchDuration

		writeStartedAt := time.Now()
		for _, payload := range payloads {
			if err := writeSegmentPayload(w, payload, payloadFormat, decoder); err != nil {
				return err
			}
		}
		writeDuration += time.Since(writeStartedAt)
	}
	if metrics != nil {
		metrics.SegmentFetch = fetchDuration
	}
	return nil
}

func writeSegmentPayload(w io.Writer, payload []byte, payloadFormat payloadFormat, decoder interface {
	DecodeAll(input, dst []byte) ([]byte, error)
	Close()
}) error {
	switch payloadFormat {
	case payloadFormatIdentity:
		return writePayload(w, payload)
	case payloadFormatFramedZstdV1:
		if decoder == nil {
			return fmt.Errorf("missing decoder for payload format %q", payloadFormat)
		}
		return decodeFramedZstdSegment(payload, decoder, w)
	default:
		return fmt.Errorf("invalid payload format: %q", payloadFormat)
	}
}

func storageDownloadBaseContext(ctx context.Context) (context.Context, func()) {
	baseCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if ctx.Err() == context.Canceled {
		cancel()
	}
	stopParent := context.AfterFunc(ctx, func() {
		if ctx.Err() == context.Canceled {
			cancel()
		}
	})
	return baseCtx, func() {
		stopParent()
		cancel()
	}
}

func storageDownloadContext(baseCtx context.Context, deadline time.Time, hasDeadline bool, writeDuration time.Duration) (context.Context, context.CancelFunc) {
	if !hasDeadline {
		return baseCtx, func() {}
	}
	return context.WithDeadline(baseCtx, deadline.Add(writeDuration))
}

func (c *Client) downloadSegmentBatch(ctx context.Context, targets []segmentTarget, parts []contentPart) ([][]byte, time.Duration, error) {
	payloads := make([][]byte, len(parts))
	group, groupCtx := errgroup.WithContext(ctx)
	fetchStartedAt := time.Now()

	for i, part := range parts {
		i := i
		target := targets[i]
		byteCount := part.ByteCount
		group.Go(func() error {
			payload, err := c.downloadSegment(groupCtx, target.url, byteCount, target.needsSecret)
			if err != nil {
				return err
			}
			payloads[i] = payload
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, 0, err
	}
	fetchDuration := time.Since(fetchStartedAt)
	return payloads, fetchDuration, nil
}

func resolveSegmentTarget(contentURL *url.URL, rawURL string) (segmentTarget, error) {
	segURL, err := url.Parse(rawURL)
	if err != nil {
		return segmentTarget{}, err
	}
	needsSecret := !segURL.IsAbs()
	if needsSecret {
		segURL = contentURL.ResolveReference(segURL)
	}
	return segmentTarget{url: segURL, needsSecret: needsSecret}, nil
}

func prepareContentDownload(contentURL *url.URL, content contentResponse) (payloadFormat, []segmentTarget, error) {
	if content.ResumeToken == "" {
		return "", nil, fmt.Errorf("content missing resume_token")
	}
	payloadFormat, err := storedPayloadFormat(content.PayloadFormat)
	if err != nil {
		return "", nil, err
	}
	targets := make([]segmentTarget, len(content.Parts))
	for i, part := range content.Parts {
		target, err := resolveContentPartTarget(contentURL, part)
		if err != nil {
			return "", nil, err
		}
		targets[i] = target
	}
	return payloadFormat, targets, nil
}

func resolveContentPartTarget(contentURL *url.URL, part contentPart) (segmentTarget, error) {
	if part.URL == "" {
		return segmentTarget{}, fmt.Errorf("content part missing url")
	}
	if part.ByteCount <= 0 {
		return segmentTarget{}, fmt.Errorf("content part invalid byte_count: %d", part.ByteCount)
	}
	target, err := resolveSegmentTarget(contentURL, part.URL)
	if err != nil {
		return segmentTarget{}, fmt.Errorf("parse content part url: %w", err)
	}
	return target, nil
}

func (c *Client) downloadSegment(ctx context.Context, segURL *url.URL, byteCount int64, needsSecret bool) ([]byte, error) {
	var payload []byte
	err := c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, segURL.String(), nil)
		if err != nil {
			return err
		}
		if needsSecret {
			c.applySpaceSecret(req)
		}

		attemptResp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		defer attemptResp.Body.Close()
		if attemptResp.StatusCode != http.StatusOK {
			return readResponseError(attemptResp)
		}
		if attemptResp.ContentLength >= 0 && attemptResp.ContentLength < byteCount {
			return fmt.Errorf("content part shorter than expected: %d < %d", attemptResp.ContentLength, byteCount)
		}

		payload, err = readSegmentPayload(attemptResp.Body, byteCount)
		return err
	})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func readSegmentPayload(r io.Reader, byteCount int64) ([]byte, error) {
	if byteCount > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("content part too large to buffer: %d", byteCount)
	}
	payload := make([]byte, int(byteCount))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writePayload(w io.Writer, payload []byte) error {
	written, err := w.Write(payload)
	if err != nil {
		return err
	}
	if written != len(payload) {
		return io.ErrShortWrite
	}
	return nil
}

func totalSegmentBytes(parts []contentPart) int64 {
	var total int64
	for _, part := range parts {
		total += part.ByteCount
	}
	return total
}

func segmentSourceCounts(parts []contentPart) map[string]int {
	counts := map[string]int{}
	for _, part := range parts {
		if part.Source == "" {
			continue
		}
		counts[part.Source]++
	}
	if len(counts) == 0 {
		return nil
	}
	return counts
}

func newReadMetrics(contentDuration time.Duration, parts []contentPart) ReadMetrics {
	return ReadMetrics{
		Content:             contentDuration,
		SegmentCount:        len(parts),
		SegmentBytes:        totalSegmentBytes(parts),
		SegmentSourceCounts: segmentSourceCounts(parts),
	}
}

func (c *Client) content(ctx context.Context, spaceID string, tapeID string, opts contentOptions) (contentResponse, *url.URL, time.Duration, error) {
	var body contentResponse
	var contentURL *url.URL
	startedAt := time.Now()
	if err := c.doWithRetry(ctx, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, c.requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(
			attemptCtx,
			http.MethodGet,
			c.endpointURL("v1", "spaces", url.PathEscape(spaceID), "tapes", url.PathEscape(tapeID), "content"),
			nil,
		)
		if err != nil {
			return err
		}
		values := req.URL.Query()
		if opts.from == contentFromLatest {
			values.Set("from", string(contentFromLatest))
		}
		if opts.allowMissing {
			values.Set("allow_missing", "1")
		}
		if opts.prefixLogicalBytes > 0 {
			values.Set("prefix_logical_bytes", strconv.FormatInt(opts.prefixLogicalBytes, 10))
		}
		req.URL.RawQuery = values.Encode()
		c.applySpaceSecret(req)

		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return readResponseError(resp)
		}

		body = contentResponse{}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return err
		}
		contentURL = resp.Request.URL
		return nil
	}); err != nil {
		return contentResponse{}, nil, 0, err
	}
	return body, contentURL, time.Since(startedAt), nil
}
