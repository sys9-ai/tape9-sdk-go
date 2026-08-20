package tape9

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

const defaultUploadChunkSizeBytes = 4 * 1024 * 1024
const bestEffortUploadCleanupTimeout = 30 * time.Second
const defaultFramedZstdReadPieceBytes = 64 * 1024
const defaultFramedZstdUploadFlushInterval = 3 * time.Second

type chunkPutter interface {
	putChunk(ctx context.Context, spaceID, tapeID, streamID string, seq int64, sha256Hex string, payload []byte) error
}

type uploadChunkReader interface {
	ReadUploadChunk([]byte) (int, error)
}

// AppendOptions configures Client.Append.
type AppendOptions struct {
	// RetainMode applies the tape-level retention mode only if Append creates
	// the target tape. Existing tapes keep their current mode.
	RetainMode RetainMode

	// Compression controls whether the SDK transforms bytes before storing them.
	// When empty, payload bytes are stored as-is.
	Compression Compression

	// UsageScope is an opaque caller-owned identifier used to aggregate related
	// tapes. A live tape can be bound once; recreate starts a new incarnation.
	UsageScope string

	// IdempotencyKey reuses one append session across retryable attempts when
	// provided. When empty, the SDK generates one internally.
	IdempotencyKey string
}

// AppendResult describes one successful tape append.
type AppendResult struct {
	// TapeID is the tape that received the appended bytes.
	TapeID string

	// Chunks is the number of chunks committed by this append.
	Chunks int64

	// LogicalBytes is the number of payload bytes committed by this append.
	LogicalBytes int64
}

type uploadResult struct {
	Chunks       int64
	LogicalBytes int64
}

type uploadRequest struct {
	ctx              context.Context
	teardownCtx      context.Context
	spaceID          string
	tapeID           string
	reader           io.Reader
	format           payloadFormat
	idempotencyKey   string
	cleanupOnFailure bool
	reportFailure    func(error)
	tapeOptions      createTapeOptions
}

type uploadSession struct {
	client                    *Client
	request                   uploadRequest
	retryConfig               retryConfig
	streamID                  string
	sequence                  int64
	format                    payloadFormat
	incarnationCommittedBytes int64
	ensureEncoder             func() (zstdEncoder, error)
}

// Append appends bytes from r into the target tape, creating the tape if needed.
//
// When opts.IdempotencyKey is empty, the SDK generates one internally and
// reuses it across retryable attempts within the call. The same key can also
// replay a completed session whose success response was lost: the server
// acknowledges already committed chunks and close is idempotent. If a
// caller-provided key fails after stream creation, the stream stays open so the
// same key can retry the session. If the SDK generated the key, it best-effort
// closes the stream before returning the failure.
//
// When the tape uses CompressionZstd, a source that waits for live bytes may
// implement io.ReadCloser to let the SDK flush partial chunks. Its Close method
// must interrupt an in-flight Read; the SDK closes it when framed reading ends.
// If framed reading never begins, the caller retains ownership of the source.
// A finite io.Reader is read synchronously and observes cancellation after Read
// returns.
func (c *Client) Append(ctx context.Context, spaceID string, tapeID string, r io.Reader, opts AppendOptions) (AppendResult, error) {
	if !IsValidID(spaceID) {
		return AppendResult{}, fmt.Errorf("invalid space_id: %q", spaceID)
	}
	if !IsValidID(tapeID) {
		return AppendResult{}, fmt.Errorf("invalid tape_id: %q", tapeID)
	}

	cleanupOnFailure := opts.IdempotencyKey == ""
	idempotencyKey := opts.IdempotencyKey
	if idempotencyKey == "" {
		key, err := NewIdempotencyKey()
		if err != nil {
			return AppendResult{}, err
		}
		idempotencyKey = key
	}

	selectedFormat, err := opts.Compression.payloadFormat()
	if err != nil {
		return AppendResult{}, err
	}

	gotTapeID, uploadFormat, err := c.createOrReuseAppendTape(ctx, spaceID, tapeID, createTapeOptions{
		RetainMode:    opts.RetainMode,
		PayloadFormat: selectedFormat,
		UsageScope:    opts.UsageScope,
	})
	if err != nil {
		return AppendResult{}, err
	}

	res, err := c.uploadWithPayloadFormat(uploadRequest{
		ctx:              ctx,
		teardownCtx:      context.Background(),
		spaceID:          spaceID,
		tapeID:           gotTapeID,
		reader:           r,
		format:           uploadFormat,
		idempotencyKey:   idempotencyKey,
		cleanupOnFailure: cleanupOnFailure,
		tapeOptions: createTapeOptions{
			RetainMode:    opts.RetainMode,
			PayloadFormat: selectedFormat,
			UsageScope:    opts.UsageScope,
		},
	})
	if err != nil {
		return AppendResult{TapeID: gotTapeID}, err
	}

	return AppendResult{
		TapeID:       gotTapeID,
		Chunks:       res.Chunks,
		LogicalBytes: res.LogicalBytes,
	}, nil
}

func (c *Client) createOrReuseAppendTape(ctx context.Context, spaceID string, tapeID string, opts createTapeOptions) (string, payloadFormat, error) {
	selectedFormat := opts.PayloadFormat.orDefault()
	gotTapeID, err := c.createTape(ctx, spaceID, tapeID, opts)
	if err == nil {
		return gotTapeID, selectedFormat, nil
	}
	if tapeID == "" {
		return "", "", err
	}
	existingFormat, ok := existingPayloadFormatConflict(err)
	if !ok {
		return "", "", err
	}
	if opts.UsageScope == "" {
		return tapeID, existingFormat, nil
	}
	opts.PayloadFormat = existingFormat
	gotTapeID, err = c.createTape(ctx, spaceID, tapeID, opts)
	if err != nil {
		return "", "", err
	}
	return gotTapeID, existingFormat, nil
}

func existingPayloadFormatConflict(err error) (payloadFormat, bool) {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusConflict {
		return "", false
	}

	var body struct {
		Error                 string `json:"error"`
		ExistingPayloadFormat string `json:"existing_payload_format"`
	}
	if json.Unmarshal([]byte(httpErr.Body), &body) != nil {
		return "", false
	}
	if body.Error != "payload_format conflict" {
		return "", false
	}
	format, err := storedPayloadFormat(body.ExistingPayloadFormat)
	if err != nil {
		return "", false
	}
	return format, true
}

func isDeleteConflict(err error) bool {
	message, ok := httpAPIErrorMessage(err, http.StatusConflict)
	return ok && message == "tape deleted"
}

func (c *Client) uploadWithPayloadFormat(request uploadRequest) (uploadResult, error) {
	if _, err := uploadChunkBufferSize(request.format); err != nil {
		return uploadResult{}, err
	}
	return c.uploadRawChunks(request)
}

func uploadChunkBufferSize(format payloadFormat) (int, error) {
	switch format {
	case payloadFormatIdentity:
		return defaultUploadChunkSizeBytes, nil
	case payloadFormatFramedZstdV1:
		return defaultFramedZstdRawWindowBytes, nil
	default:
		return 0, fmt.Errorf("invalid payload format: %q", format)
	}
}

func readUploadChunk(ctx context.Context, r io.Reader, format payloadFormat, buf []byte) (int, error) {
	switch format {
	case payloadFormatIdentity:
		return r.Read(buf)
	case payloadFormatFramedZstdV1:
		if chunkReader, ok := r.(uploadChunkReader); ok {
			return chunkReader.ReadUploadChunk(buf)
		}
		total := 0
		for total < len(buf) {
			if err := ctx.Err(); err != nil {
				return total, err
			}
			n, err := r.Read(buf[total:])
			total += n
			if err != nil {
				return total, err
			}
		}
		return total, nil
	default:
		return 0, fmt.Errorf("invalid payload format: %q", format)
	}
}

func uploadReadDone(format payloadFormat, err error) bool {
	switch format {
	case payloadFormatIdentity:
		return errors.Is(err, io.EOF)
	case payloadFormatFramedZstdV1:
		return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	default:
		return false
	}
}

func (c *Client) uploadRawChunks(request uploadRequest) (res uploadResult, err error) {
	maxChunkSize := defaultUploadChunkSizeBytes
	if defaultFramedZstdRawWindowBytes > maxChunkSize {
		maxChunkSize = defaultFramedZstdRawWindowBytes
	}

	var encoder *zstd.Encoder
	defer func() {
		if encoder != nil {
			encoder.Close()
		}
	}()

	ensureEncoder := func() (zstdEncoder, error) {
		if encoder != nil {
			return encoder, nil
		}
		next, err := newFramedZstdEncoder()
		if err != nil {
			return nil, err
		}
		encoder = next
		return encoder, nil
	}

	var framedReader *framedZstdUploadReader
	defer func() {
		if framedReader != nil {
			framedReader.Close()
		}
	}()

	readSourceForFormat := func(format payloadFormat) (io.Reader, error) {
		if framedReader != nil {
			// Once a generic reader has been upgraded to the framed wrapper,
			// later format flips must keep consuming the same wrapper so any
			// already-buffered raw bytes are not lost.
			return framedReader, nil
		}
		switch format {
		case payloadFormatIdentity:
			return request.reader, nil
		case payloadFormatFramedZstdV1:
			if _, ok := request.reader.(uploadChunkReader); ok {
				return request.reader, nil
			}
			if readCloser, ok := request.reader.(io.ReadCloser); ok {
				framedReader = newFramedZstdUploadReader(request.ctx, readCloser, c.framedZstdUploadFlushInterval)
				return framedReader, nil
			}
			return request.reader, nil
		default:
			return nil, fmt.Errorf("invalid payload format: %q", format)
		}
	}

	return c.uploadWithStream(request, func(streamID string) (uploadResult, string, error) {
		buf := make([]byte, maxChunkSize)
		res := uploadResult{}
		session := uploadSession{
			client:        c,
			request:       request,
			retryConfig:   c.retryConfig(),
			streamID:      streamID,
			format:        request.format,
			ensureEncoder: ensureEncoder,
		}

		for {
			readFormat := session.format
			chunkSize, err := uploadChunkBufferSize(readFormat)
			if err != nil {
				return uploadResult{}, session.streamID, err
			}

			readSource, err := readSourceForFormat(readFormat)
			if err != nil {
				return uploadResult{}, session.streamID, err
			}

			n, readErr := readUploadChunk(request.ctx, readSource, readFormat, buf[:chunkSize])
			if n > 0 {
				uploadedChunks, uploadedBytes, chunkErr := session.putChunk(buf[:n])
				if chunkErr != nil {
					return uploadResult{}, session.streamID, chunkErr
				}
				res.Chunks += uploadedChunks
				res.LogicalBytes += uploadedBytes
			}

			if uploadReadDone(readFormat, readErr) {
				return res, session.streamID, nil
			}
			if readErr != nil {
				return uploadResult{}, session.streamID, readErr
			}
		}
	})
}

func (c *Client) uploadWithStream(request uploadRequest, upload func(streamID string) (uploadResult, string, error)) (res uploadResult, err error) {
	streamID, err := c.createStream(request.ctx, request.spaceID, request.tapeID, createStreamOptions{IdempotencyKey: request.idempotencyKey})
	if err != nil {
		if request.reportFailure != nil {
			request.reportFailure(err)
		}
		if request.cleanupOnFailure {
			c.bestEffortResolveAndCloseUploadStream(request.teardownCtx, request.spaceID, request.tapeID, request.idempotencyKey)
		}
		return uploadResult{}, err
	}
	defer func() {
		if !request.cleanupOnFailure || err == nil {
			return
		}

		if request.reportFailure != nil {
			request.reportFailure(err)
		}
		c.bestEffortCloseUploadStream(request.teardownCtx, request.spaceID, request.tapeID, streamID)
	}()
	res, streamID, err = upload(streamID)
	if err != nil {
		return uploadResult{}, err
	}

	err = c.closeUploadStream(request.teardownCtx, request.spaceID, request.tapeID, streamID)
	if err != nil {
		return uploadResult{}, err
	}

	return res, nil
}

func (s *uploadSession) putChunk(raw []byte) (int64, int64, error) {
	for {
		nextSequence, uploadedChunks, uploadedBytes, ambiguousChunk, err := s.client.putRawChunkWithRetry(
			s.request.ctx,
			s.retryConfig,
			s.request.spaceID,
			s.request.tapeID,
			s.streamID,
			s.sequence,
			raw,
			s.format,
			s.ensureEncoder,
		)
		if err == nil {
			s.sequence = nextSequence
			s.incarnationCommittedBytes += uploadedBytes
			return uploadedChunks, uploadedBytes, nil
		}
		if !isDeleteConflict(err) {
			return uploadedChunks, uploadedBytes, err
		}
		if ambiguousChunk || s.incarnationCommittedBytes > 0 || uploadedBytes > 0 {
			// Once a transport error makes the current chunk's outcome
			// ambiguous, or once any earlier bytes already committed in the
			// current tape incarnation, replaying only the current chunk into a
			// recreated tape would silently drop the committed prefix.
			return uploadedChunks, uploadedBytes, fmt.Errorf("cannot recover delete after committed bytes: %w", err)
		}

		s.streamID, s.format, err = s.client.recreateDeletedTapeStream(s.request)
		if err != nil {
			return 0, 0, err
		}
		s.sequence = 0
	}
}

func (c *Client) putRawChunkWithRetry(
	ctx context.Context,
	retryCfg retryConfig,
	spaceID string,
	tapeID string,
	streamID string,
	seq int64,
	raw []byte,
	format payloadFormat,
	ensureEncoder func() (zstdEncoder, error),
) (int64, int64, int64, bool, error) {
	switch format {
	case payloadFormatIdentity:
		return putChunkWithSplitRetry(ctx, retryCfg, c, c.requestTimeout, spaceID, tapeID, streamID, seq, raw)
	case payloadFormatFramedZstdV1:
		encoder, err := ensureEncoder()
		if err != nil {
			return seq, 0, 0, false, err
		}
		return putFramedZstdChunkWithRetry(ctx, retryCfg, c, c.requestTimeout, spaceID, tapeID, streamID, seq, raw, encoder)
	default:
		return seq, 0, 0, false, fmt.Errorf("invalid payload format: %q", format)
	}
}

func (c *Client) recreateDeletedTapeStream(request uploadRequest) (string, payloadFormat, error) {
	_, format, err := c.createOrReuseAppendTape(request.ctx, request.spaceID, request.tapeID, request.tapeOptions)
	if err != nil {
		return "", "", err
	}

	streamID, err := c.createStream(request.ctx, request.spaceID, request.tapeID, createStreamOptions{IdempotencyKey: request.idempotencyKey})
	if err != nil {
		if request.reportFailure != nil {
			request.reportFailure(err)
		}
		if request.cleanupOnFailure {
			c.bestEffortResolveAndCloseUploadStream(request.teardownCtx, request.spaceID, request.tapeID, request.idempotencyKey)
		}
		return "", "", err
	}
	return streamID, format, nil
}

func (c *Client) closeUploadStream(ctx context.Context, spaceID, tapeID, streamID string) error {
	closeCtx, cancel := context.WithTimeout(ctx, c.maxRetryTime+c.requestTimeout)
	defer cancel()
	return c.closeStream(closeCtx, spaceID, tapeID, streamID)
}

func (c *Client) bestEffortCloseUploadStream(ctx context.Context, spaceID, tapeID, streamID string) {
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, bestEffortUploadCleanupTimeout)
	defer cleanupCancel()
	_ = c.closeStream(cleanupCtx, spaceID, tapeID, streamID)
}

func (c *Client) bestEffortResolveAndCloseUploadStream(ctx context.Context, spaceID, tapeID, idempotencyKey string) {
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, bestEffortUploadCleanupTimeout)
	defer cleanupCancel()

	streamID, err := c.createStream(cleanupCtx, spaceID, tapeID, createStreamOptions{IdempotencyKey: idempotencyKey})
	if err != nil {
		return
	}
	_ = c.closeStream(cleanupCtx, spaceID, tapeID, streamID)
}

func putChunkWithSplitRetry(ctx context.Context, cfg retryConfig, cli chunkPutter, requestTimeout time.Duration, spaceID string, tapeID string, streamID string, seq int64, payload []byte) (int64, int64, int64, bool, error) {
	if len(payload) == 0 {
		return seq, 0, 0, false, nil
	}

	sum := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(sum[:])

	ambiguousChunk, err := putChunkWithRetry(ctx, cfg, cli, requestTimeout, spaceID, tapeID, streamID, seq, shaHex, payload)
	if err == nil {
		return seq + 1, 1, int64(len(payload)), ambiguousChunk, nil
	} else if !shouldSplitChunk(err, len(payload)) {
		return seq, 0, 0, ambiguousChunk, err
	}
	// A concrete "payload too large" response proves the oversized chunk never
	// committed, so later split uploads no longer inherit any earlier transport
	// ambiguity from that impossible full-chunk write.
	ambiguousChunk = false

	mid := len(payload) / 2
	nextSeq, chunksLeft, bytesLeft, ambiguousLeft, err := putChunkWithSplitRetry(ctx, cfg, cli, requestTimeout, spaceID, tapeID, streamID, seq, payload[:mid])
	if err != nil {
		return nextSeq, chunksLeft, bytesLeft, ambiguousChunk || ambiguousLeft, err
	}

	nextSeq, chunksRight, bytesRight, ambiguousRight, err := putChunkWithSplitRetry(ctx, cfg, cli, requestTimeout, spaceID, tapeID, streamID, nextSeq, payload[mid:])
	if err != nil {
		return nextSeq, chunksLeft + chunksRight, bytesLeft + bytesRight, ambiguousChunk || ambiguousLeft || ambiguousRight, err
	}

	return nextSeq, chunksLeft + chunksRight, bytesLeft + bytesRight, ambiguousChunk || ambiguousLeft || ambiguousRight, nil
}

func putChunkWithRetry(ctx context.Context, cfg retryConfig, cli chunkPutter, requestTimeout time.Duration, spaceID string, tapeID string, streamID string, seq int64, sha256Hex string, payload []byte) (bool, error) {
	ambiguousChunk := false
	err := doWithRetry(ctx, cfg, func(attemptCtx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(attemptCtx, requestTimeout)
		err := cli.putChunk(attemptCtx, spaceID, tapeID, streamID, seq, sha256Hex, payload)
		cancel()
		if isTransportError(err) {
			ambiguousChunk = true
		}
		return err
	})
	return ambiguousChunk, err
}

func shouldSplitChunk(err error, payloadLen int) bool {
	if payloadLen <= 1 {
		return false
	}

	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}

	return httpErr.StatusCode == http.StatusBadRequest && strings.Contains(httpErr.Body, "payload too large")
}

func putFramedZstdChunkWithRetry(ctx context.Context, cfg retryConfig, cli chunkPutter, requestTimeout time.Duration, spaceID string, tapeID string, streamID string, seq int64, raw []byte, encoder interface{ EncodeAll(src, dst []byte) []byte }) (int64, int64, int64, bool, error) {
	if len(raw) == 0 {
		return seq, 0, 0, false, nil
	}

	splitAt := defaultFramedZstdRawWindowBytes
	ambiguousChunk := false
	if len(raw) <= defaultFramedZstdRawWindowBytes {
		frame := encodeFramedZstdChunk(raw, encoder)
		sum := sha256.Sum256(frame)
		shaHex := hex.EncodeToString(sum[:])

		var err error
		ambiguousChunk, err = putChunkWithRetry(ctx, cfg, cli, requestTimeout, spaceID, tapeID, streamID, seq, shaHex, frame)
		if err == nil {
			return seq + 1, 1, int64(len(raw)), ambiguousChunk, nil
		}
		if !shouldSplitChunk(err, len(frame)) || len(raw) <= 1 {
			return seq, 0, 0, ambiguousChunk, err
		}
		splitAt = len(raw) / 2
	}
	// A proactive split has not sent the oversized raw window. A reactive split
	// follows a concrete rejection, which proves the full frame did not commit.
	ambiguousChunk = false

	nextSeq, leftChunks, leftBytes, ambiguousLeft, err := putFramedZstdChunkWithRetry(ctx, cfg, cli, requestTimeout, spaceID, tapeID, streamID, seq, raw[:splitAt], encoder)
	if err != nil {
		return nextSeq, leftChunks, leftBytes, ambiguousChunk || ambiguousLeft, err
	}

	nextSeq, rightChunks, rightBytes, ambiguousRight, err := putFramedZstdChunkWithRetry(ctx, cfg, cli, requestTimeout, spaceID, tapeID, streamID, nextSeq, raw[splitAt:], encoder)
	if err != nil {
		return nextSeq, leftChunks + rightChunks, leftBytes + rightBytes, ambiguousChunk || ambiguousLeft || ambiguousRight, err
	}

	return nextSeq, leftChunks + rightChunks, leftBytes + rightBytes, ambiguousChunk || ambiguousLeft || ambiguousRight, nil
}

type framedZstdUploadReader struct {
	ctx           context.Context
	source        io.ReadCloser
	flushInterval time.Duration
	requests      chan int
	results       chan uploadReadResult
	done          chan struct{}
	startOnce     sync.Once
	closeOnce     sync.Once
}

type uploadReadResult struct {
	payload []byte
	err     error
}

// newFramedZstdUploadReader batches live source bytes without reading ahead
// beyond the current chunk. Closing the wrapper interrupts the source read.
func newFramedZstdUploadReader(ctx context.Context, r io.ReadCloser, flushInterval time.Duration) *framedZstdUploadReader {
	return &framedZstdUploadReader{
		ctx:           ctx,
		source:        r,
		flushInterval: flushInterval,
		// Allow one queued follow-up request so a terminal in-flight read can
		// still deliver its result without deadlocking a caller already asking
		// for the next piece.
		requests: make(chan int, 1),
		results:  make(chan uploadReadResult, 1),
		done:     make(chan struct{}),
	}
}

func (r *framedZstdUploadReader) start() {
	r.startOnce.Do(func() {
		go r.readLoop()
	})
}

func (r *framedZstdUploadReader) readLoop() {
	defer close(r.results)
	for {
		var readSize int
		select {
		case <-r.done:
			return
		case <-r.ctx.Done():
			return
		case readSize = <-r.requests:
		}

		if readSize <= 0 {
			readSize = 1
		}
		buf := make([]byte, readSize)
		n := 0
		var err error
		for n == 0 && err == nil {
			select {
			case <-r.done:
				return
			case <-r.ctx.Done():
				return
			default:
			}
			n, err = r.source.Read(buf)
		}
		if n > 0 {
			buf = buf[:n]
		} else {
			buf = nil
		}

		select {
		case r.results <- uploadReadResult{payload: buf, err: err}:
		case <-r.done:
			return
		case <-r.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// Close stops the read loop and closes its source.
func (r *framedZstdUploadReader) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		_ = r.source.Close()
	})
}

// Read implements io.Reader.
func (r *framedZstdUploadReader) Read(p []byte) (int, error) {
	return r.ReadUploadChunk(p)
}

// ReadUploadChunk reads one flush-bounded raw chunk for framed compression.
func (r *framedZstdUploadReader) ReadUploadChunk(p []byte) (int, error) {
	r.start()
	if len(p) == 0 {
		return 0, nil
	}

	first, err := r.readPiece(minInt(defaultFramedZstdReadPieceBytes, len(p)))
	if err != nil {
		return 0, err
	}
	if len(first.payload) == 0 {
		if first.err == nil {
			return 0, io.EOF
		}
		return 0, first.err
	}

	n := copy(p, first.payload)
	if first.err != nil {
		return n, first.err
	}
	if n == len(p) {
		return n, nil
	}

	timer := time.NewTimer(r.flushInterval)
	defer timer.Stop()

	for n < len(p) {
		pieceSize := minInt(defaultFramedZstdReadPieceBytes, len(p)-n)
		if err := r.requestPiece(pieceSize); err != nil {
			return n, err
		}

		select {
		case next, ok := <-r.results:
			if !ok {
				return n, io.EOF
			}
			if len(next.payload) > 0 {
				n += copy(p[n:], next.payload)
			}
			if next.err != nil {
				return n, next.err
			}
			if n == len(p) {
				return n, nil
			}
		case <-timer.C:
			return n, nil
		case <-r.done:
			return n, io.EOF
		case <-r.ctx.Done():
			return n, r.ctx.Err()
		}
	}

	return n, nil
}

func (r *framedZstdUploadReader) readPiece(size int) (uploadReadResult, error) {
	select {
	case next, ok := <-r.results:
		if !ok {
			return uploadReadResult{}, io.EOF
		}
		return next, nil
	default:
	}

	if err := r.requestPiece(size); err != nil {
		return uploadReadResult{}, err
	}
	select {
	case next, ok := <-r.results:
		if !ok {
			return uploadReadResult{}, io.EOF
		}
		return next, nil
	case <-r.done:
		return uploadReadResult{}, io.EOF
	case <-r.ctx.Done():
		return uploadReadResult{}, r.ctx.Err()
	}
}

func (r *framedZstdUploadReader) requestPiece(size int) error {
	select {
	case r.requests <- size:
		return nil
	case <-r.done:
		return io.EOF
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
