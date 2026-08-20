package tape9

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientAppendCreatesTapeSplitsChunksAndClosesStream(t *testing.T) {
	var (
		createTapeCalls   int
		createStreamCalls int
		closeCalls        int
		chunkPaths        []string
		chunks            [][]byte
		lastRetainMode    string
		createStreamKey   string
	)
	const idempotencyKey = "append_key_00001"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			var body struct {
				TapeID     string `json:"tape_id"`
				RetainMode string `json:"retain_mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create tape body: %v", err)
			}
			if body.TapeID != "tape" {
				t.Fatalf("tape_id = %q, want tape", body.TapeID)
			}
			createTapeCalls++
			lastRetainMode = body.RetainMode
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			var body struct {
				IdempotencyKey string `json:"idempotency_key"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create stream body: %v", err)
			}
			if body.IdempotencyKey != idempotencyKey {
				t.Fatalf("idempotency key = %q, want %q", body.IdempotencyKey, idempotencyKey)
			}
			createStreamCalls++
			createStreamKey = body.IdempotencyKey
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			if len(body) > 3 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":"payload too large"}`)
				return
			}
			chunkPaths = append(chunkPaths, r.URL.Path)
			chunks = append(chunks, body)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	res, err := client.Append(context.Background(), "space", "tape", strings.NewReader("abcdef"), AppendOptions{
		RetainMode:     RetainModeTail,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if res.TapeID != "tape" {
		t.Fatalf("unexpected result tape id: %q", res.TapeID)
	}
	if res.Chunks != 2 {
		t.Fatalf("unexpected chunk count: %d", res.Chunks)
	}
	if res.LogicalBytes != 6 {
		t.Fatalf("unexpected logical bytes: %d", res.LogicalBytes)
	}
	if createTapeCalls != 1 {
		t.Fatalf("expected one tape creation, got %d", createTapeCalls)
	}
	if lastRetainMode != "tail" {
		t.Fatalf("retain mode = %q, want tail", lastRetainMode)
	}
	if createStreamCalls != 1 {
		t.Fatalf("expected one stream creation, got %d", createStreamCalls)
	}
	if createStreamKey != idempotencyKey {
		t.Fatalf("idempotency key = %q, want %q", createStreamKey, idempotencyKey)
	}
	if closeCalls != 1 {
		t.Fatalf("expected one close call, got %d", closeCalls)
	}
	if len(chunkPaths) != 2 {
		t.Fatalf("expected two chunk uploads, got %d", len(chunkPaths))
	}
	if chunkPaths[0] != "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0" {
		t.Fatalf("unexpected first chunk path: %q", chunkPaths[0])
	}
	if chunkPaths[1] != "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/1" {
		t.Fatalf("unexpected second chunk path: %q", chunkPaths[1])
	}
	if string(chunks[0]) != "abc" {
		t.Fatalf("unexpected first chunk: %q", chunks[0])
	}
	if string(chunks[1]) != "def" {
		t.Fatalf("unexpected second chunk: %q", chunks[1])
	}
}

func TestClientAppendWithFramedZstdUploadsWholeFrames(t *testing.T) {
	var (
		chunks            [][]byte
		lastPayloadFormat string
	)
	const idempotencyKey = "append_key_00001"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			var body struct {
				TapeID        string `json:"tape_id"`
				PayloadFormat string `json:"payload_format"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create tape body: %v", err)
			}
			lastPayloadFormat = body.PayloadFormat
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks = append(chunks, body)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	payload := "hello framed zstd world"
	res, err := client.Append(context.Background(), "space", "tape", newFixedChunkReader(payload, 3), AppendOptions{
		IdempotencyKey: idempotencyKey,
		Compression:    CompressionZstd,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if got, want := res.LogicalBytes, int64(len(payload)); got != want {
		t.Fatalf("logical bytes = %d, want %d", got, want)
	}
	if got, want := lastPayloadFormat, string(payloadFormatFramedZstdV1); got != want {
		t.Fatalf("payload_format = %q, want %q", got, want)
	}
	if len(chunks) != 1 {
		t.Fatalf("chunk count = %d, want 1", len(chunks))
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(chunks[0], decoder, &decoded); err != nil {
		t.Fatalf("decode uploaded frame: %v", err)
	}
	if got, want := decoded.String(), payload; got != want {
		t.Fatalf("decoded payload = %q, want %q", got, want)
	}
}

func TestClientAppendWithFramedZstdDoesNotReadBeforeStreamExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":"invalid stream"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	readCalled := make(chan struct{}, 1)
	_, err = client.Append(context.Background(), "space", "tape", &readSignalReader{readCalled: readCalled}, AppendOptions{
		Compression: CompressionZstd,
	})
	if err == nil {
		t.Fatal("expected append error")
	}
	select {
	case <-readCalled:
		t.Fatal("zstd append read input before stream creation succeeded")
	default:
	}
}

func TestClientAppendWithFramedZstdIgnoresEmptyNilReads(t *testing.T) {
	var chunks [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks = append(chunks, body)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	res, err := client.Append(context.Background(), "space", "tape", &zeroThenDataReader{}, AppendOptions{
		Compression: CompressionZstd,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if got, want := res.LogicalBytes, int64(len("payload")); got != want {
		t.Fatalf("logical bytes = %d, want %d", got, want)
	}
	if len(chunks) != 1 {
		t.Fatalf("chunk count = %d, want 1", len(chunks))
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(chunks[0], decoder, &decoded); err != nil {
		t.Fatalf("decode uploaded frame: %v", err)
	}
	if got, want := decoded.String(), "payload"; got != want {
		t.Fatalf("decoded payload = %q, want %q", got, want)
	}
}

func TestClientAppendWithFramedZstdWaitsForEOFOnGenericReaders(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	tailReady := make(chan struct{})
	r := &delayedTailReader{tailReady: tailReady}
	type appendCall struct {
		res AppendResult
		err error
	}
	done := make(chan appendCall, 1)
	go func() {
		res, err := client.Append(context.Background(), "space", "tape", r, AppendOptions{
			Compression: CompressionZstd,
		})
		done <- appendCall{res: res, err: err}
	}()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		state.mu.Lock()
		chunkCount := len(state.chunks)
		state.mu.Unlock()
		if chunkCount > 0 {
			close(tailReady)
			t.Fatal("generic reader upload flushed before EOF")
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(tailReady)

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("append: %v", got.err)
		}
		if got.res.LogicalBytes != int64(len("head\ntail\n")) {
			t.Fatalf("logical bytes = %d, want %d", got.res.LogicalBytes, len("head\ntail\n"))
		}
		state.mu.Lock()
		requireChunkCount := len(state.chunks)
		chunk := append([]byte(nil), state.chunks[0]...)
		state.mu.Unlock()
		if requireChunkCount != 1 {
			t.Fatalf("chunk count = %d, want 1", requireChunkCount)
		}
		decoder, err := newFramedZstdDecoder()
		if err != nil {
			t.Fatalf("new decoder: %v", err)
		}
		var decoded bytes.Buffer
		if err := decodeFramedZstdSegment(chunk, decoder, &decoded); err != nil {
			decoder.Close()
			t.Fatalf("decode chunk: %v", err)
		}
		decoder.Close()
		if got, want := decoded.String(), "head\ntail\n"; got != want {
			t.Fatalf("decoded chunk = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("append did not finish after tail read")
	}
}

func TestClientAppendWithFramedZstdClosesBlockingSourceOnContextCancel(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	setFramedZstdUploadFlushIntervalForTest(t, client, time.Hour)

	reader := newCloseableDelayedTailReader()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.Append(ctx, "space", "tape", reader, AppendOptions{
			Compression: CompressionZstd,
		})
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("append error = %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("append hung past context deadline")
	}

	select {
	case <-reader.closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("append did not close the blocking reader")
	}
}

func TestClientAppendWithFramedZstdStopsEmptyReadRetryAfterContextCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	reader := &alwaysEmptyReader{}
	_, err = client.Append(ctx, "space", "tape", reader, AppendOptions{
		Compression:    CompressionZstd,
		IdempotencyKey: "append_key_00001",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("append error = %v, want %v", err, context.DeadlineExceeded)
	}

	calls := reader.calls.Load()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		next := reader.calls.Load()
		if next == calls {
			return
		}
		calls = next
	}
	t.Fatalf("reader kept polling after context cancel; calls reached %d", calls)
}

func TestClientAppendDeleteRecoverySwitchesToFramedZstdReader(t *testing.T) {
	var (
		framedCreateTapeCalls int
		createStreamCalls     int
		chunks                = make(chan []byte, 2)
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			var body struct {
				PayloadFormat string `json:"payload_format"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create tape body: %v", err)
			}
			if got, want := body.PayloadFormat, string(payloadFormatFramedZstdV1); got != want {
				t.Fatalf("payload_format = %q, want %q", got, want)
			}
			framedCreateTapeCalls++
			w.Header().Set("Content-Type", "application/json")
			if framedCreateTapeCalls == 1 {
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"payload_format conflict","existing_payload_format":"identity"}`)
				return
			}
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			createStreamCalls++
			w.Header().Set("Content-Type", "application/json")
			if createStreamCalls == 1 {
				_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
				return
			}
			_, _ = io.WriteString(w, `{"stream_id":"stream-2"}`)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
			w.WriteHeader(http.StatusConflict)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks <- body
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-2/close":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	setFramedZstdUploadFlushIntervalForTest(t, client, 20*time.Millisecond)

	tailReady := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := client.Append(context.Background(), "space", "tape", io.NopCloser(&headTailThenReleaseReader{
			tailReady: tailReady,
			release:   release,
		}), AppendOptions{
			Compression: CompressionZstd,
		})
		done <- err
	}()

	close(tailReady)

	uploadedChunks := make([][]byte, 0, 2)
	for len(uploadedChunks) < 2 {
		select {
		case chunk := <-chunks:
			uploadedChunks = append(uploadedChunks, chunk)
		case <-time.After(300 * time.Millisecond):
			close(release)
			t.Fatalf("expected replayed head chunk and flushed tail chunk before EOF after delete recovery, got %d chunks", len(uploadedChunks))
		}
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		close(release)
		t.Fatalf("new decoder: %v", err)
	}
	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(uploadedChunks[0], decoder, &decoded); err != nil {
		decoder.Close()
		close(release)
		t.Fatalf("decode first chunk: %v", err)
	}
	if got, want := decoded.String(), "head\n"; got != want {
		close(release)
		t.Fatalf("decoded first chunk = %q, want %q", got, want)
	}

	decoded.Reset()
	if err := decodeFramedZstdSegment(uploadedChunks[1], decoder, &decoded); err != nil {
		decoder.Close()
		close(release)
		t.Fatalf("decode second chunk: %v", err)
	}
	decoder.Close()
	if got, want := decoded.String(), "tail\n"; got != want {
		close(release)
		t.Fatalf("decoded second chunk = %q, want %q", got, want)
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("append: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("append did not finish after reader release")
	}
}

func TestClientAppendRecoversCurrentChunkAfterDeleteConflict(t *testing.T) {
	var (
		createTapeCalls   int
		createStreamCalls int
		closeCalls        int
		createStreamKeys  []string
		chunkPaths        []string
		chunks            [][]byte
	)
	const idempotencyKey = "append_key_00001"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			var body struct {
				TapeID     string `json:"tape_id"`
				RetainMode string `json:"retain_mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create tape body: %v", err)
			}
			if got, want := body.TapeID, "tape"; got != want {
				t.Fatalf("tape_id = %q, want %q", got, want)
			}
			if got, want := body.RetainMode, "tail"; got != want {
				t.Fatalf("retain_mode = %q, want %q", got, want)
			}
			createTapeCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			var body struct {
				IdempotencyKey string `json:"idempotency_key"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create stream body: %v", err)
			}
			createStreamCalls++
			createStreamKeys = append(createStreamKeys, body.IdempotencyKey)
			w.Header().Set("Content-Type", "application/json")
			if createStreamCalls == 1 {
				_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
				return
			}
			_, _ = io.WriteString(w, `{"stream_id":"stream-2"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunkPaths = append(chunkPaths, r.URL.Path)
			chunks = append(chunks, body)
			switch r.URL.Path {
			case "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
				w.WriteHeader(http.StatusConflict)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
			case "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/0":
				w.WriteHeader(http.StatusNoContent)
			case "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/1":
				w.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(w, r)
			}
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-2/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	res, err := client.Append(context.Background(), "space", "tape", newFixedChunkReader("headtail", 4), AppendOptions{
		RetainMode:     RetainModeTail,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if got, want := res.LogicalBytes, int64(len("headtail")); got != want {
		t.Fatalf("logical bytes = %d, want %d", got, want)
	}
	if got, want := res.Chunks, int64(2); got != want {
		t.Fatalf("chunks = %d, want %d", got, want)
	}
	if got, want := createTapeCalls, 2; got != want {
		t.Fatalf("create tape calls = %d, want %d", got, want)
	}
	if got, want := createStreamCalls, 2; got != want {
		t.Fatalf("create stream calls = %d, want %d", got, want)
	}
	if got := createStreamKeys; len(got) != 2 || got[0] != idempotencyKey || got[1] != idempotencyKey {
		t.Fatalf("unexpected stream keys: %v", got)
	}
	if got, want := closeCalls, 1; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
	requirePaths := []string{
		"/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0",
		"/v1/spaces/space/tapes/tape/streams/stream-2/chunks/0",
		"/v1/spaces/space/tapes/tape/streams/stream-2/chunks/1",
	}
	if len(chunkPaths) != len(requirePaths) {
		t.Fatalf("chunk path count = %d, want %d", len(chunkPaths), len(requirePaths))
	}
	for i := range requirePaths {
		if got, want := chunkPaths[i], requirePaths[i]; got != want {
			t.Fatalf("chunk path[%d] = %q, want %q", i, got, want)
		}
	}
	if got := string(chunks[0]); got != "head" {
		t.Fatalf("delete-conflict chunk = %q, want %q", got, "head")
	}
	if got := string(chunks[1]); got != "head" {
		t.Fatalf("replayed first chunk = %q, want %q", got, "head")
	}
	if got := string(chunks[2]); got != "tail" {
		t.Fatalf("second chunk = %q, want %q", got, "tail")
	}
}

func TestClientAppendFailsDeleteRecoveryAfterCommittedPrefix(t *testing.T) {
	var (
		createTapeCalls   int
		createStreamCalls int
		closeCalls        int
		createStreamKeys  []string
		chunkPaths        []string
		chunks            [][]byte
	)
	const idempotencyKey = "append_key_00001"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			var body struct {
				TapeID     string `json:"tape_id"`
				RetainMode string `json:"retain_mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create tape body: %v", err)
			}
			if got, want := body.TapeID, "tape"; got != want {
				t.Fatalf("tape_id = %q, want %q", got, want)
			}
			if got, want := body.RetainMode, "tail"; got != want {
				t.Fatalf("retain_mode = %q, want %q", got, want)
			}
			createTapeCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			var body struct {
				IdempotencyKey string `json:"idempotency_key"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create stream body: %v", err)
			}
			createStreamCalls++
			createStreamKeys = append(createStreamKeys, body.IdempotencyKey)
			w.Header().Set("Content-Type", "application/json")
			if createStreamCalls == 1 {
				_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
				return
			}
			_, _ = io.WriteString(w, `{"stream_id":"stream-2"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunkPaths = append(chunkPaths, r.URL.Path)
			chunks = append(chunks, body)
			switch r.URL.Path {
			case "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
				w.WriteHeader(http.StatusNoContent)
			case "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/1":
				w.WriteHeader(http.StatusConflict)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
			case "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/0":
				w.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(w, r)
			}
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-2/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Append(context.Background(), "space", "tape", newFixedChunkReader("headtail", 4), AppendOptions{
		RetainMode:     RetainModeTail,
		IdempotencyKey: idempotencyKey,
	})
	if err == nil {
		t.Fatal("append succeeded after delete removed an already committed prefix, want error")
	}
	if !strings.Contains(err.Error(), "cannot recover delete after committed bytes") {
		t.Fatalf("append error = %v, want committed-prefix delete-recovery error", err)
	}
	if got, want := createTapeCalls, 1; got != want {
		t.Fatalf("create tape calls = %d, want %d", got, want)
	}
	if got, want := createStreamCalls, 1; got != want {
		t.Fatalf("create stream calls = %d, want %d", got, want)
	}
	if got := createStreamKeys; len(got) != 1 || got[0] != idempotencyKey {
		t.Fatalf("unexpected stream keys: %v", got)
	}
	if got, want := closeCalls, 0; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
	requirePaths := []string{
		"/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0",
		"/v1/spaces/space/tapes/tape/streams/stream-1/chunks/1",
	}
	if len(chunkPaths) != len(requirePaths) {
		t.Fatalf("chunk path count = %d, want %d", len(chunkPaths), len(requirePaths))
	}
	for i := range requirePaths {
		if got, want := chunkPaths[i], requirePaths[i]; got != want {
			t.Fatalf("chunk path[%d] = %q, want %q", i, got, want)
		}
	}
	for i, want := range []string{"head", "tail"} {
		if got := string(chunks[i]); got != want {
			t.Fatalf("chunk[%d] = %q, want %q", i, got, want)
		}
	}
}

func TestClientAppendFailsDeleteRecoveryAfterCommittedSplitPrefix(t *testing.T) {
	var (
		createTapeCalls   int
		createStreamCalls int
		closeCalls        int
		chunkPaths        []string
		chunks            [][]byte
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			createTapeCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			createStreamCalls++
			w.Header().Set("Content-Type", "application/json")
			if createStreamCalls == 1 {
				_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
				return
			}
			_, _ = io.WriteString(w, `{"stream_id":"stream-2"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunkPaths = append(chunkPaths, r.URL.Path)
			chunks = append(chunks, body)
			switch len(chunkPaths) {
			case 1:
				w.WriteHeader(http.StatusBadRequest)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"error":"payload too large"}`)
			case 2:
				w.WriteHeader(http.StatusNoContent)
			case 3:
				w.WriteHeader(http.StatusConflict)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
			default:
				http.NotFound(w, r)
			}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/close"):
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Append(context.Background(), "space", "tape", strings.NewReader("abcd"), AppendOptions{})
	if err == nil {
		t.Fatal("append succeeded after delete removed a committed split prefix, want error")
	}
	if !strings.Contains(err.Error(), "cannot recover delete after committed bytes") {
		t.Fatalf("append error = %v, want committed-split-prefix delete-recovery error", err)
	}
	if got, want := createTapeCalls, 1; got != want {
		t.Fatalf("create tape calls = %d, want %d", got, want)
	}
	if got, want := createStreamCalls, 1; got != want {
		t.Fatalf("create stream calls = %d, want %d", got, want)
	}
	if got, want := closeCalls, 1; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
	wantPaths := []string{
		"/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0",
		"/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0",
		"/v1/spaces/space/tapes/tape/streams/stream-1/chunks/1",
	}
	if len(chunkPaths) != len(wantPaths) {
		t.Fatalf("chunk path count = %d, want %d", len(chunkPaths), len(wantPaths))
	}
	for i := range wantPaths {
		if got, want := chunkPaths[i], wantPaths[i]; got != want {
			t.Fatalf("chunk path[%d] = %q, want %q", i, got, want)
		}
	}
	for i, want := range []string{"abcd", "ab", "cd"} {
		if got := string(chunks[i]); got != want {
			t.Fatalf("chunk[%d] = %q, want %q", i, got, want)
		}
	}
}

func TestClientAppendStopsDeleteRecoveryAfterAmbiguousChunkCommit(t *testing.T) {
	var (
		createTapeCalls   int
		createStreamCalls int
		closeCalls        int
		chunkPaths        []string
		chunks            [][]byte
		firstChunkAttempt = true
	)
	const idempotencyKey = "append_key_00002"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			createTapeCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			createStreamCalls++
			w.Header().Set("Content-Type", "application/json")
			if createStreamCalls == 1 {
				_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
				return
			}
			_, _ = io.WriteString(w, `{"stream_id":"stream-2"}`)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunkPaths = append(chunkPaths, r.URL.Path)
			chunks = append(chunks, body)
			if firstChunkAttempt {
				firstChunkAttempt = false
				<-r.Context().Done()
				return
			}
			w.WriteHeader(http.StatusConflict)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/0":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read recreated chunk body: %v", err)
			}
			chunkPaths = append(chunkPaths, r.URL.Path)
			chunks = append(chunks, body)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-2/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.requestTimeout = 20 * time.Millisecond
	client.retryBase = time.Millisecond
	client.retryMax = 5 * time.Millisecond
	client.maxRetryTime = 100 * time.Millisecond

	_, err = client.Append(context.Background(), "space", "tape", strings.NewReader("tail"), AppendOptions{
		IdempotencyKey: idempotencyKey,
	})
	if err == nil {
		t.Fatal("append succeeded after ambiguous chunk commit and delete conflict, want error")
	}
	if !strings.Contains(err.Error(), "tape deleted") {
		t.Fatalf("append error = %v, want tape deleted context", err)
	}
	if got, want := createTapeCalls, 1; got != want {
		t.Fatalf("create tape calls = %d, want %d", got, want)
	}
	if got, want := createStreamCalls, 1; got != want {
		t.Fatalf("create stream calls = %d, want %d", got, want)
	}
	if got, want := closeCalls, 0; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
	requirePaths := []string{
		"/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0",
		"/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0",
	}
	if len(chunkPaths) != len(requirePaths) {
		t.Fatalf("chunk path count = %d, want %d", len(chunkPaths), len(requirePaths))
	}
	for i := range requirePaths {
		if got, want := chunkPaths[i], requirePaths[i]; got != want {
			t.Fatalf("chunk path[%d] = %q, want %q", i, got, want)
		}
	}
	for i := range chunks {
		if got := string(chunks[i]); got != "tail" {
			t.Fatalf("chunk[%d] = %q, want %q", i, got, "tail")
		}
	}
}

func TestPutChunkWithSplitRetryClearsAmbiguityAfterPayloadTooLargeProof(t *testing.T) {
	cfg := retryConfig{maxRetryTime: time.Second}
	var calls []struct {
		seq     int64
		payload string
	}
	putter := fakeChunkPutter{
		putFn: func(_ context.Context, _, _, _ string, seq int64, _ string, payload []byte) error {
			calls = append(calls, struct {
				seq     int64
				payload string
			}{
				seq:     seq,
				payload: string(payload),
			})
			switch len(calls) {
			case 1:
				return io.EOF
			case 2:
				return &HTTPError{StatusCode: http.StatusBadRequest, Body: `{"error":"payload too large"}`}
			case 3:
				return nil
			case 4:
				return &HTTPError{StatusCode: http.StatusConflict, Body: `{"error":"tape deleted"}`}
			default:
				t.Fatalf("unexpected putChunk call %d", len(calls))
				return nil
			}
		},
	}

	nextSeq, chunks, logicalBytes, ambiguous, err := putChunkWithSplitRetry(
		context.Background(),
		cfg,
		&putter,
		time.Second,
		"space",
		"tape",
		"stream",
		0,
		[]byte("abcd"),
	)
	if err == nil {
		t.Fatal("expected delete conflict")
	}
	if !isDeleteConflict(err) {
		t.Fatalf("expected delete conflict, got %v", err)
	}
	if ambiguous {
		t.Fatal("expected payload-too-large proof to clear chunk ambiguity")
	}
	if nextSeq != 1 || chunks != 1 || logicalBytes != 2 {
		t.Fatalf("unexpected partial result: nextSeq=%d chunks=%d logicalBytes=%d", nextSeq, chunks, logicalBytes)
	}
	if len(calls) != 4 {
		t.Fatalf("call count = %d, want 4", len(calls))
	}
	if got := calls[0]; got.seq != 0 || got.payload != "abcd" {
		t.Fatalf("call[0] = %+v, want seq=0 payload=abcd", got)
	}
	if got := calls[1]; got.seq != 0 || got.payload != "abcd" {
		t.Fatalf("call[1] = %+v, want seq=0 payload=abcd", got)
	}
	if got := calls[2]; got.seq != 0 || got.payload != "ab" {
		t.Fatalf("call[2] = %+v, want seq=0 payload=ab", got)
	}
	if got := calls[3]; got.seq != 1 || got.payload != "cd" {
		t.Fatalf("call[3] = %+v, want seq=1 payload=cd", got)
	}
}

func TestPutFramedZstdChunkWithRetryClearsAmbiguityAfterPayloadTooLargeProof(t *testing.T) {
	cfg := retryConfig{maxRetryTime: time.Second}
	var calls []struct {
		seq     int64
		payload []byte
	}
	putter := fakeChunkPutter{
		putFn: func(_ context.Context, _, _, _ string, seq int64, _ string, payload []byte) error {
			calls = append(calls, struct {
				seq     int64
				payload []byte
			}{
				seq:     seq,
				payload: append([]byte(nil), payload...),
			})
			switch len(calls) {
			case 1:
				return io.EOF
			case 2:
				return &HTTPError{StatusCode: http.StatusBadRequest, Body: `{"error":"payload too large"}`}
			case 3:
				return nil
			case 4:
				return &HTTPError{StatusCode: http.StatusConflict, Body: `{"error":"tape deleted"}`}
			default:
				t.Fatalf("unexpected putChunk call %d", len(calls))
				return nil
			}
		},
	}

	encoder, err := newFramedZstdEncoder()
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	defer encoder.Close()

	nextSeq, chunks, logicalBytes, ambiguous, err := putFramedZstdChunkWithRetry(
		context.Background(),
		cfg,
		&putter,
		time.Second,
		"space",
		"tape",
		"stream",
		0,
		[]byte("abcd"),
		encoder,
	)
	if err == nil {
		t.Fatal("expected delete conflict")
	}
	if !isDeleteConflict(err) {
		t.Fatalf("expected delete conflict, got %v", err)
	}
	if ambiguous {
		t.Fatal("expected payload-too-large proof to clear chunk ambiguity")
	}
	if nextSeq != 1 || chunks != 1 || logicalBytes != 2 {
		t.Fatalf("unexpected partial result: nextSeq=%d chunks=%d logicalBytes=%d", nextSeq, chunks, logicalBytes)
	}
	if len(calls) != 4 {
		t.Fatalf("call count = %d, want 4", len(calls))
	}
	if got := calls[0].seq; got != 0 {
		t.Fatalf("call[0].seq = %d, want 0", got)
	}
	if got := calls[1].seq; got != 0 {
		t.Fatalf("call[1].seq = %d, want 0", got)
	}
	if got := calls[2].seq; got != 0 {
		t.Fatalf("call[2].seq = %d, want 0", got)
	}
	if got := calls[3].seq; got != 1 {
		t.Fatalf("call[3].seq = %d, want 1", got)
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(calls[2].payload, decoder, &decoded); err != nil {
		t.Fatalf("decode left split frame: %v", err)
	}
	if got, want := decoded.String(), "ab"; got != want {
		t.Fatalf("left split payload = %q, want %q", got, want)
	}
	decoded.Reset()
	if err := decodeFramedZstdSegment(calls[3].payload, decoder, &decoded); err != nil {
		t.Fatalf("decode right split frame: %v", err)
	}
	if got, want := decoded.String(), "cd"; got != want {
		t.Fatalf("right split payload = %q, want %q", got, want)
	}
}

func TestPutFramedZstdChunkWithRetryCapsDecodedFrameSize(t *testing.T) {
	var frames [][]byte
	putter := fakeChunkPutter{
		putFn: func(_ context.Context, _, _, _ string, _ int64, _ string, payload []byte) error {
			frames = append(frames, append([]byte(nil), payload...))
			return nil
		},
	}
	encoder, err := newFramedZstdEncoder()
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	defer encoder.Close()

	raw := bytes.Repeat([]byte("a"), defaultFramedZstdRawWindowBytes+1)
	nextSequence, chunks, logicalBytes, ambiguous, err := putFramedZstdChunkWithRetry(
		context.Background(),
		retryConfig{maxRetryTime: time.Second},
		&putter,
		time.Second,
		"space",
		"tape",
		"stream",
		0,
		raw,
		encoder,
	)
	if err != nil {
		t.Fatalf("put framed zstd chunk: %v", err)
	}
	if nextSequence != 2 || chunks != 2 || logicalBytes != int64(len(raw)) || ambiguous {
		t.Fatalf("unexpected result: next=%d chunks=%d bytes=%d ambiguous=%t", nextSequence, chunks, logicalBytes, ambiguous)
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()
	var decoded bytes.Buffer
	for _, frame := range frames {
		if err := decodeFramedZstdSegment(frame, decoder, &decoded); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
	}
	if !bytes.Equal(decoded.Bytes(), raw) {
		t.Fatalf("decoded bytes = %d, want %d", decoded.Len(), len(raw))
	}
}

func TestClientAppendDeleteRecoverySwitchesToRequestedFramedZstdFormatAfterIdentityFallback(t *testing.T) {
	var (
		createTapeFormats []string
		createStreamCalls int
		closeCalls        int
		chunks            [][]byte
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			var body struct {
				PayloadFormat string `json:"payload_format"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create tape body: %v", err)
			}
			createTapeFormats = append(createTapeFormats, body.PayloadFormat)
			if len(createTapeFormats) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"payload_format conflict","existing_payload_format":"identity"}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			createStreamCalls++
			w.Header().Set("Content-Type", "application/json")
			if createStreamCalls == 1 {
				_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
				return
			}
			_, _ = io.WriteString(w, `{"stream_id":"stream-2"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks = append(chunks, body)
			switch r.URL.Path {
			case "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
			case "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/0":
				w.WriteHeader(http.StatusNoContent)
			case "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/1":
				w.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(w, r)
			}
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-2/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	res, err := client.Append(context.Background(), "space", "tape", newFixedChunkReader("headtail", 4), AppendOptions{
		Compression: CompressionZstd,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if got, want := res.LogicalBytes, int64(len("headtail")); got != want {
		t.Fatalf("logical bytes = %d, want %d", got, want)
	}
	if got, want := createTapeFormats, []string{"framed-zstd-v1", "framed-zstd-v1"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("create tape payload formats = %v, want %v", got, want)
	}
	if got, want := closeCalls, 1; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(chunks))
	}
	if got := string(chunks[0]); got != "head" {
		t.Fatalf("delete-conflict chunk = %q, want %q", got, "head")
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(chunks[1], decoder, &decoded); err != nil {
		t.Fatalf("decode recreated zstd first chunk: %v", err)
	}
	if got, want := decoded.String(), "head"; got != want {
		t.Fatalf("decoded recreated first chunk = %q, want %q", got, want)
	}
	decoded.Reset()
	if err := decodeFramedZstdSegment(chunks[2], decoder, &decoded); err != nil {
		t.Fatalf("decode recreated zstd chunk: %v", err)
	}
	if got, want := decoded.String(), "tail"; got != want {
		t.Fatalf("decoded recreated chunk = %q, want %q", got, want)
	}
}

func TestClientAppendDeleteRecoverySwitchesToRequestedIdentityFormatAfterZstdFallback(t *testing.T) {
	var (
		createTapeFormats []string
		createStreamCalls int
		closeCalls        int
		chunks            [][]byte
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			var body struct {
				PayloadFormat string `json:"payload_format"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create tape body: %v", err)
			}
			createTapeFormats = append(createTapeFormats, body.PayloadFormat)
			if len(createTapeFormats) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"payload_format conflict","existing_payload_format":"framed-zstd-v1"}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			createStreamCalls++
			w.Header().Set("Content-Type", "application/json")
			if createStreamCalls == 1 {
				_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
				return
			}
			_, _ = io.WriteString(w, `{"stream_id":"stream-2"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks = append(chunks, body)
			switch r.URL.Path {
			case "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
			case "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/0":
				w.WriteHeader(http.StatusNoContent)
			case "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/1":
				w.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(w, r)
			}
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-2/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	payload := append(bytes.Repeat([]byte("a"), defaultFramedZstdRawWindowBytes), []byte("tail")...)

	res, err := client.Append(context.Background(), "space", "tape", bytes.NewReader(payload), AppendOptions{})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if got, want := res.LogicalBytes, int64(len(payload)); got != want {
		t.Fatalf("logical bytes = %d, want %d", got, want)
	}
	if got, want := createTapeFormats, []string{"identity", "identity"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("create tape payload formats = %v, want %v", got, want)
	}
	if got, want := closeCalls, 1; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(chunks))
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var first bytes.Buffer
	if err := decodeFramedZstdSegment(chunks[0], decoder, &first); err != nil {
		t.Fatalf("decode delete-conflict zstd chunk: %v", err)
	}
	if got, want := first.String(), strings.Repeat("a", defaultFramedZstdRawWindowBytes); got != want {
		t.Fatalf("decoded delete-conflict chunk = %q, want %q", got, want)
	}

	if got := string(chunks[1]); got != strings.Repeat("a", defaultFramedZstdRawWindowBytes) {
		t.Fatalf("recreated identity first chunk = %q, want %q", got, strings.Repeat("a", defaultFramedZstdRawWindowBytes))
	}
	if got := string(chunks[2]); got != "tail" {
		t.Fatalf("recreated identity second chunk = %q, want %q", got, "tail")
	}
}

func TestClientAppendDeleteRecoveryFallsBackToConcurrentRecreatedFormat(t *testing.T) {
	var (
		createTapeFormats []string
		createStreamCalls int
		closeCalls        int
		chunks            [][]byte
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			var body struct {
				PayloadFormat string `json:"payload_format"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create tape body: %v", err)
			}
			createTapeFormats = append(createTapeFormats, body.PayloadFormat)
			if len(createTapeFormats) == 2 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"payload_format conflict","existing_payload_format":"identity"}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			createStreamCalls++
			w.Header().Set("Content-Type", "application/json")
			if createStreamCalls == 1 {
				_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
				return
			}
			_, _ = io.WriteString(w, `{"stream_id":"stream-2"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks = append(chunks, body)
			switch r.URL.Path {
			case "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
			case "/v1/spaces/space/tapes/tape/streams/stream-2/chunks/0":
				w.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(w, r)
			}
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-2/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	payload := "tail"
	res, err := client.Append(context.Background(), "space", "tape", strings.NewReader(payload), AppendOptions{
		Compression: CompressionZstd,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if got, want := res.LogicalBytes, int64(len(payload)); got != want {
		t.Fatalf("logical bytes = %d, want %d", got, want)
	}
	if got, want := createTapeFormats, []string{"framed-zstd-v1", "framed-zstd-v1"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("create tape payload formats = %v, want %v", got, want)
	}
	if got, want := createStreamCalls, 2; got != want {
		t.Fatalf("create stream calls = %d, want %d", got, want)
	}
	if got, want := closeCalls, 1; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunks))
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(chunks[0], decoder, &decoded); err != nil {
		t.Fatalf("decode delete-conflict zstd chunk: %v", err)
	}
	if got, want := decoded.String(), payload; got != want {
		t.Fatalf("decoded delete-conflict chunk = %q, want %q", got, want)
	}
	if got := string(chunks[1]); got != payload {
		t.Fatalf("recreated identity chunk = %q, want %q", got, payload)
	}
}

func TestClientAppendFallsBackToExistingIdentityTapeFormat(t *testing.T) {
	var (
		createTapeCalls int
		chunks          [][]byte
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			createTapeCalls++
			w.WriteHeader(http.StatusConflict)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"error":"payload_format conflict","existing_payload_format":"identity"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks = append(chunks, body)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	res, err := client.Append(context.Background(), "space", "tape", strings.NewReader("legacy"), AppendOptions{
		Compression: CompressionZstd,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if got, want := res.LogicalBytes, int64(len("legacy")); got != want {
		t.Fatalf("logical bytes = %d, want %d", got, want)
	}
	if createTapeCalls != 1 {
		t.Fatalf("create tape calls = %d, want 1", createTapeCalls)
	}
	if len(chunks) != 1 || string(chunks[0]) != "legacy" {
		t.Fatalf("unexpected identity fallback chunks: %q", chunks)
	}
}

func TestClientAppendFallsBackToExistingFramedZstdTapeFormat(t *testing.T) {
	var (
		createTapeCalls int
		chunks          [][]byte
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			createTapeCalls++
			w.WriteHeader(http.StatusConflict)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"error":"payload_format conflict","existing_payload_format":"framed-zstd-v1"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks = append(chunks, body)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	res, err := client.Append(context.Background(), "space", "tape", strings.NewReader("compressed"), AppendOptions{})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if got, want := res.LogicalBytes, int64(len("compressed")); got != want {
		t.Fatalf("logical bytes = %d, want %d", got, want)
	}
	if createTapeCalls != 1 {
		t.Fatalf("create tape calls = %d, want 1", createTapeCalls)
	}
	if len(chunks) != 1 {
		t.Fatalf("chunk count = %d, want 1", len(chunks))
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(chunks[0], decoder, &decoded); err != nil {
		t.Fatalf("decode uploaded frame: %v", err)
	}
	if got, want := decoded.String(), "compressed"; got != want {
		t.Fatalf("decoded payload = %q, want %q", got, want)
	}
}

func TestClientAppendRetriesCreateStreamWithGeneratedIdempotencyKey(t *testing.T) {
	var (
		createStreamCalls int
		firstKey          string
		chunkUploaded     bool
		closed            bool
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			var body struct {
				IdempotencyKey string `json:"idempotency_key"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create stream body: %v", err)
			}
			if len(body.IdempotencyKey) != idempotencyKeyLength {
				t.Fatalf("idempotency key length = %d, want %d", len(body.IdempotencyKey), idempotencyKeyLength)
			}

			createStreamCalls++
			if createStreamCalls == 1 {
				firstKey = body.IdempotencyKey
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "busy")
				return
			}
			if body.IdempotencyKey != firstKey {
				t.Fatalf("idempotency key changed from %q to %q", firstKey, body.IdempotencyKey)
			}

			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
			chunkUploaded = true
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			closed = true
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = time.Second

	res, err := client.Append(context.Background(), "space", "tape", strings.NewReader("payload"), AppendOptions{})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if createStreamCalls != 2 {
		t.Fatalf("create stream calls = %d, want 2", createStreamCalls)
	}
	if !chunkUploaded {
		t.Fatalf("expected chunk upload")
	}
	if !closed {
		t.Fatalf("expected close stream")
	}
	if res.LogicalBytes != int64(len("payload")) {
		t.Fatalf("logical bytes = %d, want %d", res.LogicalBytes, len("payload"))
	}
}

func TestClientAppendWithSecretSendsHeaderOnWriteRequests(t *testing.T) {
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(spaceSecretHeader); got != "correct-horse-battery" {
			t.Fatalf("%s on %s %s = %q", spaceSecretHeader, r.Method, r.URL.Path, got)
		}

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			seen["create_tape"] = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			var body struct {
				IdempotencyKey string `json:"idempotency_key"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create stream body: %v", err)
			}
			if len(body.IdempotencyKey) != idempotencyKeyLength {
				t.Fatalf("idempotency key length = %d, want %d", len(body.IdempotencyKey), idempotencyKeyLength)
			}
			seen["create_stream"] = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
			seen["put_chunk"] = true
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			seen["close_stream"] = true
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL, WithSecret("correct-horse-battery"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Append(context.Background(), "space", "tape", strings.NewReader("payload"), AppendOptions{})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	for _, name := range []string{"create_tape", "create_stream", "put_chunk", "close_stream"} {
		if !seen[name] {
			t.Fatalf("missing request: %s", name)
		}
	}
}

func TestClientAppendLeavesStreamOpenOnChunkFailureAndAllowsRetry(t *testing.T) {
	var (
		createTapeCalls   int
		createStreamCalls int
		closeCalls        int
		createStreamKeys  []string
		chunks            = map[int64]string{}
		failChunkOnce     = true
	)

	const idempotencyKey = "append_key_00001"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			createTapeCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			var body struct {
				IdempotencyKey string `json:"idempotency_key"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create stream body: %v", err)
			}
			if body.IdempotencyKey != idempotencyKey {
				t.Fatalf("idempotency key = %q, want %q", body.IdempotencyKey, idempotencyKey)
			}
			createStreamCalls++
			createStreamKeys = append(createStreamKeys, body.IdempotencyKey)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/"):
			seqStr := strings.TrimPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/")
			seq, err := strconv.ParseInt(seqStr, 10, 64)
			if err != nil {
				t.Fatalf("parse seq %q: %v", seqStr, err)
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			sum := sha256.Sum256(body)
			if got, want := r.Header.Get("X-Tape9-SHA256"), hex.EncodeToString(sum[:]); got != want {
				t.Fatalf("sha256 = %q, want %q", got, want)
			}

			if seq == 1 && failChunkOnce {
				failChunkOnce = false
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, "chunk rejected")
				return
			}

			if got, ok := chunks[seq]; ok {
				if got != string(body) {
					t.Fatalf("chunk %d = %q, want %q", seq, got, string(body))
				}
			} else {
				chunks[seq] = string(body)
			}

			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Append(context.Background(), "space", "tape", newFixedChunkReader("abcdef", 3), AppendOptions{
		IdempotencyKey: idempotencyKey,
	})
	if err == nil {
		t.Fatalf("expected append error")
	}
	if !strings.Contains(err.Error(), "chunk rejected") {
		t.Fatalf("unexpected append err: %v", err)
	}
	if closeCalls != 0 {
		t.Fatalf("expected no close call after failure, got %d", closeCalls)
	}

	res, err := client.Append(context.Background(), "space", "tape", newFixedChunkReader("abcdef", 3), AppendOptions{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatalf("append retry: %v", err)
	}
	if res.Chunks != 2 {
		t.Fatalf("unexpected chunk count: %d", res.Chunks)
	}
	if res.LogicalBytes != 6 {
		t.Fatalf("unexpected logical bytes: %d", res.LogicalBytes)
	}
	if createTapeCalls != 2 {
		t.Fatalf("expected two tape creation attempts, got %d", createTapeCalls)
	}
	if createStreamCalls != 2 {
		t.Fatalf("expected two stream creation attempts, got %d", createStreamCalls)
	}
	if len(createStreamKeys) != 2 || createStreamKeys[0] != idempotencyKey || createStreamKeys[1] != idempotencyKey {
		t.Fatalf("unexpected stream keys: %v", createStreamKeys)
	}
	if closeCalls != 1 {
		t.Fatalf("expected one close call after successful retry, got %d", closeCalls)
	}
	if got, want := chunks[0], "abc"; got != want {
		t.Fatalf("chunk 0 = %q, want %q", got, want)
	}
	if got, want := chunks[1], "def"; got != want {
		t.Fatalf("chunk 1 = %q, want %q", got, want)
	}
}

func TestClientAppendClosesGeneratedIdempotencyKeyOnReaderFailure(t *testing.T) {
	var (
		closeCalls int
		chunks     []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read chunk body: %v", err)
			}
			chunks = append(chunks, string(body))
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Append(context.Background(), "space", "tape", &readerErrorAfterData{
		payload: []byte("abc"),
		err:     errReaderBoom,
	}, AppendOptions{})
	if !errors.Is(err, errReaderBoom) {
		t.Fatalf("expected reader error, got %v", err)
	}
	if len(chunks) != 1 || chunks[0] != "abc" {
		t.Fatalf("unexpected uploaded chunks: %v", chunks)
	}
	if closeCalls != 1 {
		t.Fatalf("expected one best-effort close after reader failure, got %d", closeCalls)
	}
}

func TestClientAppendKeepsClosingAfterCallerContextExpires(t *testing.T) {
	transport := &closeAfterContextExpiryTransport{
		t:          t,
		closeDelay: 100 * time.Millisecond,
	}

	client, err := New("http://example.com", WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.requestTimeout = 200 * time.Millisecond
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	res, err := client.Append(ctx, "space", "tape", strings.NewReader("payload"), AppendOptions{})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("expected caller context to expire before append returned")
	}
	if res.TapeID != "tape" {
		t.Fatalf("tape id = %q, want tape", res.TapeID)
	}
	if res.LogicalBytes != int64(len("payload")) {
		t.Fatalf("logical bytes = %d, want %d", res.LogicalBytes, len("payload"))
	}
	if transport.closeCalls != 1 {
		t.Fatalf("expected one close call, got %d", transport.closeCalls)
	}
}

func TestClientAppendClosesRecoveredGeneratedStreamAfterAmbiguousCreateFailure(t *testing.T) {
	var (
		createStreamCalls int
		closeCalls        int
		streamKeys        []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams":
			var body struct {
				IdempotencyKey string `json:"idempotency_key"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create stream body: %v", err)
			}
			if len(body.IdempotencyKey) != idempotencyKeyLength {
				t.Fatalf("idempotency key length = %d, want %d", len(body.IdempotencyKey), idempotencyKeyLength)
			}

			createStreamCalls++
			streamKeys = append(streamKeys, body.IdempotencyKey)
			if createStreamCalls == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "busy")
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
			closeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = 5 * time.Millisecond

	_, err = client.Append(context.Background(), "space", "tape", strings.NewReader("payload"), AppendOptions{})
	if err == nil {
		t.Fatal("expected append error")
	}
	if createStreamCalls != 2 {
		t.Fatalf("create stream calls = %d, want 2", createStreamCalls)
	}
	if closeCalls != 1 {
		t.Fatalf("expected one close call after recovery, got %d", closeCalls)
	}
	if len(streamKeys) != 2 || streamKeys[0] == "" || streamKeys[1] != streamKeys[0] {
		t.Fatalf("unexpected stream keys: %v", streamKeys)
	}
}

func TestClientAppendRejectsInvalidIDBeforeNetwork(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.NotFound(w, r)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Append(context.Background(), "ab", "tape", strings.NewReader("payload"), AppendOptions{})
	if err == nil {
		t.Fatal("expected append error")
	}
	if !strings.Contains(err.Error(), "invalid space_id") {
		t.Fatalf("unexpected append err: %v", err)
	}
	if calls != 0 {
		t.Fatalf("unexpected network calls: %d", calls)
	}
}

func TestPutChunkWithRetryReturnsRetryableErrorAfterBudgetExpires(t *testing.T) {
	retryErr := &RetryableError{
		StatusCode: http.StatusServiceUnavailable,
		RetryAfter: time.Millisecond,
		Body:       "busy",
	}
	cfg := retryConfig{
		base:         time.Millisecond,
		max:          time.Millisecond,
		maxRetryTime: 5 * time.Millisecond,
	}
	putter := fakeChunkPutter{
		errFn: func(int) error {
			return retryErr
		},
	}

	_, err := putChunkWithRetry(context.Background(), cfg, &putter, defaultRequestTimeout, "space", "tape", "stream", 0, strings.Repeat("a", 64), []byte("payload"))
	if err == nil {
		t.Fatalf("expected retryable error")
	}

	var got *RetryableError
	if !errors.As(err, &got) {
		t.Fatalf("expected retryable error, got %T: %v", err, err)
	}
	if got.StatusCode != retryErr.StatusCode || got.Body != retryErr.Body {
		t.Fatalf("unexpected retryable error: %+v", got)
	}
	if putter.calls < 2 {
		t.Fatalf("expected retries, got %d attempts", putter.calls)
	}
}

func TestPutChunkWithRetryStopsOnContextCancellation(t *testing.T) {
	cfg := retryConfig{
		base:         time.Second,
		max:          time.Second,
		maxRetryTime: time.Minute,
	}
	putter := fakeChunkPutter{
		errFn: func(int) error {
			return &RetryableError{
				StatusCode: http.StatusTooManyRequests,
				RetryAfter: time.Second,
				Body:       "busy",
			}
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := putChunkWithRetry(ctx, cfg, &putter, defaultRequestTimeout, "space", "tape", "stream", 0, strings.Repeat("a", 64), []byte("payload"))
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("expected cancellation during backoff, took %s", elapsed)
	}
}

type fakeChunkPutter struct {
	calls int
	errFn func(call int) error
	putFn func(ctx context.Context, spaceID string, tapeID string, streamID string, seq int64, sha256Hex string, payload []byte) error
}

func (f *fakeChunkPutter) putChunk(ctx context.Context, spaceID string, tapeID string, streamID string, seq int64, sha256Hex string, payload []byte) error {
	f.calls++
	if f.putFn != nil {
		return f.putFn(ctx, spaceID, tapeID, streamID, seq, sha256Hex, payload)
	}
	if f.errFn == nil {
		return nil
	}
	return f.errFn(f.calls)
}

var errReaderBoom = errors.New("reader boom")

type readerErrorAfterData struct {
	payload []byte
	err     error
	done    bool
}

func (r *readerErrorAfterData) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	n := copy(p, r.payload)
	r.done = true
	return n, r.err
}

type fixedChunkReader struct {
	payload   string
	chunkSize int
	offset    int
}

func newFixedChunkReader(payload string, chunkSize int) *fixedChunkReader {
	return &fixedChunkReader{
		payload:   payload,
		chunkSize: chunkSize,
	}
}

func (r *fixedChunkReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.payload) {
		return 0, io.EOF
	}

	n := r.chunkSize
	remaining := len(r.payload) - r.offset
	if n > remaining {
		n = remaining
	}
	if n > len(p) {
		n = len(p)
	}

	copy(p, r.payload[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}

type delayedTailReader struct {
	tailReady <-chan struct{}
	sentHead  bool
	sentTail  bool
}

type closeableDelayedTailReader struct {
	closed    chan struct{}
	closeOnce sync.Once
	sentHead  bool
}

func newCloseableDelayedTailReader() *closeableDelayedTailReader {
	return &closeableDelayedTailReader{closed: make(chan struct{})}
}

func (r *closeableDelayedTailReader) Read(p []byte) (int, error) {
	if !r.sentHead {
		r.sentHead = true
		return copy(p, "head\n"), nil
	}
	<-r.closed
	return 0, io.EOF
}

func (r *closeableDelayedTailReader) Close() error {
	r.closeOnce.Do(func() {
		close(r.closed)
	})
	return nil
}

func (r *delayedTailReader) Read(p []byte) (int, error) {
	if !r.sentHead {
		r.sentHead = true
		return copy(p, "head\n"), nil
	}
	if !r.sentTail {
		<-r.tailReady
		r.sentTail = true
		return copy(p, "tail\n"), io.EOF
	}
	return 0, io.EOF
}

type headTailThenReleaseReader struct {
	tailReady <-chan struct{}
	release   <-chan struct{}
	step      int
}

func (r *headTailThenReleaseReader) Read(p []byte) (int, error) {
	switch r.step {
	case 0:
		r.step++
		return copy(p, "head\n"), nil
	case 1:
		<-r.tailReady
		r.step++
		return copy(p, "tail\n"), nil
	case 2:
		<-r.release
		r.step++
		return 0, io.EOF
	default:
		return 0, io.EOF
	}
}

type readSignalReader struct {
	readCalled chan<- struct{}
}

func (r *readSignalReader) Read([]byte) (int, error) {
	r.readCalled <- struct{}{}
	return 0, io.EOF
}

type zeroThenDataReader struct {
	step int
}

func (r *zeroThenDataReader) Read(p []byte) (int, error) {
	switch r.step {
	case 0:
		r.step++
		return 0, nil
	case 1:
		r.step++
		return copy(p, "payload"), io.EOF
	default:
		return 0, io.EOF
	}
}

type alwaysEmptyReader struct {
	calls atomic.Int64
}

func (r *alwaysEmptyReader) Read([]byte) (int, error) {
	r.calls.Add(1)
	return 0, nil
}

func setFramedZstdUploadFlushIntervalForTest(t *testing.T, client *Client, interval time.Duration) {
	t.Helper()
	previous := client.framedZstdUploadFlushInterval
	client.framedZstdUploadFlushInterval = interval
	t.Cleanup(func() {
		client.framedZstdUploadFlushInterval = previous
	})
}

type closeAfterContextExpiryTransport struct {
	t          *testing.T
	closeDelay time.Duration
	closeCalls int
}

func (tr *closeAfterContextExpiryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch {
	case req.Method == http.MethodPost && req.URL.Path == "/v1/spaces/space/tapes":
		return jsonResponse(http.StatusOK, `{"tape_id":"tape"}`), nil
	case req.Method == http.MethodPost && req.URL.Path == "/v1/spaces/space/tapes/tape/streams":
		return jsonResponse(http.StatusOK, `{"stream_id":"stream-1"}`), nil
	case req.Method == http.MethodPut && req.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/0":
		return noContentResponse(), nil
	case req.Method == http.MethodPost && req.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
		tr.closeCalls++
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(tr.closeDelay):
			return noContentResponse(), nil
		}
	}

	tr.t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
	return nil, errors.New("unexpected request")
}
