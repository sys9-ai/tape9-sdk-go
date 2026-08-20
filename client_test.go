package tape9

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const errorBodyLimitBytes = 4 * 1024

func TestNewRejectsQueryOrFragment(t *testing.T) {
	_, err := New("http://example.com?x=y")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "query or fragment") {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = New("http://example.com/#frag")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "query or fragment") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewTrimsTrailingSlash(t *testing.T) {
	c, err := New("http://example.com/api/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := c.baseURL.String(), "http://example.com/api"; got != want {
		t.Fatalf("baseURL.String() = %q, want %q", got, want)
	}
}

func TestNewRejectsInvalidEndpoint(t *testing.T) {
	_, err := New("example.com")
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestCreateTapeServiceUnavailableReturnsRetryableError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes")
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPost)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "busy")
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	client.maxRetryTime = time.Nanosecond

	_, err = client.createTape(context.Background(), "space", "tape", createTapeOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}

	var retryable *RetryableError
	if !errors.As(err, &retryable) {
		t.Fatalf("error = %T, want *RetryableError: %v", err, err)
	}
	if got, want := retryable.StatusCode, http.StatusServiceUnavailable; got != want {
		t.Fatalf("StatusCode = %d, want %d", got, want)
	}
	if got, want := retryable.RetryAfter, 3*time.Second; got != want {
		t.Fatalf("RetryAfter = %s, want %s", got, want)
	}
	if got, want := retryable.Body, "busy"; got != want {
		t.Fatalf("Body = %q, want %q", got, want)
	}
}

func TestCreateTapeRetriesRetryableStatusWithFixedTapeID(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes")
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPost)
		}
		var body struct {
			TapeID string `json:"tape_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.TapeID != "tape" {
			t.Fatalf("tape_id = %q, want tape", body.TapeID)
		}

		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "busy")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = 2 * time.Second

	tapeID, err := client.createTape(context.Background(), "space", "tape", createTapeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tapeID != "tape" {
		t.Fatalf("tapeID = %q, want tape", tapeID)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestCreateTapeDoesNotRetryServerGeneratedTapeID(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes")
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPost)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "busy")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"tape_id":"server-generated"}`)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = client.createTape(context.Background(), "space", "", createTapeOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
	var retryable *RetryableError
	if !errors.As(err, &retryable) {
		t.Fatalf("error = %T, want *RetryableError: %v", err, err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestCreateTapeEscapesSlashBearingSpaceID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawPath != "/v1/spaces/acme%2Fdev/tapes" {
			t.Fatalf("raw path = %q, want %q", r.URL.RawPath, "/v1/spaces/acme%2Fdev/tapes")
		}
		if r.URL.Path != "/v1/spaces/acme/dev/tapes" {
			t.Fatalf("path = %q, want %q", r.URL.Path, "/v1/spaces/acme/dev/tapes")
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want %q", r.Method, http.MethodPost)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tapeID, err := client.createTape(context.Background(), "acme/dev", "tape", createTapeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := tapeID, "tape"; got != want {
		t.Fatalf("tapeID = %q, want %q", got, want)
	}
}

func TestCreateTapeIncludesRetainModeInJSONBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes" {
			t.Fatalf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes")
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want %q", r.Method, http.MethodPost)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("content type = %q, want application/json", got)
		}

		var body struct {
			TapeID        string `json:"tape_id"`
			RetainMode    string `json:"retain_mode"`
			PayloadFormat string `json:"payload_format"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got, want := body.TapeID, "tape"; got != want {
			t.Fatalf("tape_id = %q, want %q", got, want)
		}
		if got, want := body.RetainMode, "tail"; got != want {
			t.Fatalf("retain_mode = %q, want %q", got, want)
		}
		if got, want := body.PayloadFormat, "identity"; got != want {
			t.Fatalf("payload_format = %q, want %q", got, want)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tapeID, err := client.createTape(context.Background(), "space", "tape", createTapeOptions{
		RetainMode: RetainModeTail,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := tapeID, "tape"; got != want {
		t.Fatalf("tapeID = %q, want %q", got, want)
	}
}

func TestCreateTapeWithSecretAddsSpaceHeaderAndOmitsSecretFromJSONBody(t *testing.T) {
	var gotSpaceSecretHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSpaceSecretHeader = r.Header.Get(spaceSecretHeader)
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("content type = %q, want application/json", got)
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got, want := body["tape_id"], "tape"; got != want {
			t.Fatalf("tape_id = %q, want %q", got, want)
		}
		if _, ok := body["secret"]; ok {
			t.Fatalf("unexpected secret in create_tape body: %v", body["secret"])
		}

		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"tape_id":"tape"}`)
	}))
	defer server.Close()

	client, err := New(server.URL, WithSecret("correct-horse-battery"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = client.createTape(context.Background(), "space", "tape", createTapeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := gotSpaceSecretHeader, "correct-horse-battery"; got != want {
		t.Fatalf("%s = %q, want %q", spaceSecretHeader, got, want)
	}
}

func TestCreateTapeRejectsInvalidRetainModeBeforeNetwork(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.NotFound(w, r)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = client.createTape(context.Background(), "space", "tape", createTapeOptions{
		RetainMode: RetainMode("side"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got != `invalid retain mode: "side"` {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 0 {
		t.Fatalf("unexpected network calls: %d", calls)
	}
}

func TestCreateTapeRejectsInvalidPayloadFormatBeforeNetwork(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.NotFound(w, r)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = client.createTape(context.Background(), "space", "tape", createTapeOptions{
		PayloadFormat: payloadFormat("opaque"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got != `invalid payload format: "opaque"` {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 0 {
		t.Fatalf("unexpected network calls: %d", calls)
	}
}

func TestCreateStream_ServiceUnavailableReturnsRetryableError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes/tape/streams" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes/tape/streams")
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPost)
		}
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "busy")
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = client.createStream(context.Background(), "space", "tape")
	if err == nil {
		t.Fatalf("expected error")
	}

	var retryable *RetryableError
	if !errors.As(err, &retryable) {
		t.Fatalf("error = %T, want *RetryableError: %v", err, err)
	}
	if got, want := retryable.StatusCode, http.StatusServiceUnavailable; got != want {
		t.Fatalf("StatusCode = %d, want %d", got, want)
	}
	if got, want := retryable.RetryAfter, 3*time.Second; got != want {
		t.Fatalf("RetryAfter = %s, want %s", got, want)
	}
	if got, want := retryable.Body, "busy"; got != want {
		t.Fatalf("Body = %q, want %q", got, want)
	}
}

func TestCreateStreamRetriesRetryableStatusWithIdempotencyKey(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes/tape/streams" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes/tape/streams")
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPost)
		}
		if got, want := r.Header.Get("Content-Type"), "application/json"; got != want {
			t.Fatalf("content type = %q, want %q", got, want)
		}

		var body struct {
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got, want := body.IdempotencyKey, "retry-safe-key-1"; got != want {
			t.Fatalf("idempotency_key = %q, want %q", got, want)
		}

		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "busy")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"stream_id":"stream-1"}`)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = time.Second

	streamID, err := client.createStream(context.Background(), "space", "tape", createStreamOptions{
		IdempotencyKey: "retry-safe-key-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if streamID != "stream-1" {
		t.Fatalf("streamID = %q, want stream-1", streamID)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestDeleteEscapesSlashBearingSpaceIDAndRetries(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, http.MethodDelete; got != want {
			t.Fatalf("method = %q, want %q", got, want)
		}
		if got, want := r.URL.RawPath, "/v1/spaces/acme%2Fdev/tapes/tape"; got != want {
			t.Fatalf("raw path = %q, want %q", got, want)
		}
		if got, want := r.URL.Path, "/v1/spaces/acme/dev/tapes/tape"; got != want {
			t.Fatalf("path = %q, want %q", got, want)
		}

		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "busy")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = time.Second

	err = client.Delete(context.Background(), "acme/dev", "tape")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := calls, 2; got != want {
		t.Fatalf("calls = %d, want %d", got, want)
	}
}

func TestCloseStream_ServiceUnavailableReturnsRetryableError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes/tape/streams/stream/close" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream/close")
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPost)
		}
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "busy")
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	client.maxRetryTime = time.Nanosecond

	err = client.closeStream(context.Background(), "space", "tape", "stream")
	if err == nil {
		t.Fatalf("expected error")
	}

	var retryable *RetryableError
	if !errors.As(err, &retryable) {
		t.Fatalf("error = %T, want *RetryableError: %v", err, err)
	}
	if got, want := retryable.StatusCode, http.StatusServiceUnavailable; got != want {
		t.Fatalf("StatusCode = %d, want %d", got, want)
	}
	if got, want := retryable.RetryAfter, 3*time.Second; got != want {
		t.Fatalf("RetryAfter = %s, want %s", got, want)
	}
	if got, want := retryable.Body, "busy"; got != want {
		t.Fatalf("Body = %q, want %q", got, want)
	}
}

func TestCloseStreamRetriesRetryableStatusThenSucceeds(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes/tape/streams/stream/close" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream/close")
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPost)
		}

		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "busy")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = time.Second

	err = client.closeStream(context.Background(), "space", "tape", "stream")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestPutChunk_ServiceUnavailableTruncatesRetryableErrorBody(t *testing.T) {
	longBody := strings.Repeat("x", errorBodyLimitBytes+128)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/spaces/space/tapes/tape/streams/stream/chunks/0" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream/chunks/0")
		}
		if r.Method != http.MethodPut {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPut)
		}
		_, _ = io.Copy(io.Discard, r.Body)

		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, longBody)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = client.putChunk(context.Background(), "space", "tape", "stream", 0, "deadbeef", []byte("hello"))
	if err == nil {
		t.Fatalf("expected error")
	}

	var retryable *RetryableError
	if !errors.As(err, &retryable) {
		t.Fatalf("error = %T, want *RetryableError: %v", err, err)
	}
	if got, want := retryable.StatusCode, http.StatusServiceUnavailable; got != want {
		t.Fatalf("StatusCode = %d, want %d", got, want)
	}
	if got, want := retryable.RetryAfter, 3*time.Second; got != want {
		t.Fatalf("RetryAfter = %s, want %s", got, want)
	}
	if got, want := len(retryable.Body), errorBodyLimitBytes; got != want {
		t.Fatalf("len(Body) = %d, want %d", got, want)
	}
	if got, want := retryable.Body, longBody[:errorBodyLimitBytes]; got != want {
		t.Fatalf("Body mismatch")
	}
}
