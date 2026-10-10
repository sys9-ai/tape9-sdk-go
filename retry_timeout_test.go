package tape9

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestClientAppendRetriesTimedOutStreamRequests(t *testing.T) {
	transport := &timeoutRetryTransport{
		t:                     t,
		streamTimeoutAttempts: 2,
		closeTimeoutAttempts:  2,
	}

	client, err := New("http://example.com", WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.requestTimeout = 5 * time.Millisecond
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = 50 * time.Millisecond

	res, err := client.Append(context.Background(), "space", "tape", strings.NewReader("payload"), AppendOptions{})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if res.TapeID != "tape" {
		t.Fatalf("unexpected tape id: %q", res.TapeID)
	}
	if transport.createTapeCalls != 1 {
		t.Fatalf("create tape calls = %d, want 1", transport.createTapeCalls)
	}
	if transport.createStreamCalls != 3 {
		t.Fatalf("create stream calls = %d, want 3", transport.createStreamCalls)
	}
	if transport.closeCalls != 3 {
		t.Fatalf("close calls = %d, want 3", transport.closeCalls)
	}
	if transport.chunkCalls != 1 {
		t.Fatalf("chunk calls = %d, want 1", transport.chunkCalls)
	}
}

func TestClientCaptureRetriesTimedOutCreateTape(t *testing.T) {
	transport := &timeoutRetryTransport{
		t:                   t,
		tapeTimeoutAttempts: 2,
	}

	client, err := New("http://example.com", WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.requestTimeout = 5 * time.Millisecond
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = 50 * time.Millisecond

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_only_exit_0"}

	res, err := client.Capture(context.Background(), "space", "tape", procArgs, io.Discard, CaptureOptions{
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if res.AppendErr != nil {
		t.Fatalf("unexpected append err: %v", res.AppendErr)
	}
	if res.ExitCode != 0 {
		t.Fatalf("unexpected exit code: %d", res.ExitCode)
	}
	if transport.createTapeCalls != 3 {
		t.Fatalf("create tape calls = %d, want 3", transport.createTapeCalls)
	}
	if transport.createStreamCalls != 1 {
		t.Fatalf("create stream calls = %d, want 1", transport.createStreamCalls)
	}
	if transport.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", transport.closeCalls)
	}
}

func TestClientPullImmediateResumeRetriesTimedOutContentRequest(t *testing.T) {
	token, err := ParseResumeToken("resume-1")
	if err != nil {
		t.Fatalf("parse resume token: %v", err)
	}

	transport := &timeoutPullResumeTransport{
		t:               t,
		timeoutAttempts: 2,
	}

	client, err := New("http://example.com", WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.requestTimeout = 5 * time.Millisecond
	client.retryBase = time.Millisecond
	client.retryMax = time.Millisecond
	client.maxRetryTime = 50 * time.Millisecond

	res, err := client.Pull(context.Background(), "space", "tape", io.Discard, PullOptions{ResumeToken: token})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if !res.NoChange {
		t.Fatal("expected no-change result")
	}
	if transport.contentCalls != 3 {
		t.Fatalf("content calls = %d, want 3", transport.contentCalls)
	}
}

type timeoutRetryTransport struct {
	t *testing.T

	tapeTimeoutAttempts   int
	createTapeCalls       int
	streamTimeoutAttempts int
	createStreamCalls     int
	closeTimeoutAttempts  int
	closeCalls            int
	chunkCalls            int
}

type timeoutPullResumeTransport struct {
	t *testing.T

	timeoutAttempts int
	contentCalls    int
}

func (tr *timeoutRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch {
	case req.Method == http.MethodPost && req.URL.Path == "/v1/spaces/space/tapes":
		tr.createTapeCalls++
		if tr.createTapeCalls <= tr.tapeTimeoutAttempts {
			return nil, waitForRequestTimeout(req)
		}
		return jsonResponse(http.StatusOK, `{"tape_id":"tape"}`), nil

	case req.Method == http.MethodPost && req.URL.Path == "/v1/spaces/space/tapes/tape/streams":
		tr.createStreamCalls++
		if tr.createStreamCalls <= tr.streamTimeoutAttempts {
			return nil, waitForRequestTimeout(req)
		}
		return jsonResponse(http.StatusOK, `{"stream_id":"stream-1"}`), nil

	case req.Method == http.MethodPut && strings.HasPrefix(req.URL.Path, "/v1/spaces/space/tapes/tape/streams/stream-1/chunks/"):
		tr.chunkCalls++
		return noContentResponse(), nil

	case req.Method == http.MethodPost && req.URL.Path == "/v1/spaces/space/tapes/tape/streams/stream-1/close":
		tr.closeCalls++
		if tr.closeCalls <= tr.closeTimeoutAttempts {
			return nil, waitForRequestTimeout(req)
		}
		return noContentResponse(), nil
	}

	tr.t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
	return nil, errors.New("unexpected request")
}

func (tr *timeoutPullResumeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.URL.Path != "/v1/spaces/space/tapes/tape/content" {
		tr.t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
	}
	if got, want := req.URL.Query().Get("resume_token"), "resume-1"; got != want {
		tr.t.Fatalf("resume_token = %q, want %q", got, want)
	}
	if got, want := req.URL.Query().Get("wait"), "0s"; got != want {
		tr.t.Fatalf("wait = %q, want %q", got, want)
	}

	tr.contentCalls++
	if tr.contentCalls <= tr.timeoutAttempts {
		return nil, waitForRequestTimeout(req)
	}
	return noContentResponse(), nil
}

func waitForRequestTimeout(req *http.Request) error {
	select {
	case <-req.Context().Done():
		return req.Context().Err()
	case <-time.After(50 * time.Millisecond):
		return errors.New("request did not time out")
	}
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    &http.Request{},
	}
}

func noContentResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    &http.Request{},
	}
}

func TestClientAppendCompressedPreservesUnexpectedSourceEOF(t *testing.T) {
	for _, closeable := range []bool{false, true} {
		t.Run(fmt.Sprint(closeable), func(t *testing.T) {
			transport := &timeoutRetryTransport{t: t}
			client, err := New("http://example.com", WithHTTPClient(&http.Client{Transport: transport}))
			if err != nil {
				t.Fatal(err)
			}
			var source io.Reader = &readerErrorAfterData{payload: []byte("partial"), err: io.ErrUnexpectedEOF}
			if closeable {
				source = io.NopCloser(source)
			}
			_, err = client.Append(context.Background(), "space", "tape", source, AppendOptions{Compression: CompressionZstd})
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("append error = %v, want unexpected EOF", err)
			}
			if transport.chunkCalls != 1 {
				t.Fatalf("chunk calls = %d, want 1", transport.chunkCalls)
			}
			if transport.closeCalls != 1 {
				t.Fatalf("close calls = %d, want 1", transport.closeCalls)
			}
		})
	}
}

func TestRetryUnexpectedEOFAndBoundSubsequentAttempt(t *testing.T) {
	attempts := 0
	started := time.Now()
	err := doWithRetry(context.Background(), retryConfig{base: time.Millisecond, max: time.Millisecond, maxRetryTime: 30 * time.Millisecond}, func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			return io.ErrUnexpectedEOF
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("retry attempt has no budget deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if time.Since(started) > time.Second {
		t.Fatal("retry exceeded budget")
	}
}
