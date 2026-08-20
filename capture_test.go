package tape9

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestClientCaptureUploadsMergedOutputAndExitCode(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "both_and_exit_7"}

	var forward bytes.Buffer
	res, err := client.Capture(context.Background(), "space", "tape", procArgs, &forward, CaptureOptions{
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if res.ExitCode != 7 {
		t.Fatalf("unexpected exit code: %d", res.ExitCode)
	}
	if res.AppendErr != nil {
		t.Fatalf("unexpected append err: %v", res.AppendErr)
	}
	if res.TapeID != state.tapeID {
		t.Fatalf("unexpected tape id: %q", res.TapeID)
	}
	if len(state.lastCreateStreamIdempotencyKey) != idempotencyKeyLength {
		t.Fatalf("idempotency key length = %d, want %d", len(state.lastCreateStreamIdempotencyKey), idempotencyKeyLength)
	}

	tapeBytes := state.concatPayload()
	if !strings.Contains(string(tapeBytes), "hello stdout\n") {
		t.Fatalf("missing stdout payload in tape: %q", string(tapeBytes))
	}
	if !strings.Contains(string(tapeBytes), "hello stderr\n") {
		t.Fatalf("missing stderr payload in tape: %q", string(tapeBytes))
	}
	if strings.Contains(string(tapeBytes), "proc_start") || strings.Contains(string(tapeBytes), "proc_exit") {
		t.Fatalf("unexpected meta lines in tape: %q", string(tapeBytes))
	}

	forwardStr := forward.String()
	if !strings.Contains(forwardStr, "hello stdout\n") {
		t.Fatalf("missing stdout payload in forward: %q", forwardStr)
	}
	if !strings.Contains(forwardStr, "hello stderr\n") {
		t.Fatalf("missing stderr payload in forward: %q", forwardStr)
	}
	if strings.Contains(forwardStr, "proc_start") || strings.Contains(forwardStr, "proc_exit") {
		t.Fatalf("unexpected meta lines in forward: %q", forwardStr)
	}
}

func TestClientCaptureReadsOriginalSpoolAfterPathReplacement(t *testing.T) {
	state := newCaptureTestServerState()
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	baseHandler := captureTestHandler(t, state)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/spaces/space/tapes" {
			close(createStarted)
			<-releaseCreate
		}
		baseHandler(w, r)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	spoolDir := t.TempDir()
	forward := &readySignalWriter{ready: make(chan struct{})}
	type captureCall struct {
		res CaptureResult
		err error
	}
	done := make(chan captureCall, 1)
	go func() {
		res, err := client.Capture(
			context.Background(),
			"space",
			"tape",
			[]string{os.Args[0], "-test.run=TestCaptureHelper", "--", "ready_then_exit"},
			forward,
			CaptureOptions{PollInterval: time.Millisecond, SpoolDir: spoolDir},
		)
		done <- captureCall{res: res, err: err}
	}()

	<-createStarted
	<-forward.ready
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("spool file count = %d, want 1", len(entries))
	}
	spoolPath := filepath.Join(spoolDir, entries[0].Name())
	if err := os.Remove(spoolPath); err != nil {
		t.Fatalf("remove spool path: %v", err)
	}
	if err := os.WriteFile(spoolPath, []byte("replacement\n"), 0o600); err != nil {
		t.Fatalf("replace spool path: %v", err)
	}
	close(releaseCreate)

	got := <-done
	if got.err != nil {
		t.Fatalf("capture: %v", got.err)
	}
	if got.res.AppendErr != nil {
		t.Fatalf("append: %v", got.res.AppendErr)
	}
	if payload := string(state.concatPayload()); payload != "ready\n" {
		t.Fatalf("uploaded payload = %q, want original spool bytes", payload)
	}
}

func TestTailReaderStopsPollingWhenWritingFinishes(t *testing.T) {
	spool, err := os.CreateTemp(t.TempDir(), "spool-*")
	if err != nil {
		t.Fatalf("create spool: %v", err)
	}
	defer spool.Close()

	doneWriting := make(chan struct{})
	reader := &tailReader{f: spool, done: doneWriting, pollInterval: time.Second}
	readDone := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		readDone <- err
	}()

	time.Sleep(20 * time.Millisecond)
	close(doneWriting)
	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("read error = %v, want EOF", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("tail reader remained blocked in poll interval")
	}
}

func TestClientCaptureForwardsRetainModeToCreateTape(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_only_exit_0"}

	_, err = client.Capture(context.Background(), "space", "tape", procArgs, io.Discard, CaptureOptions{
		RetainMode:   RetainModeTail,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if got, want := state.lastCreateTapeRetainMode, "tail"; got != want {
		t.Fatalf("retain mode = %q, want %q", got, want)
	}
}

func TestClientCaptureWithCompressionUploadsWholeFrames(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_only_exit_0"}

	res, err := client.Capture(context.Background(), "space", "tape", procArgs, io.Discard, CaptureOptions{
		Compression:  CompressionZstd,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if got, want := res.ExitCode, 0; got != want {
		t.Fatalf("exit code = %d, want %d", got, want)
	}
	if got, want := state.lastCreateTapePayloadFormat, string(payloadFormatFramedZstdV1); got != want {
		t.Fatalf("payload_format = %q, want %q", got, want)
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(state.concatPayload(), decoder, &decoded); err != nil {
		t.Fatalf("decode uploaded frame: %v", err)
	}
	if got, want := decoded.String(), "ok\n"; got != want {
		t.Fatalf("decoded payload = %q, want %q", got, want)
	}
}

func TestClientCaptureWithCompressionUploadsIncrementallyBeforeProcessExit(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	setFramedZstdUploadFlushIntervalForTest(t, client, 20*time.Millisecond)

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_head_then_slow_tail"}

	type captureCall struct {
		res CaptureResult
		err error
	}
	done := make(chan captureCall, 1)
	go func() {
		res, err := client.Capture(context.Background(), "space", "tape", procArgs, io.Discard, CaptureOptions{
			Compression:  CompressionZstd,
			PollInterval: 10 * time.Millisecond,
		})
		done <- captureCall{res: res, err: err}
	}()

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		state.mu.Lock()
		firstChunk, ok := state.chunks[0]
		chunkCount := len(state.chunks)
		state.mu.Unlock()
		if ok {
			decoder, err := newFramedZstdDecoder()
			if err != nil {
				t.Fatalf("new decoder: %v", err)
			}
			var decoded bytes.Buffer
			err = decodeFramedZstdSegment(firstChunk, decoder, &decoded)
			decoder.Close()
			if err != nil {
				t.Fatalf("decode first chunk: %v", err)
			}
			if got, want := decoded.String(), "head\n"; got != want {
				t.Fatalf("first decoded chunk = %q, want %q", got, want)
			}
			if chunkCount != 1 {
				t.Fatalf("chunk count before process exit = %d, want 1", chunkCount)
			}
			goto waitDone
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected first compressed chunk before process exit")

waitDone:
	got := <-done
	if got.err != nil {
		t.Fatalf("capture: %v", got.err)
	}
	if got.res.AppendErr != nil {
		t.Fatalf("unexpected append err: %v", got.res.AppendErr)
	}
	if got.res.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", got.res.ExitCode)
	}

	state.mu.Lock()
	secondChunk, ok := state.chunks[1]
	state.mu.Unlock()
	if !ok {
		t.Fatalf("expected second compressed chunk after process exit")
	}

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	var decoded bytes.Buffer
	if err := decodeFramedZstdSegment(secondChunk, decoder, &decoded); err != nil {
		t.Fatalf("decode second chunk: %v", err)
	}
	if got, want := decoded.String(), "tail\n"; got != want {
		t.Fatalf("second decoded chunk = %q, want %q", got, want)
	}
}

func TestClientCaptureFallsBackToExistingIdentityTapeFormat(t *testing.T) {
	state := newCaptureTestServerState()
	state.createTapeStatus = http.StatusConflict
	state.createTapeBody = `{"error":"payload_format conflict","existing_payload_format":"identity"}`
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_only_exit_0"}

	res, err := client.Capture(context.Background(), "space", "tape", procArgs, io.Discard, CaptureOptions{
		Compression:  CompressionZstd,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if got, want := res.ExitCode, 0; got != want {
		t.Fatalf("exit code = %d, want %d", got, want)
	}
	if got := string(state.concatPayload()); got != "ok\n" {
		t.Fatalf("identity fallback payload = %q, want %q", got, "ok\n")
	}
}

func TestClientCaptureFailsDeleteRecoveryAfterCommittedPrefix(t *testing.T) {
	var (
		createTapeCalls   int
		createStreamCalls int
		closeCalls        int
		chunkPaths        []string
		chunks            [][]byte
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")

		switch {
		case r.Method == http.MethodPost && len(parts) == 4 && parts[0] == "v1" && parts[1] == "spaces" && parts[3] == "tapes":
			createTapeCalls++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(struct {
				TapeID string `json:"tape_id"`
			}{TapeID: "tape-1"})
		case r.Method == http.MethodPost && len(parts) == 6 && parts[0] == "v1" && parts[1] == "spaces" && parts[3] == "tapes" && parts[5] == "streams":
			createStreamCalls++
			w.Header().Set("Content-Type", "application/json")
			streamID := "stream-1"
			if createStreamCalls > 1 {
				streamID = "stream-2"
			}
			_ = json.NewEncoder(w).Encode(struct {
				StreamID string `json:"stream_id"`
			}{StreamID: streamID})
		case r.Method == http.MethodPut && len(parts) == 9 && parts[0] == "v1" && parts[1] == "spaces" && parts[3] == "tapes" && parts[5] == "streams" && parts[7] == "chunks":
			payload, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			chunkPaths = append(chunkPaths, r.URL.Path)
			chunks = append(chunks, payload)
			switch r.URL.Path {
			case "/v1/spaces/space/tapes/tape-1/streams/stream-1/chunks/0":
				w.WriteHeader(http.StatusNoContent)
			case "/v1/spaces/space/tapes/tape-1/streams/stream-1/chunks/1":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"tape deleted"}`)
			case "/v1/spaces/space/tapes/tape-1/streams/stream-2/chunks/0":
				w.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(w, r)
			}
		case r.Method == http.MethodPost && len(parts) == 8 && parts[0] == "v1" && parts[1] == "spaces" && parts[3] == "tapes" && parts[5] == "streams" && parts[7] == "close":
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

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_head_then_tail"}

	res, err := client.Capture(context.Background(), "space", "tape", procArgs, io.Discard, CaptureOptions{
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if got, want := res.ExitCode, 0; got != want {
		t.Fatalf("exit code = %d, want %d", got, want)
	}
	if res.AppendErr == nil {
		t.Fatal("expected append err after delete removed an already committed prefix")
	}
	if !strings.Contains(res.AppendErr.Error(), "cannot recover delete after committed bytes") {
		t.Fatalf("append err = %v, want committed-prefix delete-recovery error", res.AppendErr)
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
		"/v1/spaces/space/tapes/tape-1/streams/stream-1/chunks/0",
		"/v1/spaces/space/tapes/tape-1/streams/stream-1/chunks/1",
	}
	if len(chunkPaths) != len(wantPaths) {
		t.Fatalf("chunk path count = %d, want %d", len(chunkPaths), len(wantPaths))
	}
	for i := range wantPaths {
		if got, want := chunkPaths[i], wantPaths[i]; got != want {
			t.Fatalf("chunk path[%d] = %q, want %q", i, got, want)
		}
	}
	if got := string(chunks[0]); got != "head\n" {
		t.Fatalf("first chunk = %q, want %q", got, "head\n")
	}
	if got := string(chunks[1]); got != "tail\n" {
		t.Fatalf("delete-conflict chunk = %q, want %q", got, "tail\n")
	}
}

func TestClientCaptureUsesProvidedIdempotencyKey(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_only_exit_0"}

	const idempotencyKey = "capture_key_0001"

	_, err = client.Capture(context.Background(), "space", "tape", procArgs, io.Discard, CaptureOptions{
		IdempotencyKey: idempotencyKey,
		PollInterval:   time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if got, want := state.lastCreateStreamIdempotencyKey, idempotencyKey; got != want {
		t.Fatalf("idempotency key = %q, want %q", got, want)
	}
}

func TestClientCaptureDoesNotCreateTapeBeforeLocalSpoolIsReady(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Capture(context.Background(), "space", "tape", []string{"sh", "-c", "echo should-not-run"}, io.Discard, CaptureOptions{
		SpoolDir: filepath.Join(t.TempDir(), "missing"),
	})
	if err == nil {
		t.Fatalf("expected capture error")
	}
	if state.createTapeCalls != 0 {
		t.Fatalf("expected no create tape call, got %d", state.createTapeCalls)
	}
}

func TestClientCaptureStopsRunningProcessWhenContextCanceled(t *testing.T) {
	state := newCaptureTestServerState()
	state.closeDelay = time.Second
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "ready_then_sleep_forever"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &readySignalWriter{ready: make(chan struct{})}

	type captureCall struct {
		res CaptureResult
		err error
	}
	done := make(chan captureCall, 1)
	go func() {
		res, err := client.Capture(ctx, "space", "tape", procArgs, writer, CaptureOptions{
			PollInterval: time.Millisecond,
		})
		done <- captureCall{res: res, err: err}
	}()

	select {
	case <-writer.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("capture did not start child process")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		state.mu.Lock()
		createStreamCalls := state.createStreamCalls
		state.mu.Unlock()
		if createStreamCalls == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("capture did not create remote stream")
		}
		time.Sleep(10 * time.Millisecond)
	}

	start := time.Now()
	cancel()

	select {
	case got := <-done:
		if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
			t.Fatalf("capture returned too slowly after context cancellation: %s", elapsed)
		}
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("expected context canceled error, got %v", got.err)
		}
		if got.res.ExitCode != 1 {
			t.Fatalf("unexpected exit code: %d", got.res.ExitCode)
		}
		if !errors.Is(got.res.AppendErr, context.Canceled) {
			t.Fatalf("expected append err to reflect unfinished upload cancellation, got %v", got.res.AppendErr)
		}
		if !strings.Contains(writer.String(), "ready\n") {
			t.Fatalf("missing forwarded output: %q", writer.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("capture did not return after context cancellation")
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		state.mu.Lock()
		closeCalls := state.closeCalls
		state.mu.Unlock()

		if closeCalls == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected one best-effort close after cancellation, got %d", closeCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestClientCapturePreservesAppendStateWhenContextCanceled(t *testing.T) {
	state := newCaptureTestServerState()
	state.putChunkStatus = http.StatusInternalServerError
	state.putChunkBody = "chunk rejected"
	state.closeDelay = time.Second
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "ready_then_sleep_forever"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &readySignalWriter{ready: make(chan struct{})}

	type captureCall struct {
		res CaptureResult
		err error
	}
	done := make(chan captureCall, 1)
	go func() {
		res, err := client.Capture(ctx, "space", "tape", procArgs, writer, CaptureOptions{
			PollInterval: time.Millisecond,
		})
		done <- captureCall{res: res, err: err}
	}()

	select {
	case <-writer.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("capture did not start child process")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		state.mu.Lock()
		closeCalls := state.closeCalls
		state.mu.Unlock()

		if closeCalls == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected one best-effort close before cancellation, got %d", closeCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	start := time.Now()
	cancel()

	select {
	case got := <-done:
		if elapsed := time.Since(start); elapsed >= 300*time.Millisecond {
			t.Fatalf("capture returned too slowly after context cancellation: %s", elapsed)
		}
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("expected context canceled error, got %v", got.err)
		}
		if got.res.TapeID != state.tapeID {
			t.Fatalf("unexpected tape id after cancellation: %q", got.res.TapeID)
		}
		if got.res.AppendErr == nil {
			t.Fatal("expected append err after cancellation")
		}
		if !strings.Contains(got.res.AppendErr.Error(), "chunk rejected") {
			t.Fatalf("unexpected append err after cancellation: %v", got.res.AppendErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("capture did not return after context cancellation")
	}
}

func TestClientCapturePreservesAppendFailureWhenContextExpiresAfterChildExit(t *testing.T) {
	state := newCaptureTestServerState()
	state.putChunkDelay = 6 * time.Second
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_only_exit_0"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	res, err := client.Capture(ctx, "space", "tape", procArgs, io.Discard, CaptureOptions{
		PollInterval: time.Millisecond,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("capture returned too quickly after child exit: %s", elapsed)
	}
	if res.ExitCode != 0 {
		t.Fatalf("unexpected exit code: %d", res.ExitCode)
	}
	if res.AppendErr == nil {
		t.Fatal("expected append err after context expiry during upload")
	}
	if ctx.Err() == nil {
		t.Fatal("expected caller context to expire")
	}
}

func TestClientCaptureAppendFailureDoesNotChangeChildResult(t *testing.T) {
	state := newCaptureTestServerState()
	state.putChunkStatus = http.StatusInternalServerError
	state.putChunkBody = "chunk rejected"

	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_only_exit_0"}

	var forward bytes.Buffer
	res, err := client.Capture(context.Background(), "space", "tape", procArgs, &forward, CaptureOptions{
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture returned child error: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("unexpected exit code: %d", res.ExitCode)
	}
	if res.AppendErr == nil {
		t.Fatal("expected append err")
	}
	if !strings.Contains(res.AppendErr.Error(), "chunk rejected") {
		t.Fatalf("unexpected append err: %v", res.AppendErr)
	}
	if forward.String() != "ok\n" {
		t.Fatalf("unexpected forwarded output: %q", forward.String())
	}
	if state.closeCalls != 1 {
		t.Fatalf("expected one best-effort close after append failure, got %d", state.closeCalls)
	}
}

func TestClientCaptureStartFailureDoesNotCreateTape(t *testing.T) {
	state := newCaptureTestServerState()
	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	res, err := client.Capture(context.Background(), "space", "tape", []string{"definitely-not-a-real-command-for-tape9"}, io.Discard, CaptureOptions{
		PollInterval: time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected capture start failure")
	}
	if res.TapeID != "" {
		t.Fatalf("unexpected tape id: %q", res.TapeID)
	}
	if res.ExitCode != 1 {
		t.Fatalf("unexpected exit code: %d", res.ExitCode)
	}
	if res.AppendErr != nil {
		t.Fatalf("unexpected append err: %v", res.AppendErr)
	}
	if state.createTapeCalls != 0 {
		t.Fatalf("expected no tape creation, got %d", state.createTapeCalls)
	}
	if state.createStreamCalls != 0 {
		t.Fatalf("expected no stream creation, got %d", state.createStreamCalls)
	}
	if payload := state.concatPayload(); len(payload) != 0 {
		t.Fatalf("expected no uploaded payload, got %q", string(payload))
	}
}

func TestClientCaptureCreateTapeFailureDoesNotChangeChildResult(t *testing.T) {
	state := newCaptureTestServerState()
	state.createTapeStatus = http.StatusInternalServerError
	state.createTapeBody = "create rejected"

	server := httptest.NewServer(captureTestHandler(t, state))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	procArgs := []string{os.Args[0], "-test.run=TestCaptureHelper", "--", "stdout_only_exit_0"}

	var forward bytes.Buffer
	res, err := client.Capture(context.Background(), "space", "tape", procArgs, &forward, CaptureOptions{
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("capture returned child error: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("unexpected exit code: %d", res.ExitCode)
	}
	if res.AppendErr == nil {
		t.Fatal("expected append err")
	}
	if !strings.Contains(res.AppendErr.Error(), "create rejected") {
		t.Fatalf("unexpected append err: %v", res.AppendErr)
	}
	if forward.String() != "ok\n" {
		t.Fatalf("unexpected forwarded output: %q", forward.String())
	}
	if state.createStreamCalls != 0 {
		t.Fatalf("expected no stream creation, got %d", state.createStreamCalls)
	}
}

func TestProcOutputWriteRejectsForwardShortWrite(t *testing.T) {
	spoolFile, err := os.CreateTemp(t.TempDir(), "capture-spool-*")
	if err != nil {
		t.Fatalf("create spool file: %v", err)
	}
	defer spoolFile.Close()

	output := &procOutput{
		forward: shortWriter{n: 2},
		spool:   spoolFile,
	}

	n, err := output.Write([]byte("hello"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected io.ErrShortWrite, got %v", err)
	}
	if n != 2 {
		t.Fatalf("unexpected written bytes: %d", n)
	}
	if output.spoolErr != nil {
		t.Fatalf("unexpected spool err: %v", output.spoolErr)
	}

	if _, err := spoolFile.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek spool file: %v", err)
	}
	data, err := io.ReadAll(spoolFile)
	if err != nil {
		t.Fatalf("read spool file: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected no spool bytes after forward short write, got %q", string(data))
	}
}

func TestCaptureHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	args := os.Args
	dash := -1
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			dash = i
			break
		}
	}
	if dash < 0 || dash+1 >= len(args) {
		fmt.Fprintln(os.Stderr, "missing -- args")
		os.Exit(2)
	}

	switch args[dash+1] {
	case "both_and_exit_7":
		_, _ = fmt.Fprint(os.Stdout, "hello stdout\n")
		_, _ = fmt.Fprint(os.Stderr, "hello stderr\n")
		os.Exit(7)
	case "stdout_only_exit_0":
		_, _ = fmt.Fprint(os.Stdout, "ok\n")
		os.Exit(0)
	case "ready_then_exit":
		_, _ = fmt.Fprint(os.Stdout, "ready\n")
		os.Exit(0)
	case "stdout_head_then_tail":
		_, _ = fmt.Fprint(os.Stdout, "head\n")
		time.Sleep(200 * time.Millisecond)
		_, _ = fmt.Fprint(os.Stdout, "tail\n")
		os.Exit(0)
	case "stdout_head_then_slow_tail":
		_, _ = fmt.Fprint(os.Stdout, "head\n")
		time.Sleep(time.Second)
		_, _ = fmt.Fprint(os.Stdout, "tail\n")
		os.Exit(0)
	case "ready_then_sleep_forever":
		_, _ = fmt.Fprint(os.Stdout, "ready\n")
		for {
			time.Sleep(time.Second)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown helper mode")
		os.Exit(2)
	}
}

type readySignalWriter struct {
	buf   bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

func (w *readySignalWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("ready\n")) {
		w.once.Do(func() {
			close(w.ready)
		})
	}
	return w.buf.Write(p)
}

func (w *readySignalWriter) String() string {
	return w.buf.String()
}

type shortWriter struct {
	n int
}

func (w shortWriter) Write(p []byte) (int, error) {
	if w.n > len(p) {
		return len(p), nil
	}
	return w.n, nil
}

type captureTestServerState struct {
	tapeID                         string
	streamID                       string
	createTapeCalls                int
	createTapeStatus               int
	createTapeBody                 string
	createStreamCalls              int
	lastCreateStreamIdempotencyKey string
	closeCalls                     int
	lastCreateTapeID               string
	lastCreateTapeRetainMode       string
	lastCreateTapePayloadFormat    string
	putChunkStatus                 int
	putChunkBody                   string
	putChunkDelay                  time.Duration
	closeDelay                     time.Duration

	mu     sync.Mutex
	chunks map[int64][]byte
}

func newCaptureTestServerState() *captureTestServerState {
	return &captureTestServerState{
		tapeID:   "tape-1",
		streamID: "stream-1",
		chunks:   map[int64][]byte{},
	}
}

func (s *captureTestServerState) concatPayload() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.chunks) == 0 {
		return nil
	}

	maxSeq := int64(-1)
	for seq := range s.chunks {
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	var out []byte
	for seq := int64(0); seq <= maxSeq; seq++ {
		out = append(out, s.chunks[seq]...)
	}
	return out
}

func captureTestHandler(t *testing.T, state *captureTestServerState) http.HandlerFunc {
	t.Helper()

	type createTapeResponse struct {
		TapeID string `json:"tape_id"`
	}
	type createStreamResponse struct {
		StreamID string `json:"stream_id"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		t.Helper()

		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")

		switch {
		case r.Method == http.MethodPost && len(parts) == 4 && parts[0] == "v1" && parts[1] == "spaces" && parts[3] == "tapes":
			var body struct {
				TapeID        string `json:"tape_id"`
				RetainMode    string `json:"retain_mode"`
				PayloadFormat string `json:"payload_format"`
			}
			if r.Body != nil {
				_ = json.NewDecoder(r.Body).Decode(&body)
			}
			state.createTapeCalls++
			state.lastCreateTapeID = body.TapeID
			state.lastCreateTapeRetainMode = body.RetainMode
			state.lastCreateTapePayloadFormat = body.PayloadFormat
			if state.createTapeStatus != 0 {
				http.Error(w, state.createTapeBody, state.createTapeStatus)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(createTapeResponse{TapeID: state.tapeID})
			return

		case r.Method == http.MethodPost && len(parts) == 6 && parts[0] == "v1" && parts[1] == "spaces" && parts[3] == "tapes" && parts[5] == "streams":
			var body struct {
				IdempotencyKey string `json:"idempotency_key"`
			}
			if r.Body != nil {
				_ = json.NewDecoder(r.Body).Decode(&body)
			}
			state.mu.Lock()
			state.createStreamCalls++
			state.lastCreateStreamIdempotencyKey = body.IdempotencyKey
			state.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(createStreamResponse{StreamID: state.streamID})
			return

		case r.Method == http.MethodPut && len(parts) == 9 && parts[0] == "v1" && parts[1] == "spaces" && parts[3] == "tapes" && parts[5] == "streams" && parts[7] == "chunks":
			if state.putChunkDelay > 0 {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(state.putChunkDelay):
				}
			}
			if state.putChunkStatus != 0 {
				http.Error(w, state.putChunkBody, state.putChunkStatus)
				return
			}

			seq, err := strconv.ParseInt(parts[8], 10, 64)
			if err != nil {
				http.Error(w, "invalid seq", http.StatusBadRequest)
				return
			}

			payload, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}

			sum := sha256.Sum256(payload)
			want := hex.EncodeToString(sum[:])
			got := r.Header.Get("X-Tape9-SHA256")
			if got != want {
				http.Error(w, "bad sha256", http.StatusBadRequest)
				return
			}

			state.mu.Lock()
			state.chunks[seq] = append([]byte(nil), payload...)
			state.mu.Unlock()

			w.WriteHeader(http.StatusNoContent)
			return

		case r.Method == http.MethodPost && len(parts) == 8 && parts[0] == "v1" && parts[1] == "spaces" && parts[3] == "tapes" && parts[5] == "streams" && parts[7] == "close":
			state.mu.Lock()
			state.closeCalls++
			state.mu.Unlock()
			if state.closeDelay > 0 {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(state.closeDelay):
				}
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	}
}
