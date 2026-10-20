package tape9

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
)

func TestAppendAfterGeneratedIDAcceptsLeadingDashEntropy(t *testing.T) {
	// Raw URL-safe base64 starts with '-' for these bytes. Batch IDs must
	// remain valid even though ordinary stream retry keys allow that prefix.
	originalReader := rand.Reader
	rand.Reader = bytes.NewReader(bytes.Repeat([]byte{0xf8}, 12))
	t.Cleanup(func() { rand.Reader = originalReader })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = io.WriteString(w, `{"tape_id":"tape-one"}`)
			return
		}
		appendID := path.Base(r.URL.Path)
		if !IsValidID(appendID) {
			http.Error(w, "invalid append_id", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"append_id": appendID})
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.AppendAfter(context.Background(), "space-one", "tape-one", nil, AppendAfterOptions{})
	if err != nil || !IsValidID(result.AppendID) {
		t.Fatalf("generated batch identity must satisfy the server contract: result=%+v err=%v", result, err)
	}
}

func TestAppendAfterConflictPreservesIdentityAndDoesNotFollowTail(t *testing.T) {
	puts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var create struct {
				Conditional *bool `json:"conditional"`
			}
			if err := json.NewDecoder(r.Body).Decode(&create); err != nil || create.Conditional != nil {
				t.Errorf("create must not select a write mode: %+v, %v", create, err)
			}
			_, _ = io.WriteString(w, `{"tape_id":"tape-one"}`)
			return
		}
		puts++
		if r.Header.Get("X-Tape9-After") != "batch-before" {
			t.Error("predecessor changed")
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"code":"append_conflict","error":"conflict","tail_id":"batch-other"}`)
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.AppendAfter(context.Background(), "space-one", "tape-one", []byte("payload"), AppendAfterOptions{After: "batch-before"})
	var conflict *AppendConflictError
	if !errors.As(err, &conflict) || conflict.TailID != "batch-other" {
		t.Fatalf("expected typed conflict with current tail, got %v", err)
	}
	if !IsValidID(result.AppendID) || puts != 1 {
		t.Fatalf("identity lost or conflict retried: result=%+v puts=%d", result, puts)
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusConflict {
		t.Fatalf("missing wrapped HTTP response: %v", err)
	}
}

func TestAppendAfterEncodesMultipleFramesIntoOneRequest(t *testing.T) {
	payload := bytes.Repeat([]byte("hello"), 900000)
	puts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = io.WriteString(w, `{"tape_id":"tape-one"}`)
			return
		}
		puts++
		if values := r.Header.Values("X-Tape9-After"); len(values) != 1 || values[0] != "" {
			t.Errorf("first batch requires explicit empty predecessor: %v", values)
		}
		decoder, err := newFramedZstdDecoder()
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer decoder.Close()
		var raw bytes.Buffer
		if err := decodeFramedZstdReader(r.Body, decoder, &raw); err != nil || !bytes.Equal(raw.Bytes(), payload) {
			t.Errorf("batch did not contain complete raw payload: size=%d err=%v", raw.Len(), err)
		}
		_, _ = io.WriteString(w, `{"append_id":"batch-one"}`)
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.AppendAfter(context.Background(), "space-one", "tape-one", payload, AppendAfterOptions{AppendID: "batch-one", Compression: CompressionZstd})
	if err != nil || result.AppendID != "batch-one" || puts != 1 {
		t.Fatalf("result=%+v puts=%d err=%v", result, puts, err)
	}
}

func TestAppendAfterRejectsOversizeBeforeCreatingTape(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AppendAfter(context.Background(), "space-one", "tape-one", make([]byte, MaxAppendAfterBytes+1), AppendAfterOptions{})
	if err == nil || requests != 0 {
		t.Fatalf("oversized append must fail without side effects: requests=%d err=%v", requests, err)
	}
}

func TestAppendStateReadsConditionalTail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/spaces/space-one/tapes/with-batch/append-state" {
			_, _ = io.WriteString(w, `{"tail_id":"batch-one"}`)
			return
		}
		_, _ = io.WriteString(w, `{"tail_id":""}`)
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	state, err := client.AppendState(context.Background(), "space-one", "with-batch")
	if err != nil || state.TailID != "batch-one" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	state, err = client.AppendState(context.Background(), "space-one", "ordinary")
	if err != nil || state.TailID != "" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
}

func TestAppendAfterCreatesTapeOnlyUntilABatchIsAccepted(t *testing.T) {
	var creates, puts int
	tapeExists := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			creates++
			tapeExists = true
			_, _ = io.WriteString(w, `{"tape_id":"tape-one"}`)
			return
		}
		puts++
		if !tapeExists {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"space not found"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"append_id": path.Base(r.URL.Path)})
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	appendBatch := func(after, id string) {
		t.Helper()
		if _, err := client.AppendAfter(ctx, "space-one", "tape-one", []byte(id), AppendAfterOptions{After: after, AppendID: id}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}

	appendBatch("", "batch-1")
	appendBatch("batch-1", "batch-2")
	appendBatch("batch-2", "batch-3")
	if creates != 1 || puts != 3 {
		t.Fatalf("an accepted Tape must not be created again: creates=%d puts=%d", creates, puts)
	}

	// A server that lost the Tape entirely gets it created again.
	tapeExists = false
	appendBatch("", "batch-4")
	if creates != 2 || puts != 5 {
		t.Fatalf("a missing pinned Tape must be recreated once: creates=%d puts=%d", creates, puts)
	}
}

func TestAppendAfterBindsLateUsageScopeAndRejectsConflicts(t *testing.T) {
	var creates, puts int
	var scope string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			creates++
			var request struct {
				UsageScope string `json:"usage_scope"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if scope != "" && request.UsageScope != "" && scope != request.UsageScope {
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"usage_scope conflict"}`)
				return
			}
			if scope == "" {
				scope = request.UsageScope
			}
			_, _ = io.WriteString(w, `{"tape_id":"tape-one"}`)
			return
		}
		puts++
		_ = json.NewEncoder(w).Encode(map[string]string{"append_id": path.Base(r.URL.Path)})
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	appendBatch := func(after, id, usageScope string) error {
		_, err := client.AppendAfter(context.Background(), "space-one", "tape-one", []byte(id),
			AppendAfterOptions{After: after, AppendID: id, UsageScope: usageScope})
		return err
	}
	if err := appendBatch("", "batch-1", ""); err != nil {
		t.Fatal(err)
	}
	if err := appendBatch("batch-1", "batch-2", "scope-one"); err != nil {
		t.Fatal(err)
	}
	if scope != "scope-one" || creates != 2 || puts != 2 {
		t.Fatalf("late scope must be bound before append: scope=%q creates=%d puts=%d", scope, creates, puts)
	}
	if err := appendBatch("batch-2", "batch-3", "scope-one"); err != nil {
		t.Fatal(err)
	}
	if err := appendBatch("batch-3", "batch-4", ""); err != nil {
		t.Fatal(err)
	}
	if err := appendBatch("batch-4", "batch-5", "scope-one"); err != nil {
		t.Fatal(err)
	}
	if creates != 2 || puts != 5 {
		t.Fatalf("confirmed scope must avoid repeated create/reuse: creates=%d puts=%d", creates, puts)
	}
	err = appendBatch("batch-5", "batch-6", "scope-other")
	if message, ok := httpAPIErrorMessage(err, http.StatusConflict); !ok || message != "usage_scope conflict" {
		t.Fatalf("expected scope conflict, got %v", err)
	}
	if creates != 3 || puts != 5 {
		t.Fatalf("scope conflict must prevent append: creates=%d puts=%d", creates, puts)
	}
}

func TestAppendAfterDoesNotInferExistingScopeFromUnscopedRequest(t *testing.T) {
	var creates, puts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			creates++
			var request struct {
				UsageScope string `json:"usage_scope"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			// This Tape was already bound by another client before our first append.
			if request.UsageScope != "" && request.UsageScope != "existing-scope" {
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":"usage_scope conflict"}`)
				return
			}
			_, _ = io.WriteString(w, `{"tape_id":"tape-one"}`)
			return
		}
		puts++
		_ = json.NewEncoder(w).Encode(map[string]string{"append_id": path.Base(r.URL.Path)})
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AppendAfter(context.Background(), "space-one", "tape-one", nil, AppendAfterOptions{AppendID: "batch-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AppendAfter(context.Background(), "space-one", "tape-one", nil,
		AppendAfterOptions{After: "batch-1", AppendID: "batch-2", UsageScope: "different-scope"})
	if message, ok := httpAPIErrorMessage(err, http.StatusConflict); !ok || message != "usage_scope conflict" {
		t.Fatalf("expected existing scope conflict, got %v", err)
	}
	if creates != 2 || puts != 1 {
		t.Fatalf("unknown scope must be checked before append: creates=%d puts=%d", creates, puts)
	}
}

func TestAppendAfterValidatesOptionsOnPinnedTape(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method == http.MethodPost {
			_, _ = io.WriteString(w, `{"tape_id":"tape-one"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"append_id": path.Base(r.URL.Path)})
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AppendAfter(context.Background(), "space-one", "tape-one", nil, AppendAfterOptions{AppendID: "batch-1"})
	if err != nil {
		t.Fatal(err)
	}
	for name, options := range map[string]AppendAfterOptions{
		"retain":      {RetainMode: RetainMode("invalid")},
		"scope":       {UsageScope: "invalid scope"},
		"compression": {Compression: Compression("invalid")},
	} {
		t.Run(name, func(t *testing.T) {
			options.After = "batch-1"
			options.AppendID = "batch-2"
			if _, err := client.AppendAfter(context.Background(), "space-one", "tape-one", nil, options); err == nil {
				t.Fatal("invalid option must fail on a pinned Tape")
			}
			if requests != 2 {
				t.Fatalf("invalid option must send no request: requests=%d", requests)
			}
		})
	}
}
