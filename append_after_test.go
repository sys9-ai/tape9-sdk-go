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
