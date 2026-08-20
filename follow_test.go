package tape9

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWaitForContentChangeSplitsLongWaitIntoMultipleRequests(t *testing.T) {
	token := "resume-1"

	var waits []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Path, "/v1/spaces/space/tapes/tape/content"; got != want {
			t.Fatalf("path = %q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("resume_token"), token; got != want {
			t.Fatalf("resume_token = %q, want %q", got, want)
		}
		waits = append(waits, r.URL.Query().Get("wait"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, _, _, noChange, err := client.waitForContentChange(context.Background(), "space", "tape", token, 65*time.Second)
	if err != nil {
		t.Fatalf("waitForContentChange: %v", err)
	}
	if !noChange {
		t.Fatal("expected noChange after finite wait budget expires")
	}
	if got, want := waits, []string{"30s", "30s", "5s"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
}

func TestClientFollowStartWritesInitialAndLaterContent(t *testing.T) {
	initial := []byte("hello ")
	later := []byte("world")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	laterServed := make(chan struct{}, 1)
	go func() {
		<-laterServed
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	var onInitial ReadResult
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			switch r.URL.Query().Get("resume_token") {
			case "":
				requireEncodeContent(t, w, contentResponse{
					ResumeToken: "resume-1",
					Parts: []contentPart{
						{ByteCount: int64(len(initial)), URL: "content-part?part_token=initial"},
					},
				})
			case "resume-1":
				requireEncodeContent(t, w, contentResponse{
					ResumeToken: "resume-2",
					Parts: []contentPart{
						{ByteCount: int64(len(later)), URL: "content-part?part_token=later"},
					},
				})
			default:
				w.WriteHeader(http.StatusNoContent)
			}
		case "/v1/spaces/space/tapes/tape/content-part":
			switch r.URL.Query().Get("part_token") {
			case "initial":
				_, _ = w.Write(initial)
			case "later":
				_, _ = w.Write(later)
				laterServed <- struct{}{}
			default:
				t.Fatalf("unexpected part_token: %q", r.URL.Query().Get("part_token"))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	err = client.Follow(ctx, "space", "tape", &out, FollowOptions{
		OnInitial: func(res ReadResult) {
			onInitial = res
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("follow error = %v, want context canceled", err)
	}
	if got, want := out.String(), "hello world"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	if got, want := onInitial.Metrics.SegmentCount, 1; got != want {
		t.Fatalf("initial segment count = %d, want %d", got, want)
	}
}

func TestClientFollowLatestSkipsInitialContent(t *testing.T) {
	later := []byte("world")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	laterServed := make(chan struct{}, 1)
	go func() {
		<-laterServed
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			switch r.URL.Query().Get("resume_token") {
			case "":
				if got, want := r.URL.Query().Get("from"), "latest"; got != want {
					t.Fatalf("from = %q, want %q", got, want)
				}
				requireEncodeContent(t, w, contentResponse{ResumeToken: "resume-1"})
			case "resume-1":
				requireEncodeContent(t, w, contentResponse{
					ResumeToken: "resume-2",
					Parts: []contentPart{
						{ByteCount: int64(len(later)), URL: "content-part?part_token=later"},
					},
				})
			default:
				w.WriteHeader(http.StatusNoContent)
			}
		case "/v1/spaces/space/tapes/tape/content-part":
			if got, want := r.URL.Query().Get("part_token"), "later"; got != want {
				t.Fatalf("part_token = %q, want %q", got, want)
			}
			_, _ = w.Write(later)
			laterServed <- struct{}{}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	err = client.Follow(ctx, "space", "tape", &out, FollowOptions{From: FollowFromLatest})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("follow error = %v, want context canceled", err)
	}
	if got, want := out.String(), string(later); got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

func TestClientFollowAllowMissingAddsQueryFlag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			switch r.URL.Query().Get("resume_token") {
			case "":
				if got, want := r.URL.Query().Get("allow_missing"), "1"; got != want {
					t.Fatalf("allow_missing = %q, want %q", got, want)
				}
				requireEncodeContent(t, w, contentResponse{ResumeToken: "resume-1"})
			case "resume-1":
				cancel()
				w.WriteHeader(http.StatusNoContent)
			default:
				t.Fatalf("unexpected resume_token: %q", r.URL.Query().Get("resume_token"))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	err = client.Follow(ctx, "space", "tape", &out, FollowOptions{AllowMissing: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("follow error = %v, want context canceled", err)
	}
}

func TestClientFollowLatestRejectsUnexpectedInitialContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireEncodeContent(t, w, contentResponse{
			ResumeToken: "resume-1",
			Parts: []contentPart{
				{ByteCount: 1, URL: "content-part?part_token=unexpected"},
			},
		})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	err = client.Follow(context.Background(), "space", "tape", &out, FollowOptions{From: FollowFromLatest})
	if got, want := err.Error(), "latest follow unexpectedly returned initial content"; got != want {
		t.Fatalf("follow error = %q, want %q", got, want)
	}
}

func requireEncodeContent(t *testing.T, w http.ResponseWriter, body contentResponse) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Fatalf("encode content: %v", err)
	}
}
