package tape9

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResumeTokenJSONRoundTrip(t *testing.T) {
	token, err := ParseResumeToken("resume-1")
	if err != nil {
		t.Fatalf("parse resume token: %v", err)
	}

	body, err := json.Marshal(struct {
		Token ResumeToken `json:"token"`
	}{Token: token})
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	if got, want := string(body), `{"token":"resume-1"}`; got != want {
		t.Fatalf("json = %q, want %q", got, want)
	}

	var decoded struct {
		Token ResumeToken `json:"token"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal json: %v", err)
	}
	if got, want := decoded.Token.String(), "resume-1"; got != want {
		t.Fatalf("token = %q, want %q", got, want)
	}
}

func TestClientPullStartDownloadsCurrentContent(t *testing.T) {
	payload := []byte("hello world")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			requireEncodeContent(t, w, contentResponse{
				ResumeToken: "resume-1",
				Parts: []contentPart{
					{ByteCount: int64(len(payload)), URL: "content-part?part_token=part-1"},
				},
			})
		case "/v1/spaces/space/tapes/tape/content-part":
			if got, want := r.URL.Query().Get("part_token"), "part-1"; got != want {
				t.Fatalf("part_token = %q, want %q", got, want)
			}
			_, _ = w.Write(payload)
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
	res, err := client.Pull(context.Background(), "space", "tape", &out, PullOptions{})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got, want := out.String(), string(payload); got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	if got, want := res.ResumeToken.String(), "resume-1"; got != want {
		t.Fatalf("resume token = %q, want %q", got, want)
	}
	if res.NoChange {
		t.Fatal("expected changed result")
	}
}

func TestClientPullLogicalPrefixThenCompleteRemainder(t *testing.T) {
	prefixPayload := []byte("prefix")
	tailPayload := []byte("tail")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			switch r.URL.Query().Get("resume_token") {
			case "":
				if got, want := r.URL.Query().Get("prefix_logical_bytes"), "5"; got != want {
					t.Fatalf("prefix_logical_bytes = %q, want %q", got, want)
				}
				requireEncodeContent(t, w, contentResponse{
					ResumeToken: "resume-prefix",
					Parts: []contentPart{
						{ByteCount: int64(len(prefixPayload)), URL: "content-part?part_token=prefix"},
					},
				})
			case "resume-prefix":
				if got := r.URL.Query().Get("prefix_logical_bytes"); got != "" {
					t.Fatalf("resumed prefix_logical_bytes = %q, want empty", got)
				}
				requireEncodeContent(t, w, contentResponse{
					ResumeToken: "resume-end",
					Parts: []contentPart{
						{ByteCount: int64(len(tailPayload)), URL: "content-part?part_token=tail"},
					},
				})
			default:
				t.Fatalf("unexpected resume_token: %q", r.URL.Query().Get("resume_token"))
			}
		case "/v1/spaces/space/tapes/tape/content-part":
			switch r.URL.Query().Get("part_token") {
			case "prefix":
				_, _ = w.Write(prefixPayload)
			case "tail":
				_, _ = w.Write(tailPayload)
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

	var prefix bytes.Buffer
	prefixResult, err := client.Pull(context.Background(), "space", "tape", &prefix, PullOptions{PrefixLogicalBytes: 5})
	if err != nil {
		t.Fatalf("pull prefix: %v", err)
	}
	if got, want := prefix.String(), string(prefixPayload); got != want {
		t.Fatalf("prefix payload = %q, want %q", got, want)
	}
	if got, want := prefixResult.ResumeToken.String(), "resume-prefix"; got != want {
		t.Fatalf("prefix resume token = %q, want %q", got, want)
	}

	var tail bytes.Buffer
	tailResult, err := client.Pull(context.Background(), "space", "tape", &tail, PullOptions{ResumeToken: prefixResult.ResumeToken})
	if err != nil {
		t.Fatalf("pull tail: %v", err)
	}
	if got, want := tail.String(), string(tailPayload); got != want {
		t.Fatalf("tail payload = %q, want %q", got, want)
	}
	if got, want := tailResult.ResumeToken.String(), "resume-end"; got != want {
		t.Fatalf("tail resume token = %q, want %q", got, want)
	}
}

func TestClientPullLatestReturnsCursorWithoutBytes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("from"), "latest"; got != want {
			t.Fatalf("from = %q, want %q", got, want)
		}
		requireEncodeContent(t, w, contentResponse{ResumeToken: "resume-1"})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	res, err := client.Pull(context.Background(), "space", "tape", &out, PullOptions{From: PullFromLatest})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got := out.String(); got != "" {
		t.Fatalf("payload = %q, want empty", got)
	}
	if got, want := res.ResumeToken.String(), "resume-1"; got != want {
		t.Fatalf("resume token = %q, want %q", got, want)
	}
	if res.NoChange {
		t.Fatal("expected initial latest pull to establish a cursor, not no-change")
	}
}

func TestClientPullStartAllowMissingAddsQueryFlag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("allow_missing"), "1"; got != want {
			t.Fatalf("allow_missing = %q, want %q", got, want)
		}
		requireEncodeContent(t, w, contentResponse{ResumeToken: "resume-1"})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	res, err := client.Pull(context.Background(), "space", "tape", &out, PullOptions{AllowMissing: true})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got, want := res.ResumeToken.String(), "resume-1"; got != want {
		t.Fatalf("resume token = %q, want %q", got, want)
	}
}

func TestClientPullLatestDoesNotReportRetainForSkippedInitialSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("from"), "latest"; got != want {
			t.Fatalf("from = %q, want %q", got, want)
		}
		requireEncodeContent(t, w, contentResponse{
			ResumeToken:   "resume-1",
			RetainApplied: true,
			DroppedBytes:  7,
		})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	res, err := client.Pull(context.Background(), "space", "tape", &out, PullOptions{From: PullFromLatest})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if res.RetainApplied {
		t.Fatal("expected latest pull to suppress retain metadata for skipped initial snapshot")
	}
	if got := res.DroppedBytes; got != 0 {
		t.Fatalf("dropped bytes = %d, want 0", got)
	}
}

func TestClientPullResumeImmediateNoChange(t *testing.T) {
	token, err := ParseResumeToken("resume-1")
	if err != nil {
		t.Fatalf("parse resume token: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("resume_token"), "resume-1"; got != want {
			t.Fatalf("resume_token = %q, want %q", got, want)
		}
		if got := r.URL.Query().Get("wait"); got != "0s" {
			t.Fatalf("wait = %q, want %q", got, "0s")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	res, err := client.Pull(context.Background(), "space", "tape", &out, PullOptions{ResumeToken: token})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if !res.NoChange {
		t.Fatal("expected no-change result")
	}
	if got, want := res.ResumeToken.String(), "resume-1"; got != want {
		t.Fatalf("resume token = %q, want %q", got, want)
	}
	if got := out.String(); got != "" {
		t.Fatalf("payload = %q, want empty", got)
	}
}

func TestClientPullResumeWaitsForLaterContent(t *testing.T) {
	token, err := ParseResumeToken("resume-1")
	if err != nil {
		t.Fatalf("parse resume token: %v", err)
	}
	payload := []byte("world")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			if got, want := r.URL.Query().Get("resume_token"), "resume-1"; got != want {
				t.Fatalf("resume_token = %q, want %q", got, want)
			}
			if got, want := r.URL.Query().Get("wait"), "5s"; got != want {
				t.Fatalf("wait = %q, want %q", got, want)
			}
			requireEncodeContent(t, w, contentResponse{
				ResumeToken: "resume-2",
				Parts: []contentPart{
					{ByteCount: int64(len(payload)), URL: "content-part?part_token=part-2"},
				},
			})
		case "/v1/spaces/space/tapes/tape/content-part":
			if got, want := r.URL.Query().Get("part_token"), "part-2"; got != want {
				t.Fatalf("part_token = %q, want %q", got, want)
			}
			_, _ = w.Write(payload)
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
	res, err := client.Pull(context.Background(), "space", "tape", &out, PullOptions{
		ResumeToken: token,
		Wait:        5 * time.Second,
	})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if res.NoChange {
		t.Fatal("expected changed result")
	}
	if got, want := out.String(), string(payload); got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	if got, want := res.ResumeToken.String(), "resume-2"; got != want {
		t.Fatalf("resume token = %q, want %q", got, want)
	}
}

func TestClientPullResumeWaitKeepsRemainingBudgetAfterDeletedSlice(t *testing.T) {
	token, err := ParseResumeToken("resume-1")
	if err != nil {
		t.Fatalf("parse resume token: %v", err)
	}
	payload := []byte("world")

	var waits []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			if got, want := r.URL.Query().Get("resume_token"), "resume-1"; got != want {
				t.Fatalf("resume_token = %q, want %q", got, want)
			}
			waits = append(waits, r.URL.Query().Get("wait"))
			switch len(waits) {
			case 1:
				if got, want := waits[0], "30s"; got != want {
					t.Fatalf("wait[0] = %q, want %q", got, want)
				}
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"tape not found"}`))
			case 2:
				if got, want := waits[1], "5s"; got != want {
					t.Fatalf("wait[1] = %q, want %q", got, want)
				}
				requireEncodeContent(t, w, contentResponse{
					ResumeToken: "resume-2",
					Parts: []contentPart{
						{ByteCount: int64(len(payload)), URL: "content-part?part_token=part-2"},
					},
				})
			default:
				t.Fatalf("unexpected content call %d", len(waits))
			}
		case "/v1/spaces/space/tapes/tape/content-part":
			if got, want := r.URL.Query().Get("part_token"), "part-2"; got != want {
				t.Fatalf("part_token = %q, want %q", got, want)
			}
			_, _ = w.Write(payload)
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
	res, err := client.Pull(context.Background(), "space", "tape", &out, PullOptions{
		ResumeToken: token,
		Wait:        35 * time.Second,
	})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if got, want := waits, []string{"30s", "5s"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
	if got, want := out.String(), string(payload); got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	if got, want := res.ResumeToken.String(), "resume-2"; got != want {
		t.Fatalf("resume token = %q, want %q", got, want)
	}
	if res.NoChange {
		t.Fatal("expected changed result")
	}
}

func TestClientPullRejectsResumeTokenWithFrom(t *testing.T) {
	token, err := ParseResumeToken("resume-1")
	if err != nil {
		t.Fatalf("parse resume token: %v", err)
	}

	client, err := New("https://example.com")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	_, err = client.Pull(context.Background(), "space", "tape", &out, PullOptions{
		From:        PullFromStart,
		ResumeToken: token,
	})
	if got, want := err.Error(), "resume token conflicts with from"; got != want {
		t.Fatalf("pull error = %q, want %q", got, want)
	}
}

func TestClientPullRejectsInvalidLogicalPrefixOptions(t *testing.T) {
	client, err := New("http://example.com")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.Pull(context.Background(), "space", "tape", io.Discard, PullOptions{PrefixLogicalBytes: -1})
	if got, want := err.Error(), "prefix logical bytes must be >= 0"; got != want {
		t.Fatalf("negative prefix error = %q, want %q", got, want)
	}

	_, err = client.Pull(context.Background(), "space", "tape", io.Discard, PullOptions{
		From:               PullFromLatest,
		PrefixLogicalBytes: 1,
	})
	if got, want := err.Error(), "prefix logical bytes conflict with latest pull"; got != want {
		t.Fatalf("latest prefix error = %q, want %q", got, want)
	}

	token, err := ParseResumeToken("resume-1")
	if err != nil {
		t.Fatalf("parse resume token: %v", err)
	}
	_, err = client.Pull(context.Background(), "space", "tape", io.Discard, PullOptions{
		ResumeToken:        token,
		PrefixLogicalBytes: 1,
	})
	if got, want := err.Error(), "resume token conflicts with prefix logical bytes"; got != want {
		t.Fatalf("resumed prefix error = %q, want %q", got, want)
	}
}

func TestClientPullRejectsResumeTokenWithAllowMissing(t *testing.T) {
	token, err := ParseResumeToken("resume-1")
	if err != nil {
		t.Fatalf("parse resume token: %v", err)
	}

	client, err := New("https://example.com")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	_, err = client.Pull(context.Background(), "space", "tape", &out, PullOptions{
		ResumeToken:  token,
		AllowMissing: true,
	})
	if got, want := err.Error(), "resume token conflicts with allow_missing"; got != want {
		t.Fatalf("pull error = %q, want %q", got, want)
	}
}

func TestClientPullRejectsInvalidFrom(t *testing.T) {
	client, err := New("https://example.com")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	_, err = client.Pull(context.Background(), "space", "tape", &out, PullOptions{From: PullFrom("bad")})
	if got, want := err.Error(), `invalid pull from: "bad"`; got != want {
		t.Fatalf("pull error = %q, want %q", got, want)
	}
}
