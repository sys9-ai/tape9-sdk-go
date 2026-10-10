package tape9

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCloseTapeEscapesIDsAuthenticatesAndRetries(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.EscapedPath() != "/v1/spaces/acme%2Fdev/tapes/log%2Fone/close" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.EscapedPath())
		}
		if r.Header.Get("X-Space-Secret") != "test-secret" {
			t.Error("missing space secret")
		}
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := New(server.URL, WithSecret("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	client.retryBase = time.Millisecond
	if err := client.CloseTape(context.Background(), "acme/dev", "log/one"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestClosedContentPropagatesAndFollowStopsAfterFinalBytes(t *testing.T) {
	for _, mode := range []string{"read", "pull", "pull-resume-empty", "follow-initial", "follow-final", "follow-latest"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/part" {
					_, _ = io.WriteString(w, "final")
					return
				}
				calls++
				if mode == "follow-final" && calls == 1 {
					_, _ = io.WriteString(w, `{"resume_token":"first","payload_format":"identity","parts":[]}`)
					return
				}
				if calls > 2 {
					t.Error("continued polling after close")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				parts := `[{"byte_count":5,"offset":0,"url":"/part"}]`
				if mode == "pull-resume-empty" || mode == "follow-latest" {
					parts = `[]`
				}
				_, _ = fmt.Fprintf(w, `{"resume_token":"last","payload_format":"identity","closed":true,"total_bytes":5,"parts":%s}`, parts)
			}))
			defer server.Close()
			client, err := New(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var out bytes.Buffer
			switch mode {
			case "read":
				res, err := client.Read(ctx, "space", "tape", &out)
				if err != nil {
					t.Fatal(err)
				}
				if !res.Closed || res.TotalBytes != 5 {
					t.Fatalf("result = %+v", res)
				}
			case "pull", "pull-resume-empty":
				opts := PullOptions{}
				if mode == "pull-resume-empty" {
					opts.ResumeToken = ResumeToken{raw: "first"}
					opts.Wait = time.Second
				}
				res, err := client.Pull(ctx, "space", "tape", &out, opts)
				if err != nil {
					t.Fatal(err)
				}
				if !res.Closed || res.NoChange || res.TotalBytes != 5 {
					t.Fatalf("result = %+v", res)
				}
			default:
				opts := FollowOptions{}
				if mode == "follow-latest" {
					opts.From = FollowFromLatest
				}
				if mode == "follow-initial" {
					opts.OnInitial = func(res ReadResult) {
						if !res.Closed || res.TotalBytes != 5 {
							t.Errorf("initial = %+v", res)
						}
					}
				}
				if err := client.Follow(ctx, "space", "tape", &out, opts); err != nil {
					t.Fatal(err)
				}
			}
			want := "final"
			if mode == "pull-resume-empty" || mode == "follow-latest" {
				want = ""
			}
			if out.String() != want {
				t.Fatalf("output = %q, want %q", out.String(), want)
			}
			wantCalls := 1
			if mode == "follow-final" {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("content calls = %d, want %d", calls, wantCalls)
			}
		})
	}
}
