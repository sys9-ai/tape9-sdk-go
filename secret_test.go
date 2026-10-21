package tape9

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSecretChangesAuthenticateNameTheSecretAndRetry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		change func(*Client) error
	}{
		{"add", "/v1/spaces/acme%2Fdev/secrets", func(c *Client) error {
			return c.AddSecret(context.Background(), "acme/dev", "next-secret")
		}},
		{"revoke", "/v1/spaces/acme%2Fdev/secrets/revoke", func(c *Client) error {
			return c.RevokeSecret(context.Background(), "acme/dev", "next-secret")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodPost || r.URL.EscapedPath() != tc.path {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.EscapedPath())
				}
				if r.Header.Get("X-Space-Secret") != "current-secret" {
					t.Error("missing space secret")
				}
				var body struct {
					Secret string `json:"secret"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Secret != "next-secret" {
					t.Errorf("body secret = %q, %v", body.Secret, err)
				}
				if calls == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client, err := New(server.URL, WithSecret("current-secret"))
			if err != nil {
				t.Fatal(err)
			}
			client.retryBase = time.Millisecond
			if err := tc.change(client); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatalf("calls = %d, want 2", calls)
			}
		})
	}
}

func TestSecretChangeReturnsServerRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"space already has two secrets; revoke one first"}`))
	}))
	defer server.Close()
	client, err := New(server.URL, WithSecret("current-secret"))
	if err != nil {
		t.Fatal(err)
	}
	err = client.AddSecret(context.Background(), "space", "third-secret")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusConflict {
		t.Fatalf("err = %v, want 409", err)
	}
}
