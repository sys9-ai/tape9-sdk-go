package tape9

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUsageMethodsUseAuthenticatedMetadataRoutes(t *testing.T) {
	startedAt := time.Date(2026, time.August, 11, 1, 2, 3, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get(spaceSecretHeader), "space-secret"; got != want {
			t.Fatalf("secret = %q, want %q", got, want)
		}
		switch r.URL.Path {
		case "/v1/spaces/acme/dev/tapes/tape/usage":
			if r.Method != http.MethodGet {
				t.Fatalf("method = %q, want GET", r.Method)
			}
			_, _ = w.Write([]byte(`{"tape_id":"tape","logical_bytes":120,"stored_bytes":40,"retain_applied":true,"dropped_bytes":12,"retention_dropped_logical_bytes":20,"retention_tracking_started_at":"2026-08-11T01:02:03Z"}`))
		case "/v1/spaces/acme/dev/usage":
			if r.Method != http.MethodPost {
				t.Fatalf("method = %q, want POST", r.Method)
			}
			var body struct {
				UsageScopes []string `json:"usage_scopes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if got, want := body.UsageScopes, []string{"project-a", "project-b"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
				t.Fatalf("usage scopes = %#v, want %#v", got, want)
			}
			_, _ = w.Write([]byte(`{"tape_count":2,"logical_bytes":300,"stored_bytes":100,"retention_dropped_logical_bytes":20,"retention_tracking_complete_since":"2026-08-11T01:02:03Z"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.URL, WithSecret("space-secret"))
	if err != nil {
		t.Fatal(err)
	}

	tapeUsage, err := client.TapeUsage(context.Background(), "acme/dev", "tape")
	if err != nil {
		t.Fatal(err)
	}
	if tapeUsage.LogicalBytes == nil || *tapeUsage.LogicalBytes != 120 || tapeUsage.StoredBytes != 40 {
		t.Fatalf("unexpected tape usage: %#v", tapeUsage)
	}
	if !tapeUsage.RetainApplied || tapeUsage.DroppedBytes != 12 {
		t.Fatalf("unexpected tape retention: %#v", tapeUsage)
	}
	if tapeUsage.RetentionTrackingStartedAt == nil || !tapeUsage.RetentionTrackingStartedAt.Equal(startedAt) {
		t.Fatalf("tracking start = %v, want %v", tapeUsage.RetentionTrackingStartedAt, startedAt)
	}

	summary, err := client.UsageSummary(context.Background(), "acme/dev", []string{"project-a", "project-b"})
	if err != nil {
		t.Fatal(err)
	}
	if summary.TapeCount != 2 || summary.LogicalBytes == nil || *summary.LogicalBytes != 300 || summary.StoredBytes != 100 {
		t.Fatalf("unexpected usage summary: %#v", summary)
	}
}

func TestCreateTapeSendsUsageScope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UsageScope string `json:"usage_scope"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if got, want := body.UsageScope, "project-a"; got != want {
			t.Fatalf("usage_scope = %q, want %q", got, want)
		}
		_, _ = w.Write([]byte(`{"tape_id":"tape"}`))
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.createTape(context.Background(), "space", "tape", createTapeOptions{UsageScope: "project-a"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUsageScopeBindsWhenExistingTapeKeepsItsPayloadFormat(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body struct {
			PayloadFormat string `json:"payload_format"`
			UsageScope    string `json:"usage_scope"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.UsageScope != "project-a" {
			t.Fatalf("usage_scope = %q, want project-a", body.UsageScope)
		}
		if calls == 1 {
			if body.PayloadFormat != string(payloadFormatFramedZstdV1) {
				t.Fatalf("first payload_format = %q", body.PayloadFormat)
			}
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"payload_format conflict","existing_payload_format":"identity"}`))
			return
		}
		if body.PayloadFormat != string(payloadFormatIdentity) {
			t.Fatalf("second payload_format = %q", body.PayloadFormat)
		}
		_, _ = w.Write([]byte(`{"tape_id":"tape"}`))
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	tapeID, format, err := client.createOrReuseAppendTape(context.Background(), "space", "tape", createTapeOptions{
		PayloadFormat: payloadFormatFramedZstdV1,
		UsageScope:    "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if tapeID != "tape" || format != payloadFormatIdentity || calls != 2 {
		t.Fatalf("tape=%q format=%q calls=%d", tapeID, format, calls)
	}
}
