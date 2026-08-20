package tape9

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientReadDownloadsContentParts(t *testing.T) {
	payload := []byte("hello world")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			if err := json.NewEncoder(w).Encode(contentResponse{
				ResumeToken: "resume-1",
				Parts: []contentPart{
					{ByteCount: int64(len(payload)), URL: "content-part?part_token=part-1", Source: "server"},
				},
			}); err != nil {
				t.Fatalf("encode content: %v", err)
			}
		case "/v1/spaces/space/tapes/tape/content-part":
			if got, want := r.URL.Query().Get("part_token"), "part-1"; got != want {
				t.Fatalf("part_token = %q, want %q", got, want)
			}
			if _, err := w.Write(payload); err != nil {
				t.Fatalf("write content part: %v", err)
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
	res, err := client.Read(context.Background(), "space", "tape", &out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got, want := out.String(), string(payload); got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	if got, want := res.Metrics.SegmentCount, 1; got != want {
		t.Fatalf("segment count = %d, want %d", got, want)
	}
	if got, want := res.Metrics.SegmentBytes, int64(len(payload)); got != want {
		t.Fatalf("segment bytes = %d, want %d", got, want)
	}
}

func TestClientReadDecodesFramedZstdContentParts(t *testing.T) {
	part1 := encodeFramedZstdSegment(t, [][]byte{[]byte("hello ")})
	part2 := encodeFramedZstdSegment(t, [][]byte{[]byte("world")})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spaces/space/tapes/tape/content":
			if err := json.NewEncoder(w).Encode(contentResponse{
				ResumeToken:   "resume-1",
				PayloadFormat: string(payloadFormatFramedZstdV1),
				Parts: []contentPart{
					{ByteCount: int64(len(part1)), URL: "content-part?part_token=part-1"},
					{ByteCount: int64(len(part2)), URL: "content-part?part_token=part-2"},
				},
			}); err != nil {
				t.Fatalf("encode content: %v", err)
			}
		case "/v1/spaces/space/tapes/tape/content-part":
			switch r.URL.Query().Get("part_token") {
			case "part-1":
				_, _ = w.Write(part1)
			case "part-2":
				_, _ = w.Write(part2)
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
	if _, err := client.Read(context.Background(), "space", "tape", &out); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got, want := out.String(), "hello world"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

func TestClientReadRejectsContentWithoutResumeToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(contentResponse{}); err != nil {
			t.Fatalf("encode content: %v", err)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	_, err = client.Read(context.Background(), "space", "tape", &out)
	if got, want := err.Error(), "content missing resume_token"; got != want {
		t.Fatalf("read error = %q, want %q", got, want)
	}
}

func TestClientReadRejectsContentPartWithoutURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(contentResponse{
			ResumeToken: "resume-1",
			Parts: []contentPart{
				{ByteCount: 1},
			},
		}); err != nil {
			t.Fatalf("encode content: %v", err)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var out bytes.Buffer
	_, err = client.Read(context.Background(), "space", "tape", &out)
	if got, want := err.Error(), "content part missing url"; got != want {
		t.Fatalf("read error = %q, want %q", got, want)
	}
}
